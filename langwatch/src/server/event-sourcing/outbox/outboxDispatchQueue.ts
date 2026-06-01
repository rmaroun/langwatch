import { tenantIdFromGroupId } from "../observability/tenantRateTracker";
import type {
  DeduplicationConfig,
  EventSourcedQueueDefinition,
  EventSourcedQueueProcessorOptions,
  QueueAuditAdapter,
} from "../queues/queue.types";
import { isDispatchError } from "./dispatchError";
import type { OutboxDispatchPayload } from "./pgAuditAdapter";

export const OUTBOX_DISPATCH_QUEUE_NAME = "langwatch:outbox:dispatch";

/**
 * The dispatcher a reactor registers with the outbox dispatch queue.
 * Receives every payload coalesced into the same batch (digest grouping
 * via the queue's `processBatch` config) and performs the actual side
 * effect — sender HTTP call, dataset write, etc. Throws `DispatchError`
 * to signal retry semantics per ADR-027.
 *
 * Failures are caught by the queue-shim around this callback; the
 * shim invokes the audit adapter (`onFailed`/`onDead`) and rethrows so
 * the GroupQueue's retry policy kicks in.
 */
export type OutboxDispatcherFn<TInner = unknown> = (
  payloads: OutboxDispatchPayload<TInner>[],
) => Promise<void>;

/**
 * Builds the `EventSourcedQueueDefinition` for the outbox dispatch
 * queue. Wraps the user-supplied dispatcher with the audit adapter so
 * every lifecycle transition (lease, dispatched, failed, dead) projects
 * into PG before the queue considers the transition complete.
 *
 * Per ADR-023 (revised): the queue carries the full dispatch payload
 * (not wakeup-only). Digest coalescing falls out of the
 * `coalesceMaxBatch` + `processBatch` configuration. Per-tenant
 * fairness uses `tenantIdFromGroupId` on the `${projectId}/...`
 * groupKey shape.
 */
export function defineOutboxDispatchQueue<TInner>(params: {
  dispatcher: OutboxDispatcherFn<TInner>;
  auditAdapter: QueueAuditAdapter<OutboxDispatchPayload<TInner>>;
  /**
   * GroupQueue routing key — defaults to the conventional
   * `${projectId}/${reactorName}` so `tenantIdFromGroupId` parses
   * cleanly. Override only when a reactor needs finer-grained
   * per-trigger FIFO (e.g. `${projectId}/${reactorName}:${triggerId}`
   * for per-trigger ordering).
   */
  groupKey?: (payload: OutboxDispatchPayload<TInner>) => string;
  /**
   * When set, the queue invokes `processBatch` with up to this many
   * same-`groupKey` payloads in one go — digest coalescing for cadence
   * windows.
   */
  coalesceMaxBatch?: (
    payload: OutboxDispatchPayload<TInner>,
  ) => number | undefined;
  deduplication?: DeduplicationConfig<OutboxDispatchPayload<TInner>>;
  options?: EventSourcedQueueProcessorOptions;
}): EventSourcedQueueDefinition<OutboxDispatchPayload<TInner>> {
  const {
    dispatcher,
    auditAdapter,
    groupKey,
    coalesceMaxBatch,
    deduplication,
    options,
  } = params;

  const defaultGroupKey = (payload: OutboxDispatchPayload<TInner>) =>
    `${payload.projectId}/${payload.reactorName}`;

  return {
    name: OUTBOX_DISPATCH_QUEUE_NAME,
    process: async (payload) => {
      await runWithAudit([payload], dispatcher, auditAdapter);
    },
    processBatch: async (payloads) => {
      if (payloads.length === 0) return;
      await runWithAudit(payloads, dispatcher, auditAdapter);
    },
    coalesceMaxBatch,
    deduplication,
    groupKey: groupKey ?? defaultGroupKey,
    options,
  };
}

/**
 * Audit-shim: invoke `onLeased` for every payload in the batch, run
 * the dispatcher, then `onDispatched` on success. On throw, classify
 * via `DispatchError` (retryable vs terminal — ADR-027), invoke
 * `onFailed` or `onDead`, and rethrow so the GroupQueue's retry
 * policy schedules the next attempt.
 *
 * The shim treats the batch as atomic — every payload sees the same
 * verdict, matching how `leaseGroup`-based group dispatch worked in
 * the pre-revision design.
 */
async function runWithAudit<TInner>(
  payloads: OutboxDispatchPayload<TInner>[],
  dispatcher: OutboxDispatcherFn<TInner>,
  auditAdapter: QueueAuditAdapter<OutboxDispatchPayload<TInner>>,
): Promise<void> {
  for (const payload of payloads) {
    await auditAdapter.onLeased({ payload });
  }

  try {
    await dispatcher(payloads);
    const at = new Date();
    for (const payload of payloads) {
      await auditAdapter.onDispatched({ payload, at });
    }
  } catch (error) {
    const retryable = !isDispatchError(error) || error.retryable;
    const message = error instanceof Error ? error.message : String(error);

    for (const payload of payloads) {
      if (retryable) {
        await auditAdapter.onFailed({
          payload,
          error: message,
          willRetry: true,
        });
      } else {
        await auditAdapter.onDead({ payload, lastError: message });
      }
    }
    throw error;
  }
}

/** Re-export for setup wiring sites that don't want to import payload separately. */
export { tenantIdFromGroupId };
