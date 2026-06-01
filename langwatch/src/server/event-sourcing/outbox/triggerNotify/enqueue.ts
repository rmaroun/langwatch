import type { TraceSummaryData } from "~/server/app-layer/traces/types";
import type { TriggerSummary } from "~/server/app-layer/triggers/repositories/trigger.repository";
import {
  NOTIFY_TRIGGER_ACTIONS,
  computeScheduledFor,
} from "~/server/event-sourcing/pipelines/shared/triggerActionDispatch";
import type {
  EventSourcedQueueProcessor,
  QueueAuditAdapter,
} from "../../queues/queue.types";
import {
  TRIGGER_NOTIFY_REACTOR_NAME,
  triggerNotifyDedupKey,
  triggerNotifyGroupKey,
  type TriggerNotifyDispatchPayload,
  type TriggerNotifyInner,
} from "./payload";

/**
 * Enqueue one matched (trigger, trace) onto the outbox dispatch queue
 * (ADR-021 revision + ADR-025 digest cadence).
 *
 * `delay` = `computeScheduledFor(action, cadence, now) - now`, so the
 * queue holds the job until the cadence window closes. Every match
 * arriving inside the same window shares a group key and lands in the
 * same `processBatch` invocation — that's how digest grouping works
 * without a `leaseGroup`.
 *
 * The send invokes `auditAdapter.onEnqueue` before the queue send,
 * which writes the `ReactorOutbox` row in the `queued` status. The
 * adapter's `onEnqueue` is idempotent (`createMany skipDuplicates`),
 * matching the queue's dedup-collapse semantics — a replayed enqueue
 * for the same (reactorName, dedupKey) is a no-op on both sides.
 *
 * Caller contract: only call for notify-class triggers — persist
 * actions dispatch inline. The guard fails loudly here so a misuse
 * doesn't silently drop a dataset write into the notify queue.
 */
export async function enqueueTriggerNotify({
  queue,
  auditAdapter,
  projectId,
  trigger,
  traceId,
  foldState,
  now = new Date(),
}: {
  queue: EventSourcedQueueProcessor<TriggerNotifyDispatchPayload>;
  auditAdapter: QueueAuditAdapter<TriggerNotifyDispatchPayload>;
  projectId: string;
  trigger: TriggerSummary;
  traceId: string;
  foldState: TraceSummaryData;
  now?: Date;
}): Promise<void> {
  if (!NOTIFY_TRIGGER_ACTIONS.has(trigger.action)) {
    throw new Error(
      `enqueueTriggerNotify: ${trigger.action} is not a notify action — enqueue called for the wrong trigger class`,
    );
  }

  const inner: TriggerNotifyInner = {
    triggerId: trigger.id,
    match: {
      traceId,
      input: foldState.computedInput ?? "",
      output: foldState.computedOutput ?? "",
    },
  };

  const dedupKey = triggerNotifyDedupKey({
    projectId,
    triggerId: trigger.id,
    traceId,
  });
  const groupKey = triggerNotifyGroupKey({
    projectId,
    triggerId: trigger.id,
  });
  const scheduledAt = computeScheduledFor({
    action: trigger.action,
    cadence: trigger.notificationCadence,
    now,
  });

  const payload: TriggerNotifyDispatchPayload = {
    projectId,
    reactorName: TRIGGER_NOTIFY_REACTOR_NAME,
    dedupKey,
    inner,
  };

  // Audit row first, then queue send. If the audit write fails the
  // adapter logs but doesn't throw — the queue still gets the send,
  // which is the desired ordering (execution beats audit in failure
  // modes; ADR-021 revision's "best-effort audit" rule).
  await auditAdapter.onEnqueue({
    payload,
    groupKey,
    dedupKey,
    scheduledAt,
  });

  const delayMs = Math.max(0, scheduledAt.getTime() - now.getTime());
  await queue.send(payload, {
    delay: delayMs > 0 ? delayMs : undefined,
  });
}
