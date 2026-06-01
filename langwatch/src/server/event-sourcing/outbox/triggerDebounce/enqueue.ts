import type { TriggerSummary } from "~/server/app-layer/triggers/repositories/trigger.repository";
import type { EventSourcedQueueProcessor } from "../../queues/queue.types";
import {
  triggerDebounceDedupId,
  triggerDebounceGroupKey,
  type TriggerMatchRequest,
} from "./payload";

/**
 * Enqueue a debounced trigger evaluation request (ADR-030).
 *
 * Each call schedules an evaluation for `(projectId, triggerId, traceId)`
 * `trigger.traceDebounceMs` from now. The GroupQueue's Debounce Mode
 * collapses repeat calls with the same dedup ID onto a single pending
 * job whose TTL resets — so a trace receiving N spans only ever runs the
 * evaluator once, and only after the span stream goes silent.
 *
 * Triggers with `traceDebounceMs === 0` are special-cased to
 * `ttlMs: 1` so the queue's Debounce Mode still applies (replace +
 * extend semantics) — but the evaluator fires effectively immediately.
 * The legacy "evaluate on every span" behavior is preserved for
 * operators who explicitly opt out of debounce.
 *
 * The send is fire-and-forget on the producer side; the evaluator at
 * the other end is responsible for at-most-once enforcement via
 * `TriggerSent`.
 */
export async function enqueueTriggerMatch({
  queue,
  projectId,
  trigger,
  traceId,
}: {
  queue: EventSourcedQueueProcessor<TriggerMatchRequest>;
  projectId: string;
  trigger: TriggerSummary;
  traceId: string;
}): Promise<void> {
  const ttlMs = trigger.traceDebounceMs > 0
    ? trigger.traceDebounceMs
    : 1;

  await queue.send(
    { projectId, triggerId: trigger.id, traceId },
    {
      deduplication: {
        makeId: (req) =>
          triggerDebounceDedupId({
            projectId: req.projectId,
            triggerId: req.triggerId,
            traceId: req.traceId,
          }),
        ttlMs,
        extend: true,
        replace: true,
      },
    },
  );
}

/**
 * Re-export so reactor code that just wants the group key for
 * observability doesn't need to import payload.ts as well.
 */
export { triggerDebounceGroupKey };
