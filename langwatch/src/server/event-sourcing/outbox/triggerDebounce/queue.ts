import type { EventSourcedQueueDefinition } from "../../queues/queue.types";
import {
  TRIGGER_DEBOUNCE_QUEUE_NAME,
  triggerDebounceDedupId,
  triggerDebounceGroupKey,
  type TriggerMatchRequest,
} from "./payload";

/**
 * GroupQueue definition for trigger-evaluation debounce (ADR-030).
 *
 * Debounce Mode (`extend: true, replace: true`) means every enqueue
 * with the same `(projectId, triggerId, traceId)` dedup ID **resets**
 * the TTL — so a trace receiving a steady stream of spans keeps
 * pushing the evaluator out until the span stream stops. Once the
 * TTL elapses without a new enqueue, the dispatcher fires once with
 * the latest payload.
 *
 * `ttlMs` defaults to the safe non-zero default; per-trigger overrides
 * come in via the enqueue helper's `deduplication.ttlMs` option (the
 * runtime honors per-send overrides on top of the queue-wide default).
 */
export function defineTriggerDebounceQueue({
  process,
  defaultDebounceMs,
}: {
  process: (request: TriggerMatchRequest) => Promise<void>;
  defaultDebounceMs: number;
}): EventSourcedQueueDefinition<TriggerMatchRequest> {
  return {
    name: TRIGGER_DEBOUNCE_QUEUE_NAME,
    process,
    groupKey: (req) => triggerDebounceGroupKey({ projectId: req.projectId }),
    deduplication: {
      makeId: (req) =>
        triggerDebounceDedupId({
          projectId: req.projectId,
          triggerId: req.triggerId,
          traceId: req.traceId,
        }),
      ttlMs: defaultDebounceMs,
      extend: true,
      replace: true,
    },
    options: {
      // Evaluator is IO-bound (Redis cross-pipeline fold read + filter
      // matching + maybe a CH evaluations read). Same envelope as the
      // outbox drainer.
      concurrency: 10,
      globalConcurrency: 300,
    },
  };
}
