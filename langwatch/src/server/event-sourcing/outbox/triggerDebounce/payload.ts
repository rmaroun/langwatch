import { z } from "zod";

/**
 * Request payload for the trigger-evaluation debounce queue (ADR-030).
 * The reactor enqueues one of these per (active trigger × incoming
 * trace event); the GroupQueue's Debounce Mode dedup collapses repeats
 * for the same `(projectId, triggerId, traceId)` onto a single pending
 * job whose TTL resets on each new span. After `traceDebounceMs`
 * of silence, the queue's process function fires once with the latest
 * payload and runs the evaluator.
 *
 * Variable-size data (fold state, evaluations list) is NOT cached on
 * the payload — by the time the debounce fires, anything cached would
 * be stale. The evaluator re-reads what it needs at lease time.
 */
export const triggerEvaluationRequestSchema = z.object({
  projectId: z.string(),
  triggerId: z.string(),
  traceId: z.string(),
});

export type TriggerMatchRequest = z.infer<
  typeof triggerEvaluationRequestSchema
>;

export const TRIGGER_DEBOUNCE_QUEUE_NAME = "langwatch:trigger-evaluation";

/**
 * GroupQueue routing key — per-project FIFO so a busy tenant cannot
 * monopolise the worker. Matches the ADR-023 convention used by the
 * outbox wakeup queue.
 */
export function triggerDebounceGroupKey(params: {
  projectId: string;
}): string {
  return `${params.projectId}/trigger-evaluation`;
}

/**
 * Deduplication ID — scopes the debounce window to a single
 * (trigger, trace) pair. Two different triggers settling against the
 * same trace, or two different traces settling against the same
 * trigger, are independent windows.
 */
export function triggerDebounceDedupId(params: {
  projectId: string;
  triggerId: string;
  traceId: string;
}): string {
  return `${params.projectId}:${params.triggerId}:${params.traceId}`;
}
