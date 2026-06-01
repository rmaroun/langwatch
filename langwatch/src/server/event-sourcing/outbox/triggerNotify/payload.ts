import { z } from "zod";
import type { OutboxDispatchPayload } from "../pgAuditAdapter";

export const TRIGGER_NOTIFY_REACTOR_NAME = "triggerNotify" as const;

/**
 * Inner payload nested inside `OutboxDispatchPayload<TriggerNotifyInner>`.
 * Carries one matched (trigger, trace) pair. The outbox dispatch queue
 * (ADR-021 revision) coalesces every payload in the same cadence
 * window into a single dispatcher invocation via `processBatch` —
 * that's how digest grouping works without a `leaseGroup`-style
 * PG read.
 *
 * `input`/`output` are cached from the fold state at enqueue time so
 * the dispatcher doesn't need to refetch the fold to render the
 * digest. The full trace (only needed by Slack's events block) is
 * fetched on demand at dispatch time.
 */
export const triggerNotifyInnerSchema = z.object({
  triggerId: z.string(),
  match: z.object({
    traceId: z.string(),
    input: z.string(),
    output: z.string(),
  }),
});

export type TriggerNotifyInner = z.infer<typeof triggerNotifyInnerSchema>;

export type TriggerNotifyDispatchPayload =
  OutboxDispatchPayload<TriggerNotifyInner>;

/**
 * GroupQueue routing key — one group per (project, trigger). Every
 * match for the trigger lands in the same group; the windowed
 * `delay` (ADR-025) determines which subset the queue dispatches per
 * cadence window.
 *
 * Format mirrors the ADR-023 convention: `${projectId}/...` so
 * `tenantIdFromGroupId` extracts the tenant cleanly.
 */
export function triggerNotifyGroupKey(params: {
  projectId: string;
  triggerId: string;
}): string {
  return `${params.projectId}/${TRIGGER_NOTIFY_REACTOR_NAME}:${params.triggerId}`;
}

/**
 * Per-(trigger, trace) dedup identity. Collisions on
 * (reactorName, dedupKey) are the claim primitive that makes pipeline
 * replays safe — a re-fired event for the same trace never produces a
 * second digest entry.
 */
export function triggerNotifyDedupKey(params: {
  projectId: string;
  triggerId: string;
  traceId: string;
}): string {
  return `${params.projectId}/${params.triggerId}:trace:${params.traceId}`;
}
