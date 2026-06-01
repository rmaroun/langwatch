import type { PrismaClient } from "@prisma/client";
import { createLogger } from "~/utils/logger/server";
import type { QueueAuditAdapter } from "../queues/queue.types";

const logger = createLogger("langwatch:outbox:pg-audit-adapter");

/**
 * Carried by every job on the outbox dispatch queue. The queue treats
 * `inner` as opaque (it's the reactor-specific dispatch context); the
 * adapter reads `projectId` / `reactorName` / `dedupKey` to identify
 * the audit row, and the dispatcher reads `inner` to do the actual
 * side effect.
 *
 * Two-layer payload (vs. flat) so the adapter contract is stable
 * across reactors — every reactor's specific data nests under `inner`
 * and the audit adapter never has to type-narrow it.
 */
export interface OutboxDispatchPayload<TInner = unknown>
  extends Record<string, unknown> {
  projectId: string;
  reactorName: string;
  dedupKey: string;
  inner: TInner;
}

/**
 * Writes `ReactorOutbox` rows for every queue lifecycle event on the
 * outbox dispatch queue (ADR-021 revision).
 *
 * The queue is the source of truth for scheduling and execution; this
 * adapter projects each transition into PG so operator dashboards have
 * a stable, queryable view. Row identity is `(reactorName, dedupKey)`
 * — the existing `@@unique` constraint on `ReactorOutbox` — so every
 * lifecycle hook locates the row by the payload's identity fields
 * without needing a queue-internal job id.
 *
 * Adapter writes are non-fatal — if a PG write fails, the queue keeps
 * running and the next transition's write brings the projection back
 * into sync.
 *
 * Schema co-existence note: the Phase-0 `ReactorOutbox` columns
 * (`leasedUntil`, `nextAttemptAt`) are still present in the table at
 * the time of this PR. The adapter writes `nextAttemptAt` with the
 * "scheduledAt" semantic and never touches `leasedUntil` (the queue
 * owns the lease). The Phase-0 cleanup PR (out of scope here) drops
 * `leasedUntil` and renames `nextAttemptAt` → `scheduledAt`.
 */
export class PgOutboxAuditAdapter
  implements QueueAuditAdapter<OutboxDispatchPayload>
{
  constructor(private readonly prisma: PrismaClient) {}

  async onEnqueue(event: {
    payload: OutboxDispatchPayload;
    groupKey: string;
    dedupKey: string | undefined;
    scheduledAt: Date;
    maxAttempts?: number;
  }): Promise<void> {
    const { projectId, reactorName, dedupKey, inner } = event.payload;
    // `skipDuplicates` makes onEnqueue replay-safe: a dedup-collapsed
    // re-send for the same (reactorName, dedupKey) becomes a no-op
    // here too, which matches the queue's actual behavior.
    await this.write(() =>
      this.prisma.reactorOutbox.createMany({
        data: [
          {
            projectId,
            reactorName,
            dedupKey,
            groupKey: event.groupKey,
            payload: inner as object,
            status: "queued",
            attempts: 0,
            maxAttempts: event.maxAttempts ?? 8,
            nextAttemptAt: event.scheduledAt,
          },
        ],
        skipDuplicates: true,
      }),
    );
  }

  async onLeased(event: {
    payload: OutboxDispatchPayload;
  }): Promise<void> {
    const { reactorName, dedupKey } = event.payload;
    await this.write(() =>
      this.prisma.reactorOutbox.updateMany({
        where: { reactorName, dedupKey },
        data: { status: "dispatching", attempts: { increment: 1 } },
      }),
    );
  }

  async onDispatched(event: {
    payload: OutboxDispatchPayload;
    at: Date;
  }): Promise<void> {
    const { reactorName, dedupKey } = event.payload;
    await this.write(() =>
      this.prisma.reactorOutbox.updateMany({
        where: { reactorName, dedupKey },
        data: { status: "dispatched", dispatchedAt: event.at },
      }),
    );
  }

  async onFailed(event: {
    payload: OutboxDispatchPayload;
    error: string;
    willRetry: boolean;
    nextAttemptAt?: Date;
  }): Promise<void> {
    const { reactorName, dedupKey } = event.payload;
    await this.write(() =>
      this.prisma.reactorOutbox.updateMany({
        where: { reactorName, dedupKey },
        data: {
          status: event.willRetry ? "failed_retryable" : "dead",
          lastError: event.error,
          lastErrorAt: new Date(),
          ...(event.willRetry && event.nextAttemptAt
            ? { nextAttemptAt: event.nextAttemptAt }
            : { nextAttemptAt: null }),
        },
      }),
    );
  }

  async onDead(event: {
    payload: OutboxDispatchPayload;
    lastError: string;
  }): Promise<void> {
    const { reactorName, dedupKey } = event.payload;
    await this.write(() =>
      this.prisma.reactorOutbox.updateMany({
        where: { reactorName, dedupKey },
        data: {
          status: "dead",
          lastError: event.lastError,
          lastErrorAt: new Date(),
          nextAttemptAt: null,
        },
      }),
    );
  }

  /**
   * Adapter writes are best-effort relative to the queue's own state.
   * A PG error logs + metrics; the queue keeps running. The next
   * transition's write brings the projection back into sync — which is
   * fine because each call writes the latest projection, not an event
   * log. ADR-021 revision's "audit-lag" metric is what surfaces
   * sustained reconciliation drift.
   */
  private async write(op: () => Promise<unknown>): Promise<void> {
    try {
      await op();
    } catch (error) {
      logger.warn(
        { error: error instanceof Error ? error.message : String(error) },
        "PgOutboxAuditAdapter write failed; queue keeps running, audit lags",
      );
    }
  }
}
