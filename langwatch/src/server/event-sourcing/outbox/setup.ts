import type { PrismaClient } from "@prisma/client";
import type { Cluster, Redis } from "ioredis";
import type { ProcessRole } from "~/server/app-layer/config";
import type { ProjectService } from "~/server/app-layer/projects/project.service";
import type { TriggerService } from "~/server/app-layer/triggers/trigger.service";
import { getProtectionsForProject } from "~/server/api/utils";
import { TraceService } from "~/server/traces/trace.service";
import { EventSourcedQueueProcessorMemory } from "../queues/memory";
import { GroupQueueProcessor } from "../queues/groupQueue/groupQueue";
import type { EventSourcedQueueProcessor } from "../queues/queue.types";
import { defineOutboxDispatchQueue } from "./outboxDispatchQueue";
import {
  PgOutboxAuditAdapter,
  type OutboxDispatchPayload,
} from "./pgAuditAdapter";
import { createTriggerNotifyDispatcher } from "./triggerNotify/dispatcher";
import type { TriggerNotifyInner } from "./triggerNotify/payload";

export interface OutboxStack {
  /**
   * The outbox dispatch queue's send-side handle. Reactors (or
   * `dispatchTriggerAction`) enqueue via the `enqueueTriggerNotify`
   * helper, which wraps this queue. Held here so presets.ts can pass
   * it through to the registry's deps.
   */
  triggerNotifyQueue: EventSourcedQueueProcessor<
    OutboxDispatchPayload<TriggerNotifyInner>
  >;
  /**
   * Same adapter the queue's process callback writes through —
   * exposed so `enqueueTriggerNotify` can call `onEnqueue` before
   * `queue.send` (audit-first, dispatch-second ordering).
   */
  auditAdapter: PgOutboxAuditAdapter;
}

/**
 * Composition root for the outbox dispatch stack (ADR-021 revision).
 *
 * Builds:
 *   - PgOutboxAuditAdapter: writes ReactorOutbox rows on every queue
 *     lifecycle event.
 *   - The outbox dispatch queue (GroupQueueProcessor) with the
 *     trigger-notify dispatcher wired as its processBatch callback.
 *     The audit shim inside `defineOutboxDispatchQueue` calls the
 *     adapter at every transition.
 *
 * The consumer loop only runs on `processRole === "worker"`; the web
 * process can still enqueue (the queue's send-side works regardless
 * of consumerEnabled). Web should still skip calling `setupOutbox`
 * entirely — gate on `processRole === "worker"` in the caller to
 * avoid the Redis-client cost.
 */
export function setupOutbox({
  prisma,
  redis,
  processRole,
  triggers,
  projects,
}: {
  prisma: PrismaClient;
  redis: Redis | Cluster | null;
  processRole: ProcessRole;
  triggers: TriggerService;
  projects: ProjectService;
}): OutboxStack {
  const auditAdapter = new PgOutboxAuditAdapter(prisma);

  const triggerNotifyDispatcher = createTriggerNotifyDispatcher({
    triggers,
    projects,
    traceById: async (projectId, traceId) => {
      const traceService = TraceService.create(prisma);
      const protections = await getProtectionsForProject(prisma, {
        projectId,
      });
      return traceService.getById(projectId, traceId, protections);
    },
  });

  const definition = defineOutboxDispatchQueue<TriggerNotifyInner>({
    dispatcher: triggerNotifyDispatcher,
    auditAdapter,
    // Per-trigger FIFO so the cadence window for a single trigger is
    // strictly ordered. groupKey includes the triggerId so different
    // triggers parallelise; same trigger serialises through one
    // dispatcher invocation per window.
    groupKey: (payload) =>
      `${payload.projectId}/${payload.reactorName}:${payload.inner.triggerId}`,
  });

  const triggerNotifyQueue: EventSourcedQueueProcessor<
    OutboxDispatchPayload<TriggerNotifyInner>
  > = redis
    ? new GroupQueueProcessor(definition, redis, {
        consumerEnabled: processRole === "worker",
      })
    : new EventSourcedQueueProcessorMemory(definition);

  return { triggerNotifyQueue, auditAdapter };
}
