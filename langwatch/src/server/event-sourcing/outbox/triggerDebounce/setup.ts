import type { PrismaClient } from "@prisma/client";
import type { Cluster, Redis } from "ioredis";
import type { ProcessRole } from "~/server/app-layer/config";
import type { EvaluationRunService } from "~/server/app-layer/evaluations/evaluation-run.service";
import type { ProjectService } from "~/server/app-layer/projects/project.service";
import type { SpanStorageService } from "~/server/app-layer/traces/span-storage.service";
import type { TraceSummaryRepository } from "~/server/app-layer/traces/repositories/trace-summary.repository";
import type { TriggerService } from "~/server/app-layer/triggers/trigger.service";
import { getProtectionsForProject } from "~/server/api/utils";
import { createManyDatasetRecords } from "~/server/api/routers/datasetRecord.utils";
import { createOrUpdateQueueItems } from "~/server/api/routers/annotation";
import { TraceService } from "~/server/traces/trace.service";
import type {
  OutboxDispatchPayload,
  PgOutboxAuditAdapter,
} from "../pgAuditAdapter";
import type { TriggerNotifyInner } from "../triggerNotify/payload";
import { TraceReadDerivationService } from "~/server/app-layer/traces/trace-read-derivation.service";
import { TraceSummaryStore } from "../../pipelines/trace-processing/projections/traceSummary.store";
import { DEFAULT_TRACE_DEBOUNCE_MS } from "../../pipelines/shared/triggerActionDispatch";
import { RedisCachedFoldStore } from "../../projections/redisCachedFoldStore";
import { EventSourcedQueueProcessorMemory } from "../../queues/memory";
import { GroupQueueProcessor } from "../../queues/groupQueue/groupQueue";
import type { EventSourcedQueueProcessor } from "../../queues/queue.types";
import { createTriggerMatcher } from "./evaluator";
import type { TriggerMatchRequest } from "./payload";
import { defineTriggerDebounceQueue } from "./queue";

export interface TriggerEvaluationStack {
  queue: EventSourcedQueueProcessor<TriggerMatchRequest>;
}

/**
 * Composition root for the trigger-evaluation debounce stack (ADR-030).
 *
 * - Builds the GroupQueue whose Debounce Mode dedup holds (trigger,
 *   trace) pairs for the per-trigger `traceDebounceMs`.
 * - Builds the evaluator (the queue's `process` callback) that re-loads
 *   trace fold + evaluations after settlement, runs the filter check,
 *   claims `TriggerSent`, and dispatches.
 * - Consumer loop only runs on `processRole === "worker"`; the web
 *   process can still enqueue but never drains. Web should not call
 *   this — gate the call site on `processRole === "worker"` to skip
 *   the Redis-client cost entirely.
 *
 * The `triggerNotify` param is the queue + audit adapter pair from
 * `setupOutbox`. Passing it here threads through to
 * `dispatchTriggerAction`, which routes notify-class triggers through
 * the digest path (ADR-021 revision + ADR-025).
 */
export function setupTriggerDebounce({
  prisma,
  redis,
  processRole,
  triggers,
  projects,
  evaluations,
  traces,
  traceSummaryRepository,
  triggerNotify,
}: {
  prisma: PrismaClient;
  redis: Redis | Cluster | null;
  processRole: ProcessRole;
  triggers: TriggerService;
  projects: ProjectService;
  evaluations: { runs: EvaluationRunService };
  traces: { spans: SpanStorageService };
  traceSummaryRepository: TraceSummaryRepository;
  triggerNotify?: {
    queue: EventSourcedQueueProcessor<OutboxDispatchPayload<TriggerNotifyInner>>;
    auditAdapter: PgOutboxAuditAdapter;
  };
}): TriggerEvaluationStack {
  const traceReadDerivation = new TraceReadDerivationService(traces.spans);
  const traceSummaryStore = redis
    ? new RedisCachedFoldStore(
        new TraceSummaryStore(traceSummaryRepository),
        redis,
        { keyPrefix: "trace_summaries" },
      )
    : new TraceSummaryStore(traceSummaryRepository);

  const evaluator = createTriggerMatcher({
    triggers,
    projects,
    evaluationRuns: evaluations.runs,
    traceSummaryStore,
    deriveEvents: (params) => traceReadDerivation.deriveEvents(params),
    traceById: async (projectId, traceId) => {
      const traceService = TraceService.create(prisma);
      const protections = await getProtectionsForProject(prisma, {
        projectId,
      });
      return traceService.getById(projectId, traceId, protections);
    },
    addToAnnotationQueue: async (params) => {
      await createOrUpdateQueueItems({ ...params, prisma });
    },
    addToDataset: async (params) => {
      await createManyDatasetRecords(params);
    },
    triggerNotify,
  });

  const definition = defineTriggerDebounceQueue({
    process: evaluator,
    defaultDebounceMs: DEFAULT_TRACE_DEBOUNCE_MS,
  });

  const queue: EventSourcedQueueProcessor<TriggerMatchRequest> = redis
    ? new GroupQueueProcessor(definition, redis, {
        consumerEnabled: processRole === "worker",
      })
    : new EventSourcedQueueProcessorMemory(definition);

  return { queue };
}
