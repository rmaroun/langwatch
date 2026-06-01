import type { EvaluationRunData } from "~/server/app-layer/evaluations/types";
import type { TriggerSummary } from "~/server/app-layer/triggers/repositories/trigger.repository";
import type { TriggerService } from "~/server/app-layer/triggers/trigger.service";
import { classifyTriggerFilters } from "~/server/filters/triggerFilter.matcher";
import { createLogger } from "~/utils/logger/server";
import { captureException } from "~/utils/posthogErrorCapture";
import { enqueueTriggerMatch } from "../../../outbox/triggerDebounce/enqueue";
import type { TriggerMatchRequest } from "../../../outbox/triggerDebounce/payload";
import type {
  ReactorContext,
  ReactorDefinition,
} from "../../../reactors/reactor.types";
import type { EventSourcedQueueProcessor } from "../../../queues/queue.types";
import type { EvaluationProcessingEvent } from "../schemas/events";
import {
  isEvaluationCompletedEvent,
  isEvaluationReportedEvent,
} from "../schemas/typeGuards";

const logger = createLogger(
  "langwatch:evaluation-processing:evaluation-alert-trigger-reactor",
);

export interface EvaluationAlertTriggerReactorDeps {
  triggers: TriggerService;
  /**
   * See AlertTriggerReactorDeps.triggerDebounceQueue. Optional for the
   * same web-registry / worker-runtime split — handler guards if missing.
   */
  triggerDebounceQueue?: EventSourcedQueueProcessor<TriggerMatchRequest>;
}

/**
 * Evaluation-pipeline reactor that schedules trigger matching when
 * evaluations complete.
 *
 * Fires on the evaluation-processing pipeline after an evaluation
 * completes. For each active trigger WITH evaluation filters (the
 * trace pipeline owns the trace-only ones), the reactor enqueues a
 * debounced match request. The GroupQueue's Debounce Mode dedup
 * collapses repeats on `(projectId, triggerId, traceId)` onto one
 * pending job — so a trace receiving multiple evaluations only runs
 * the matcher once, after the eval stream goes quiet for the
 * trigger's `traceDebounceMs`.
 *
 * No filter eval, no trace fold cross-read, no dispatch here — all
 * of that moved into the matcher so the verdict is made against a
 * settled state. See ADR-030.
 */
export function createEvaluationAlertTriggerReactor(
  deps: EvaluationAlertTriggerReactorDeps,
): ReactorDefinition<EvaluationProcessingEvent, EvaluationRunData> {
  return {
    name: "evaluationAlertTrigger",
    options: {
      makeJobId: (payload) =>
        `eval-alert-trigger:${payload.event.tenantId}:${payload.event.aggregateId}`,
      ttl: 30_000,
      delay: 10_000,
    },

    async handle(
      event: EvaluationProcessingEvent,
      context: ReactorContext<EvaluationRunData>,
    ): Promise<void> {
      // Only fire on terminal evaluation events.
      if (
        !isEvaluationCompletedEvent(event) &&
        !isEvaluationReportedEvent(event)
      ) {
        return;
      }

      const { tenantId, foldState: evalRun } = context;

      // Guard: skip non-terminal statuses (fold may still be in_progress).
      if (
        evalRun.status !== "processed" &&
        evalRun.status !== "error" &&
        evalRun.status !== "skipped"
      ) {
        return;
      }

      // Guard: must have a traceId for the (trigger, trace) dedup key.
      if (!evalRun.traceId) return;

      const traceId = evalRun.traceId;

      // Guard: skip old evaluations (resyncing).
      if (event.occurredAt < Date.now() - 60 * 60 * 1000) return;

      if (!deps.triggerDebounceQueue) {
        logger.warn(
          { tenantId, traceId },
          "Trigger debounce queue not wired — skipping (web-only registration?)",
        );
        return;
      }

      const triggers =
        await deps.triggers.getActiveTraceTriggersForProject(tenantId);
      if (triggers.length === 0) return;

      const candidates = triggers.filter(
        (t: TriggerSummary) => classifyTriggerFilters(t.filters).hasEvaluationFilters,
      );

      const queue = deps.triggerDebounceQueue;
      for (const trigger of candidates) {
        try {
          await enqueueTriggerMatch({
            queue,
            projectId: tenantId,
            trigger,
            traceId,
          });
        } catch (error) {
          logger.error(
            {
              tenantId,
              traceId,
              triggerId: trigger.id,
              evaluationId: evalRun.evaluationId,
              error: error instanceof Error ? error.message : String(error),
            },
            "Failed to enqueue trigger match (eval pipeline)",
          );
          captureException(error, {
            extra: {
              tenantId,
              traceId,
              triggerId: trigger.id,
              evaluationId: evalRun.evaluationId,
            },
          });
        }
      }
    },
  };
}
