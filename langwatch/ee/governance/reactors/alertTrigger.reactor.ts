// SPDX-License-Identifier: LicenseRef-LangWatch-Enterprise

import type { TraceSummaryData } from "~/server/app-layer/traces/types";
import type { TriggerSummary } from "~/server/app-layer/triggers/repositories/trigger.repository";
import type { TriggerService } from "~/server/app-layer/triggers/trigger.service";
import { classifyTriggerFilters } from "~/server/filters/triggerFilter.matcher";
import { createLogger } from "~/utils/logger/server";
import { captureException } from "~/utils/posthogErrorCapture";
import type { ReactorDefinition } from "~/server/event-sourcing/reactors/reactor.types";
import { enqueueTriggerMatch } from "~/server/event-sourcing/outbox/triggerDebounce/enqueue";
import type { TriggerMatchRequest } from "~/server/event-sourcing/outbox/triggerDebounce/payload";
import type { EventSourcedQueueProcessor } from "~/server/event-sourcing/queues/queue.types";
import type { TraceProcessingEvent } from "~/server/event-sourcing/pipelines/trace-processing/schemas/events";
import { defineOriginGuardedTraceReactor } from "~/server/event-sourcing/pipelines/trace-processing/reactors/_originGuardedReactor";

const logger = createLogger("langwatch:trace-processing:alert-trigger-reactor");

export interface AlertTriggerReactorDeps {
  triggers: TriggerService;
  /**
   * GroupQueue that holds debounced trigger-match requests (ADR-030). The
   * reactor only ENQUEUES; the queue's matcher runs after the trace has
   * been quiet for the trigger's `traceDebounceMs`.
   *
   * Optional because the registry runs on every process role but reactors
   * only fire on the worker — the queue lives on the worker, so the web
   * registry passes `undefined`. The handler guards defensively just in
   * case the production wiring ever ships a reactor without a queue.
   */
  triggerDebounceQueue?: EventSourcedQueueProcessor<TriggerMatchRequest>;
}

/**
 * Trace-pipeline reactor that schedules trigger matching when traces arrive.
 *
 * Fires on every trace event. For each active trigger whose filters are
 * trace-only (no evaluation filters — those land on the eval pipeline),
 * the reactor enqueues a debounced match request. The GroupQueue's
 * Debounce Mode dedup collapses repeats from the same (trigger, trace)
 * pair onto one pending job whose TTL resets per new span. Once the
 * trace goes silent for `trigger.traceDebounceMs`, the matcher runs the
 * filter check and dispatches.
 *
 * No filter eval here — that moved into the matcher so the verdict is
 * made against a settled trace, not a half-formed fold. See ADR-030.
 */
export function createAlertTriggerReactor(
  deps: AlertTriggerReactorDeps,
): ReactorDefinition<TraceProcessingEvent, TraceSummaryData> {
  return defineOriginGuardedTraceReactor({
    name: "alertTrigger",
    jobIdPrefix: "alert-trigger",
    async handle(_event, context) {
      const { tenantId, aggregateId: traceId } = context;

      if (!deps.triggerDebounceQueue) {
        logger.warn(
          { tenantId, traceId },
          "Trigger debounce queue not wired — skipping (web-only registration?)",
        );
        return;
      }

      const triggers = await deps.triggers.getActiveTraceTriggersForProject(
        tenantId,
      );
      if (triggers.length === 0) return;

      // Trace-pipeline candidates: only triggers WITHOUT evaluation filters.
      // The eval pipeline owns eval-filtered triggers — a trace event can't
      // advance their match verdict, so enqueuing here would be wasted work.
      const candidates = triggers.filter(
        (t: TriggerSummary) => !classifyTriggerFilters(t.filters).hasEvaluationFilters,
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
          // Enqueue failures are not retryable here (the reactor is a
          // best-effort signal). Capture and continue so one bad row
          // doesn't stop the rest of the candidate set.
          logger.error(
            {
              tenantId,
              traceId,
              triggerId: trigger.id,
              error: error instanceof Error ? error.message : String(error),
            },
            "Failed to enqueue trigger match",
          );
          captureException(error, {
            extra: { tenantId, traceId, triggerId: trigger.id },
          });
        }
      }
    },
  });
}
