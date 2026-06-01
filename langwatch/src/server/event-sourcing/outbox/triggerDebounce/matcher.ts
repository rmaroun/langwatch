import type { EvaluationRunService } from "~/server/app-layer/evaluations/evaluation-run.service";
import type { TraceSummaryData } from "~/server/app-layer/traces/types";
import {
  buildPreconditionTraceDataFromFoldState,
  classifyTriggerFilters,
  matchesEvaluationFilters,
  matchesTriggerFilters,
  triggerFiltersReferenceEvents,
} from "~/server/filters/triggerFilter.matcher";
import { createLogger } from "~/utils/logger/server";
import { captureException } from "~/utils/posthogErrorCapture";
import { createTenantId } from "../../domain/tenantId";
import { isDispatchError } from "../dispatchError";
import type { DerivedTraceEvent } from "../../pipelines/trace-processing/projections/services/trace-events.derivation";
import {
  dispatchTriggerAction,
  type TriggerActionDispatchDeps,
} from "../../pipelines/shared/triggerActionDispatch";
import type { FoldProjectionStore } from "../../projections/foldProjection.types";
import type { TriggerMatchRequest } from "./payload";

const logger = createLogger("langwatch:trigger-evaluation");

export interface TriggerMatcherDeps extends TriggerActionDispatchDeps {
  traceSummaryStore: FoldProjectionStore<TraceSummaryData>;
  evaluationRuns: EvaluationRunService;
  deriveEvents: (params: {
    tenantId: string;
    traceId: string;
    occurredAtMs?: number;
    foldVersion?: number;
  }) => Promise<DerivedTraceEvent[]>;
}

/**
 * Process function for the trigger-evaluation queue (ADR-030).
 *
 * Runs after the trace has been quiet for `traceDebounceMs` —
 * loads the now-settled fold state, runs the filter check, claims
 * the at-most-once `TriggerSent` row, and calls `dispatchTriggerAction`
 * (which routes notify through the ADR-025 outbox digest path).
 *
 * Silently no-ops in every "trigger or trace gone since enqueue"
 * branch — the reactor has no way to cancel a pending evaluation, so
 * the evaluator must be tolerant of state that disappeared during the
 * debounce window.
 */
export function createTriggerMatcher(
  deps: TriggerMatcherDeps,
): (req: TriggerMatchRequest) => Promise<void> {
  return async function matchAndDispatchTrigger(req) {
    const { projectId, triggerId, traceId } = req;

    const triggers =
      await deps.triggers.getActiveTraceTriggersForProject(projectId);
    const trigger = triggers.find((t) => t.id === triggerId);
    if (!trigger) {
      logger.debug(
        { projectId, triggerId, traceId },
        "Trigger missing / deactivated during debounce — skipping",
      );
      return;
    }

    const brandedTenantId = createTenantId(projectId);
    const foldState = await deps.traceSummaryStore.get(traceId, {
      tenantId: brandedTenantId,
      aggregateId: traceId,
    });
    if (!foldState) {
      logger.debug(
        { projectId, triggerId, traceId },
        "Trace fold gone during debounce — skipping",
      );
      return;
    }

    const { traceFilters, evaluationFilters, hasEvaluationFilters } =
      classifyTriggerFilters(trigger.filters);

    const events = triggerFiltersReferenceEvents(traceFilters)
      ? await deps.deriveEvents({
          tenantId: projectId,
          traceId,
          occurredAtMs: foldState.occurredAt,
          foldVersion: foldState.spanCount,
        })
      : null;
    const traceData = buildPreconditionTraceDataFromFoldState(
      foldState,
      events,
    );

    if (
      Object.keys(traceFilters).length > 0 &&
      !matchesTriggerFilters(traceData, traceFilters)
    ) {
      return;
    }

    if (hasEvaluationFilters) {
      const allEvaluations = await deps.evaluationRuns.findByTraceId(
        projectId,
        traceId,
      );
      if (!matchesEvaluationFilters(allEvaluations, evaluationFilters)) {
        return;
      }
    }

    // Atomic claim is still the at-most-once gate (ADR-022). A re-enqueue
    // after the dispatch has already happened (e.g. a span arriving an
    // hour later for the same trace) sees `claimed: false` and no-ops.
    const claimed = await deps.triggers.claimSend({
      triggerId: trigger.id,
      traceId,
      projectId,
    });
    if (!claimed) return;

    try {
      await dispatchTriggerAction({
        deps,
        trigger,
        traceId,
        tenantId: projectId,
        foldState,
      });
    } catch (error) {
      // Dispatch errors flow through ADR-027's DispatchError contract.
      // The claim already landed, so the inline path doesn't retry —
      // the outbox layer (when wired) is what carries durable retry.
      const retryable = isDispatchError(error) ? error.retryable : undefined;
      logger.error(
        {
          projectId,
          triggerId,
          traceId,
          retryable,
          error: error instanceof Error ? error.message : String(error),
        },
        "Trigger evaluation dispatch failed",
      );
      captureException(error, {
        extra: {
          projectId,
          triggerId,
          traceId,
          triggerAction: trigger.action,
          retryable,
        },
      });
    }
  };
}
