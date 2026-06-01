import { TriggerAction } from "@prisma/client";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { EvaluationRunData } from "~/server/app-layer/evaluations/types";
import type { TriggerSummary } from "~/server/app-layer/triggers/repositories/trigger.repository";
import type { EventSourcedQueueProcessor } from "../../../../queues/queue.types";
import type { TriggerMatchRequest } from "../../../../outbox/triggerDebounce/payload";
import type { ReactorContext } from "../../../../reactors/reactor.types";
import type { EvaluationProcessingEvent } from "../../schemas/events";
import {
  createEvaluationAlertTriggerReactor,
  type EvaluationAlertTriggerReactorDeps,
} from "../evaluationAlertTrigger.reactor";

/**
 * Reactor's job collapsed to "enqueue match requests for the right
 * subset of triggers" once ADR-030 moved filter evaluation into the
 * trigger-debounce matcher. These tests assert the enqueue contract;
 * the actual filter / claim / dispatch path is covered by matcher
 * unit tests against `createTriggerMatcher`.
 */

function makeQueue(): EventSourcedQueueProcessor<TriggerMatchRequest> &
  { send: ReturnType<typeof vi.fn> } {
  return {
    send: vi.fn(async () => undefined),
    sendBatch: vi.fn(async () => undefined),
    close: vi.fn(async () => undefined),
    waitUntilReady: vi.fn(async () => undefined),
  } as any;
}

function makeTrigger(overrides: Partial<TriggerSummary> = {}): TriggerSummary {
  return {
    id: "trigger-1",
    projectId: "tenant-1",
    name: "Quality Alert",
    action: TriggerAction.SEND_EMAIL,
    actionParams: { members: ["user@example.com"] },
    filters: {
      "evaluations.passed": { "evaluator-1": ["true"] },
    },
    alertType: "WARNING",
    message: "Evaluation passed",
    customGraphId: null,
    notificationCadence: "immediate",
    traceDebounceMs: 30_000,
    ...overrides,
  };
}

function makeContext(
  overrides: Partial<EvaluationRunData> = {},
): ReactorContext<EvaluationRunData> {
  return {
    tenantId: "tenant-1",
    aggregateId: "trace-1",
    foldState: {
      status: "processed",
      traceId: "trace-1",
      evaluationId: "eval-1",
      ...overrides,
    } as EvaluationRunData,
  } as ReactorContext<EvaluationRunData>;
}

function makeEvent(): EvaluationProcessingEvent {
  return {
    type: "EvaluationCompleted",
    tenantId: "tenant-1",
    aggregateId: "eval-1",
    occurredAt: Date.now(),
    data: { evaluationId: "eval-1", status: "processed" },
  } as unknown as EvaluationProcessingEvent;
}

function makeDeps(
  triggers: TriggerSummary[],
): EvaluationAlertTriggerReactorDeps {
  return {
    triggers: {
      getActiveTraceTriggersForProject: vi.fn(async () => triggers),
    } as any,
    triggerDebounceQueue: makeQueue(),
  };
}

describe("createEvaluationAlertTriggerReactor", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe("when the trigger has evaluation filters", () => {
    it("enqueues a trigger-match request", async () => {
      const deps = makeDeps([makeTrigger()]);
      const reactor = createEvaluationAlertTriggerReactor(deps);

      await reactor.handle(makeEvent(), makeContext());

      expect(deps.triggerDebounceQueue.send).toHaveBeenCalledTimes(1);
      expect(deps.triggerDebounceQueue.send).toHaveBeenCalledWith(
        {
          projectId: "tenant-1",
          triggerId: "trigger-1",
          traceId: "trace-1",
        },
        expect.objectContaining({
          deduplication: expect.objectContaining({
            ttlMs: 30_000,
            extend: true,
            replace: true,
          }),
        }),
      );
    });
  });

  describe("when the trigger has only trace-level filters", () => {
    it("does not enqueue — the trace pipeline owns that one", async () => {
      const deps = makeDeps([
        makeTrigger({
          filters: { "spans.model": ["gpt-5-mini"] },
        }),
      ]);
      const reactor = createEvaluationAlertTriggerReactor(deps);

      await reactor.handle(makeEvent(), makeContext());

      expect(deps.triggerDebounceQueue.send).not.toHaveBeenCalled();
    });
  });

  describe("when the evaluation has no traceId", () => {
    it("skips enqueue — dedup key needs the trace", async () => {
      const deps = makeDeps([makeTrigger()]);
      const reactor = createEvaluationAlertTriggerReactor(deps);

      await reactor.handle(makeEvent(), makeContext({ traceId: undefined }));

      expect(deps.triggerDebounceQueue.send).not.toHaveBeenCalled();
    });
  });

  describe("when the evaluation is older than 1h", () => {
    it("skips enqueue — resync events shouldn't refire triggers", async () => {
      const deps = makeDeps([makeTrigger()]);
      const reactor = createEvaluationAlertTriggerReactor(deps);
      const oldEvent = {
        ...makeEvent(),
        occurredAt: Date.now() - 2 * 60 * 60 * 1000,
      };

      await reactor.handle(oldEvent, makeContext());

      expect(deps.triggerDebounceQueue.send).not.toHaveBeenCalled();
    });
  });
});
