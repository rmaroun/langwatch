import { TriggerAction } from "@prisma/client";
import type { TriggerSummary } from "~/server/app-layer/triggers/repositories/trigger.repository";
import type { ProjectService } from "~/server/app-layer/projects/project.service";
import type { TriggerService } from "~/server/app-layer/triggers/trigger.service";
import type { TraceSummaryData } from "~/server/app-layer/traces/types";
import { sendTriggerEmail } from "~/server/mailer/triggerEmail";
import { sendSlackWebhook } from "~/server/triggers/sendSlackWebhook";
import type { DatasetRecordEntry } from "~/server/datasets/types";
import type {
  OutboxDispatchPayload,
  PgOutboxAuditAdapter,
} from "~/server/event-sourcing/outbox/pgAuditAdapter";
import type { EventSourcedQueueProcessor } from "~/server/event-sourcing/queues/queue.types";
import { enqueueTriggerNotify } from "~/server/event-sourcing/outbox/triggerNotify/enqueue";
import type { TriggerNotifyInner } from "~/server/event-sourcing/outbox/triggerNotify/payload";
import {
  mapTraceToDatasetEntry,
  TRACE_EXPANSIONS,
  type TraceMapping,
} from "~/server/tracer/tracesMapping";
import type { Trace } from "~/server/tracer/types";
import { createLogger } from "~/utils/logger/server";

const logger = createLogger("langwatch:trigger-action-dispatch");

/**
 * Trigger actions split into two classes that dispatch on different schedules.
 * See dev/docs/adr/025-notify-persistent-action-classification.md.
 *
 * - Notify actions land in front of a human; they may be batched into a digest
 *   window to avoid notification storms.
 * - Persist actions write durable data the customer asked for; batching them
 *   would defeat the intent, so they always dispatch immediately.
 *
 * The two sets must together cover every TriggerAction value, with no overlap
 * (enforced by the unit test). A new action type must be classified here at the
 * point it is introduced.
 */
export const NOTIFY_TRIGGER_ACTIONS = new Set<TriggerAction>([
  TriggerAction.SEND_EMAIL,
  TriggerAction.SEND_SLACK_MESSAGE,
]);

export const PERSIST_TRIGGER_ACTIONS = new Set<TriggerAction>([
  TriggerAction.ADD_TO_DATASET,
  TriggerAction.ADD_TO_ANNOTATION_QUEUE,
]);

export const NOTIFICATION_CADENCES = [
  "immediate",
  "5min_digest",
  "15min_digest",
  "hourly_digest",
] as const;

export type NotificationCadence = (typeof NOTIFICATION_CADENCES)[number];

export const CADENCE_WINDOW_MS: Record<NotificationCadence, number> = {
  immediate: 0,
  "5min_digest": 5 * 60 * 1000,
  "15min_digest": 15 * 60 * 1000,
  hourly_digest: 60 * 60 * 1000,
};

/**
 * UI-allowed values for `Trigger.traceDebounceMs` (ADR-030). The
 * schema stores an int so a future window is one entry away, but the
 * supported authoring options live here as a single source of truth
 * that the router input schema, the drawer field, and the
 * settings-list label all consume.
 *
 * Order matches the visible order in the drawer (Off first, ascending).
 */
export const TRACE_DEBOUNCE_OPTIONS_MS = [
  0,
  15_000,
  30_000,
  60_000,
  120_000,
  300_000,
] as const;

export type TraceDebounceOptionMs =
  (typeof TRACE_DEBOUNCE_OPTIONS_MS)[number];

/**
 * App-layer default for new triggers per ADR-030. Matches the migration
 * default so a new row and an existing row land on the same value.
 */
export const DEFAULT_TRACE_DEBOUNCE_MS: TraceDebounceOptionMs = 30_000;

/**
 * Resolves when a matched trigger should dispatch. The outbox dispatch
 * queue (ADR-021 revision) consumes this value as `delay` on send:
 * persist actions and immediate-cadence notify actions fire now;
 * digest-cadence notify actions snap to the next wall-clock window
 * boundary so every match arriving inside the same window shares a
 * scheduled time and the queue coalesces them via `processBatch`.
 *
 * Windowed (not sliding) semantics: a 5min_digest with the first match
 * at 12:03 closes the window at 12:05, so a 12:04 match joins the
 * same digest. A 12:06 match opens the next window, closing at 12:10.
 */
export function computeScheduledFor({
  action,
  cadence,
  now,
}: {
  action: TriggerAction;
  cadence: NotificationCadence;
  now: Date;
}): Date {
  if (PERSIST_TRIGGER_ACTIONS.has(action)) return now;
  if (cadence === "immediate") return now;
  const windowMs = CADENCE_WINDOW_MS[cadence];
  const windowStart = Math.floor(now.getTime() / windowMs) * windowMs;
  return new Date(windowStart + windowMs);
}

export interface TriggerActionDispatchDeps {
  triggers: TriggerService;
  projects: ProjectService;
  traceById: (projectId: string, traceId: string) => Promise<Trace | undefined>;
  addToAnnotationQueue: (params: {
    traceIds: string[];
    projectId: string;
    annotators: string[];
    userId: string;
  }) => Promise<void>;
  addToDataset: (params: {
    datasetId: string;
    projectId: string;
    datasetRecords: DatasetRecordEntry[];
  }) => Promise<void>;
  /**
   * When both fields are set, notify-class actions (email, Slack) are
   * routed through the outbox dispatch queue so matches inside the
   * same cadence window coalesce into one dispatched digest. Persist
   * actions (dataset, annotation queue) always run inline regardless
   * — they want every match to land.
   *
   * Wiring `triggerNotify` is the switch that flips a deployment from
   * "one notification per match" to ADR-025 digest grouping. Leave it
   * absent (e.g. in unit tests that don't care) to keep the legacy
   * inline notify path.
   */
  triggerNotify?: {
    queue: EventSourcedQueueProcessor<OutboxDispatchPayload<TriggerNotifyInner>>;
    auditAdapter: PgOutboxAuditAdapter;
  };
}

interface ActionParams {
  members?: string[] | null;
  slackWebhook?: string | null;
  datasetId?: string;
  datasetMapping?: {
    mapping: Record<string, { source: string; key: string; subkey: string }>;
    expansions: string[];
  };
  annotators?: { id: string; name: string }[];
  createdByUserId?: string;
}

export async function dispatchTriggerAction({
  deps,
  trigger,
  traceId,
  tenantId,
  foldState,
}: {
  deps: TriggerActionDispatchDeps;
  trigger: TriggerSummary;
  traceId: string;
  tenantId: string;
  foldState: TraceSummaryData;
}): Promise<void> {
  // Notify-class actions go through the outbox dispatch queue when
  // wired, so a burst of matches inside the same cadence window
  // dispatches as one digest. The queue's processBatch callback
  // (createTriggerNotifyDispatcher) renders the digest at lease time;
  // everything below here stays inline only for persist-class actions
  // or the legacy unwired path.
  if (deps.triggerNotify && NOTIFY_TRIGGER_ACTIONS.has(trigger.action)) {
    await enqueueTriggerNotify({
      queue: deps.triggerNotify.queue,
      auditAdapter: deps.triggerNotify.auditAdapter,
      projectId: tenantId,
      trigger,
      traceId,
      foldState,
    });
    return;
  }

  const project = await deps.projects.getById(tenantId);

  if (!project) {
    logger.warn({ tenantId, triggerId: trigger.id }, "Project not found");
    return;
  }

  // Fetch full trace once — used by Slack (events), email (events), and ADD_TO_DATASET (mapping).
  // Best-effort: if trace not found, actions that only need input/output still work with a stub.
  const fullTrace = await deps.traceById(tenantId, traceId) ?? { trace_id: traceId } as Trace;

  const triggerData = buildTriggerData(traceId, tenantId, foldState, fullTrace);
  const params = (trigger.actionParams ?? {}) as ActionParams;

  switch (trigger.action) {
    case TriggerAction.SEND_EMAIL:
      await sendTriggerEmail({
        triggerEmails: params.members ?? [],
        triggerData: [triggerData],
        triggerName: trigger.name,
        projectSlug: project.slug,
        triggerType: trigger.alertType,
        triggerMessage: trigger.message ?? "",
      });
      break;

    case TriggerAction.SEND_SLACK_MESSAGE:
      await sendSlackWebhook({
        triggerWebhook: params.slackWebhook ?? "",
        triggerData: [triggerData],
        triggerName: trigger.name,
        projectSlug: project.slug,
        triggerType: trigger.alertType,
        triggerMessage: trigger.message ?? "",
      });
      break;

    case TriggerAction.ADD_TO_ANNOTATION_QUEUE:
      await deps.addToAnnotationQueue({
        traceIds: [traceId],
        projectId: tenantId,
        annotators: (params.annotators ?? []).map((a) => a.id),
        userId: params.createdByUserId ?? "",
      });
      break;

    case TriggerAction.ADD_TO_DATASET:
      await addTraceToDataset({
        deps,
        trigger,
        traceId,
        tenantId,
        params,
        fullTrace,
      });
      break;
  }

  await deps.triggers.updateLastRunAt(trigger.id, tenantId);

  logger.info(
    { tenantId, traceId, triggerId: trigger.id, action: trigger.action },
    "Trigger fired",
  );
}

function buildTriggerData(
  traceId: string,
  tenantId: string,
  foldState: TraceSummaryData,
  fullTrace: Trace,
): { traceId: string; input: string; output: string; projectId: string; fullTrace: Trace } {
  return {
    traceId,
    input: foldState.computedInput ?? "",
    output: foldState.computedOutput ?? "",
    projectId: tenantId,
    fullTrace,
  };
}

async function addTraceToDataset({
  deps,
  trigger,
  traceId,
  tenantId,
  params,
  fullTrace,
}: {
  deps: TriggerActionDispatchDeps;
  trigger: TriggerSummary;
  traceId: string;
  tenantId: string;
  params: ActionParams;
  fullTrace: Trace;
}): Promise<boolean> {
  if (!params.datasetId || !params.datasetMapping) {
    logger.warn(
      { tenantId, triggerId: trigger.id },
      "ADD_TO_DATASET trigger missing datasetId or datasetMapping",
    );
    return false;
  }

  // Full trace was already fetched by dispatchTriggerAction; check it has spans
  if (!fullTrace.spans || fullTrace.spans.length === 0) {
    logger.warn(
      { tenantId, traceId, triggerId: trigger.id },
      "Trace not found or has no spans for ADD_TO_DATASET action",
    );
    return false;
  }

  const trace = fullTrace;

  const { mapping, expansions: expansionsArray } = params.datasetMapping;
  const expansions = new Set(
    expansionsArray.filter(
      (e): e is keyof typeof TRACE_EXPANSIONS => e in TRACE_EXPANSIONS,
    ),
  );

  const entries: DatasetRecordEntry[] = [];

  const mappedEntries = mapTraceToDatasetEntry(
    trace,
    mapping as TraceMapping,
    expansions,
    undefined,
    undefined,
  );

  for (let i = 0; i < mappedEntries.length; i++) {
    const entry = mappedEntries[i]!;
    const sanitizedEntry = Object.fromEntries(
      Object.entries(entry).map(([key, value]) => [
        key,
        typeof value === "string" ? value.replace(/\u0000/g, "") : value,
      ]),
    );
    entries.push({
      id: `${trigger.id}-${traceId}-${i}`,
      selected: true,
      ...sanitizedEntry,
    });
  }

  await deps.addToDataset({
    datasetId: params.datasetId,
    projectId: tenantId,
    datasetRecords: entries,
  });

  return true;
}
