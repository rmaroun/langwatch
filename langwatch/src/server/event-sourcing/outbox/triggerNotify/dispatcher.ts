import { TriggerAction } from "@prisma/client";
import type { ProjectService } from "~/server/app-layer/projects/project.service";
import type { TriggerService } from "~/server/app-layer/triggers/trigger.service";
import { sendTriggerEmail } from "~/server/mailer/triggerEmail";
import { sendSlackWebhook } from "~/server/triggers/sendSlackWebhook";
import type { Trace } from "~/server/tracer/types";
import { createLogger } from "~/utils/logger/server";
import { DispatchError } from "../dispatchError";
import type { OutboxDispatcherFn } from "../outboxDispatchQueue";
import {
  triggerNotifyInnerSchema,
  type TriggerNotifyInner,
} from "./payload";

const logger = createLogger("langwatch:outbox:trigger-notify-dispatcher");

interface ActionParams {
  members?: string[] | null;
  slackWebhook?: string | null;
}

export interface TriggerNotifyDispatcherDeps {
  triggers: TriggerService;
  projects: ProjectService;
  traceById: (
    projectId: string,
    traceId: string,
  ) => Promise<Trace | undefined>;
}

/**
 * Outbox dispatcher for the `triggerNotify` reactor (ADR-021 revision +
 * ADR-025 digest cadence). Receives every payload coalesced into the
 * same cadence-window batch by the queue's `processBatch` — that's how
 * digest grouping works post-revision (no `leaseGroup` against PG).
 *
 * The function decodes each payload, asserts the same-triggerId
 * invariant (the queue's groupKey is `${projectId}/triggerNotify:${triggerId}`
 * so a single batch is always one trigger), looks up the trigger +
 * project, builds the `triggerData[]` digest, and calls the right
 * sender exactly once with every match.
 *
 * Failure semantics:
 *   - Trigger deactivated / missing since enqueue → silent success
 *     (operator intent: don't fire what's been turned off).
 *   - Project missing → terminal `DispatchError` (project deletion is
 *     final, no point retrying).
 *   - Malformed payload (schema parse fails) → terminal.
 *   - Mixed-trigger batch (impossible if groupKey is well-formed) → terminal.
 *   - Sender throws → propagates the sender's `DispatchError`
 *     classification (5xx retryable, 4xx terminal — ADR-027).
 */
export function createTriggerNotifyDispatcher(
  deps: TriggerNotifyDispatcherDeps,
): OutboxDispatcherFn<TriggerNotifyInner> {
  return async function triggerNotifyDispatcher(payloads) {
    if (payloads.length === 0) return;

    const projectId = payloads[0]!.projectId;
    const inners: TriggerNotifyInner[] = payloads.map((p) => {
      const parsed = triggerNotifyInnerSchema.safeParse(p.inner);
      if (!parsed.success) {
        throw new DispatchError({
          message: `triggerNotify payload malformed: ${parsed.error.message}`,
          retryable: false,
        });
      }
      return parsed.data;
    });

    const triggerId = inners[0]!.triggerId;
    for (const inner of inners) {
      if (inner.triggerId !== triggerId) {
        throw new DispatchError({
          message: `triggerNotify batch has mixed triggerIds (${triggerId} vs ${inner.triggerId})`,
          retryable: false,
        });
      }
    }

    const triggers =
      await deps.triggers.getActiveTraceTriggersForProject(projectId);
    const trigger = triggers.find((t) => t.id === triggerId);
    if (!trigger) {
      logger.info(
        { projectId, triggerId, batchSize: payloads.length },
        "Trigger gone / deactivated since enqueue — dropping digest",
      );
      return;
    }

    const project = await deps.projects.getById(projectId);
    if (!project) {
      throw new DispatchError({
        message: `project ${projectId} not found at dispatch time`,
        retryable: false,
      });
    }

    const params = (trigger.actionParams ?? {}) as ActionParams;
    const triggerData = await Promise.all(
      inners.map(async (p) => {
        const fullTrace =
          (await deps.traceById(projectId, p.match.traceId)) ??
          ({ trace_id: p.match.traceId } as Trace);
        return {
          traceId: p.match.traceId,
          input: p.match.input,
          output: p.match.output,
          projectId,
          fullTrace,
        };
      }),
    );

    switch (trigger.action) {
      case TriggerAction.SEND_EMAIL:
        await sendTriggerEmail({
          triggerEmails: params.members ?? [],
          triggerData,
          triggerName: trigger.name,
          projectSlug: project.slug,
          triggerType: trigger.alertType,
          triggerMessage: trigger.message ?? "",
        });
        break;

      case TriggerAction.SEND_SLACK_MESSAGE:
        await sendSlackWebhook({
          triggerWebhook: params.slackWebhook ?? "",
          triggerData,
          triggerName: trigger.name,
          projectSlug: project.slug,
          triggerType: trigger.alertType,
          triggerMessage: trigger.message ?? "",
        });
        break;

      default:
        throw new DispatchError({
          message: `triggerNotify cannot dispatch action ${trigger.action} — enqueue path is misclassified`,
          retryable: false,
        });
    }

    await deps.triggers.updateLastRunAt(triggerId, projectId);
    logger.info(
      {
        projectId,
        triggerId,
        action: trigger.action,
        digestSize: payloads.length,
      },
      "triggerNotify digest dispatched",
    );
  };
}
