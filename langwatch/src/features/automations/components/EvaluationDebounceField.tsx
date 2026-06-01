import { createListCollection, Field, Text } from "@chakra-ui/react";
import { useMemo } from "react";
import { Select } from "~/components/ui/select";
import {
  EVALUATION_DEBOUNCE_OPTIONS_MS,
  type EvaluationDebounceOptionMs,
} from "~/server/event-sourcing/pipelines/shared/triggerActionDispatch";
import { useAutomationStore } from "../state/automationStore";
import { useDraft } from "../state/selectors";

const DEBOUNCE_LABELS: Record<EvaluationDebounceOptionMs, string> = {
  0: "Off (evaluate on every span)",
  15_000: "15 seconds",
  30_000: "30 seconds",
  60_000: "1 minute",
  120_000: "2 minutes",
  300_000: "5 minutes",
};

// Stored as ms internally; the Chakra Select API works with strings, so we
// pivot through the numeric value's string form at the boundary.
const DEBOUNCE_OPTIONS = EVALUATION_DEBOUNCE_OPTIONS_MS.map((value) => ({
  value: String(value),
  label: DEBOUNCE_LABELS[value],
}));

/**
 * Per-trigger trace-readiness debounce (ADR-030). Visible for every
 * action class — persist triggers benefit as much as notify because a
 * dataset row captured from a half-formed fold corrupts the eval set
 * silently. Required field; the non-zero default lives in
 * `INITIAL_DRAFT` and the router's create path.
 */
export function EvaluationDebounceField() {
  const draft = useDraft();
  const dispatch = useAutomationStore((s) => s.dispatch);

  const collection = useMemo(
    () => createListCollection({ items: DEBOUNCE_OPTIONS }),
    [],
  );

  return (
    <Field.Root>
      <Field.Label>Wait for trace to settle</Field.Label>
      <Select.Root
        collection={collection}
        value={[String(draft.evaluationDebounceMs)]}
        onValueChange={({ value }) => {
          const next = value[0];
          if (next === undefined) return;
          const parsed = Number(next);
          if (
            !(EVALUATION_DEBOUNCE_OPTIONS_MS as readonly number[]).includes(
              parsed,
            )
          ) {
            return;
          }
          dispatch({
            type: "SET_EVALUATION_DEBOUNCE",
            value: parsed as EvaluationDebounceOptionMs,
          });
        }}
      >
        <Select.Trigger>
          <Select.ValueText />
        </Select.Trigger>
        <Select.Content>
          {DEBOUNCE_OPTIONS.map((opt) => (
            <Select.Item key={opt.value} item={opt}>
              {opt.label}
            </Select.Item>
          ))}
        </Select.Content>
      </Select.Root>
      <Text textStyle="xs" color="fg.muted" mt={1}>
        {draft.evaluationDebounceMs === 0
          ? "Fires as soon as filters match — risks dispatching from a half-formed trace."
          : "Waits for the trace to be quiet this long before evaluating filters."}
      </Text>
    </Field.Root>
  );
}
