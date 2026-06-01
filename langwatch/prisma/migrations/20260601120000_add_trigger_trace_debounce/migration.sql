-- Per-trigger trace-readiness debounce. The trigger matcher runs filters
-- and dispatches only after `traceDebounceMs` of silence on the trace,
-- so partially-assembled traces do not produce half-formed dispatch.
--
-- See dev/docs/adr/030-trace-readiness-debounce-for-trigger-evaluation.md.
--
-- DEFAULT 30000 (30s) is the non-zero default — picking 0 here would
-- ship the half-formed-dispatch behavior as the default for every
-- existing and new trigger. Existing rows migrate to 30s; operators
-- who depended on eager evaluation can flip the field to 0 in the
-- automation drawer.

-- AlterTable
ALTER TABLE "Trigger" ADD COLUMN "traceDebounceMs" INTEGER NOT NULL DEFAULT 30000;

-- To roll back, uncomment and run manually:
-- ALTER TABLE "Trigger" DROP COLUMN "traceDebounceMs";
