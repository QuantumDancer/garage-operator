---
description: Plan, implement, test, and summarize the next step from PLAN.md
---

Work the next step of the garage-operator. Follow these phases in order and do
not skip the human checkpoint.

## Phase 1 — Plan (interactive, stop for my review)

1. Read `PLAN.md` and identify the next unstarted step. State which step you
   picked and why.
2. Propose the user-facing API shape for that step (the CRD spec/status fields
   the user writes). Remember: the CRD shape is human-owned — you propose, I decide.
3. **Compare the proposed CRD against the real Garage Admin API** to surface gaps
   and inconsistencies _before_ we commit to a shape. The source of truth is the
   vendored spec `internal/garageadmin/openapi/garage-admin-v2.json` (cross-check
   `references/garage/doc/api/` if helpful). Specifically call out:
   - CRD fields with no backing Admin API operation/field (can we actually fulfill them?)
   - Admin API capabilities the CRD exposes incompletely or not at all
   - Type/enum/required mismatches between the CRD and the API
   - Anything the API makes mandatory that the CRD lets the user omit (or vice versa)
4. Present the proposal + the gap analysis and **STOP**. Do not write code until I
   have agreed on the API surface. If anything is ambiguous, ask rather than assume.

## Phase 2 — Implement (after I approve)

The goal is a **complete feature that passes every test stage** — not a partial
slice. Iterate within this phase until all stages are green:

1. Implement the controller/client/webhook logic behind the approved surface.
2. If you touched `*_types.go` or kubebuilder markers: `make manifests generate`.
   If you touched the Admin API spec/config: `make generate-client`.
3. Run the full test ladder and fix failures until all pass:
   - `make lint-fix`
   - `make test` (unit + envtest)
   - `make test-e2e` (Kind — a phase is not done without passing e2e)
4. Do not proceed to the summary while any stage is red. If a stage is genuinely
   blocked (e.g. e2e depends on a later step), say so explicitly and record it.

## Phase 3 — Summarize & commit

1. Summarize what changed and the state of each test stage.
2. If no open questions arose during coding, commit on a branch (not directly on
   `main`) and open a GitHub PR. If anything was ambiguous or needs a decision, ask instead of committing.
   Do not commit `PLAN.md` or `references/` — they stay untracked.
