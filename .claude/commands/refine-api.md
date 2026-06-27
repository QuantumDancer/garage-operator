---
description: Compare a CRD's user-facing surface against the real Garage Admin API
argument-hint: "[Kind or area, e.g. GarageBucket — optional]"
---

Sanity-check a user-facing CRD surface against the real Garage Admin API and
report gaps. This is analysis only — **do not implement anything, do not edit
files.** Stop after presenting the report.

## Scope

$ARGUMENTS

If no scope is given, ask me which CRD/Kind or area you should analyze before
proceeding.

## What to read

- The CRD surface: the relevant `api/<version>/*_types.go` (spec/status fields,
  kubebuilder markers — defaults, enums, required, patterns). If the type does not
  exist yet and I gave you an example CR, infer the surface from that instead.
- The source of truth for Garage's API: the vendored spec
  `internal/garageadmin/openapi/garage-admin-v2.json` (cross-check
  `references/garage/doc/api/` if useful).

## What to report

Map every CRD field to the Admin API operation/field that backs it, then call out:

- **Unbacked CRD fields** — fields with no corresponding Admin API operation or
  field. Can the operator actually fulfill them, or are they aspirational?
- **Unexposed API capabilities** — things the Admin API supports that the CRD
  exposes incompletely or not at all (flag as possible gaps, not necessarily bugs).
- **Type / enum / shape mismatches** — CRD type, enum set, or structure that
  diverges from what the API accepts or returns.
- **Required/optional mismatches** — API-mandatory inputs the CRD lets the user
  omit (and would have to default in the controller — see the nested-defaults
  caveat), or CRD-required fields the API treats as optional.
- **Version/feature gating** — capabilities tied to a specific Garage version.

Present findings as a table or grouped list, ordered by impact. Flag uncertainty
explicitly rather than guessing. End with a short recommendation on whether the
surface is ready or what needs to change — then stop.
