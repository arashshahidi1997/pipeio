# Pipeline Documentation

How pipeline flows are documented for humans and agents: source layout, the
`docs/index.md` template, per-mod facet docs, the flow-level `CHANGELOG.md`
convention, and how everything is collected into the MkDocs site.

The source of truth is `code/pipelines/<flow>/`. All site content under
`docs/pipelines/` is a build artifact assembled by `pipeio_docs_collect`.

## Audience

Flow documentation has two readers:

- **Supervisors / collaborators** skimming a flow page for the first time —
  they want to know what the flow does, whether it works, and what the
  outputs look like.
- **Returning developers** (including future-you and agents) — they want to
  know *why* the flow looks the way it does and what has been tried before.

These two audiences map onto different sections of the docs: the `index.md`
answers "what does this flow do now", and the `CHANGELOG.md` answers "why
are we here".

## Source tree layout

```
code/pipelines/<flow>/
  Snakefile
  config.yml
  rules/           # .smk files per mod
  scripts/         # per-rule Python
  notebooks/       # jupytext notebooks (see notebook.md spec)
  docs/
    index.md       # flow landing page (the main overview)
    <mod>/         # per-mod facet directory
      theory.md    # science — why and how the step works
      spec.md      # engineering — I/O contract, params, entry points
      delta.md     # optional — history of changes scoped to this mod
  CHANGELOG.md     # design history at the flow root (not under docs/)
  publish.yml      # collection gating — see below
```

Two conventions are being phased in:

- **`docs/index.md`** replaces the older `docs/overview.md`.  `overview.md`
  is still accepted by `docs_collect` as a fallback but new flows should use
  `index.md`.
- **`CHANGELOG.md`** lives at the flow root (sibling of `Snakefile`), not
  under `docs/`.  This matches the Python package convention and separates
  history-of-decisions from current-state documentation.

## Flow index template

`docs/index.md` is the landing page for the flow. It should be readable top
to bottom in a few minutes and answer the supervisor-level questions without
clicking through to mod pages.

The canonical section list:

| Section | Purpose | Author |
|---------|---------|--------|
| `# <flow>` | Page title | scaffold |
| `## Purpose` | One paragraph: what this flow produces and why it exists | user |
| `## Input` | Directory, manifest, modality | scaffold + user |
| `## Output` | Directory, manifest | scaffold + user |
| `### Final products (per session)` | Table: File \| Content | user |
| `## Mod Chain` | Table: Order \| Mod \| Rules \| Purpose | user |
| `## Design Decisions` | Bullet list with rationale | user |
| `## Data Availability` | Session/subject coverage summary | user |
| `## DAG` | Embedded `![Rule DAG](dag.svg)` | auto-injected |
| `## Report` | Link to `report.md` | auto-present when `publish.report: true` |
| `## Changelog` | Link to `changelog.md` | auto-present when `CHANGELOG.md` exists |
| `## Known Gaps` | Flow-level TODO list | user |

Sections in the "user" column are placeholders in the scaffold; the flow
author fills them in as the pipeline takes shape. Sections marked
"auto-injected" or "auto-present" are added by tooling — don't remove the
link lines, and don't write the content below them by hand.

### Reference example

`preprocess_motion` (pixecog) is the canonical reference for a fully-populated
flow index. The patterns worth copying:

**Final products table** — explicit file list, not just a directory reference:

```markdown
### Final products (per session)

| File | Content |
|------|---------|
| `preprocess/*_motion.tsv` | Full time series (14 columns) |
| `epochs/*_epochs.tsv` | Motor state bouts with statistics |
| `summary/*_summary.json` | Session QC: speed, state durations, lap metrics |
| `plot/*_trajectory.svg` | 2D XY position colored by speed |
```

**Mod chain with rules column** — links the mod abstraction to actual
Snakemake rule names:

```markdown
## Mod Chain

| Order | Mod | Rules | Purpose |
|-------|-----|-------|---------|
| 1 | detect | detect | Auto-detect head rigid body from position variance |
| 2 | parse | parse, clean | Extract + clean position (NaN interpolation, lowpass) |
| 3 | kinematics | kinematics, linearize | Velocity, speed, 1D track projection |
| 4 | preprocess | preprocess, epochs, summary | Final assembly, epoch extraction, QC summary |
| 5 | plot | plot_trajectory, ... | QC visualizations (report() tagged) |
```

**Data availability** — one paragraph, supervisor-friendly:

> 48 sessions across 5 subjects. Sub-05 has the best coverage (25 sessions).
> Sub-01 only has 4 homecage sessions.

## Mod documentation

Per-mod facet directories live under `docs/<mod>/`. The name must match the
mod name exactly and must contain at least one of `theory.md`, `spec.md`,
or `delta.md`. `DocsCollector` uses the presence of these files to detect
mod facet dirs and routes them to `mods/<mod>/` in the output.

The three facets have complementary roles:

| File | Role | Audience | Goes stale when |
|------|------|----------|-----------------|
| `theory.md` | Science — why and how the method works | Reader learning the domain | Current math/algorithm changes |
| `spec.md` | Engineering — inputs, outputs, params, script paths | Developer integrating or calling the mod | Rule signature changes |
| `delta.md` | History of changes scoped to this mod | Returning developer wanting mod-specific context | Never — it's append-only |

`theory.md` and `spec.md` are required; `delta.md` is optional and most flows
will not need it (the flow-level CHANGELOG usually suffices).

## Flow CHANGELOG

`CHANGELOG.md` at the flow root records high-level design history: strategy
shifts, DAG reorganizations, architecture decisions, and the key discoveries
that drove them. It is the audit trail that explains *why* theory.md and
spec.md look the way they do.

### What belongs

- **Strategy shifts** — e.g. "template subtraction → time-sample masking",
  with rationale and links to the meeting note where the decision was made.
- **DAG reorganizations** — when rule order changes and why (e.g. "ttl_removal
  moved before feature extraction so badlabel sees clean data").
- **Architecture decisions** — tier structures, auto-skip logic, method choices.
- **Key discoveries that drive design** — e.g. "TTL contamination is sub-01
  only; other subjects skip the step", "TDM row scan order means per-row lags".
- **Cross-references** to notio notes (meetings, ideas, results) and external
  references.

### What doesn't belong

- Every bug fix or parameter tweak — git log is authoritative for these.
- Internal refactors that don't change behavior.
- Info already in CLAUDE.md, theory.md, or spec.md. The CHANGELOG is the
  history of *changes*, not a mirror of the current state.

### Format

Keep-a-Changelog style. `## Unreleased` stays at the top as a buffer for
entries that haven't landed in a version yet; dated entries below go in
reverse chronological order.

```markdown
# Changelog — <flow>

High-level changes to the <flow> pipeline: strategy shifts, DAG
reorganizations, and key discoveries that drive design decisions. Minor
fixes and parameter tweaks belong in git log, not here.

## YYYY-MM-DD — Short one-line title of the change

**Strategy change:** one-paragraph summary of what changed.

**Why:**
- Bullet point on motivation
- Another bullet tying to a meeting/discovery

**New approach:** (optional) numbered list if there's a new procedure to
document.

**DAG reorder:** (optional) what moved where and why.

**Refs:**
- [meeting-arash-YYYYMMDD-*.md](../../../docs/log/meeting/...) — context
- [idea-arash-YYYYMMDD-*.md](../../../docs/log/idea/...) — decision note
- [result-arash-...] — empirical finding that drove the change
```

### Why this is valuable

1. **Onboarding** — a new agent or collaborator can read the CHANGELOG and
   understand the current design without grepping months of commits.
2. **Meeting handoffs** — decisions made in meetings often span multiple
   commits; the CHANGELOG captures the "why" once, linked to the meeting note.
3. **Regret prevention** — documents what was tried and why it was abandoned
   so nobody reintroduces it six months later.
4. **Freshness signal** — `pipeio_flow_status` can surface "last changelog
   entry: YYYY-MM-DD" as a proxy for how recently the design has been
   revisited.

`preprocess_ieeg/CHANGELOG.md` (pixecog) is the reference example: two
entries documenting the TTL removal strategy pivot and the DAG reorder that
accompanied it.

## Collection pipeline

`pipeio_docs_collect` runs a chain of collectors over each registered flow
and writes the results to `docs/pipelines/<flow>/`. See
`docs.py` for the implementation; this section summarizes what each
collector reads and writes.

| Collector | Source | Target | Gated by |
|-----------|--------|--------|----------|
| `DocsCollector` | `{flow}/docs/*.md` and `{flow}/docs/{mod}/*.md` | `docs/pipelines/{flow}/` (mods routed to `mods/{mod}/`) | always on |
| `NotebookCollector` | `{flow}/.build/notebooks/*.{html,md}` | `docs/pipelines/{flow}/notebooks/` | always on |
| `DagCollector` | `{flow}/.build/dag.svg` (preferred) or `{flow}/dag.svg` | `docs/pipelines/{flow}/dag.svg` | `publish.dag` |
| `ReportCollector` | `derivatives/{flow}/report.html` or `{flow}/report.html` | `docs/pipelines/{flow}/report.{html,md}` | `publish.report` |
| `ChangelogCollector` | `{flow}/CHANGELOG.md` | `docs/pipelines/{flow}/changelog.md` | `publish.changelog` |
| `ScriptsCollector` | `{flow}/scripts/*.py` | `docs/pipelines/{flow}/scripts.md` | `publish.scripts` |
| `IndexCollector` | *stub generator* | `docs/pipelines/{flow}/index.md` | only when no source index |

`IndexCollector` is a fallback — if a flow has no `docs/index.md` (and no
legacy `docs/overview.md`), it generates a minimal stub so the page still
renders. Compliant flows will always have a real source index and the stub
path is never taken.

## `publish.yml` gating

Each flow has an optional `publish.yml` at its root that controls which
optional collectors run. Example:

```yaml
dag: true            # collect DAG SVG into docs/pipelines/
report: true         # copy snakemake --report HTML into docs/pipelines/
scripts: true        # generate scripts index page
changelog: true      # copy CHANGELOG.md into docs/pipelines/ as changelog.md
```

Defaults are all `true` (permissive). Set a flag to `false` to hide the
corresponding section from the site — useful for in-progress flows where the
report would be noisy, or sandbox flows where a changelog isn't warranted
yet.

## Scaffolding

`pipeio_flow_new` creates a new flow's directory tree with the docs
conventions already in place:

- `docs/index.md` — populated with all canonical sections from the template
  above, including the auto-injection anchors (DAG, Report, Changelog links)
- `CHANGELOG.md` at the flow root — with the header stub and an empty
  `## Unreleased` section
- `publish.yml` with permissive defaults

The `docs/index.md` sections marked "user" in the table above are
placeholders with TODO-style comments. The author fills them in as real
content emerges.

`pipeio_flow_new` is **idempotent**: running it on an existing flow only
writes files that don't yet exist. Use it as the non-destructive migration
path for flows created before this spec landed — existing content is never
overwritten.

## Spec compliance and migration

Use `pipeio_flow_audit` (or `pipeio flow audit <flow>`) to check whether a
flow complies with this spec. The audit is read-only and reports:

- Which scaffolded files exist (Snakefile, config.yml, publish.yml, CHANGELOG.md)
- Which canonical sections are present in `docs/index.md`
- Which mod facet dirs have `theory.md` / `spec.md` pairs
- A list of issues and suggestions with a fix hint

```bash
pipeio flow audit <flow>    # detailed per-flow report
pipeio flow audit all       # summary across every registered flow
```

**Migration pattern** for retrofitting pre-spec flows:

1. `pipeio flow audit all` — see which flows are non-compliant and why.
2. `pipeio flow new <flow>` on each — adds missing `CHANGELOG.md`,
   `publish.yml`, and scaffold directories without touching existing content.
3. Hand-edit any missing canonical sections in `docs/index.md`. The audit
   never injects content into user-authored docs; that's an explicit design
   choice to avoid corrupting prose the author wrote.
4. `pipeio docs collect` — rebuild the `docs/pipelines/` tree.
5. `pipeio flow audit all` — confirm compliance.

Auditing does not depend on `docs_collect` — it reads directly from the
flow source tree, so it's safe to run in tight loops while migrating.

## Relationship to results, deliverables, and questions

Flow pages are an **engineering surface**. They describe the machine
(Purpose, Input/Output, Mod Chain, DAG, status, CHANGELOG) and link to
scientific outputs — they do not embed result plots, hypothesis evaluations,
or narrative writeups.

The ecosystem splits ownership along an engineering-vs-science boundary
documented in [delegation-model.md](../../../../../../docs/explanation/delegation-model.md):

- **pipeio flows** own pipeline machinery (this spec)
- **questio** owns research questions and hypothesis tracking
- **notio `result` notes** own individual scientific findings
- **`docs/deliverables/`** owns narrative artifacts for external audiences

### Linking direction

Cross-references flow **downstream → upstream**. The upstream object (a
flow, a question) does not track its downstream dependents; those are
computed at render time by scanning descendants.

Downstream objects set one or more of these frontmatter fields:

| Note type | Fields that reference a flow |
|---|---|
| `result` | `source_flow: <flow-name>` — the flow that produced the data the finding is based on |
| `deliverable` | `source_flows: [<flow-1>, <flow-2>]` — flows cited by the narrative |

A result also sets `question:` (required) and a deliverable sets
`questions:` and `results:`, per the delegation model. See the
[notio result template](../../../../../../.projio/notio/templates/result.md)
and [deliverables spec](../../../../../../docs/specs/deliverables.md).

### Backlinks on the flow page

When `docs_collect` runs, a (planned) `ResultsLinkCollector` and
`DeliverablesLinkCollector` will scan notio and `docs/deliverables/` for
notes whose frontmatter references this flow and render backlink sections
in the flow's collected `index.md`:

```markdown
## Results

- [TTL contamination is sub-01 only](../../log/result/result-arash-20260410-132442-873270.md)
  — 2026-04-10 — q-ieeg-artifact-characterization
- [Template subtraction distortion analysis](../../log/result/...)
  — 2026-04-09 — q-ieeg-artifact-characterization

## Deliverables

- [Preprocessing methods writeup](../../deliverables/reports/preprocessing-methods.md)
```

Until the collectors land, flow authors can hand-maintain a
`## Related work` section in `docs/index.md`. The convention above is
what the collectors will honor once implemented — tag your results with
`source_flow:` now and the backlinks will appear automatically later.

### What does **not** belong on a flow page

- Embedded result plots or QC figures (live in result notes and the
  snakemake `report.html`)
- Hypothesis state or milestone progress (lives in questio)
- Narrative writeups of findings (live in `docs/deliverables/`)
- Prior art citations (live in questio + biblio)

A supervisor visiting the flow page should get a health check and a
crumb trail to the science, not the science itself.

## See also

- [overview.md](overview.md) — pipeio's broader architecture and where this
  spec fits.
- [registry.md](registry.md) — how flows are discovered and registered.
- [notebook.md](notebook.md) — notebook lifecycle and `.build/notebooks/`.
- [cli.md](cli.md) — `pipeio flow new`, `pipeio docs collect`.
- [mcp-tools.md](mcp-tools.md) — `pipeio_flow_new`, `pipeio_flow_audit`,
  `pipeio_docs_collect`, `pipeio_dag_export` tool reference.
- [delegation-model.md](../../../../../../docs/explanation/delegation-model.md)
  — the engineering-vs-science boundary and linking-direction rule.
