# FitCV Pipeline

## Purpose

FitCV turns noisy raw job postings into a smaller set of grounded, reviewable
outputs that an operator can inspect, trust, and act on.

This document is an explainer, not the editable stage contract. Canonical stage
and feature contract sources are maintained in the private governance layer and
not mirrored into this public repository.

## End-to-End Flow

```text
Input jobs
  -> normalize
  -> enrich
  -> rule_filter
  -> shortlist
  -> ranking
  -> cv_analysis
  -> cv_generation
  -> persisted outputs and run artifacts
```

The control plane wraps that flow with run creation, settings snapshots,
artifact persistence, inspection, and lifecycle actions.

## What The Pipeline Is Trying To Do

The easiest way to understand FitCV is as a staged narrowing and grounding
system:

- first, make raw job inputs stable enough for automation
- then, remove obviously weak paths before expensive work
- then, spend deeper analysis and generation effort only where the evidence is
  strong enough
- finally, persist enough artifacts that an operator can inspect what happened

This mental model matters more here than individual stage-level contract
details, because those details are owned elsewhere.

## Who The Pipeline Serves

The pipeline is designed for an operator who needs to:

- process many jobs in one run
- understand why rows passed, failed, or stopped
- trust the difference between weak and strong matches
- generate CV outputs only when the evidence is good enough
- inspect artifacts without reverse-engineering internal state

## The Four-Layer Mental Model

### 1. Understand The Job

The front of the pipeline converts noisy job text into a more stable internal
representation that later stages can reason about consistently.

### 2. Narrow The Candidate Set

The middle of the pipeline keeps expensive downstream work focused on the most
plausible jobs instead of treating late stages as cleanup for weak candidates.

That is the core cost-control idea in FitCV: narrowing should happen in layers,
with more expensive work reserved for better candidates.

### 3. Personalize Safely

The late stages analyze whether a ranked job is genuinely ready for CV work,
then generate and validate outputs under bounded evidence rules.

This is where FitCV tries to be useful without becoming hand-wavy: generation
is downstream of analysis, and accepted outputs are meant to stay grounded to
owned evidence surfaces.

### 4. Operate And Inspect

FitCV is not only a scoring pipeline. It is also an operator workflow with
execution controls, checkpoints, artifact downloads, and run-history surfaces.

That is what turns the pipeline into a debuggable system instead of a black-box
batch job.

## Execution Modes

The same stage order supports two operator-facing run modes:

- `run_all` (`Run All`)
- `manual_staged` (`Stage by Stage`)

This document intentionally avoids restating checkpoint transition rules.
Use contract-oriented docs when exact trigger and continuation semantics matter:

- [pipeline.md](pipeline.md)
- [api.md](api.md)
- [usage.md](usage.md)

## Artifact Model

The pipeline exposes a layered artifact model so readers do not need one file to
serve every purpose.

- compact run ledgers summarize per-job outcomes
- stage-owned artifacts carry deeper stage diagnostics
- heavier debug payloads remain available when operators need them

The design goal is separation of concerns: small summary surfaces for everyday
inspection, deeper artifacts for detailed debugging, and explicit ownership for
where each kind of truth belongs.

For the current public architecture and evidence narrative, use:

- [architecture.md](architecture.md)
- [pipeline.md](pipeline.md)

## Why The Staged Model Matters

The main value of FitCV is not just that it can generate a CV. It is that it
does so through a staged process that:

- narrows expensive work instead of spraying it everywhere
- keeps late-stage work grounded to upstream evidence
- exposes artifacts that help operators understand outcomes
- supports both routine throughput and deliberate inspection

That is the durable story this document should tell. The exact active contract
for any stage, feature, or artifact belongs in the owning source and generated
views linked above.
