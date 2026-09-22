# Partial PipelineRun Retry — Research & Design

This directory contains research, design proposals, and architectural decisions for implementing partial PipelineRun retry functionality in Tekton Pipelines.

**Jira Epic:** [SRVKP-14121](https://redhat.atlassian.net/browse/SRVKP-14121)
**Status:** Active research and evaluation
**Last Updated:** 2026-09-22

---

## Directory Structure

```
partial_retry/
├── README.md                           # This file
├── research/                           # Research & analysis (what exists today)
│   ├── architecture-analysis.md        # Tekton internals & constraints (Parts 1-4)
│   ├── prior-art-analysis.md           # Industry comparison (GitHub, GitLab, Argo, etc.)
│   └── adr-new-object-model.md         # ADR: New PipelineRun per retry
├── design/                             # Design proposals (what we'll build)
│   └── proposal.md                     # Design evaluations (Parts 5-11)
└── [legacy files]                      # Historical versions (can be archived)
```

---

## Document Map

### Phase 1: Understanding Current State 

**1. Architecture Analysis** (`research/architecture-analysis.md`)
- **Purpose:** Document how Tekton works today
- **Contents:**
  - Part 1: Custom Resources (Task, TaskRun, Pipeline, PipelineRun, CustomRun, ResolutionRequest)
  - Part 2: Controllers (TaskRun, PipelineRun, ResolutionRequest reconcilers)
  - Part 3: Scheduling logic (DAG construction, state resolution, execution queue)
  - Part 4: Known implementation constraints (PVC cleanup, naming conflicts, task-level retries, etc.)
- **Jira:** Story 1 - Tekton Architecture & Internal Constraints Analysis

**2. Prior Art Analysis** (`research/prior-art-analysis.md`)
- **Purpose:** Learn from other CI/CD systems
- **Contents:** Detailed analysis of retry mechanisms in GitHub Actions, GitLab CI, CircleCI, Argo Workflows, and Jenkins
- **Jira:** Story 2 - Industry Prior Art Analysis

**3. Architecture Decision Record** (`research/adr-new-object-model.md`)
- **Purpose:** Document the core architectural decision
- **Decision:** Use new PipelineRun object per retry (not in-place mutation)
- **Rationale:** Avoids naming conflicts, preserves immutability, aligns with K8s patterns
- **Jira:** Story 3 - ADR: Define Execution Model

---

### Phase 2-3: Design Proposals

**4. Design Proposal** (`design/proposal.md`)
- **Purpose:** Evaluate implementation approaches and document design decisions
- **Contents:**
  - Part 5: Failed Subgraph Computation Rules
  - Part 6: Result Re-injection (Memoization)
  - Part 7: Retry Planning Boundary (controller vs. client-side)
  - Part 8: Workspace PVC Handling
  - Part 9: Pipeline Definition Stability
  - Part 10: Data Availability Strategies
  - Part 11: API Shape, Security & Provenance
- **Jira:** Stories 3.5-11 (evaluation spikes)

---

## Reading Order

**For new contributors:**
1. Start with `research/adr-new-object-model.md` (5 min read) - understand the high-level decision
2. Read `research/architecture-analysis.md` Part 4 (constraints) - understand the problems
3. Skim `research/prior-art-analysis.md` - see how others solved it
4. Read `design/proposal.md` - see proposed solutions

**For implementers:**
1. Read all of `research/architecture-analysis.md` - deep technical foundation
2. Read `design/proposal.md` completely - all design decisions
3. Reference back to specific sections as needed during implementation

---


## Related Links

- **Jira Epic:** [SRVKP-14121](https://redhat.atlassian.net/browse/SRVKP-14121)
- **Foundational Spike:** [SRVKP-14219](https://redhat.atlassian.net/browse/SRVKP-14219)
- **Upstream Repo:** [tektoncd/pipeline](https://github.com/tektoncd/pipeline)
- **TEP Repository:** [tektoncd/community](https://github.com/tektoncd/community)

---

