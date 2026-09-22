# Partial PipelineRun Retry Using a New PipelineRun per Retry

Date: 2026-09-10

## Status

Proposed 

## Context

Tekton supports Task-level retries (`Task.spec.retries`) and full
`PipelineRun` re-submission, but not retrying only the failed part of a
completed `PipelineRun`. A late failure therefore reruns successful, expensive Tasks and may repeat external side
effects.

For example:

```text
        A ✓ ──► B ✓ ──► C ✗ ──► E
          \
           └──► D ✓
```

A failed-only retry should reuse `A`, `B`, and `D`, rerun `C`, and then run
`E`. 

### Why This Matters

Large pipelines with expensive Tasks (builds, scans, deployments) waste significant time and resources on full re-runs when only a small portion failed.
Other modern CI/CD systems provide "retry failed only" functionality. 

### Constraints

- **Kubernetes immutability:** Status fields should represent observed state, not execution intent
- **Deterministic naming:** TaskRun names follow `<pipelinerun-name>-<task-name>` pattern
- **Ephemeral state:** TaskRuns are pruned from etcd after completion
- **Workspace lifecycle:** PVCs may be deleted when PipelineRuns fail
- **Definition consistency:** Retries must use the same Task/Pipeline definitions as the original (not resolve new versions)

### Key Design Tensions

Two fundamental approaches exist:

1. **In-place mutation:** Edit the existing PipelineRun to reset failed tasks
2. **New object creation:** Create a new PipelineRun linked to the original

The choice affects TaskRun naming, audit trails, controller complexity, and how retry semantics are surfaced to users and UIs.

## Decision

**Partial PipelineRun retry will be implemented by creating a new `PipelineRun` object for each retry attempt, linked to the original via metadata annotations.**

The original PipelineRun remains unchanged as an immutable audit record. The retry PipelineRun receives a new UID and owns new TaskRuns. Lineage is preserved through annotations (e.g., `tekton.dev/retryOf: <original-uid>`).

This architectural decision establishes the foundation for retry semantics. Implementation details—including which Tasks re-run, how Results are forwarded, how workspaces are handled, whether planning occurs in the controller or clients, and how state is injected—are deferred to the Tekton Enhancement Proposal (TEP) process.

## Consequences

### Benefits

- **Preserves immutability:** Original PipelineRun stays terminal; no rewriting of status, timestamps, or audit history
- **Avoids naming conflicts:** New PipelineRun generates new TaskRun names automatically
- **Simpler reconciler semantics:** Retry is "create new object," not "mutate existing object"
- **Clear lineage:** Users and UIs can track the relationship between original and retry via annotations
- **Aligns with Kubernetes patterns:** Declarative model (apply new spec) rather than imperative mutation

### Drawbacks

- **Multiple objects per retry chain:** Original + retry + retry-of-retry creates several PipelineRuns (not a single mutable run)
- **Requires linkage metadata:** UIs need to understand annotations to group related runs
- **Implementation complexity deferred:** The architectural decision does not solve:
  - Which Tasks to re-run (failed subgraph definition)
  - How to forward Results from succeeded Tasks
  - How to validate workspace/PVC availability
  - Where retry planning logic lives (controller vs. clients)
  - How the new PipelineRun recognizes succeeded Tasks
  - How to pin remote resolver references
  - How to handle nested PipelineRuns, matrices, `finally` tasks, `onError: continue`
- **Does not address pruning:** If original TaskRuns/PipelineRuns are garbage collected, retry may need external storage (e.g., Tekton Results)

### Follow-up Actions

The TEP must define:

1. **Retry scope semantics:** Which Tasks re-run (failed, blocked, dependent, matrix combinations, `finally` tasks)
2. **Result forwarding mechanism:** How succeeded Task Results are made available to the retry
3. **Workspace handling:** Validation rules for PVC availability; behavior when workspaces are unavailable
4. **Planning boundary:** Whether controller/API calculates retry plan, or clients do
5. **State injection:** How the new PipelineRun knows which Tasks to skip
6. **Definition stability:** How to ensure retry uses original Task/Pipeline definitions (not new resolves)
7. **Safety guardrails:** Warnings for side effects, non-idempotent Tasks
8. **Nested pipeline semantics:** Retry boundaries for Pipeline-in-Pipeline
9. **UI integration:** Dashboard and Console surfaces for retry actions and lineage visualization
10. **CLI surface:** `tkn pipelinerun retry` command design

