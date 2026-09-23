# Partial PipelineRun Retry — Design Proposal

> **Document Type:** Design Proposal & Evaluation 
> **Status:** Work in Progress 
> **Last Updated:** 2026-09-22 
> **Jira Epic:** [SRVKP-14121](https://redhat.atlassian.net/browse/SRVKP-14121)

---

## Purpose

This document proposes specific design decisions and implementation approaches for partial PipelineRun retry functionality in Tekton Pipelines. It builds on the technical foundation established in `research/architecture-analysis.md`.

**Scope:** 
- Failed subgraph computation rules
- Result re-injection (memoization) mechanisms
- Retry planning boundary evaluation (controller vs. client-side)
- Workspace/PVC handling strategies
- Pipeline definition stability approaches
- Data availability strategies (pruning, Tekton Results integration)
- API shape, security, and provenance requirements

**Prerequisites:** 
- Read `research/architecture-analysis.md` first (Parts 1-4)
- Familiarity with `research/adr-new-object-model.md` (decision to use new PipelineRun per retry)

---

## Table of Contents

1. [Part 5 — Design: Failed Subgraph Computation Rules](#part-5--design-failed-subgraph-computation-rules)
2. [Part 6 — Design: Result Re-injection (Memoization)](#part-6--design-result-re-injection-memoization)
3. [Part 7 — Design Evaluation: Retry Planning Boundary](#part-7--design-evaluation-retry-planning-boundary)
4. [Part 8 — Design Evaluation: Workspace PVC Handling](#part-8--design-evaluation-workspace-pvc-handling)
5. [Part 9 — Design Evaluation: Pipeline Definition Stability](#part-9--design-evaluation-pipeline-definition-stability)
6. [Part 10 — Design Evaluation: Data Availability Strategies](#part-10--design-evaluation-data-availability-strategies)
7. [Part 11 — Design Evaluation: API Shape, Security & Provenance](#part-11--design-evaluation-api-shape-security--provenance)

---

## Part 5 — Design: Failed Subgraph Computation Rules

### 5.1 Subgraph Computation Rules

The retry subset is determined by applying the following rules to the original `PipelineRunFacts` to compute which DAG tasks to re-run and which to bypass.

1. **Explicit Failures, Cancellations & Interrupted Tasks (The Roots):** 
   Any DAG task whose run object meets one of the following conditions is added to the re-run set:
   
   **a) Terminal failure:** `Condition{Type: Succeeded, Status: False}` in its status. 
   This includes `Failed`, `Cancelled`, `TaskRunTimeout`, `PipelineRunTimeout`, and infrastructure 
   errors (e.g., `ImagePullBackOff`, `OOMKilled`).
   
   **b) Interrupted mid-flight:** Task does not have `Condition{Type: Succeeded, Status: True}` 
   **and** the `PipelineRun` was stopped (cancelled, timed out, or gracefully stopped) before the 
   task could reach a terminal state.
2. **Skip Cascades (The Victims):** Any DAG task that was skipped due to a downstream cascade from a failure must be re-run.
This includes tasks with `SkippingReason` set to `ParentTasksSkip`, `StoppingSkip`, `MissingResultsSkip`, `PipelineRunTimeoutSkip`, or `GracefullyStoppedSkip`.
3. **Intentional Skips (The Bypasses):** Tasks skipped with `WhenExpressionsSkip` made a deliberate conditional decision and are **excluded** from the re-run set. In the new `PipelineRun`, their `WhenExpressions` will be re-evaluated naturally against the new context.
4. **`onError: continue` Resolution:** 
    *   **The failed task itself:** Always included in the re-run set, regardless of the pipeline's configured tolerance for it.
    *   **Scenario A (Upstream failed, emitted result, downstream succeeded):** Downstream tasks are **excluded** from the re-run set. 
    *Tradeoff:* If the retried upstream task emits a different result value, the downstream task's previously successful output becomes stale and inconsistent. This risk is accepted for v1 to align with CI/CD immutability norms.
    *   **Scenario B (Upstream failed, no result, downstream ValidationFailed):** The downstream task is **included** in the re-run set, as it functionally never ran because it lacked required inputs .
    *   **Scenario C (Upstream failed with onError: continue, downstream has ordering-only dependency via runAfter):** 
    If the downstream task depends on the upstream via `runAfter` but does NOT reference any of its results, 
    and the downstream task **succeeded** in the original run, it is **excluded** from the re-run set.
    *Rationale:* The downstream's execution was valid even though the upstream failed — it only required 
    ordering guarantees, not data outputs. Re-running it would duplicate valid work.
5. **Matrix Granularity (Policy B):** For v1, if any combination within a `matrix` task fails, **all N combinations** are included in the re-run set.
The entire task's `ChildReferences` are removed and re-fanned out by the controller .
6. **Finally Tasks:** Finally tasks are **excluded** from the failed subgraph calculation. Because the retry is a new `PipelineRun` object, all finally tasks execute fresh automatically after the retry's DAG phase completes.
This ensures cleanup and notification logic accurately reflects the retry's outcome, rather than preserving the original run's state. 
7. **Custom Tasks (`CustomRun`):** Apply the same terminal state rules to `CustomRun.Status.Conditions` as built-in tasks . For v1, treat the `CustomRun` as an atomic unit;
the reconciler will not recurse into nested external controllers to retry partial custom state.
8. **Nested PipelineRuns:** Apply the same terminal state rules to the child `PipelineRun.Status.Conditions`. For v1, the child `PipelineRun` is treated as an atomic unit. If a child pipeline fails, the entire nested pipeline is re-run;
the partial retry logic does not recursively traverse down into the nested DAG.
9. **Task-Level Retries (Fresh Retry Budget):** Each retry `PipelineRun` is a new execution context. 
   Tasks with `taskSpec.retries` or `taskRef.retries` get their **full retry budget reset** in the new `PipelineRun`.
   

### 5.2 Example: Failed Subgraph Computation

**Original Pipeline Execution:**

A ✓ ──────► B ✗ ──────► D ⊘ (ParentTasksSkip)
│           (onError:    │
│            continue)   │
└─────► C ✓ ────────► E ✓ ────► F ⊘ (ParentTasksSkip)
                       │
                       $(tasks.B.results.data)
                
**Task States:**
- **A** : ✓ Succeeded
- **B** : ✗ Failed (onError: continue)
- **C** : ✓ Succeeded
- **D** : ⊘ Skipped (ParentTasksSkip - runAfter B)
- **E** : ✓ Succeeded (used B's result despite failure)
- **F** : ⊘ Skipped (ParentTasksSkip - depends on D)
- **cleanup**: ✓ Succeeded (finally)
- **notify**: ✓ Succeeded (finally)

**Failed Subgraph Analysis:**

| Task | Reason | Re-run? |
|------|--------|---------|
| **A** | Succeeded, no downstream failures | ✗ **Bypass** (reuse results) |
| **B** | Explicit failure (Rule 1) | ✓ **Re-run** |
| **C** | Succeeded, independent branch | ✗ **Bypass** (reuse results) |
| **D** | ParentTasksSkip cascade (Rule 2) | ✓ **Re-run** |
| **E** | Succeeded, referenced B's result (Rule 4, Scenario A) | ✗ **Bypass** (accept staleness risk) |
| **F** | ParentTasksSkip cascade (Rule 2) | ✓ **Re-run** |
| **cleanup** | Finally task (Rule 6) | ✓ **Re-run fresh** (automatic) |
| **notify** | Finally task (Rule 6) | ✓ **Re-run fresh** (automatic) |

**Retry Execution:**

[A reused] ──► B ⟳ ──────► D ⟳
│                          │
│                          │
└─────► [C reused] ──► [E reused] ──► F ⟳
                           │
                           (uses stale B result)
                    

**Key Observations:**
- **Failed subgraph:** B, D, F (3 tasks)
- **Reused:** A, C, E (3 tasks)
- **Finally tasks:** Always run fresh, not part of subgraph calculation
- **E uses stale data:** Acceptable tradeoff for v1 (Rule 4, Scenario A)

### 5.3 Edge Cases & Validation

**Edge Case 1: All DAG tasks failed**
- **Subgraph:** Entire DAG
- **Behavior:** Equivalent to a full pipeline retry, but preserves lineage metadata and retry count
- **Action:** Accept the retry request

**Edge Case 2: Only finally tasks failed**
- **Subgraph:** Empty (no DAG tasks to retry)
- **Behavior:** Finally tasks run after DAG completion; retrying them in isolation is ambiguous
- **Action:** For v1, **reject** the retry request with error: "Cannot retry: only finally tasks failed. 
  Finally-only retry requires manual intervention or full pipeline rerun."
- **Future Work:** Phase 2 could support finally-only retry with explicit user confirmation

**Edge Case 3: Mixed error policies (stopAndFail + continue)**
- **Subgraph:** Compute independently per branch
- **Behavior:** A `stopAndFail` task failure creates `StoppingSkip` cascades (always re-run). 
  An `onError: continue` task failure in a different branch follows Rule 4 scenarios.
- **Action:** Apply rules 1-4 consistently regardless of other branches' error policies

**Edge Case 4: WhenExpressions re-evaluation changes outcome**
- **Original:** Task skipped with `WhenExpressionsSkip`
- **Retry:** When re-evaluates to `true` (e.g., param changed, time-based condition)
- **Behavior:** Task was intentionally skipped before (Rule 3), but condition now passes
- **Action:** Task **will execute** in retry. This is correct behavior — When conditions are dynamic.
- **User Warning:** CLI/UI should warn if re-evaluation may change skip decisions

**Edge Case 5: Manual TaskRun deletion**
- **Original:** User manually deleted a succeeded TaskRun before retry
- **Subgraph:** Would exclude it (Rule: succeeded tasks bypassed)
- **Behavior:** Result data unavailable for downstream tasks
- **Action:** Pre-flight validation detects missing TaskRun, rejects retry with error: 
  "Cannot retry: TaskRun for bypassed task 'X' was deleted. Required results unavailable."

**Edge Case 6: Nested pipeline partial failure**
- **Original:** Child PipelineRun has internal partial failure
- **Subgraph:** Treats child PipelineRun as atomic (Rule 8)
- **Behavior:** Entire child pipeline re-runs; does not compute subgraph within nested DAG
- **Action:** For v1, accept this limitation. Warn user that nested pipelines retry fully.
- **Future Work:** Recursive subgraph computation for nested pipelines

**Edge Case 7: Task exhausted retries in original run**
- **Original:** Task with `retries: 3` failed after 4 total attempts (initial + 3 retries)
- **Retry:** Task is in the failed subgraph (Rule 1a)
- **Behavior:** New `PipelineRun` gives the task a **fresh** `retries: 3` budget (Rule 9)
- **Action:** Task can attempt up to 4 more executions in the retry run
- **User Warning:** CLI/UI should show: "Task 'X' previously exhausted 3 retries. 
  Retry will grant fresh retry budget."

**Edge Case 8: Pipeline cancelled mid-execution**
- **Original:** Pipeline cancelled while tasks B, C were still running (`Status: Unknown`)
- **Subgraph:** B and C added to rerun set (Rule 1b - not explicitly True)
- **Behavior:** All interrupted tasks retry from scratch
- **Action:** Accept retry request. Tasks with partial side effects (e.g., wrote half a file to PVC) 
  may cause issues if not idempotent.
- **User Warning:** "Pipeline was cancelled mid-execution. Some tasks may have partial side effects."


## Part 6 — Design: Result Re-injection (Memoization)

Because the retry operation generates a new `PipelineRun` object, the stateless reconciler will not natively see the `TaskRun` objects from the original execution in its `ChildReferences`. To prevent downstream tasks from failing to resolve their `$(tasks.X.results.Y)` parameter expressions, the system must explicitly inject the preserved results into the new run.

### 6.1 Synthetic Result Injection Mechanism

The result re-injection process happens exactly once, at the moment the new retry `PipelineRun` object is generated. For v1, this extraction and generation is handled client-side (e.g., by the `tkn` CLI or the Tekton Dashboard) to keep controller logic simple. 

When a user executes `tkn pipelinerun retry`, the client performs the following:

1.  **Extraction:** During the Failed Subgraph computation, the client queries the original `PipelineRun`'s runtime state for every task identified as a **bypassed success** (tasks that ran and succeeded in the original execution).
2.  **Harvesting (Priority Order):** The client extracts the required result key-value pairs using a strict priority order to maximize resilience against garbage collection:
    *   *Priority 1:* Check `PipelineRun.Status.Results[]` for bubbled-up results. (These survive `TaskRun` pruning).
    *   *Priority 2:* If not bubbled up, check `TaskRun.Status.Results[]` on the original `TaskRun` object.
    *   *Priority 3:* If the `TaskRun` is pruned and the result was not bubbled up, the extraction fails and the retry request is rejected.
3.  **Injection (Memoization):** The harvested results are injected into the new `PipelineRun` object.

*Implementation Note:* This design explores **Option A (new spec field)** for state injection. The retry `PipelineRun.Spec` includes a new field, `Spec.RetriedTaskResults`, which acts as a lookup table for memoized results. Alternative approaches (Option B: pre-created TaskRuns, Option C: retry-specific reconciler) are discussed in the TEP ADR but not detailed here.

```yaml
spec:
  # ... original pipeline spec ...
  retriedTaskResults:
    - pipelineTaskName: clone
      results:
        - name: commit-sha
          value: "a1b2c3d4"
          
```
### 6.2 Modifying `ResolveResultRef`

The existing `ResolveResultRef()` function in `pkg/reconciler/pipelinerun/resources/resultrefresolution.go` must be updated to support a three-tier lookup:

1. **Primary Lookup (Live state):** Check if the target task actually executed in this retry (i.e., it has a live `TaskRun.Status.Results`). This prevents stale memoized results from overriding fresh executions.
2. **Secondary Lookup (Memoized state):** Check `PipelineRun.Spec.RetriedTaskResults` for pre-injected results carried over from the original run.
3. **Tertiary Lookup (Tekton Results API - Phase 2):** If Tekton Results is enabled and the result is missing from tiers 1 and 2, query the external Results API for the original TaskRun Record.

If all three lookups fail, the function returns a missing result error, which cascades to a `ValidationFailedTask` status.

### 6.3 Known Limitations: Data Availability

Phase 1 (Kubernetes-only) relies on original `PipelineRun` and `TaskRun` objects still existing in etcd. The extraction phase will fail if:

1. The original `PipelineRun` has been completely pruned from the cluster.
2. A succeeded `TaskRun` was pruned AND its result was not bubbled up to `PipelineRun.Status.Results[]`.
3. A `TaskRun` was manually deleted by an administrator.

In these scenarios, the client will reject the retry request with a clear error:

> "Cannot perform partial retry: Required result 'X.Y' is unavailable. The TaskRun was pruned and the result was not bubbled up. To enable retry after GC: 1. Bubble up results to the Pipeline level (declare Pipeline.spec.results), or 2. Install Tekton Results (Phase 2 feature)."

---

## Part 7 — Design Evaluation: Retry Planning Boundary

### 7.1 The Core Question

When a user requests a retry, **who calculates which tasks to re-run**? Two fundamental approaches exist:

**Option A — Controller/API calculates (backend-native):**
- User calls API endpoint: `POST /apis/tekton.dev/v1/namespaces/default/pipelineruns/pr-123/retry`
- Controller reads original PipelineRun/TaskRuns, computes failed subgraph, creates new PipelineRun with injected state
- Clients (tkn, Dashboard) are thin wrappers around the API call

**Option B — Client calculates (client-side orchestration):**
- User runs `tkn pipelinerun retry pr-123`
- CLI reads K8s objects, computes failed subgraph, extracts results, generates new PipelineRun YAML
- CLI applies the YAML to cluster
- Controller reconciles it normally (no special "retry mode")

### 7.2 Option A: Controller/API Calculates

**Implementation approach:**
- New API endpoint or webhook: `/retry` subresource on PipelineRun
- Controller grows a retry path in `ReconcileKind()` or a separate retry controller
- Retry-specific admission validation

**Benefits:**
- ✅ **Centralized logic:** All clients get identical retry behavior
- ✅ **Consistent** across tkn, Dashboard, Console - no risk of divergent implementations
- ✅ **Simpler client code:** Just make API call, don't implement complex graph traversal
- ✅ **Server-side validation:** Controller validates PVC availability, result availability before creating retry
- ✅ **Easier to update:** Fix a bug once in controller, all clients benefit immediately

**Drawbacks:**
- ❌ **Controller complexity:** Adds new code path to already complex PipelineRun reconciler
- ❌ **New API surface:** Requires designing/versioning a retry API (breaking change risk)
- ❌ **Testing overhead:** Must test controller retry path in isolation from normal reconcile
- ❌ **Tight coupling:** Retry logic embedded in controller lifecycle (harder to maintain separately)
- ❌ **Does not align with K8s patterns:** Most K8s operations are declarative (apply YAML), not imperative (call endpoint)

### 7.3 Option B: Client Calculates

**Implementation approach:**
- Shared library `pkg/retry` in tektoncd/pipeline repo
- Exported functions: `ComputeFailedSubgraph()`, `ExtractResults()`, `GenerateRetryPipelineRun()`
- tkn, Dashboard, Console import the library
- Controller unchanged (no retry-specific logic)

**Benefits:**
- ✅ **Simpler controller:** No new code paths, just normal reconcile
- ✅ **Declarative model:** Retry is "apply new PipelineRun YAML" (fits K8s paradigm)
- ✅ **Separation of concerns:** Retry planning separate from runtime execution
- ✅ **Easier to prototype:** Can implement `tkn retry` without touching controller
- ✅ **Lower risk:** Controller doesn't change, existing behavior unaffected
- ✅ **Matches prior art:** Argo Workflows uses client-side (`argo retry` CLI computes, controller reconciles)

**Drawbacks:**
- ❌ **Duplication risk:** Each client could reimplement logic independently (if library not used)
- ❌ **Shared library required:** Must maintain `pkg/retry` with stable API
- ❌ **Version skew:** Client library version might not match controller version
- ❌ **Client must understand Tekton internals:** Graph traversal, result propagation, skip reasons

### 7.4 Prior Art Comparison

| System | K8s-Native? | Retry Orchestration | Notes |
|--------|-------------|---------------------|-------|
| **Argo Workflows** | ✅ Yes | **Client-side** | `argo retry` CLI computes memoized state, generates new Workflow YAML, applies it |
| **GitHub Actions** | ❌ No (SaaS) | Server-side | API endpoint handles retry planning |
| **GitLab CI** | ❌ No (SaaS) | Server-side | Backend recalculates pipeline state |
| **CircleCI** | ❌ No (SaaS) | Server-side | API-driven retry |
| **Jenkins** | ❌ No (pre-K8s) | Server-side | Master (controller) handles restart-from-stage |

**Key insight:** The only Kubernetes-native system (Argo) uses **client-side orchestration**. SaaS systems use server-side, but they're not constrained by CRD immutability or K8s declarative patterns.

### 7.5 Recommendation

**For v1: Controller/API calculates (Option A)** with retry endpoint.

**Rationale:**
1. **Avoids repeated calculations:** Failed subgraph computation, result extraction, and validation happen once on the server rather than by every client that needs to display or execute a retry
2. **Consistent behavior:** All clients get identical retry plans without risk of version skew between client library and controller
3. **Better for multi-client scenarios:** Dashboard preview, CLI dry-run, Console UI all query the same server-calculated plan
4. **Simpler client code:** Clients just call API endpoint, don't need to understand graph traversal or skip cascade rules

**Trade-offs accepted:**
- Controller grows more complex (new retry calculation path)
- Requires designing/versioning retry API surface
- Deviates slightly from pure declarative K8s pattern (but Kubernetes itself has imperative operations like `scale`, `rollout restart`)

---

## Part 8 — Design Evaluation: Workspace PVC Handling

### 8.1 PVC Lifecycle Analysis (Expanded from Part 4.1)

#### AffinityAssistant Mode Comparison

| Mode | PVC Ownership | Deletion Trigger | Retry Impact |
|------|---------------|------------------|--------------|
| `AffinityAssistantPerPipelineRun` | Owned by StatefulSet VolumeClaimTemplate | PipelineRun failure (immediate) | **Blocks retry** - data lost |
| `AffinityAssistantPerWorkspace` | Owned by Tekton directly | Only if `tekton.dev/auto-cleanup-pvc=true` | **Allows retry** (default config) |
| `AffinityAssistantDisabled` | Owned by Tekton directly | Never auto-deleted | **Always allows retry** |
| User-provided PVC | External to Tekton | Never deleted by Tekton | **Always allows retry** |

### 8.2 Validation Approaches

**Option A — Pre-flight validation (reject early):**
- At retry-request time, check PVC availability
- Reject immediately with clear error if PVC missing
- **Pro:** User gets instant feedback
- **Con:** Requires client/controller to query PVC status

**Option B — Runtime validation (fail during reconcile):**
- Retry PipelineRun created without PVC validation
- Fails during TaskRun pod creation when volume mount fails
- **Pro:** No special validation logic
- **Con:** Poor UX - error happens deep in execution

**Option C — Best-effort (allow with warning):**
- Always allow retry, warn about staleness risk
- Let TaskRun fail naturally if PVC missing
- **Pro:** Maximum flexibility
- **Con:** Confusing for users (why did retry fail?)

**Recommended:** **Option A** - pre-flight validation with mode-specific rejection.

### 8.3 Workspace Staleness Detection

**Question:** Can we detect if workspace contents are stale?

**Example scenario:**
```
Original run:
  clone (writes to /workspace/source, commit abc123) → build → test FAILS

Retry run:
  test re-runs → uses OLD /workspace/source contents (still has abc123)
  
But: maybe user pushed commit def456 since original run failed
     → test runs against wrong code!
```

**Evaluation of detection approaches:**

**Option A — Static validation (check task dependencies):**
- Validate that workspace-producing task (clone) is in preserve set
- **Pro:** Can detect at retry-request time
- **Con:** Cannot detect if workspace contents actually stale (user may have pushed new commit)

**Option B — Content hashing:**
- Store hash of workspace contents in TaskRun status
- Validate hash at retry time
- **Pro:** Accurate staleness detection
- **Con:** Expensive to compute, doesn't work for large workspaces

**Option C — No validation (document risk):**
- Accept that staleness cannot be validated statically
- Document as known limitation with mitigation guidance
- **Pro:** Simple, no implementation cost
- **Con:** User must understand the risk

**Recommended:** **Option C** for v1 - document staleness risk clearly.

### 8.4 Side Effect Warnings

**Required warning before retry execution:**

```
⚠️  Retry will re-execute failed tasks. If tasks have side effects,
    they will repeat:
    
    - Deployments may create duplicate resources
    - Notifications may send duplicate messages  
    - Git operations may increment API rate limits
    - External API calls may not be idempotent
    
    Continue? [y/N]
```

**Where to show:**
- **CLI:** Interactive prompt (can be bypassed with `--yes` flag)
- **Dashboard/Console:** Modal dialog with checkbox confirmation

**Why required:** Tekton cannot determine task idempotency automatically. Warning makes user responsible for side effect management (industry standard approach).

---

## Part 9 — Design Evaluation: Pipeline Definition Stability

### 9.1 The Resolution Drift Problem

**Scenario:**
```
Day 1: User creates PipelineRun referencing remote Task:
  taskRef:
    resolver: git
    params:
      - name: url
        value: https://github.com/org/tasks
      - name: revision  
        value: main
      - name: pathInRepo
        value: task.yaml

  → Resolves to commit abc123

Day 2: Pipeline fails, user pushes fix to main branch
  → main is now at commit def456

Day 3: User retries pipeline
  → Question: Which commit should be used?
```

**Industry standard:** Use **same commit as original run** (abc123, not def456).

### 9.2 Extraction Strategy Evaluation

**Option A — Extract from inlined specs:**

When a Task/Pipeline is resolved remotely, the reconciler stores the full resolved spec in status:
- `TaskRun.Status.TaskSpec` (resolved Task)
- `PipelineRun.Status.PipelineSpec` (resolved Pipeline)

**Extraction:** Read these inlined specs from original run, populate retry run with them.

**Benefits:**
- ✅ Always available (stored in status)
- ✅ Exact copy of what was executed
- ✅ No need to query ResolutionRequest

**Drawbacks:**
- ❌ Large status size (full YAML embedded)
- ❌ Not all resources get inlined (implementation-dependent)
- ❌ Doesn't capture original remote reference metadata (provenance loss)

**Option B — Store ResolutionRequest content hash:**

Store hash of `ResolutionRequest.Status.Data` in PipelineRun/TaskRun annotations at creation time:
```yaml
metadata:
  annotations:
    tekton.dev/resolved-content-sha256: "abc123..."
    tekton.dev/resolver-ref: "git:org/tasks:main:task.yaml"
```

**Extraction:** At retry time, validate that re-resolving produces same hash.

**Benefits:**
- ✅ Small footprint (just hash + ref metadata)
- ✅ Preserves provenance information
- ✅ Can detect drift (resolution failure if hash doesn't match)

**Drawbacks:**
- ❌ Requires re-resolution (slower)
- ❌ Resolution might fail if remote source unavailable
- ❌ Doesn't work for cluster-local Tasks (no ResolutionRequest)

**Option C — Inline resolved specs in retry PipelineRun:**

For retry, use `pipelineSpec` and `taskSpec` (inline) instead of remote refs:
```yaml
spec:
  pipelineSpec:  # Full resolved Pipeline spec copied from original
    tasks:
      - name: build
        taskSpec:  # Full resolved Task spec copied from original
          steps: [...]
```

**Benefits:**
- ✅ Guarantees exact same definitions
- ✅ No re-resolution needed
- ✅ Works even if remote source deleted

**Drawbacks:**
- ❌ Very large PipelineRun spec
- ❌ Loses remote reference information (provenance)
- ❌ May trigger different validation path than ref-based runs

### 9.3 Recommended Approach

**Hybrid strategy:**
1. **Primary:** Store inlined specs in retry PipelineRun (Option C)
2. **Metadata:** Add annotations with original remote refs for provenance (Option B)

This ensures definition stability while preserving provenance information for audit/security tools.

---

## Part 10 — Design Evaluation: Data Availability Strategies

### 10.1 The Pruning Timeline (Expanded from Part 6.3)

**Kubernetes garbage collection phases:**

```
t=0     PipelineRun completes (Succeeded or Failed)
t+1h    TaskRun pods deleted (completed pods pruned quickly)
t+24h   TaskRuns pruned from etcd (default TTL-after-finished)
        └─ TaskRun.Status.Results[] lost if not bubbled up
t+7d    PipelineRuns pruned from etcd
        └─ PipelineRun.Status.Results[] lost
t+90d   Tekton Results records expire (configurable)
```

**Time window for retry:**
- **Phase 1 (K8s-only):** Hours to ~1 day (until TaskRuns pruned)
- **Phase 2 (with Results):** Weeks to months (until Results records expire)

### 10.2 Phase 1 Validation Approaches

**Option A — Pre-flight validation (fail fast):**
```go
// At retry-request time
validateRetryAvailability(originalPR) error {
    // Check 1: PipelineRun still exists
    if originalPR not found → return "PipelineRun pruned, cannot retry"
    
    // Check 2: Required TaskRuns exist
    for each bypassed task:
        tr := getTaskRun(task)
        if tr == nil:
            if result bubbled up → OK
            else → return "TaskRun pruned, result not bubbled up"
    
    return nil
}
```

**Benefits:**
- ✅ User gets instant feedback
- ✅ Clear error message with actionable guidance
- ✅ No wasted work (don't create retry if it will fail)

**Drawbacks:**
- ❌ Requires checking each TaskRun (N API calls for N bypassed tasks)
- ❌ Race condition: TaskRun could be pruned between validation and retry creation

**Option B — Best-effort (fail during reconcile):**
- Allow retry creation without validation
- Fail during ResolveResultRef() when result unavailable
- ValidationFailedTask → PipelineRun fails

**Benefits:**
- ✅ Simple - no validation logic
- ✅ Reuses existing result resolution code

**Drawbacks:**
- ❌ Poor UX - failure happens during execution, not immediately
- ❌ Wasted reconcile loops before failure detected

**Recommended:** **Option A** - pre-flight validation with clear error messages.

### 10.3 Phase 2 Integration Approaches

**Option A — Tertiary lookup in ResolveResultRef (controller-side):**
```go
ResolveResultRef(pipelineRunState, resultRef) (string, error) {
    // Tier 1: Live state (task ran in this retry)
    if task in pipelineRunState && task.TaskRuns[0] != nil {
        return task.TaskRuns[0].Status.Results[resultName]
    }
    
    // Tier 2: Memoized state (Spec.retriedTaskResults)
    if pr.Spec.retriedTaskResults[taskName][resultName] {
        return pr.Spec.retriedTaskResults[taskName][resultName]
    }
    
    // Tier 3: Tekton Results API (Phase 2)
    if resultsEnabled {
        return queryResultsAPI(originalPR, taskName, resultName)
    }
    
    return "", ErrResultNotFound
}
```

**Benefits:**
- ✅ Transparent - works during normal reconcile
- ✅ Handles all cases (live, memoized, archived)
- ✅ Graceful fallback chain

**Drawbacks:**
- ❌ Controller must query external API (Results)
- ❌ Latency - Results API call during reconcile
- ❌ Requires feature flag and configuration

**Option B — Client-side fallback (during extraction):**
- Client queries Results API when extracting results from pruned TaskRuns
- Populates Spec.retriedTaskResults with archived data
- Controller never queries Results

**Benefits:**
- ✅ Controller unchanged
- ✅ Results query happens once (at extraction time, not every reconcile)

**Drawbacks:**
- ❌ Client needs Results API access
- ❌ Doesn't help with live result resolution during retry execution

**Recommended:** **Option A** - tertiary lookup in controller with feature flag.

### 10.4 Error Message Design

**For each failure scenario:**

| Scenario | Error Message |
|----------|---------------|
| **PipelineRun pruned** | `Cannot perform partial retry: Original PipelineRun 'pr-123' no longer exists in cluster. Full re-run required.` |
| **TaskRun pruned, result not bubbled** | `Cannot perform partial retry: Result 'clone.commit-sha' unavailable. TaskRun was pruned and result not bubbled up.\n\nTo enable retry after GC:\n  1. Bubble up results to Pipeline level (declare Pipeline.spec.results), OR\n  2. Install Tekton Results (Phase 2 feature)` |
| **TaskRun manually deleted** | `Cannot perform partial retry: TaskRun 'pr-123-clone' was deleted. Result data lost.\n\nPartial retry after manual TaskRun deletion is not supported. Full re-run required.` |
| **Results unavailable (Phase 2)** | `Cannot perform partial retry: Tekton Results API unavailable or record expired.\n\nFull re-run required.` |

---

## Part 11 — Design Evaluation: API Shape, Security & Provenance

### 11.1 API Shape (Dependent on Part 7 Decision)

#### If Client-Side Orchestration (Option B from Part 7):

**New PipelineRun fields:**

```yaml
apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  name: pr-123-retry-1
  annotations:
    tekton.dev/retryOf: "original-pr-uid-abc"  # Links to original
    tekton.dev/retryAttempt: "1"                # Retry sequence number
spec:
  pipelineRef:
    name: my-pipeline
  # Option A (from Part 6): New field for memoized results
  retriedTaskResults:
    - pipelineTaskName: clone
      results:
        - name: commit-sha
          value: "abc123def456"
    - pipelineTaskName: build
      results:
        - name: image-digest
          value: "sha256:789..."
  params: [...]
  workspaces: [...]
```

**CLI command:**
```bash
tkn pipelinerun retry <name> [--failed-only]

# What it does:
# 1. Read original PR + TaskRuns
# 2. Compute failed subgraph (Part 5 rules)
# 3. Extract results (Part 6 harvesting)
# 4. Validate PVC availability (Part 8)
# 5. Generate new PipelineRun YAML
# 6. kubectl apply
```

#### If Controller-Side Orchestration (Option A from Part 7):

**New API endpoint:**
```
POST /apis/tekton.dev/v1/namespaces/{ns}/pipelineruns/{name}/retry
{
  "failedOnly": true,
  "dryRun": false  // Optional: preview what would be retried
}

Response:
{
  "retryPipelineRun": "pr-123-retry-1",
  "rerunTasks": ["test", "scan", "deploy"],
  "preservedTasks": ["clone", "build"],
  "warnings": ["Task 'deploy' has side effects - may create duplicate resources"]
}
```

### 11.2 Security & RBAC Analysis

#### Cross-Run Access Control

**Question:** Should caller need read access to original PipelineRun to retry it?

**Answer:** **Yes** - retry is effectively "read original state + create new run"

**RBAC requirements:**
```yaml
# Minimum permissions for retry
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
rules:
  # Read original PipelineRun + TaskRuns
  - apiGroups: ["tekton.dev"]
    resources: ["pipelineruns", "taskruns"]
    verbs: ["get", "list"]
  
  # Create retry PipelineRun
  - apiGroups: ["tekton.dev"]
    resources: ["pipelineruns"]
    verbs: ["create"]
```

#### Cross-Namespace Retries

**Question:** Should retries across namespaces be allowed?

**Scenario:**
```
Namespace A: pr-production (failed)
Namespace B: pr-production-retry (retry attempt by different team)
```

**Answer:** **No** - explicitly block cross-namespace retries.

**Rationale:**
1. Security boundary - namespace isolation is fundamental to K8s
2. Service accounts don't cross namespaces
3. PVC/workspace access would fail
4. Result forwarding across namespaces = unauthorized data access

**Validation:** Retry PipelineRun must be created in same namespace as original.

#### Result Access Authorization

**Question:** Can retry bypass result access controls?

**Attack scenario:**
```
User A: Cannot read original PipelineRun 'pr-secret' (contains secret results)
User A: Creates retry of 'pr-secret' → gets results via Spec.retriedTaskResults?
```

**Mitigation:**
- Admission webhook validates: `CREATE retry-pr` → requires `GET original-pr` permission
- If user lacks read access to original → retry creation rejected

### 11.3 SLSA Provenance Requirements

**Core requirement:** Provenance must distinguish **inherited** work from **newly executed** work.

#### Provenance Contract for Chains

**For bypassed (reused) tasks:**
```json
{
  "taskName": "clone",
  "status": "succeeded",
  "inheritedFrom": {
    "pipelineRun": "pr-123",
    "pipelineRunUID": "abc-def-123",
    "taskRun": "pr-123-clone",
    "taskRunUID": "xyz-789",
    "originalCompletionTime": "2026-09-01T10:30:00Z"
  },
  "executedInRetry": false,  // Not executed in this run
  "results": {
    "commit-sha": "abc123"  // Mark as inherited
  }
}
```

**For re-run tasks:**
```json
{
  "taskName": "test",
  "status": "succeeded",
  "executedInRetry": true,    // Executed fresh in this run
  "retryOf": {
    "pipelineRun": "pr-123",
    "previousStatus": "failed"
  },
  "results": {
    "test-report": "junit.xml"  // Fresh result
  }
}
```

#### Status Field Design

**To enable Chains to detect retry context:**

```yaml
status:
  # Existing fields
  conditions: [...]
  childReferences: [...]
  
  # New field for retry metadata
  retryMetadata:
    retryOf: "pr-123-uid"
    retryAttempt: 1
    preservedTasks: ["clone", "build"]  # Inherited from original
    rerunTasks: ["test", "scan", "deploy"]  # Fresh execution
```

This allows provenance producers to:
1. Detect this is a retry run
2. Identify which tasks were inherited vs. fresh
3. Link provenance back to original run

### 11.4 Recommended API Design (Final)

**For v1:**
- **Orchestration:** Controller-side (Part 7 Option A)
- **API endpoint:** `POST /apis/tekton.dev/v1/namespaces/{ns}/pipelineruns/{name}/retry`
- **API fields:** `annotations` (lineage) + `spec.retriedTaskResults` (memoized results) in generated PipelineRun
- **CLI:** `tkn pipelinerun retry <name>` → calls API endpoint
- **Security:** Same-namespace only, require read access to original
- **Provenance:** Add `status.retryMetadata` for Chains integration

**For v2 consideration:**
- Cross-namespace retries (if strong use case emerges)
- Enhanced provenance with full task lineage graph
- Retry preview/dry-run mode (calculate plan without creating PipelineRun)

---

## Related Documents

- **Architecture Research:** `research/architecture-analysis.md` (Parts 1-4)
- **Prior Art Analysis:** `research/prior-art-analysis.md`
- **Architecture Decision Record:** `research/adr-new-object-model.md`
