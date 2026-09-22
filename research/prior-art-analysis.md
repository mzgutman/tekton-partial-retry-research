# Prior Art Research: Partial Retry Mechanisms in CI/CD

**5-System Deep Dive: GitHub Actions, GitLab CI, CircleCI, Argo Workflows, and Jenkins**

**Status:** Complete synthesis of retry mechanisms across leading CI/CD engines 
**Purpose:** Foundation for Tekton partial retry design (--failed-only) 
**Date:** 2026

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [System Profiles](#system-profiles)
3. [7-Dimension Comparative Analysis](#7-dimension-comparative-analysis)
4. [Consensus Patterns (What All Systems Agree On)](#consensus-patterns-what-all-systems-agree-on)
5. [Key Divergences (Where and Why Systems Differ)](#key-divergences-where-and-why-systems-differ)
6. [Kubernetes-Native Lessons (Argo + Tekton)](#kubernetes-native-lessons-argo--tekton)
7. [SaaS Lessons (GitHub Actions + GitLab CI + CircleCI)](#saas-lessons-github-actions--gitlab-ci--circleci)
8. [Direct Recommendations for Tekton](#direct-recommendations-for-tekton)

---

## Executive Summary

This document synthesizes the retry/rerun mechanisms of five leading CI/CD engines to inform Tekton's design for partial PipelineRun retry (`--failed-only`).

### Key Finding: Convergence on Core Principles with Architectural Variations

Across all five systems, we observe **convergence on core retry principles**:
-  **Targeted subgraph execution** (only failed tasks + downstream dependents re-run)
-  **Result/artifact reuse** from bypassed successful tasks
-  **No side-effect protection** (user responsibility for idempotency)
-  **Contextual re-evaluation of cleanup/finally tasks**

However, systems diverge significantly on **implementation architecture**:
- **Object model:** In-place mutation (GitHub, GitLab) vs. New objects (CircleCI, Jenkins, Argo resubmit)
- **Storage strategy:** Cloud-backed (SaaS) vs. Kubernetes-native (Argo, Tekton)
- **DAG definition:** Explicit (GitHub, GitLab, CircleCI, Argo) vs. Implicit + explicit (Tekton)

### The Critical Insight for Tekton

**Argo Workflows** is the most instructive prior art because it faces identical Kubernetes constraints as Tekton (ephemeral etcd, pod-based execution, garbage collection). Argo's dual-model support (in-place mutation + new object with memoization) proves both approaches are technically viable in Kubernetes operators, but new object generation with synthetic result injection is the safer long-term strategy.

---

## System Profiles

### GitHub Actions

**Architecture Type:** Stateful SaaS 
**Key Storage:** PostgreSQL database + S3 object storage (90-day TTL) 
**Execution Model:** Jobs → Steps (parallel by default) 
**DAG Definition:** Explicit (`needs: [job_a, job_b]`)

**Retry Mechanism:** "Re-run failed jobs" API endpoint
- Backend queries database for failed jobs
- Parses `needs` arrays to calculate downstream dependents
- Pulls cached artifacts from S3 for bypass jobs
- Injects artifacts into downstream jobs
- **Object Model:** In-place mutation with "Attempts" counter

**Strengths:**
- Seamless UI integration (full visual context preserved)
- 90-day artifact retention is reliable
- Explicit DAG makes subgraph calculation trivial

**Constraints:**
- Centralized architecture not portable to Kubernetes
- SaaS-specific (not open-source)

---

### GitLab CI

**Architecture Type:** Stateful SaaS 
**Key Storage:** PostgreSQL database + S3-compatible object storage 
**Execution Model:** Pipelines → Stages → Jobs (sequential by stage, but can DAG via `needs`) 
**DAG Definition:** Explicit separation of `needs:` (ordering) vs. `dependencies:` (data)

**Retry Mechanism:** "Retry failed pipeline" or per-job retry
- Backend identifies failed jobs and unexecuted downstream jobs
- Spawns new Job instances under same Pipeline ID
- Downloads artifacts via `dependencies:` declarations
- **Object Model:** In-place mutation with Job Versioning (same Pipeline ID, new Job IDs)

**Strengths:**
- Explicit separation of ordering (`needs:`) from data (`dependencies:`) is mathematically clean
- Job versioning preserves audit trail while updating pipeline status
- Configurable artifact expiration (`expire_in` TTL)

**Constraints:**
- Centralized SaaS architecture
- Assumes user understands two separate keywords for explicit and implicit dependencies

---

### CircleCI

**Architecture Type:** Stateful SaaS 
**Key Storage:** Cloud control plane + cloud object storage 
**Execution Model:** Workflows → Jobs → Steps (parallel by default) 
**DAG Definition:** Explicit (`requires: [job_a]`)

**Retry Mechanism:** "Rerun workflow from failed"
- Backend calculates failed subgraph from `requires:` array
- Spawns brand new Workflow Run (new execution ID)
- Automatically re-attaches Workspace archives to retried jobs
- Fresh containers for every job (full isolation)
- **Object Model:** New Object (new Workflow Run ID)

**Strengths:**
- New object model validates client-side generation approach
- Workspace architecture (archive + re-attach) is clean separation of concerns
- Fresh containers ensure complete isolation
- Cloud storage ensures high availability

**Constraints:**
- Centralized SaaS
- Workspaces must be explicitly persisted/attached

---

### Argo Workflows

**Architecture Type:** Kubernetes-native (open-source) 
**Key Storage:** Kubernetes etcd + external S3-compatible artifact repository 
**Execution Model:** Workflows (Templates) → DAG or Steps (pods spawned per step) 
**DAG Definition:** Explicit (`dependencies: [node_A]`)

**Retry Mechanism:** Dual support
- **`argo retry`** (in-place mutation): Patches .status of existing Workflow CRD, clears Failed nodes, controller resumes
- **`argo resubmit --memoized`** (new object): Generates new Workflow CRD with synthetic state injection from archived run

**Strengths:**
- **MOST RELEVANT FOR TEKTON** — shares all K8s constraints
- Dual model validates both retry approaches are viable
- External S3 sidestepping PVC lifecycle issues
- Open-source and community-driven

**Constraints:**
- Requires external S3 for reliable artifact storage
- Garbage collection can still delete CRDs before retry is triggered

---

### Jenkins

**Architecture Type:** Stateful monolithic server 
**Key Storage:** Permanent JENKINS_HOME filesystem (local disk) 
**Execution Model:** Pipelines → Stages → Steps (linear by default, parallel via nested blocks) 
**DAG Definition:** Linear stages (not pure DAG by default)

**Retry Mechanism:** "Restart from Stage"
- User manually selects a stage to restart from
- Jenkins generates new Build ID (e.g., Build #55)
- Restarts chosen stage + all downstream stages
- Restores workspace data via `stash`/`unstash` (requires `preserveStashes()`)
- **Object Model:** New Object (new Build number)

**Strengths:**
- Permanent storage (no garbage collection worries... until disk fills up)
- New Build ID model is long-established practice
- `preserveStashes()` validates explicit storage retention logic

**Constraints:**
- User-driven stage selection (not automatic subgraph calculation)
- Permanent disk fills up even with local storage
- Linear stage model requires executing all downstream stages (less granular)

---

## 7-Dimension Comparative Analysis

### Dimension 1: Subgraph Definition (Which tasks re-run?)

| System | Approach | Mechanism | Granularity |
|---|---|---|---|
| **GitHub Actions** | **Targeted** | Backend parses `needs` arrays; identifies failed jobs + downstream dependents | Fine-grained |
| **GitLab CI** | **Targeted** | Backend identifies failed jobs via status; calculates `needs` closure for dependents | Fine-grained |
| **CircleCI** | **Targeted** | Backend calculates failed subgraph from `requires:` | Fine-grained |
| **Argo Workflows** | **Targeted** | Controller natively tracks node status; skips Succeeded, re-executes Failed + unexecuted | Fine-grained |
| **Jenkins** | **Linear/Downstream Sweep** | User selects stage; Jenkins executes chosen stage + ALL downstream stages (no granularity) | Coarse-grained |
| **Tekton (Proposed)** | **Targeted** | Must dynamically calculate failed subgraph; traverse both explicit (`runAfter`) and implicit (`$(tasks.X.results.Y)`) edges | Fine-grained |

**Consensus:** 4 out of 5 systems use targeted subgraph (only Jenkins is linear). Tekton should align with consensus.

---

### Dimension 2: Result/Artifact Reuse (How is data passed?)

| System | Storage Backend | Reuse Mechanism | Reliability |
|---|---|---|---|
| **GitHub Actions** | S3 (90-day TTL) | `actions/download-artifact` HTTP API | Very High |
| **GitLab CI** | S3-compatible (configurable TTL) | `dependencies:` auto-downloads via HTTP | Very High |
| **CircleCI** | Cloud object storage | Auto-attach Workspace (archive + mount) | Very High |
| **Argo Workflows** | External S3 (configurable) | Auto-push/pull artifacts to/from S3 | Very High |
| **Jenkins** | JENKINS_HOME (permanent) | `stash`/`unstash` with `preserveStashes()` | High (with caveats) |
| **Tekton (Proposed)** | Kubernetes PVCs + Tekton Results | Two-tier: etcd (fast) → Tekton Results (fallback) | Medium (lifecycle issues) |

**Consensus:** All systems push data to external, durable storage or local persistent disk. All require explicit lifecycle management (TTL or `preserveStashes()`).

**Key Issue for Tekton:** PVCs are ephemeral and tied to pod lifecycle. Tekton Results (PostgreSQL) mitigates this, but pre-flight PVC checks are mandatory.

---

### Dimension 3: Ordering vs. Data Dependencies

| System | Ordering Mechanism | Data Dependency Mechanism | Coupling |
|---|---|---|---|
| **GitHub Actions** | `needs: [job_a]` | Implicit in artifact outputs | Coupled (same keyword) |
| **GitLab CI** | `needs:` | `dependencies:` (explicit separation) | Decoupled (two keywords) |
| **CircleCI** | `requires: [job_a]` | Implicit in workspace attachment | Coupled (same keyword) |
| **Argo Workflows** | `dependencies: [node_A]` | Implicit parameter passing | Coupled (same keyword) |
| **Jenkins** | Stage order in file | Explicit `unstash` calls | Decoupled (two mechanisms) |
| **Tekton (Current)** | `runAfter` | `$(tasks.X.results.Y)` strings | Decoupled (two mechanisms) |

**Consensus:** Split between coupled (GHA, CircleCI, Argo) and decoupled (GitLab, Jenkins, Tekton).

**GitLab's advantage:** Explicit separation (`needs:` vs. `dependencies:`) makes DAG traversal mathematically simpler. Tekton's implicit string parsing requires more computation.

**Implication for Tekton:** Current implicit model is viable but computationally expensive. Consider documenting the distinction clearly to users.

---

### Dimension 4: Side Effects (Non-idempotent actions)

| System | Protection Mechanism | Documentation | User Responsibility |
|---|---|---|---|
| **GitHub Actions** | None | Assumes idempotency | High |
| **GitLab CI** | None | Assumes idempotency | High |
| **CircleCI** | None | Assumes idempotency | High |
| **Argo Workflows** | None | **Explicitly warns** "user's responsibility" | High |
| **Jenkins** | None | Assumes idempotency | High |
| **Tekton (Proposed)** | None | Should explicitly document | High |

**Universal Consensus:** Every single system assumes side effects are user-handled. No system attempts automatic detection or mitigation.

**Implication for Tekton:** Document clearly that retried TaskRuns execute from step 1, re-triggering all side effects. Users are responsible for idempotent design.

---

### Dimension 5: Finally/Cleanup Steps

| System | Mechanism | Re-evaluation | Contextual? |
|---|---|---|---|
| **GitHub Actions** | `if: always()` or `if: failure()` | Yes, dynamic against new retry outcome | Yes |
| **GitLab CI** | `when: always` or `after_script` | Yes, dynamic against new retry outcome | Yes |
| **CircleCI** | Cleanup jobs with flexible conditions | Yes, re-evaluated per attempt | Yes |
| **Argo Workflows** | `onExit` handlers | Yes, executes after new attempt | Yes |
| **Jenkins** | `post { always { ... } }` blocks | Yes, re-evaluated per build | Yes |
| **Tekton (Proposed)** | Finally tasks | Must reset `FinallyStartTime` to nil | Yes |

**Universal Consensus:** All systems re-evaluate finally/cleanup logic based on new retry outcome. All reset timers/state.

**Implication for Tekton:** Finally tasks must execute at conclusion of retry. `FinallyStartTime` must be reset to prevent timeout based on original run's clock.

---

### Dimension 6: Object Model (In-Place Mutation vs. New Object)

| System | Model | ID Behavior | Audit Trail | Linkage |
|---|---|---|---|---|
| **GitHub Actions** | **In-Place Mutation** | Same Run ID; Attempt counter (1, 2, 3) | Preserved; all attempts visible | UI groups attempts |
| **GitLab CI** | **In-Place Mutation** | Same Pipeline ID; new Job IDs | Preserved; original job logs kept | UI shows "Retry" badge |
| **CircleCI** | **New Object** | New Workflow Run ID; linked via commit SHA | Preserved; both runs visible | UI links to original |
| **Argo Workflows** | **Dual Support** | `argo retry`: mutate; `argo resubmit`: new CRD | Both preserved | UI links mutation to new |
| **Jenkins** | **New Object** | New Build number (e.g., #55 from #54) | Preserved; both builds visible | UI shows "Restarted from #54" |
| **Tekton (Proposed)** | **New Object** | New PipelineRun UID; annotation `tekton.dev/replaces: <original-UID>` | Preserved | Dashboard links via annotation |

**Split Consensus:** 3 SaaS systems (GHA, GitLab) use in-place mutation; 2 systems (CircleCI, Jenkins) use new objects; Argo supports both.

**Why the divergence?**
- **In-place:** Simpler UX (single graph updates); but breaks Kubernetes immutability
- **New object:** Preserves auditability; requires explicit linkage; aligns with Kubernetes principles

**Recommendation for Tekton:** New object model (CircleCI/Jenkins/Argo-resubmit) aligns with Kubernetes immutability and is safer for long-term maintainability.

---

### Dimension 7: UX & Visualization

| System | Trigger Mechanism | Visualization | Audit Access |
|---|---|---|---|
| **GitHub Actions** | "Re-run failed jobs" button on run page | In-place graph update; green/yellow/red flow | Single UI pane, all attempts visible |
| **GitLab CI** | "Retry" button per job or pipeline | Preserves original; adds "Retry" badge to new attempt | Single UI pane, both visible |
| **CircleCI** | "Rerun Workflow from Failed" button | Grayed-out skipped jobs; links to original run | Dual timeline; links between runs |
| **Argo Workflows** | CLI buttons: "Retry" and "Resubmit" | In-place or new graph; color-coded node status | Dual timeline in Argo UI |
| **Jenkins** | "Restart from Stage" dropdown (user selects stage) | Blue Ocean greying out upstream stages | Build history with "Restarted from" links |
| **Tekton (Proposed)** | CLI: `tkn pipelinerun retry --failed-only <run-id>` | Dashboard must group original + retry via annotation | Links via `tekton.dev/replaces` annotation |

**Universal Consensus:** All systems provide integrated, one-click retry. All preserve audit links to original run.

**Implication for Tekton:** New PipelineRun requires Dashboard integration to group original + retry. Annotation-based linkage allows async UI updates.

---

## Consensus Patterns (What All Systems Agree On)

### Pattern 1: Targeted Subgraph Execution
**Every system except Jenkins** calculates which tasks actually need re-execution (failed + downstream dependents) rather than blindly re-running everything downstream.

**For Tekton:** Implement automatic subgraph calculation. Do not require users to select which tasks to retry.

### Pattern 2: Result/Artifact Reuse from Bypassed Tasks
**All 5 systems** reuse outputs from successful tasks, avoiding re-execution.

**For Tekton:** Synthetic result injection (memoization) is not an optional nice-to-have; it's a core requirement for correctness.

### Pattern 3: No Side-Effect Protection
**All 5 systems** re-execute tasks from scratch, triggering side effects again. None attempt automatic idempotency detection.

**For Tekton:** Document explicitly that retried tasks execute from step 1. Users are responsible for idempotent design.

### Pattern 4: Contextual Re-evaluation of Cleanup/Finally
**All 5 systems** re-evaluate finally/cleanup logic based on the new retry outcome, not the original failure state.

**For Tekton:** Reset `FinallyStartTime` to nil. Finally tasks must execute at the conclusion of the retry.

### Pattern 5: Explicit Linkage Between Original and Retry
**All 5 systems** maintain a clear link between the original run and the retried run (via UI, build numbers, run IDs, or annotations).

**For Tekton:** Use `tekton.dev/replaces: <original-UID>` annotation on new PipelineRun to enable Dashboard grouping.

### Pattern 6: Time Windows / Data Expiration
**All systems except Jenkins** enforce a TTL on artifact/result storage (90 days for GitHub, configurable for GitLab/CircleCI, configurable for Argo, permanent for Jenkins).

**For Tekton:** Document that `--failed-only` is best-effort. Pre-flight checks must validate PVC existence. Tekton Results retention policy (default ~90 days) is the fallback.

---

## Key Divergences (Where and Why Systems Differ)

### Divergence 1: Object Model (In-Place vs. New Object)

**In-Place Mutation** (GitHub Actions, GitLab CI):
- Simpler UX (single graph, in-place update)
- Breaks Kubernetes immutability
- Complex etcd state reversal

**New Object** (CircleCI, Jenkins, Argo resubmit):
- Preserves auditability and immutability
- Aligns with Kubernetes principles
- Requires explicit linkage for UI grouping

**Why the divergence?** SaaS systems (GHA, GitLab) have no immutability constraints; Kubernetes systems (Argo, Jenkins) prefer new objects to avoid state reversal.

**For Tekton:** Choose new object model. Aligns with Kubernetes values.

---

### Divergence 2: Storage Architecture (Cloud-Backed vs. Kubernetes-Native)

**Cloud-Backed** (GitHub Actions, GitLab CI, CircleCI):
- Artifacts stored in S3/cloud (persistent, reliable)
- Decoupled from cluster lifecycle
- Longer TTL (30-90 days default)

**Kubernetes-Native** (Argo, Tekton):
- Argo: External S3 required; recommends this architecture
- Tekton: PVCs (tied to cluster lifecycle); Tekton Results (PostgreSQL) as fallback
- Shorter effective TTL (GC can delete immediately after pod completion)

**Why the divergence?** SaaS can afford centralized cloud storage; Kubernetes systems must handle ephemeral storage.

**For Tekton:** Validate PVCs before retry. Integrate with Tekton Results as fallback. Consider recommending external S3 (following Argo's pattern).

---

### Divergence 3: DAG Dependency Declaration (Implicit vs. Explicit)

**Explicit** (GitHub Actions, GitLab CI, CircleCI, Argo, Jenkins):
- `needs:`, `dependencies:`, `requires:` keywords
- Simple DAG traversal algorithm
- Deterministic, easy to reason about

**Implicit (+ Explicit)** (Tekton):
- Explicit: `runAfter`
- Implicit: `$(tasks.X.results.Y)` string parsing
- Computationally expensive graph traversal
- More flexible (string expressions are powerful)

**Why the divergence?** Tekton chose implicit references for ergonomics (fewer keywords, more expression power). Others chose explicit for simplicity.

**For Tekton:** Current model is viable but requires sophisticated string parsing + graph traversal. Document this complexity clearly to users.

---

### Divergence 4: User Control Over Retry Point

**Automatic** (GitHub Actions, GitLab CI, CircleCI, Argo, Tekton):
- System calculates failed subgraph automatically
- User clicks "retry" with no further input

**Manual** (Jenkins):
- User selects the stage to restart from (dropdown)
- System then executes that stage + all downstream stages

**Why the divergence?** Jenkins is linear (stages don't branch); users intuitively understand "restart from stage N". Modern DAG systems calculate subgraph automatically.

**For Tekton:** Implement automatic subgraph calculation. Do not require user input on which tasks to retry.

---

## Kubernetes-Native Lessons (Argo + Tekton)

### Lesson 1: Argo's Dual-Model Proves Both Approaches Viable

Argo Workflows supports **both** in-place mutation (`argo retry`) and new object (`argo resubmit --memoized`). This is instructive:

- **In-place mutation works IF** the CRD still exists in etcd (hasn't been garbage-collected)
- **New object + memoization works IF** archived state is available (Argo Server/PostgreSQL)

**For Tekton:** Implement new object model primarily. In-place mutation is harder to reason about in Kubernetes context.

### Lesson 2: External S3 Sidesteps PVC Lifecycle Issues

Argo **requires** an external S3-compatible artifact repository. This is a design decision, not an accident:

```
Argo pod finishes → controller auto-zips outputs → pushes to S3
Retried pod starts → controller auto-pulls from S3 before container runs
```

This completely decouples artifact availability from Kubernetes pod/PVC lifecycle.

**For Tekton:** 
- Option A: Recommend external S3 for large workspaces (following Argo)
- Option B: Integrate tightly with Tekton Results (PostgreSQL) as the external store
- Option C: Validate PVCs are still Bound before retry; fail explicitly if missing

**Recommendation:** Combine B + C. Require Tekton Results for durable state; fail gracefully if PVCs missing.

### Lesson 3: Explicit DAG Is Simpler Than Implicit + Explicit

Argo, GitLab (via `dependencies:`), and others use purely explicit DAG keywords. Tekton's hybrid model (explicit `runAfter` + implicit `$(tasks.X.results.Y)`) is more powerful but computationally harder.

**For Tekton:** Document the difference clearly. Explain that implicit dependencies incur graph traversal overhead. Consider adding lint rules to catch complex dependency patterns.

---

## SaaS Lessons (GitHub Actions + GitLab CI + CircleCI)

### Lesson 1: Centralized Database Is The Simplest Storage Model

SaaS systems have a single source of truth: the central database remembers every job's status, outputs, and artifacts.

```
Central DB Query: "What failed?" → [job_A, job_B]
Central DB Query: "What artifacts did job_A produce?" → [artifact_1, artifact_2]
Auto-inject into Job_B → done
```

This simplicity is not available in Kubernetes without replicating it (via Tekton Results).

**For Tekton:** Tekton Results (PostgreSQL) provides this central database. Integrate with it for state recovery when etcd objects are pruned.

### Lesson 2: Explicit Separation of Ordering from Data (GitLab's Insight)

GitLab's `needs:` (ordering) vs. `dependencies:` (data) split is elegant:

```yaml
job_B:
  needs: [job_A]           # execute after job_A finishes
  dependencies: [job_A]    # download job_A's artifacts
```

This separation makes it **impossible** to forget artifact dependencies (you must explicitly declare both).

**For Tekton:** Consider whether clearer documentation on `runAfter` vs. `$(tasks.X.results.Y)` would help users avoid mistakes.

### Lesson 3: Artifact TTL Must Be Configurable

GitHub (90 days), GitLab (configurable), CircleCI (default-safe): all offer control over artifact lifespan.

**For Tekton:** Document Tekton Results retention policy clearly. Allow cluster admins to configure PVC/artifact TTLs.

---

## Direct Recommendations for Tekton

Based on synthesizing all 5 systems, here are concrete design recommendations for Tekton's `--failed-only` partial retry feature:

### Recommendation 1: Use New Object Model (Not In-Place Mutation)

 **Align with:** CircleCI, Jenkins, Argo (resubmit) 
 **Rationale:** Preserves Kubernetes immutability and auditability 
 **Implementation:** Generate new PipelineRun with `tekton.dev/replaces: <original-UID>` annotation 
 **Avoid:** In-place status mutation (GitHub Actions, GitLab CI model)

---

### Recommendation 2: Implement Automatic Targeted Subgraph Calculation

 **Align with:** 4 out of 5 systems (only Jenkins is manual) 
 **Rationale:** Users should not select which tasks to retry; system should compute it 
 **Implementation:**
1. Identify tasks with `ConditionSucceeded == False` or terminal states (Cancelled, TimedOut, Infrastructure errors)
2. Traverse DAG using `dag.Build()` + `PipelineTask.Deps()`
3. Include downstream dependents (both explicit `runAfter` and implicit `$(tasks.X.results.Y)`)
4. Mark failed + downstream as "must re-run"; mark successful upstream as "bypass"

---

### Recommendation 3: Implement Synthetic Result Injection (Memoization)

 **Align with:** Argo (argo resubmit --memoized) 
 **Rationale:** Prevent downstream tasks from crashing due to missing parameters 
 **Implementation:**
1. Extract `TaskRun.Status.Results` from all bypassed (successful) tasks
2. Inject as hardcoded parameters into new PipelineRun spec
3. Let controller natively resolve `$(tasks.X.results.Y)` expressions using injected values

---

### Recommendation 4: Implement Two-Tier Data Recovery (Etcd + Tekton Results)

 **Align with:** Argo (in-place + archive fallback) 
 **Rationale:** Handle both cases: objects still in etcd, or pruned to Tekton Results 
 **Implementation:**
1. **Fast path:** Query etcd for original TaskRun objects and their results
2. **Fallback:** If missing, query Tekton Results PostgreSQL for archived PipelineRun/TaskRun definitions and results
3. Fail gracefully with clear error if neither path succeeds

---

### Recommendation 5: Validate PVC Existence Before Retry

 **Align with:** Jenkins (preserveStashes requirement) 
 **Rationale:** PVCs are ephemeral; retry cannot proceed if workspace data is gone 
 **Implementation:**
1. Pre-flight check: Query Kubernetes API for all PVCs referenced by original PipelineRun
2. Verify each PVC is still Bound and exists
3. If any PVC is missing, reject retry with clear error: "Workspace PVC no longer exists. Full re-run required."

---

### Recommendation 6: Reset Finally Task State on Retry

 **Align with:** All 5 systems (re-evaluate finally based on new outcome) 
 **Rationale:** Finally timeout should start fresh, not expire immediately based on original run 
 **Implementation:**
1. Identify finally tasks that failed in original run
2. Include them in re-run set
3. Reset `FinallyStartTime` to nil in new PipelineRun
4. Finally timeout restarts from scratch during the retry

---

### Recommendation 7: Define Grace Periods / Time Windows

 **Align with:** All systems (implicit TTL) 
 **Rationale:** Retry is best-effort, bounded by storage retention 
 **Implementation:** Document that `--failed-only` is supported for:
- **PipelineRuns:** Up to Tekton Results retention window (~90 days default)
- **Workspaces/PVCs:** Up to cluster admin's PVC garbage collection policy (can be immediately after pod completion)

Pre-flight checks will fail explicitly if either window has expired.

---

### Recommendation 8: Use Annotation-Based Linkage for Dashboard Integration

 **Align with:** All systems (explicit audit links) 
 **Rationale:** Allows Dashboard to group original + retry runs 
 **Implementation:**
```yaml
apiVersion: tekton.dev/v1
kind: PipelineRun
metadata:
  name: my-pipeline-run-retry-001
  annotations:
    tekton.dev/replaces: "abc123-original-uid"  # Link to original PipelineRun UID
spec:
  # ... retried pipeline spec
```

Dashboard can query: "Show me all PipelineRuns where `tekton.dev/replaces` matches this UID" → group them visually.

---

### Recommendation 9: Implement via tkn CLI + API Surface

 **Align with:** All systems (user-facing CLI) 
 **Rationale:** Easy discovery and use 
 **Implementation:**
```bash
# User-facing CLI command
tkn pipelinerun retry <run-id> --failed-only

# Under the hood:
# 1. Fetch original PipelineRun from cluster or Tekton Results
# 2. Calculate failed subgraph
# 3. Extract bypassed task results
# 4. Generate new PipelineRun with synthetic result injection
# 5. Submit to API server
# 6. Link via tekton.dev/replaces annotation
```

Also expose via Kubernetes API (kubectl) and Tekton API server for programmatic access.

---

## Summary: Convergence Points for Tekton Design

| Aspect | Consensus | Divergence | Tekton Recommendation |
|---|---|---|---|
| **Subgraph Definition** | Targeted (4/5 systems) | Jenkins is linear | Automatic calculation |
| **Result Reuse** | All systems | Storage backend differs | Synthetic injection + Tekton Results fallback |
| **Side Effects** | No protection (all 5) | – | Document user responsibility |
| **Finally Tasks** | Contextual re-eval (all 5) | – | Reset FinallyStartTime |
| **Object Model** | 3 in-place, 2 new, 1 dual | SaaS vs K8s preference | New object + annotation linkage |
| **Storage** | All have TTL | Cloud vs K8s strategy | Two-tier (etcd + Tekton Results) |
| **DAG Syntax** | Explicit (4/5) | Tekton has implicit | Document the difference |
| **UX Trigger** | One-click (all 5) | – | `tkn pipelinerun retry --failed-only` |
| **Audit Links** | All systems preserve | Mechanism differs | `tekton.dev/replaces` annotation |

---

## Conclusion

The five-system analysis reveals that **partial retry is a solved problem in modern CI/CD**, with remarkable convergence on core principles (targeted subgraph, result reuse, contextual finally tasks). The divergences are architectural (in-place vs. new object, cloud vs. Kubernetes) rather than conceptual.

**For Tekton**, the most instructive prior art is **Argo Workflows**, which faces identical Kubernetes constraints and has already solved both the in-place and new-object approaches. Tekton should:

1. Adopt the **new object model** (CircleCI/Jenkins/Argo-resubmit pattern)
2. Implement **automatic subgraph calculation** (consensus across 4/5 systems)
3. Use **synthetic result injection** (Argo's proven memoization strategy)
4. Integrate **two-tier data recovery** (etcd + Tekton Results)
5. Require **pre-flight PVC validation** (Jenkins's `preserveStashes` lesson)
6. Link original + retry via **annotations** for Dashboard integration
7. Document **result freshness tradeoffs** clearly

This design will deliver a `--failed-only` experience that matches GitHub Actions' efficiency while remaining a pure Kubernetes citizen.

---

**End of Prior Art Research Document**


