# Tekton Architecture Analysis for Partial PipelineRun Retry

> **Document Type:** Research & Analysis 
> **Status:** Completed 
> **Last Updated:** 2026-09-22
> **Jira Epic:** [SRVKP-14121](https://redhat.atlassian.net/browse/SRVKP-14121) 
> **Foundational Spike:** [SRVKP-14219](https://redhat.atlassian.net/browse/SRVKP-14219)

---

## Purpose

This document analyzes the current Tekton Pipelines implementation to establish a technical foundation for designing partial PipelineRun retry functionality. It describes **how Tekton works today**, without proposing design changes.

**Scope:** 
- Tekton custom resource definitions (CRDs) and their relationships
- Controller architecture and reconciliation logic
- DAG-based scheduling and execution queue semantics
- Known implementation constraints that impact retry design

**Out of Scope:** 
- Design proposals for partial retry (see `design/proposal.md`)
- Prior art analysis (see `research/prior-art-analysis.md`)

---

## Table of Contents

1. [Part 1 — Custom Resources (the data model)](#part-1--custom-resources-the-data-model)
2. [Part 2 — Controllers (the runtime)](#part-2--controllers-the-runtime)
3. [Part 3 — Scheduling logic deep-dive](#part-3--scheduling-logic-deep-dive)
4. [Part 4 — Known Implementation Constraints](#part-4--known-implementation-constraints)

---

## Part 1 — Custom Resources (the data model)

Tekton defines its domain objects as Kubernetes Custom Resource Definitions (CRDs). Understanding their structure — what fields they expose, what they own, and how they refer to each other — is the prerequisite for any design work.

---

### 1.1 `Task`

**What it is:** A reusable, named, versioned unit of work. It declares *what* to run but is completely inert on its own — nothing executes until a `TaskRun` instantiates it.

**API group/version:** `tekton.dev/v1`
**Key source file:** `pkg/apis/pipeline/v1/task_types.go`

**Key `spec` fields (`TaskSpec`):**

| Field            | Type                       | Purpose                                                                                           |
|------------------|----------------------------|---------------------------------------------------------------------------------------------------|
| `steps[]`        | `[]Step`                   | Ordered list of containers. Run sequentially inside one Pod via the injected `entrypoint` binary. |
| `params[]`       | `[]ParamSpec`              | Typed input declarations (string / array / object). Consumers pass values at `TaskRun` time.     |
| `results[]`      | `[]TaskResult`             | Named outputs that steps write to `/tekton/results/<name>`. Consumed by downstream tasks.        |
| `workspaces[]`   | `[]WorkspaceDeclaration`   | Named volume mounts the task expects. Bound to actual volumes/PVCs/ConfigMaps at `TaskRun` time. |
| `sidecars[]`     | `[]Sidecar`                | Long-running containers alongside steps (e.g. a Docker daemon, a test database).                 |
| `stepTemplate`   | `*StepTemplate`            | Default resource requests, env vars, etc., applied to every step.                                |
| `volumes[]`      | `[]corev1.Volume`          | Extra volumes available to steps and sidecars within the task.                                                    |

**Two ways to supply a Task to a TaskRun:**

```yaml
# Option A — by reference (the Task object must exist in the cluster or be resolvable remotely)
spec:
  taskRef:
    name: my-task

# Option B — inline (the full TaskSpec is embedded; no separate Task object needed)
spec:
  taskSpec:
    steps:
      - name: say-hello
        image: alpine
        script: echo hello
```

Both use the same `TaskSpec` struct. When `taskRef` is used, the reconciler fetches the Task, extracts its `.spec`, and proceeds as if it were inline.

**Relationships:**
- Referenced by `TaskRun.spec.taskRef` or embedded via `TaskRun.spec.taskSpec`.
- Referenced by `PipelineTask.taskRef` or embedded via `PipelineTask.taskSpec` inside a `Pipeline`.

---

### 1.2 `TaskRun`

**What it is:** A single execution instance of a `Task`. The TaskRun reconciler turns it into a Kubernetes `Pod` and tracks its progress.

**API group/version:** `tekton.dev/v1`
**Key source files:**
- `pkg/apis/pipeline/v1/taskrun_types.go` — type definitions
- `pkg/reconciler/taskrun/taskrun.go` — reconciler

**Key `spec` fields (`TaskRunSpec`):**

| Field                    | Purpose                                                                                                             |
|--------------------------|---------------------------------------------------------------------------------------------------------------------|
| `taskRef` / `taskSpec`   | Which Task to run — mutually exclusive. *"no more than one of the TaskRef and TaskSpec may be specified."*          |
| `params[]`               | Concrete values for the Task's declared params.                                                                     |
| `workspaces[]`           | Bindings: maps each Task workspace name to an actual volume, PVC, ConfigMap, or Secret.                            |
| `timeout`                | Maximum duration; reconciler calls `failTaskRun(TimedOut)` when elapsed.                                           |
| `serviceAccountName`     | Kubernetes SA used for the Pod.                                                                                     |
| `podTemplate`            | Low-level Pod overrides (node selector, tolerations, security context, etc.).                                      |
| `retries`                | How many times to retry on step failure before giving up.                                                           |
| `status`                 | Can be set to `TaskRunCancelled` by the user or by the PipelineRun reconciler to stop a running Pod.               |

**Key `status` fields (`TaskRunStatus`):**

| Field                            | Purpose                                                                              |
|----------------------------------|--------------------------------------------------------------------------------------|
| `podName`                        | The Pod created for this TaskRun.                                                    |
| `steps[]`                        | Per-step state mirrored from Pod container statuses (running / terminated / waiting).|
| `sidecars[]`                     | Per-sidecar state.                                                                   |
| `results[]`                      | `name`/`value` pairs emitted by the Task steps. This is what downstream tasks consume.|
| `conditions[ConditionSucceeded]` | `True` (succeeded) / `False` (failed) / `Unknown` (running).                        |
| `startTime` / `completionTime`   | Wall-clock times for metrics and timeout enforcement.                                |
| `taskSpec`                       | Snapshot of the resolved TaskSpec (stored for auditability).                         |
| `retriesStatus[]`                | Status of each previous retry attempt.                                               |

**Relationships:**
- Owned by a `PipelineRun` via `metadata.ownerReferences` when created by the PipelineRun reconciler.
- Owns one `Pod` (also via ownerReference).
- May own a `ResolutionRequest` when `taskRef` points to a remote resource.
- Tracked by the parent `PipelineRun` via `status.childReferences[].name`.

---

### 1.3 `Pipeline`

**What it is:** A DAG of `PipelineTask` entries that reference `Task`s (or other `Pipeline`s). Declares the shape of a workflow — ordering, data flow, conditional branching — but does not execute anything itself.

**API group/version:** `tekton.dev/v1`
**Key source files:**
- `pkg/apis/pipeline/v1/pipeline_types.go` — `PipelineSpec`, `PipelineTask`, `Deps()`
- `pkg/apis/pipeline/v1/resultref.go` — `PipelineTaskResultRefs()`: extracts implicit deps from result reference strings

**Key `spec` fields (`PipelineSpec`):**

| Field          | Purpose                                                                                                     |
|----------------|-------------------------------------------------------------------------------------------------------------|
| `tasks[]`      | The main DAG. Each entry is a `PipelineTask` (see below).                                                   |
| `finally[]`    | Tasks that always run after the main DAG completes (success or failure). Run concurrently, no ordering.     |
| `params[]`     | Pipeline-level input declarations. Passed in by `PipelineRun.spec.params`.                                  |
| `results[]`    | Pipeline-level outputs — expressions like `$(tasks.build.results.image-digest)` collected at the end.      |
| `workspaces[]` | Pipeline-level workspace declarations. Bound at `PipelineRun` time and forwarded to tasks.                 |

**`PipelineTask` fields (one entry per node in the DAG):**

| Field                  | Purpose                                                                                                          |
|------------------------|------------------------------------------------------------------------------------------------------------------|
| `name`                 | Unique within the Pipeline. Used to reference this task in `runAfter`, result refs, and `ChildReferences`.       |
| `taskRef` / `taskSpec` | Which Task (or Pipeline) to execute.                                                                             |
| `params[]`             | Values passed to the Task — may contain `$(tasks.<n>.results.<r>)` expressions creating implicit ordering deps.  |
| `runAfter[]`           | Explicit ordering: this task waits for the listed tasks to finish. Creates explicit DAG edges.                   |
| `when[]`               | Conditional execution: CEL expressions or `input/operator/values` guards. False → `WhenExpressionsSkip`.        |
| `retries`              | Number of retry attempts on failure before the task is considered failed.                                        |
| `matrix`               | Fan-out: run the task N times in parallel with different param combinations.                                     |
| `workspaces[]`         | Maps Pipeline-level workspace names to Task workspace names.                                                     |
| `timeout`              | Per-task timeout (independent of the pipeline-level timeout).                                                    |
| `onError`              | `stopAndFail` (default) or `continue` — whether a task failure should stop the whole pipeline.                  |

**How dependencies are collected — `Deps()` method:**

```go
// pkg/apis/pipeline/v1/pipeline_types.go
func (pt *PipelineTask) Deps() []string {
    // 1. Explicit: everything in runAfter[]
    // 2. Implicit: task names extracted from $(tasks.<name>.results.<r>) in params and when expressions
}
```

This is what `dag.Build()` uses to construct the graph edges.

**Relationships:**
- Referenced by `PipelineRun.spec.pipelineRef` or embedded via `PipelineRun.spec.pipelineSpec`.

---

### 1.4 `PipelineRun`

**What it is:** A single execution instance of a `Pipeline`. This is the most complex resource in Tekton. The PipelineRun reconciler is the orchestrator — it owns TaskRuns and drives the DAG forward on every reconcile loop.

**API group/version:** `tekton.dev/v1`
**Key source files:**
- `pkg/apis/pipeline/v1/pipelinerun_types.go` — type definitions
- `pkg/reconciler/pipelinerun/pipelinerun.go` — reconciler
- `pkg/reconciler/pipelinerun/resources/pipelinerunstate.go` — runtime state management

**Key `spec` fields (`PipelineRunSpec`):**

| Field                          | Purpose                                                                                  |
|--------------------------------|------------------------------------------------------------------------------------------|
| `pipelineRef` / `pipelineSpec` | Which Pipeline to run — mutually exclusive.                                              |
| `params[]`                     | Top-level parameter values passed into the Pipeline.                                     |
| `workspaces[]`                 | Bindings for Pipeline-level workspaces (forwarded to individual TaskRuns).               |
| `taskRunTemplate`              | Default SA, pod template, etc., applied to all created TaskRuns.                         |
| `taskRunSpecs[]`               | Per-task overrides of the template (e.g. different SA for one task).                     |
| `timeouts.pipeline`            | Hard deadline for the whole PipelineRun.                                                 |
| `timeouts.tasks`               | Deadline for the main DAG phase only (excluding `finally`).                              |
| `timeouts.finally`             | Deadline for the `finally` phase only.                                                   |
| `status`                       | User-settable: `PipelineRunCancelled`, `PipelineRunPending`, `StoppedRunFinally`.        |

**Key `status` fields (`PipelineRunStatus`):**

| Field                                               | Purpose                                                                                                                                                        |
|-----------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `childReferences[]`                                 | **The single source of truth** for which child runs exist. One entry per created TaskRun/CustomRun/child PipelineRun. Contains: `name`, `pipelineTaskName`, `kind`, `displayName`, `whenExpressions`, `skippedDueTo`. |
| `conditions[ConditionSucceeded]`                    | `True` / `False` / `Unknown` + reason + message.                                                                                                               |
| `startTime` / `completionTime` / `finallyStartTime` | Wall-clock timestamps. `finallyStartTime` is set the first time a finally task is created.                                                                     |
| `results[]`                                         | Pipeline-level results collected from task results at completion.                                                                                              |
| `skippedTasks[]`                                    | Tasks not executed, with the skip reason.                                                                                                                      |
| `pipelineSpec`                                      | Snapshot of the resolved Pipeline spec at execution time.                                                                                                      |

**Why `childReferences` is critical:** The reconciler is stateless — `PipelineRunFacts` is rebuilt from scratch on every reconcile loop. `childReferences` is how it knows which TaskRuns already exist so it doesn't re-create them and can read their current status from the informer cache.

**Relationships:**
- Owns `TaskRun`s, `CustomRun`s, child `PipelineRun`s via `metadata.ownerReferences`.
- Tracks them via `status.childReferences`.
- May own `ResolutionRequest`s for remote Pipeline/Task resolution.

---

### 1.5 `CustomRun`

**What it is:** A delegate object for custom task execution. When a `PipelineTask.taskRef` has a non-empty `apiVersion` (e.g. `wait.tekton.dev/v1alpha1`) and `kind`, Tekton knows this is not a built-in `Task` — it creates a `CustomRun` object instead of a `TaskRun` and leaves execution entirely to an external controller.

**API group/version:** `tekton.dev/v1beta1`
**Key source files:**
- `pkg/apis/pipeline/v1beta1/customrun_types.go`
- `pkg/apis/pipeline/v1/taskref_types.go` — `IsCustomTask()` method (checks `apiVersion != ""`)

**How it works:**

```
PipelineRun reconciler
 └─ sees PipelineTask with taskRef.apiVersion = "wait.tekton.dev/v1alpha1"
 └─ calls IsCustomTask() → true
 └─ calls createCustomRuns() → creates a CustomRun object

External custom task controller
 └─ watches CustomRun objects with its apiVersion/kind
 └─ does its work (wait, approval gate, ML training, etc.)
 └─ writes results to CustomRun.status.results[]
 └─ sets CustomRun.status.conditions[ConditionSucceeded] = True/False

PipelineRun reconciler (next reconcile)
 └─ reads CustomRun.status.conditions → task is done
 └─ reads CustomRun.status.results[] → propagates to downstream tasks
```

**Key `status` fields:**
- `conditions[ConditionSucceeded]` — the PipelineRun reconciler polls this to know if the custom task finished.
- `results[]` — emitted by the external controller, consumed exactly like `TaskRun.status.results[]`.
- `extraFields` — arbitrary JSON blob for controller-specific state.

---

### 1.6 `ResolutionRequest`

**What it is:** An asynchronous request to fetch a remote resource definition — a `Task` or `Pipeline` stored outside the cluster (Git repo, OCI bundle, cluster catalog, HTTP URL). The PipelineRun or TaskRun reconciler creates it when a `taskRef`/`pipelineRef` points to a remote source and then re-queues until the request is resolved.

**API group/version:** `resolution.tekton.dev/v1beta1`
**Key source files:**
- `pkg/apis/resolution/v1beta1/resolutionrequest_types.go`
- `pkg/reconciler/resolutionrequest/resolutionrequest.go` — lifecycle management only
- `cmd/resolvers/` — the actual resolver implementations (Git, Bundle, Cluster, HTTP)

**Key fields:**

| Field                                  | Purpose                                                                                        |
|----------------------------------------|------------------------------------------------------------------------------------------------|
| `spec.params[]`                        | Resolver-specific parameters, e.g. `url`, `revision`, `pathInRepo` for the Git resolver.      |
| `status.data`                          | The resolved YAML content. Written by the resolver process, **not** by the reconciler.         |
| `status.conditions[ConditionSucceeded]`| `True` when data is available; `False` on timeout or error.                                    |
| `status.refSource`                     | Provenance: exactly where the content came from (for trusted resources).                       |

**Important separation of concerns:**
- The `ResolutionRequest` reconciler only enforces the global resolution timeout and marks the object Succeeded/Failed.
- The actual fetching is done by separate resolver processes (hosted in `cmd/resolvers`) that watch for `ResolutionRequest` objects matching their type and write the YAML into `status.data`.

---

### 1.7 Relationship diagram

```
User
 └─ kubectl apply ──► PipelineRun
                         │
                         ├─ ownerRef ──► TaskRun ──────────────► Pod
                         │               │
                         │               └─ may create ──────► ResolutionRequest
                         │                                      (remote taskRef)
                         │
                         ├─ ownerRef ──► CustomRun
                         │               └─ driven by external controller
                         │
                         ├─ ownerRef ──► child PipelineRun
                         │               └─ (pipeline-in-pipeline, recursive)
                         │
                         └─ may create ──► ResolutionRequest
                                           (remote pipelineRef)
```

---

## Part 2 — Controllers (the runtime)

All three controllers run in a single binary: `cmd/controller/main.go`. They are registered with Knative's `sharedmain` framework which handles informer setup, work queue management, leader election, and the reconcile loop.

```go
// cmd/controller/main.go:113-116
sharedmain.MainWithConfig(ctx, ControllerLogKey, cfg,
    taskrun.NewController(opts, clock.RealClock{}),
    pipelinerun.NewController(opts, clock.RealClock{}),
    resolutionrequest.NewController(clock.RealClock{}),
)
```

**How Kubernetes controllers work (background):** A controller registers an informer — a local cache of Kubernetes objects backed by a watch stream. When an object changes in the API server, the watch event updates the local cache and enqueues the object's key for reconciliation. The reconciler then calls `ReconcileKind()` with the object, compares actual vs desired state, takes action, and writes back updated status. If an error is returned, the key is re-queued with exponential backoff.

Controllers never call each other's Go functions. All interaction is through Kubernetes objects.

---

### 2.1 TaskRun reconciler

**File:** `pkg/reconciler/taskrun/taskrun.go`

**Triggered by:**
- Watch on `TaskRun` objects (create/update).
- Watch on `Pod` objects owned by a `TaskRun` — when the Pod status changes (a step finishes, the Pod completes), the owning TaskRun is re-enqueued.

**`Reconciler` struct** holds:
- `KubeClientSet` — to create/get/patch Pods.
- `PipelineClientSet` — to update TaskRun status.
- `taskRunLister`, `podLister`, `limitrangeLister` — informer caches.
- `entrypointCache` — caches resolved entrypoint binary digests to avoid repeated lookups.
- `resolutionRequester` — interface to create/read `ResolutionRequest` objects.

**`ReconcileKind()` flow** (`taskrun.go` line 137):

```
ReconcileKind(tr *TaskRun)
 │
 ├─ [fast path] tr.IsDone()
 │   └─ stopSidecars()          ← inject nop image or use native K8s sidecar stop
 │   └─ emitReconcileEvents()   ← Kubernetes events + CloudEvents
 │   └─ return nil
 │
 ├─ [fast path] tr.IsCancelled()
 │   └─ failTaskRun(Cancelled)  ← patch Pod to stop + update tr.Status
 │   └─ return
 │
 ├─ [fast path] tr.IsPending()
 │   └─ MarkResourceOngoing(Pending)
 │   └─ return
 │
 ├─ [fast path] tr.HasTimedOut()
 │   └─ updateStepStatusesFromPod()  ← capture final step states before marking failed
 │   └─ failTaskRun(TimedOut)
 │   └─ return
 │
 ├─ checkPodFailed()
 │   └─ detects ImagePullBackOff, InvalidImageName, CreateContainerConfigError, etc.
 │   └─ applies configurable grace periods (imagePullBackOffTimeout, createContainerErrorTimeout)
 │   └─ failTaskRun(ImagePullFailed / PodCreationFailed / ...)
 │
 ├─ prepare(tr)
 │   ├─ SetDefaults()
 │   ├─ resolve Task spec: local lister → or → create ResolutionRequest + requeue
 │   ├─ verify trusted resources (VerificationPolicy)
 │   ├─ store resolved TaskSpec in tr.Status (so future reconciles don't re-fetch)
 │   ├─ validate params, workspaces, step resource requirements
 │   └─ apply param substitution / workspace substitution
 │
 └─ reconcile(tr, resolvedTask)
     ├─ look up existing Pod (tr.Status.PodName)
     ├─ if no Pod: build Pod spec (pkg/pod) → createPod()
     │   ├─ inject entrypoint binary into every step container
     │   ├─ add result sidecar (sidecarlogresults or file-based)
     │   ├─ set up workspace volume mounts
     │   └─ apply LimitRange defaults
     ├─ if Pod exists: read Pod status → update tr.Status.Steps / tr.Status.Sidecars
     ├─ if Pod succeeded: collect results → tr.Status.Results
     └─ if Pod failed: failTaskRun()
```

**Step sequencing via `entrypoint`:**

The `entrypoint` binary (`cmd/entrypoint`) is injected as the command of every step container. Kubernetes starts all containers in a Pod simultaneously, but `entrypoint` uses a file-based semaphore under `/tekton/run/<step-index>/out` to ensure steps run in declaration order:

```
Step 0 entrypoint: wait for /tekton/run/0/out → does not exist on start → runs immediately → writes /tekton/run/0/out when done
Step 1 entrypoint: waits for /tekton/run/0/out to exist → then runs
Step 2 entrypoint: waits for /tekton/run/1/out → then runs
...
```

**Result collection — two modes:**
- **Sidecar log results** (default): a sidecar container (`cmd/sidecarlogresults`) tails step stdout for result markers and writes them to `TaskRun.status.results`.
- **File-based results**: steps write to `/tekton/results/<name>`; the reconciler reads these files after the Pod completes.

**Retry logic:** In `emitReconcileEvents()`, if the TaskRun failed and `tr.IsRetriable()` is true (retries remaining), `retryTaskRun()` is called — it appends the current failed status to `retriesStatus[]`, resets the condition to `Unknown`, and clears `podName`. The next reconcile creates a fresh Pod.

---

### 2.2 PipelineRun reconciler

**File:** `pkg/reconciler/pipelinerun/pipelinerun.go`

**Triggered by:**
- Watch on `PipelineRun` objects (create/update).
- Watch on `TaskRun` objects owned by a `PipelineRun` — when a TaskRun's status changes (it finishes, or a step completes), the owning PipelineRun is re-enqueued. This is the primary driver of DAG progression.
- Watch on `CustomRun` objects (same owner-reference mechanism).
- Periodic requeue (controlled by `--resync-period`) for timeout enforcement.

**`Reconciler` struct** holds:
- `KubeClientSet`, `PipelineClientSet`.
- `pipelineRunLister`, `taskRunLister`, `customRunLister` — informer caches.
- `resolutionRequester` — to create/read `ResolutionRequest` objects.
- `pvcHandler` — to create PVCs for workspace bindings.
- `metrics` — Prometheus metrics recorder.

**`ReconcileKind()` flow** (`pipelinerun.go` line 191):

```
ReconcileKind(pr *PipelineRun)
 │
 ├─ [fast path] pr.IsDone()
 │   └─ cleanupAffinityAssistantsAndPVCs()
 │   └─ emitReconcileEvents()
 │   └─ return
 │
 ├─ [emergency] pr.HasTimedOutForALongTime()
 │   └─ timeoutPipelineRun() immediately (avoids etcd size limit issues from large status)
 │   └─ return PermanentError
 │
 ├─ [fast path] pr.IsCancelled()
 │   └─ cancelPipelineRun() → patch all running TaskRuns as Cancelled
 │   └─ return
 │
 ├─ updatePipelineRunStatusFromInformer()
 │   └─ walks pr.Status.ChildReferences
 │   └─ for each child: reads current state from informer cache
 │   └─ detects orphaned ChildReferences (TaskRun deleted externally) → removes them
 │
 ├─ reconcile()   ← main work
 │
 ├─ [post] if pr.IsDone(): cleanupAffinityAssistantsAndPVCs()
 │
 └─ emitReconcileEvents() + requeue after timeout
```

**`reconcile()` flow** (line 545):

```
reconcile(pr, getPipelineFunc)
 │
 ├─ pr.IsPending() → MarkRunning(Pending), return
 │
 │  ┌─────────────────────────────────────────────────────────────────────┐
 │  │ PHASE 1 — Fetch & build the static Pipeline structure               │
 │  └─────────────────────────────────────────────────────────────────────┘
 │
 ├─ GetPipelineData()                         produces: pipelineSpec, pipelineMeta
 │   ├─ local: fetch from cluster lister
 │   └─ remote: create ResolutionRequest → ErrRequestInProgress → requeue
 │
 ├─ storePipelineSpecAndMergeMeta()           uses: pipelineSpec, pipelineMeta
 │                                            → snapshots Pipeline spec into pr.Status.PipelineSpec
 │
 ├─ dag.Build(pipelineSpec.Tasks)             uses: pipelineSpec       produces: d (main DAG)
 ├─ dag.Build(pipelineSpec.Finally)           uses: pipelineSpec       produces: dfinally
 │
 ├─ validate params, workspace bindings, param types, array indices
 │                                            uses: pipelineSpec, pr.Spec.Params
 ├─ ApplyParameters(), ApplyContexts(),
 │  ApplyWorkspaces()                         uses: pipelineSpec, pr.Spec.Params/Workspaces
 │                                            produces: pipelineSpec (with substituted values)
 │
 │  ┌─────────────────────────────────────────────────────────────────────┐
 │  │ PHASE 2 — Reconstruct the current runtime state from K8s objects    │
 │  └─────────────────────────────────────────────────────────────────────┘
 │
 ├─ resolvePipelineState() [PASS 1: ran/running tasks]
 │   uses: pipelineSpec (substituted), pr.Status.ChildReferences, informer cache
 │   └─ for each task that has a ChildReference:
 │       ├─ GetTaskRunName() → look up name in pr.Status.ChildReferences
 │       ├─ GetTaskRun()     → fetch live TaskRun from informer cache
 │       └─ GetTask()        → resolve Task spec (local or ResolutionRequest)
 │   produces: pipelineRunState (partial — ran/running tasks only, WITH results)
 │
 ├─ resolvePipelineState() [PASS 2: not-started tasks]
 │   uses: pipelineSpec (substituted), pipelineRunState from Pass 1 ← results available now
 │   └─ same as pass 1 but TaskRun is nil; ResolvedTask still resolved (for validation)
 │   produces: pipelineRunState (complete — all tasks)
 │
 ├─ build PipelineRunFacts                    uses: pipelineRunState, d, dfinally, pr timeout fields
 │   → {State: pipelineRunState, TasksGraph: d, FinalTasksGraph: dfinally, TimeoutsState}
 │   produces: facts  ← ALL remaining steps use facts instead of raw K8s objects
 │
 │  ┌─────────────────────────────────────────────────────────────────────┐
 │  │ PHASE 3 — Validate & pre-flight (before creating any new TaskRuns)  │
 │  └─────────────────────────────────────────────────────────────────────┘
 │
 ├─ ValidateResolvedTask() for each task      uses: facts.State (ResolvedTask specs + PipelineTask params)
 ├─ EvaluateCEL() for each PipelineTask       uses: facts.State (when expressions + substituted params)
 │                                            produces: facts.State[*].EvaluatedCEL
 │
 ├─ [if gracefully cancelling]
 │   gracefullyCancelPipelineRun()            uses: facts.IsRunning()
 │                                            → patches running TaskRuns to Cancelled
 │
 ├─ [before first task — IsBeforeFirstTaskRun()]
 │   ValidatePipelineTaskResults()            uses: facts.State (result ref expressions vs declared results)
 │   ValidatePipelineResults()                uses: facts.State + pipelineSpec.Results
 │   ValidateOptionalWorkspaces()             uses: facts.State + pipelineSpec.Workspaces
 │   createOrUpdateAffinityAssistantsAndPVCs() uses: pr.Spec.Workspaces
 │                                            → creates PVC objects in cluster
 │
 ├─ timeout handling                          uses: facts.TimeoutsState, facts.State
 │   timeoutPipelineTasksForTaskNames()       → patches running TaskRuns to TimedOut
 │
 │  ┌─────────────────────────────────────────────────────────────────────┐
 │  │ PHASE 4 — Schedule new work                                         │
 │  └─────────────────────────────────────────────────────────────────────┘
 │
 ├─ runNextSchedulableTask()                  uses: facts (DAGExecutionQueue, GetFinalTasks, Skip, ...)
 │                                            → creates TaskRun/CustomRun/child PipelineRun objects
 │                                            → updates facts.State[*].TaskRuns with new runs
 │
 ├─ [post] timeoutPipelineRun()              uses: facts.TimeoutsState
 │
 │  ┌─────────────────────────────────────────────────────────────────────┐
 │  │ PHASE 5 — Write results back to pr.Status                           │
 │  └─────────────────────────────────────────────────────────────────────┘
 │
 ├─ GetPipelineConditionStatus()              uses: facts (task counts, timeout state)
 │                                            → True / False / Unknown + reason + message
 ├─ pr.Status.ChildReferences =
 │   facts.GetChildReferences()              uses: facts.State (TaskRun names per task)
 ├─ pr.Status.SkippedTasks    =
 │   facts.GetSkippedTasks()                 uses: facts.State + facts.SkipCache
 └─ pr.Status.Results         =
     ApplyTaskResultsToPipelineResults()     uses: facts.State.GetTaskRunsResults() + pipelineSpec.Results
```

**Why `PipelineRunFacts` is rebuilt on every reconcile:**

Kubernetes controllers are stateless. Between two reconcile calls, the process may restart, or a different HA replica may handle the next event. All state must be derived from Kubernetes objects. `PipelineRunFacts` is an in-memory view rebuilt each loop from:
- `pr.Status.ChildReferences` — what child runs exist and what their names are.
- The informer cache — the current status of each `TaskRun`.
- The resolved `Pipeline` spec — the DAG structure.

This means the reconciler is purely functional: given the same Kubernetes state, it always produces the same decisions.

---

### 2.3 ResolutionRequest reconciler

**File:** `pkg/reconciler/resolutionrequest/resolutionrequest.go`

**Role:** Lifecycle management only — it does **not** perform the resolution.

**Triggered by:** Watch on `ResolutionRequest` objects.

**`ReconcileKind()` flow:**

```
ReconcileKind(rr)
 │
 ├─ rr.IsDone()     → return (already finished)
 │
 ├─ rr.IsResolved() → MarkSucceeded()   ← resolver already wrote status.data
 │
 ├─ requestDuration > maximumResolutionDuration
 │   └─ MarkFailed(ReasonResolutionTimedOut)
 │
 └─ else → MarkInProgress(MessageWaitingForResolver)
            requeue after (timeout - elapsed)
```

**Who actually fills `status.data`:** The resolver processes in `cmd/resolvers`. Each resolver implementation (Git, Bundle, Cluster, HTTP) watches for `ResolutionRequest` objects whose `spec.params` match its resolver type. When it finds one, it fetches the content and writes it into `status.data` using a direct API patch. The ResolutionRequest reconciler then sees `IsResolved() == true` on the next reconcile and calls `MarkSucceeded()`.

**How the PipelineRun/TaskRun reconcilers use it:**

```go
// Simplified pseudocode
taskSpec, err := getTask(ctx, taskRef)
if errors.Is(err, remote.ErrRequestInProgress) {
    // ResolutionRequest was just created; wait for it to be resolved
    pr.Status.MarkRunning(ReasonResolvingTaskRef, "awaiting remote resource")
    return controller.NewRequeueAfter(1 * time.Second)
}
// On the next reconcile, getTask() finds the resolved spec in the ResolutionRequest
// and returns it without creating a new one
```

---

## Part 3 — Scheduling logic deep-dive

This section traces the complete path from "user creates a `PipelineRun`" to "a `TaskRun` is created for a specific task." This is the core of the orchestration engine.

---

### 3.1 DAG construction

**Called:** once per reconcile, inside `reconcile()`.

```go
// pipelinerun.go:593-613
d, err := dag.Build(
    v1.PipelineTaskList(pipelineSpec.Tasks),
    v1.PipelineTaskList(pipelineSpec.Tasks).Deps(),
)
dfinally, err := dag.Build(
    v1.PipelineTaskList(pipelineSpec.Finally),
    map[string][]string{},  // no edges between finally tasks
)
```

**`dag.Build()` internals** (`pkg/reconciler/pipeline/dag/dag.go`):

1. Creates one `Node` per `PipelineTask`, keyed by `task.HashKey()` (= `task.Name`).
2. Iterates over the deps map: for each `(task → [dep1, dep2, ...])` pair, adds a directed edge `dep → task` in the graph.
3. Runs **cycle detection** via Kahn's algorithm (`findCyclesInDependencies`):
   - Computes in-degree for all nodes.
   - Repeatedly removes nodes with in-degree 0 (no remaining deps).
   - If any nodes remain after the algorithm terminates, they form a cycle → `PermanentError`.

**`Deps()` method** on `PipelineTask` (`pipeline_types.go`):

```go
func (pt *PipelineTask) Deps() []string {
    deps := sets.NewString()
    // 1. Explicit ordering
    deps.Insert(pt.RunAfter...)
    // 2. Implicit: task names extracted from result reference strings
    //    e.g. "$(tasks.fetch.results.sha)" → "fetch"
    for _, ref := range pt.Params.extractResultRefs() {
        deps.Insert(ref.PipelineTask)
    }
    // same for When expressions and Matrix params
    return deps.List()
}
```

**`finally` DAG:** All finally tasks get independent nodes with no edges. They run concurrently once the main DAG is complete.

**Example:**

```
Pipeline:
  tasks:
    - name: clone    (no deps)
    - name: test     runAfter: [clone]
    - name: build    runAfter: [clone]
    - name: deploy   params: image=$(tasks.build.results.digest)   ← implicit dep on build
  finally:
    - name: notify

DAG (d):
  clone ──► test
  clone ──► build ──► deploy

DAG (dfinally):
  notify   (isolated node)
```

---

### 3.2 State resolution (two-pass)

**Why two passes?** Results emitted by already-completed tasks must be available before evaluating `when` conditions and param expressions for not-yet-started tasks. Pass 1 processes completed/running tasks (results known); Pass 2 processes not-started tasks (can now reference those results).

```go
// pipelinerun.go:735-771
// Split tasks into two groups
ranOrRunningTaskNames := sets.Set[string]{}
for _, child := range pr.Status.ChildReferences {
    ranOrRunningTaskNames.Insert(child.PipelineTaskName)
}
for _, task := range tasks {
    if ranOrRunningTaskNames.Has(task.Name) {
        ranOrRunningTasks = append(ranOrRunningTasks, task)
    } else {
        notStartedTasks = append(notStartedTasks, task)
    }
}

// Pass 1: tasks with existing ChildReferences (have run or are running)
pipelineRunState, _ = c.resolvePipelineState(ctx, ranOrRunningTasks, ...)

// Pass 2: tasks with no ChildReference (not started yet)
// receives the partial state from Pass 1 so result values are available
pipelineRunState, _ = c.resolvePipelineState(ctx, notStartedTasks, ..., pipelineRunState)
```

**`resolvePipelineState()` for each `PipelineTask`:**

```
resolvePipelineState(pipelineTasks, pr, existingState)
 │
 └─ for each pipelineTask:
     ├─ GetTaskRunName(pr.Status.ChildReferences, task.Name, pr.Name)
     │   → returns deterministic name: "<pr-name>-<task-name>" or from ChildReferences
     │
     ├─ GetTaskFunc() → builds a function that either:
     │   ├─ fetches Task from cluster lister (taskRef with no apiVersion)
     │   └─ creates/reads a ResolutionRequest (remote taskRef)
     │
     ├─ GetTaskRun() → fetches the TaskRun from informer cache (nil if not started)
     │
     └─ returns ResolvedPipelineTask {
             PipelineTask: the spec
             TaskRuns:     []*TaskRun (nil if not started; list if matrixed)
             ResolvedTask: resolved TaskSpec + metadata
         }
```

The result is `PipelineRunState` — a `[]*ResolvedPipelineTask` slice, one entry per PipelineTask (both main and finally).

---

### 3.3 `PipelineRunFacts`

```go
// pkg/reconciler/pipelinerun/resources/pipelinerunstate.go
type PipelineRunFacts struct {
    State           PipelineRunState    // all ResolvedPipelineTasks
    SpecStatus      PipelineRunSpecStatus
    TasksGraph      *dag.Graph          // main DAG
    FinalTasksGraph *dag.Graph          // finally DAG
    TimeoutsState   PipelineRunTimeoutsState
    // runtime-only fields:
    ValidationFailedTask   []*ResolvedPipelineTask
    ValidationFailedErrors map[string]string
}
```

`PipelineRunFacts` is the central object for all scheduling decisions. Every scheduling/skip/status function takes it as input.

---

### 3.4 Execution queue — `DAGExecutionQueue()`

```go
// pipelinerunstate.go
func (state PipelineRunState) DAGExecutionQueue() (PipelineRunState, error)
```

Returns the set of `ResolvedPipelineTask`s that are ready to be scheduled in this reconcile loop.

A task is in the queue if **all** of the following are true:
1. All its DAG dependencies are **done** — `isDone()` returns true for each dep.
2. The task itself is not yet done.
3. `IsStopping()` is **false** — the pipeline is not in a stopping state.

**`isDone(rpt)`** returns true if the task's `TaskRun` is:
- Succeeded (`ConditionSucceeded = True`), OR
- Failed/Cancelled/TimedOut (`ConditionSucceeded = False`), OR
- Skipped for any reason.

A skipped task is "done" for DAG advancement — its children can proceed (subject to skip cascade rules).

**`IsStopping()`** returns true if any task has failed with `onError: stopAndFail` (the default). While stopping, no new DAG tasks are scheduled (but `finally` tasks still run).

---

### 3.5 `runNextSchedulableTask()`

**File:** `pkg/reconciler/pipelinerun/pipelinerun.go` line 989.

```
runNextSchedulableTask(pr, pipelineRunFacts)
│                        ▲                ▲
│                        │                └── pipelineRunFacts.State          = []ResolvedPipelineTask (all tasks + live TaskRuns + results)
│                        └────────────────── pipelineRunFacts.TasksGraph      = main DAG
│                                            pipelineRunFacts.FinalTasksGraph  = finally DAG
│
│  ┌─────────────────────────────────────────────────────────────────────┐
│  │ STEP 1 — Ask the DAG which tasks are ready to run now               │
│  └─────────────────────────────────────────────────────────────────────┘
│
├─ nextRpts = DAGExecutionQueue()
│    uses:    pipelineRunFacts.TasksGraph          ← DAG edges (dependency structure)
│    uses:    pipelineRunFacts.State               ← calls isDone() on every dependency node
│    uses:    pipelineRunFacts.IsStopping()        ← if true, returns empty queue immediately
│    produces: nextRpts                            → []*ResolvedPipelineTask whose deps are all done
│
│  ┌─────────────────────────────────────────────────────────────────────┐
│  │ STEP 2 — Validate result references for each ready task             │
│  └─────────────────────────────────────────────────────────────────────┘
│
├─ for each rpt in nextRpts:
│   └─ CheckMissingResultReferences(state, rpt)
│        uses:   pipelineRunFacts.State            ← reads upstream TaskRun.Status.Results
│        uses:   rpt.PipelineTask.Params           ← scans for $(tasks.X.results.Y) expressions
│        ├─ result ref → failed task          → MissingResultsSkip (soft, pipeline continues)
│        ├─ result ref → succeeded task but result name not emitted
│        │               → ValidationFailedTask (hard, pipeline fails)
│        └─ result ref → resolved OK          → continue
│        [if any task fails validation]
│            produces: pipelineRunFacts.ValidationFailedTask  ← task added to failed list
│            side-effect: nextRpts = nil      ← ALL DAG tasks cancelled for this loop
│                                               (finally tasks can still proceed via STEP 3)
│
│  ┌─────────────────────────────────────────────────────────────────────┐
│  │ STEP 3 — Check if main DAG is done → unlock finally tasks           │
│  └─────────────────────────────────────────────────────────────────────┘
│
├─ fNextRpts = GetFinalTasks()
│    uses:    pipelineRunFacts.State               ← completedOrSkippedDAGTasks(): all isDone()?
│    uses:    pipelineRunFacts.FinalTasksGraph     ← finally nodes not yet started
│    uses:    pipelineRunFacts.GetPipelineTaskStatus() ← injects task outcome context into finally params
│    └─ for each finally task:
│         ResolveResultRef() + ApplyTaskResults()  uses: pipelineRunFacts.State (reads TaskRun.Status.Results)
│         EvaluateCEL()                            uses: rpt.PipelineTask.When (CEL expressions)
│    produces: fNextRpts                           → []*ResolvedPipelineTask (finally tasks only)
│    side-effect: nextRpts = append(nextRpts, fNextRpts) ← finally tasks join the creation loop
│
│  ┌─────────────────────────────────────────────────────────────────────┐
│  │ STEP 4 — Timestamps and workspace wiring                            │
│  └─────────────────────────────────────────────────────────────────────┘
│
├─ setFinallyStartedTimeIfNeeded()
│    uses:    pipelineRunFacts.IsFinalTaskStarted()
│    produces: pr.Status.FinallyStartTime          ← written to PipelineRun status
│              pipelineRunFacts.TimeoutsState.FinallyStartTime  ← for finally timeout enforcement
│
├─ ApplyResultsToWorkspaceBindings()
│    uses:    pipelineRunFacts.State.GetTaskRunsResults() ← completed task result values
│    uses:    pr.Spec.Workspaces                          ← workspace binding expressions
│    side-effect: pr.Spec.Workspaces updated with resolved dynamic paths
│
│  ┌─────────────────────────────────────────────────────────────────────┐
│  │ STEP 5 — Create run objects for every ready non-skipped task        │
│  └─────────────────────────────────────────────────────────────────────┘
│
└─ for each rpt in nextRpts:        ← nextRpts (STEP 1) + fNextRpts (STEP 3)
    │
    ├─ rpt.Skip(facts) / rpt.IsFinallySkipped(facts)
    │    uses:   pipelineRunFacts   ← re-evaluates when: + IsStopping() + timeout state
    │    if skipped → continue      (no object created; task recorded in pr.Status.SkippedTasks)
    │
    ├─ PropagateResults(rpt, state)
    │    uses:   pipelineRunFacts.State              ← reads upstream TaskRun.Status.Results
    │    uses:   rpt.PipelineTask.Params             ← scans for $(tasks.X.results.Y)
    │    side-effect: rpt.PipelineTask.Params updated ← literal values substituted in-place
    │                                                   (creation in STEP 5 uses substituted params)
    │
    ├─ PropagateArtifacts(rpt, state)
    │    uses:   pipelineRunFacts.State              ← reads upstream artifact metadata
    │    side-effect: rpt artifact bindings resolved
    │
    ├─ ValidateParameterTypesInMatrix()  [if matrixed]
    │    uses:   pipelineRunFacts.State + rpt.PipelineTask.Matrix
    │
    └─ switch on task type:
        │
        ├─ rpt.IsChildPipeline() → createChildPipelineRuns()
        │    uses:  rpt          ← fully substituted params (from PropagateResults above)
        │    uses:  pr           ← owner reference, namespace, taskRunSpecs
        │    produces: PipelineRun object written to K8s API
        │              rpt.ChildPipelineRuns updated  ← facts updated with the new child run
        │
        ├─ rpt.IsCustomTask()   → createCustomRuns()
        │    uses:  rpt, pr, pipelineRunFacts
        │    produces: CustomRun object written to K8s API
        │              rpt.CustomRuns updated
        │
        └─ default              → createTaskRuns()
             uses:  rpt         ← PipelineTask.Params (substituted), Retries, Matrix combos
             uses:  pr          ← GetTaskRunSpec(): merges taskRunTemplate + per-task overrides
             produces: TaskRun object written to K8s API
                         Name      = deterministic "<pr-name>-<task-name>"
                         OwnerRef  → PipelineRun  (auto-GC if PR deleted)
                         Spec.Retries from PipelineTask.Retries
                       rpt.TaskRuns updated           ← facts updated with the new TaskRun
```

---

### 3.6 Skip reasons and their effects

**Source:** `pkg/reconciler/pipelinerun/resources/pipelinerunresolution.go` — `Skip()` method and `skipBecause*` functions.

| Skip reason              | What causes it                                 | Cascades to children? | Notes                                                         |
|--------------------------|------------------------------------------------|-----------------------|---------------------------------------------------------------|
| `WhenExpressionsSkip`    | Task's own `when:` condition evaluated to false| **No**                | Children run if their other deps are met. Intentional skip.   |
| `ParentTasksSkip`        | A parent was skipped for a non-`when` reason   | **Yes**               | Detected by `skipBecauseParentTaskWasSkipped()`.              |
| `StoppingSkip`           | `IsStopping()` is true (a sibling failed)      | **Yes**               | Prevents new tasks from starting while pipeline is failing.   |
| `MissingResultsSkip`     | A result ref from upstream couldn't be resolved| **Yes**               | The upstream task failed before emitting the result.          |
| `PipelineRunTimeoutSkip` | A timeout was reached                          | **Yes**               |                                                               |
| `GracefullyStoppedSkip`  | User requested `StoppedRunFinally`             | **Yes** (DAG only)    |                                                               |

**`skipBecauseParentTaskWasSkipped()` logic:**

```go
// For each ancestor of the current task in the DAG:
//   if ancestor.Skip().SkippingReason != WhenExpressionsSkip
//   → current task must also be skipped (ParentTasksSkip)
```

This ensures only `WhenExpressionsSkip` is "transparent" — all other skip reasons propagate downward through the DAG.

**Worked example:**

```
A ──► B ──► C
A ──► D

Scenario 1: A skipped due to when: condition → WhenExpressionsSkip
  B and C: NOT skipped (run normally if their other deps are met)
  D: NOT skipped

Scenario 2: A failed (IsStopping = true)
  B: StoppingSkip
  C: ParentTasksSkip (because B is StoppingSkip, not WhenExpressionsSkip)
  D: StoppingSkip

Scenario 3: A succeeded but emitted no result that B needs
  B: MissingResultsSkip
  C: ParentTasksSkip
  D: runs normally (no dep on the missing result)
```

---

### 3.7 Result propagation

```go
// pkg/reconciler/pipelinerun/resources/apply.go
ApplyTaskResults(pipelineRunState, resolvedResultRefs)
PropagateResults(rpt, state)
```

**`ResolveResultRef(state, rpt)`:**
1. Scans `rpt.PipelineTask.Params` for `$(tasks.<name>.results.<result>)` expressions.
2. Finds the `ResolvedPipelineTask` for `<name>` in `state`.
3. Reads the result value from `taskRun.Status.Results`.
4. Returns a `ResolvedResultRef` mapping expression → value.

**`ApplyTaskResults(state, resolvedResultRefs)`:**
- Substitutes the resolved values into param expressions in the target task's spec.

**`PropagateResults(rpt, state)`:**
- Handles embedded `taskSpec` tasks (inline Task definitions in a Pipeline). Results propagate through the `PipelineTask` param expressions.

**Critical implication for partial retry:** Result values live in `TaskRun.Status.Results`, not in the `PipelineRun`. As long as a succeeded TaskRun still exists in `ChildReferences` with its status intact, downstream tasks can get their result values on a retry without re-running the upstream task.

---

### 3.8 Finally tasks

**Gate function:** `GetFinalTasks()` in `pipelinerunstate.go`.

```go
func (facts *PipelineRunFacts) GetFinalTasks() PipelineRunState {
    // Only return finally tasks when ALL main DAG tasks are done
    if !facts.completedOrSkippedDAGTasks() {
        return nil
    }
    // Return finally tasks that are not yet done (not yet started or running)
    ...
}
```

**`completedOrSkippedDAGTasks()`** returns true when every task in `facts.TasksGraph` has `isDone() == true` (succeeded, failed, or skipped for any reason).

**`FinallyStartTime`:** Set on `PipelineRun.Status` the first time a finally task is created. Used to enforce the `finally` timeout independently of the `tasks` timeout. If `finallyStartTime` is set, `ReconcileKind` uses it for requeue scheduling.

**Finally task skip:** A finally task is skipped (`IsFinallySkipped()`) if:
- `IsStopping()` is true AND `pr.IsGracefullyCancelled()` is true — the user requested graceful cancel, so finally tasks run. But if `pr.IsCancelled()` (hard cancel), finally tasks are skipped.
- The finally timeout has been reached.

---

## Part 4 — Known Implementation Constraints

This section documents **non-obvious problems** in the current Tekton implementation that are directly relevant to implementing partial PipelineRun retry. These are not bugs — they are deliberate design choices that create constraints a retry feature must work around.

---

### 4.1 PVC cleanup race condition

#### Problem

When a PipelineRun is marked `Failed`, the reconciler calls `cleanupAffinityAssistantsAndPVCs()` **synchronously in the same reconcile loop** — before any user has a chance to request a retry. By the time the user sees the failure, the PVC (and the data written by succeeded tasks) may already be deleted.

```
t=0   "test" task fails
t=1   PipelineRun condition set to False
t=2   cleanupAffinityAssistantsAndPVCs() runs → PVC deleted
t=3   user sees the failure notification
t=4   user requests partial retry           ← TOO LATE, PVC is gone
```

Whether the PVC is deleted depends on the cluster's AffinityAssistant mode:

| AA mode | PVC fate on failure | Partial retry possible? |
|---|---|---|
| `AffinityAssistantPerPipelineRun` | **Always deleted** (PVC owned by StatefulSet VolumeClaimTemplate) | No, without mitigation |
| `AffinityAssistantPerWorkspace` | Survives unless `tekton.dev/auto-cleanup-pvc=true` is set | Yes (default config) |
| `AffinityAssistantDisabled` | Survives (Tekton creates PVC directly, no auto-delete) | Yes |
| User-provided `persistentVolumeClaim` | **Never deleted** by Tekton | Always yes |

#### Solution options

**Option A1 — Opt-in retention flag (simplest, recommended for v1)**

User declares intent at PipelineRun creation time:
```yaml
spec:
  retainWorkspacesOnFailure: true
```
The reconciler skips PVC cleanup on failure when this flag is set. No race condition because the decision is made upfront. Downside: user must anticipate failure before the run starts.

**Option A2 — Grace period / deferred cleanup**

Instead of deleting the PVC immediately, the reconciler stamps a `cleanup-after` timestamp (e.g. 24h) on the PipelineRun and requeues. The PVC is only deleted when the deadline expires. Gives the user a window to request a retry without requiring upfront opt-in. More complex to implement.

**Option A3 — Finalizer on the PVC**

A Kubernetes finalizer is added to the PVC at creation time. Even if the reconciler issues a delete, Kubernetes will not remove the PVC until the finalizer is released. The retry controller removes the finalizer only after the retry completes (or after a configurable timeout). Most robust; most implementation effort.

**Option B — Restrict to safe modes (simplest, no cleanup change needed)**

Validate at retry-request time that the cluster is in `PerWorkspace` or `Disabled` AA mode and that `auto-cleanup-pvc=true` is not set. Reject the retry request otherwise with a clear error message. Workable for v1 since most production clusters with shared storage use `PerWorkspace` mode.

#### Recommended approach

Start with **Option B** for v1 (mode restriction + validation). Track **Option A1** as a follow-up to lift the restriction — it requires a new `spec` field but no change to the cleanup timing logic.

---

### 4.2 Deterministic TaskRun naming causes name conflicts on retry

#### Problem

TaskRun names are computed deterministically at creation time:

```
taskRunName = "<pipelinerun-name>-<pipelinetask-name>"
// e.g. "pr1-build"
```

Source: `GetTaskRunName()` in `pkg/reconciler/pipelinerun/resources/pipelinerunresolution.go`.

The old failed TaskRun (`pr1-build`) still exists in the cluster after failure — it is not deleted by cleanup. If a partial retry tries to create a new TaskRun with the same name, the Kubernetes API returns `409 Conflict`. The reconciler handles this by treating 409 as a no-op and reading the existing object — meaning it would see the old *failed* TaskRun, not a new one, and make no progress.

#### Implication

The partial retry mechanism cannot simply remove a `ChildReference` entry and let the reconciler re-create the TaskRun with the same name. It must either:
- Generate a new deterministic name for retry TaskRuns (e.g. `<pr-name>-<task-name>-retry-1`), or
- Delete the old failed TaskRun before re-scheduling (loses audit history), or
- Create a new PipelineRun object that references the same Pipeline but carries forward the succeeded tasks' state (the "new PR" approach).

This naming constraint is one of the strongest arguments for the **new PipelineRun approach** over in-place mutation of an existing one.

---

### 4.3 Task-level retries are transparent — only exhausted failures are visible

#### Problem (or clarification)

When a TaskRun fails and has remaining retries (`spec.retries > 0`), the TaskRun reconciler handles the retry internally:
1. Appends current failed status to `retriesStatus[]`
2. Resets `conditionSucceeded` to `Unknown`
3. Clears `podName`

From the PipelineRun reconciler's perspective the task is just "still running" — it never sees the individual retry attempts. Only when all retries are exhausted does the task appear as `Failed` (`conditionSucceeded = False, permanent`).

#### Implication for partial retry

By the time a user requests a partial retry, every task is in a clean terminal state — there are no "partially-retried" tasks. This **simplifies** the failed subgraph computation: you never need to handle ambiguous mid-retry state.

However, there is a design decision on **retry budget for the new run**: does the re-scheduled TaskRun get a fresh budget (retries = PipelineTask.Retries) or a reduced one (retries = PipelineTask.Retries - attempts_used)? Most prior art defaults to a fresh budget.

---

### 4.4 Result values live in TaskRun.Status, not in PipelineRun

#### Problem

Result propagation (`$(tasks.clone.results.sha)`) works by reading `TaskRun.Status.Results` on every reconcile loop. The values are **not** copied into `PipelineRun.Status`. This means:

- If a succeeded TaskRun is deleted, its results are gone — downstream tasks in the retry would fail to resolve their param expressions.
- If the succeeded TaskRun still exists in `ChildReferences`, its results are available to the retry without re-running the upstream task.

#### Implication

The partial retry mechanism must preserve succeeded `ChildReferences` entries (and their corresponding TaskRun objects) intact. Only the failed/skipped tasks' `ChildReferences` should be removed or replaced. This is actually **good news** — it means result re-injection is free if the TaskRun objects survive.

> **Unsupported edge case:** If a user manually deletes a succeeded TaskRun object from the cluster after the PipelineRun has failed (e.g. via `kubectl delete taskrun pr1-clone`), its results are gone — the retry will fail to resolve any downstream `$(tasks.clone.results.*)` expressions. Partial retry after manual TaskRun deletion is explicitly **not supported**.

Source: `PropagateResults()` and `ResolveResultRef()` in `pkg/reconciler/pipelinerun/resources/apply.go` and `resultrefresolution.go`.

---

### 4.5 `ChildReferences` is the single source of truth — removing an entry re-schedules a task

#### How it works

The stateless reconciler derives all runtime state from `pr.Status.ChildReferences` on every loop. If an entry for a task is absent, `resolvePipelineState()` returns a `ResolvedPipelineTask` with `TaskRuns = nil`, and `DAGExecutionQueue()` will include it as a candidate to schedule (if its dependencies are done).

#### Implication

Removing a `ChildReference` entry is the **correct lever** to make the reconciler re-schedule a task — but only if the naming conflict (4.2) is also resolved. Together, these two constraints mean a partial retry implementation must:

1. Remove the `ChildReference` for the failed task.
2. Either delete the old failed TaskRun, or use a new name for the retry TaskRun so the 409 is avoided.
3. Keep all succeeded tasks' `ChildReferences` intact so results remain accessible (4.4).

---

### 4.6 Skip cascade affects which tasks count as "needing retry"

#### Problem

When a task fails with `onError: stopAndFail` (the default), `IsStopping()` becomes true and downstream tasks are skipped with `StoppingSkip` or `ParentTasksSkip`. These skipped tasks are *victims* of the failure — they never ran, not because of their own conditions.

The failed subgraph for a partial retry is therefore not just the directly-failed task but also all its downstream victims:

```
clone  → succeeded
test   → FAILED  (onError: stopAndFail)
build  → StoppingSkip   ← victim, must also re-run
deploy → ParentTasksSkip ← victim, must also re-run
notify → WhenExpressionsSkip ← intentional skip, should NOT re-run
```

`WhenExpressionsSkip` is the only skip reason that is **transparent** — it does not cascade to children and those children should not be re-run just because of the partial retry.

#### Implication

The failed subgraph computation must:
- Re-run: tasks with `conditionSucceeded = False` + tasks skipped with any reason **other than** `WhenExpressionsSkip`.
- Not re-run: tasks skipped with `WhenExpressionsSkip` (they made a deliberate conditional decision).
- Evaluate fresh: tasks whose `when:` condition might now evaluate differently given new params or context.

---

### 4.7 `onError: continue` tasks complicate the failed subgraph

#### How `onError: continue` works (two independent mechanisms)

**Mechanism 1 — `IsStopping()` (`pipelinerunstate.go` line 420):**

```go
func (facts *PipelineRunFacts) IsStopping() bool {
    for _, t := range facts.State {
        if facts.isDAGTask(t.PipelineTask.Name) {
            if t.isFailure() && t.PipelineTask.OnError != v1.PipelineTaskContinue {
                return true
            }
        }
    }
    return false
}
```

A failed task with `onError: continue` is **explicitly excluded** from this check. `IsStopping()` stays `false` → no `StoppingSkip` is applied to siblings or children → the pipeline keeps scheduling new work.

**Mechanism 2 — result availability (`pipelinerunresolution.go` line 545):**

`skipBecauseResultReferencesAreMissing()` checks whether a downstream task can get the result values it needs. If a result cannot be found it skips the downstream task — but **only** if the downstream task itself has `onError: continue`, is a finally task, or the upstream was `WhenExpressionsSkip`. It does **not** check the upstream's `onError`.

This means: if an `onError: continue` task fails without emitting a result, downstream tasks that reference that result are **not skipped** — they enter `CheckMissingResultReferences()` in `runNextSchedulableTask()` (line 1007, `pipelinerun.go`) and get added to `ValidationFailedTask`. The pipeline eventually fails.

---

#### The three concrete scenarios

```
test (onError: continue) → FAILS
badge-gen (downstream, needs test.results.coverage)
```

**Scenario A — `test` fails but DID emit a result before failing:**

| Step | What happens |
|---|---|
| `IsStopping()` | `false` — pipeline does not stop |
| `ResolveResultRefs(badge-gen)` | Finds `coverage` in `test`'s `TaskRun.Status.Results` → `err = nil` |
| `ApplyTaskResults()` | Substitutes the value into `badge-gen`'s params |
| `badge-gen` | Runs normally; may succeed |

**Scenario B — `test` fails WITHOUT emitting a result:**

| Step | What happens |
|---|---|
| `IsStopping()` | `false` |
| `skipBecauseResultReferencesAreMissing(badge-gen)` | `err != nil` but `badge-gen.OnError != continue` → returns `false` → badge-gen NOT skipped |
| `DAGExecutionQueue()` | badge-gen enters the queue |
| `CheckMissingResultReferences(badge-gen)` | `test.isSuccessful() = false` → generic error returned (not `MissingResultFromCompletedTaskError`) |
| badge-gen | Added to `ValidationFailedTask`; `nextRpts = nil`; pipeline eventually marked **Failed** |

**Scenario C — `test` fails, `badge-gen` has only an ordering dep (`runAfter: [test]`, no result ref):**

| Step | What happens |
|---|---|
| `IsStopping()` | `false` |
| `skipBecauseResultReferencesAreMissing(badge-gen)` | Not triggered — badge-gen has no result refs |
| `badge-gen` | Runs normally; can succeed ✅ |

**In plain English:** `onError: continue` only prevents `IsStopping()`. It does not conjure a result that doesn't exist. The result-availability gate is independent and still applies.

---

#### Implications for partial retry

| Scenario | State after original run | In re-run set? | Rationale |
|---|---|---|---|
| A — upstream failed, emitted result, downstream succeeded | downstream: `conditionSucceeded = True` | ❌ No (by default) | Downstream genuinely succeeded. However, its output may be **stale** if upstream re-runs and emits a different result value. This is the only truly ambiguous case. |
| B — upstream failed, no result, downstream ValidationFailed | downstream: `isValidationFailed = True` → pipeline failed | ✅ Yes | Functionally a failure — the downstream never actually ran. |
| C — upstream failed, downstream had ordering dep only | downstream: `conditionSucceeded = True` | ❌ No | Output is fully independent of upstream's result. Genuinely reusable. |

**Scenario A is the only real design problem.** It is the case where a succeeded task's output could become stale after a retry of the upstream. The recommended default is to **not** re-run the downstream (preserve its succeeded state) and document this as a known limitation: if the retried upstream emits a different result, the downstream's prior output may be inconsistent. This matches the behavior of most CI systems (GitHub Actions, Argo) which never retroactively invalidate completed jobs.

An advanced future policy could re-run any succeeded task whose result inputs came from a task in the re-run set — but this is explicitly a v2 concern.

#### Open design decision

The re-run policy for the **upstream** `onError: continue` task itself:
- **Always include** in re-run set: re-run it regardless of `onError`. Clean; avoids inconsistency with Scenario A.
- **Never include**: the pipeline already declared this failure acceptable; retrying changes semantics.
- **User-controlled** via a flag on the retry request.

This must be explicitly resolved in the TEP before implementation.

---

### 4.8 Finally tasks need special handling on retry

#### Problem

`finally` tasks run once when the main DAG is complete — regardless of success or failure. On a partial retry:

- If a finally task **succeeded** in the original run → it has a `ChildReference` → `isDone()` returns true → it will **not** re-run. This is correct.
- If a finally task **failed** in the original run → should the retry re-run it?
- If a finally task was **skipped** (hard cancel, timeout) → should the retry re-run it?
- `FinallyStartTime` is set on `pr.Status` and drives the finally timeout. On a retry, this timestamp is stale — if it is not reset, the finally timeout may already be expired.

#### Implication

The retry must decide the policy for finally tasks and handle `FinallyStartTime` reset. The simplest v1 approach: include failed finally tasks in the retry subgraph, reset `FinallyStartTime` to nil so the timeout starts fresh.

---

### 4.9 Matrix tasks — granularity of the failed subgraph

#### How matrix works

Matrix is Tekton's fan-out mechanism. One `PipelineTask` with a `matrix:` field produces **N TaskRuns in parallel**, one per param combination:

```yaml
tasks:
  - name: test
    matrix:
      params:
        - name: platform
          value: [linux, windows, mac]
        - name: go-version
          value: ["1.21", "1.22"]
# → 6 TaskRuns: pr1-test-0 … pr1-test-5
```

All N TaskRuns share the same `pipelineTaskName: "test"` in `ChildReferences` and are collected into `ResolvedPipelineTask.TaskRuns []` (a slice, not a single item).

#### Failure semantics (asymmetric)

```
isSuccessful() → true only if ALL N TaskRuns succeeded
isFailure()    → true if ANY TaskRun failed AND all others are done
```

From the PipelineRun reconciler's perspective, the whole `test` task is either succeeded or failed — there is no concept of "partially failed matrix" at the task level.

#### The partial retry granularity problem

If 4 of 6 matrix combinations succeeded and 2 failed:

```
pr1-test-0  linux/1.21   → succeeded
pr1-test-1  linux/1.22   → succeeded
pr1-test-2  windows/1.21 → FAILED
pr1-test-3  windows/1.22 → FAILED
pr1-test-4  mac/1.21     → succeeded
pr1-test-5  mac/1.22     → succeeded
```

There are two possible retry policies:

**Policy A — Re-run only failed combinations (granular)**
Remove only `ChildReferences` for `pr1-test-2` and `pr1-test-3`. The reconciler would need to create only the `windows` combinations, not all 6. Problem: `GetNamesOfTaskRuns()` derives names for all N combinations based on `Matrix.CountCombinations()` — there is no mechanism today to fan out a subset. Requires new scheduling logic.

**Policy B — Re-run all N combinations (simple)**
Remove all 6 `ChildReferences` for `test`. The reconciler fans out all 6 again. 4 combinations re-run unnecessarily but the existing fan-out logic is reused completely unchanged.

#### Naming conflict multiplier

The 409 conflict problem from constraint 4.2 is multiplied: for a matrix task with N combinations, there are N name collisions to resolve (`pr1-test-0` through `pr1-test-N-1`), not just one.

#### Recommended approach

- **v1:** Policy B — re-run all combinations when any combination fails. Simple, no new scheduling logic, consistent with the existing task-level failure view.
- **Future:** Policy A — track which specific combinations failed (requires matrix index in `ChildReference`) and re-run only those.

---

## Related Documents

- **Design Proposals:** `design/proposal.md` (Parts 5-11)
- **Prior Art Analysis:** `research/prior-art-analysis.md`
- **Architecture Decision Record:** `research/adr-new-object-model.md`
