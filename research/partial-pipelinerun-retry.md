# Partial PipelineRun Retry — Research & TEP Draft

**Jira Epic:** [SRVKP-14121](https://redhat.atlassian.net/browse/SRVKP-14121)
**Foundational spike:** [SRVKP-14219](https://redhat.atlassian.net/browse/SRVKP-14219)
**Repo:** `github.com/tektoncd/pipeline`
**Status:** Research in progress

---

## Table of Contents

1. [Part 1 — Custom Resources (the data model)](#part-1--custom-resources-the-data-model)
2. [Part 2 — Controllers (the runtime)](#part-2--controllers-the-runtime)
3. [Part 3 — Scheduling logic deep-dive](#part-3--scheduling-logic-deep-dive)
4. [Part 4 — Known Implementation Constraints](#part-4--known-implementation-constraints)
5. [Part 5 — Design: Failed Subgraph Computation Rules](#part-5--design-failed-subgraph-computation-rules)
6. [Part 6 — Design: Result Re-injection (Memoization)](#part-6--design-result-re-injection-memoization)
7. [Part 7 — Design Evaluation: Retry Planning Boundary](#part-7--design-evaluation-retry-planning-boundary)
8. [Part 8 — Design Evaluation: Workspace PVC Handling](#part-8--design-evaluation-workspace-pvc-handling)
9. [Part 9 — Design Evaluation: Pipeline Definition Stability](#part-9--design-evaluation-pipeline-definition-stability)
10. [Part 10 — Design Evaluation: Data Availability Strategies](#part-10--design-evaluation-data-availability-strategies)
11. [Part 11 — Design Evaluation: API Shape, Security & Provenance](#part-11--design-evaluation-api-shape-security--provenance)

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

## Part 5 — Design: Failed Subgraph Computation Rules

### 5.1 Subgraph Computation Rules

The retry subset is determined by applying the following rules to the original `PipelineRunFacts` to compute which DAG tasks to re-run and which to bypass.

1. **Explicit Failures & Cancellations (The Roots):** Any DAG task whose run object has `Condition{Type: Succeeded, Status: False}` in its terminal status is added to the re-run set.
This includes `Failed`, `Cancelled`, `TaskRunTimeout`, `PipelineRunTimeout`, and infrastructure errors (e.g., `ImagePullBackOff`, `OOMKilled`).
2. **Skip Cascades (The Victims):** Any DAG task that was skipped due to a downstream cascade from a failure must be re-run.
This includes tasks with `SkippingReason` set to `ParentTasksSkip`, `StoppingSkip`, `MissingResultsSkip`, `PipelineRunTimeoutSkip`, or `GracefullyStoppedSkip`.
3. **Intentional Skips (The Bypasses):** Tasks skipped with `WhenExpressionsSkip` made a deliberate conditional decision and are **excluded** from the re-run set. In the new `PipelineRun`, their `WhenExpressions` will be re-evaluated naturally against the new context.
4. **`onError: continue` Resolution:** 
    *   **The failed task itself:** Always included in the re-run set, regardless of the pipeline's configured tolerance for it.
    *   **Scenario A (Upstream failed, emitted result, downstream succeeded):** Downstream tasks are **excluded** from the re-run set. 
    *Tradeoff:* If the retried upstream task emits a different result value, the downstream task's previously successful output becomes stale and inconsistent. This risk is accepted for v1 to align with CI/CD immutability norms.
    *   **Scenario B (Upstream failed, no result, downstream ValidationFailed):** The downstream task is **included** in the re-run set, as it functionally never ran because it lacked required inputs .
5. **Matrix Granularity (Policy B):** For v1, if any combination within a `matrix` task fails, **all N combinations** are included in the re-run set.
The entire task's `ChildReferences` are removed and re-fanned out by the controller .
6. **Finally Tasks:** Finally tasks are **excluded** from the failed subgraph calculation. Because the retry is a new `PipelineRun` object, all finally tasks execute fresh automatically after the retry's DAG phase completes.
This ensures cleanup and notification logic accurately reflects the retry's outcome, rather than preserving the original run's state. 
7. **Custom Tasks (`CustomRun`):** Apply the same terminal state rules to `CustomRun.Status.Conditions` as built-in tasks . For v1, treat the `CustomRun` as an atomic unit;
the reconciler will not recurse into nested external controllers to retry partial custom state.
8. **Nested PipelineRuns:** Apply the same terminal state rules to the child `PipelineRun.Status.Conditions`. For v1, the child `PipelineRun` is treated as an atomic unit. If a child pipeline fails, the entire nested pipeline is re-run;
the partial retry logic does not recursively traverse down into the nested DAG.


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
