# Tekton Partial PipelineRun Retry - Research & Design
Research and design documents for implementing partial PipelineRun retry functionality in Tekton Pipelines.
## Overview
This repository contains research, architectural decisions, and design evaluations for a partial PipelineRun retry feature that allows users to re-run only failed portions of a Pipeline execution without repeating successful tasks.
## Contents
### Phase 1: Research & Architecture (Completed)
- [**Architecture Analysis**](research/architecture-analysis.md) - Tekton internals, scheduling logic, implementation constraints
- [**Prior Art Analysis**](research/prior-art-analysis.md) - Industry comparison (GitHub Actions, GitLab CI, Argo, etc.)
- [**ADR: Execution Model**](research/adr-new-object-model.md) - Decision to use new PipelineRun per retry
### Phase 2-3: Design Evaluations (In Progress)
- [**Consolidated Research**](research/partial-pipelinerun-retry.md) - Complete technical deep-dive
## Related Links
- **Upstream Tracking**: [Tekton Pipeline Issue #XXXX](https://github.com/tektoncd/pipeline/issues/XXXX)
- **TEP**: To be submitted to [tektoncd/community](https://github.com/tektoncd/community)
- **Internal Tracking**: Red Hat Jira SRVKP-14121
## Status
🔬 **Research Phase** - Actively developing design proposals and evaluating implementation approaches.
## Contributing
This is active research. Feedback and questions are welcome via GitHub issues.
## License
Apache 2.0 (aligned with Tekton project)
