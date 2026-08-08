# Model Registry and Caching Directory Structure Rules

This document defines the rules and structure for registering and maintaining model weights, task configurations, and hardware runtime specifications across the LLM Observability Platform.

---

## Core Rules
1. **Zero Weight Baking**: Large model weights must **never** be baked directly into the application container build layers. Images must remain lightweight (< 1.0GB for CPU).
2. **Dynamic Caching**: Container runtimes must download model weights dynamically at startup/runtime or load them from mapped persistent volume cache mounts (e.g., mapping `~/.cache/huggingface` to the container's `/root/.cache/huggingface`).
3. **Registry Declaration**: Every ML model utilized across packages must be documented under the root `models/` directory using the standard namespace layout.


---

## Registry Folder Structure Layout

Below is the detailed directory map of the model registry, shepherding rules, and consumer packages:

```text
.

├── models/                             
│   ├── <type of model>/          
│   │   └── <model name>/  
│   │       ├── model.yaml            
│   │       └── README.md  
│   │
│   └── README.md
```

---

## Model Specification Schema (`model.yaml`)

Every registered model directory must contain a `model.yaml` matching this specification:

```yaml
id: ""                              # Unique Hugging Face repository identifier
version: ""                         # Semantic version or commit hash pin of model weights
task: ""                            # ML Task (sequence-classification, token-classification, causal-lm)
framework: ""                       # Primary runtime framework (pytorch, onnx, tensorflow)
precision: ""                       # Quantization precision (fp32, fp16, int8)
size_bytes:                         # Weight size on disk in bytes
parameters:                         # Active parameter count
sha256: ""                          # SHA-256 checksum for download verification
max_sequence_length:                # Max tokens model context window accepts
vocab_size:                         # Number of vocabulary tokens
license: ""                         # Model open-source or proprietary license type

source:
  registry: ""                       # Source registry (huggingface, local, custom-s3)
  url: ""                            # Registry source url

deployment:
  memory_limit_bytes:                # Minimum recommended RAM allocation for execution (1GiB)
  hardware_platforms:                # Supported platform runtimes
    - ""
  volume_mounts:                     # Required persistent directory mapping
    host_path: ""
    container_path: ""

metadata:
  owner: ""
  description: ""
```

---

## Detailed Configuration Parameters

Every `model.yaml` registry file requires these configurations:
- **`id` (string)**: The exact model repository identifier (Hugging Face ID or model path) that the runtime loaders query.
- **`version` (string)**: Pinned semantic version (e.g. `1.0.0`) or git commit SHA of model weights to prevent runtime drifts.
- **`task` (string)**: The machine learning task type. Must be one of `sequence-classification`, `token-classification`, or `causal-lm`.
- **`framework` (string)**: The software execution framework. Must be one of `pytorch`, `onnx`, or `tensorflow`.
- **`precision` (string)**: The numerical data precision. Commonly `fp32`, `fp16`, or `int8`.
- **`size_bytes` (integer)**: The size of model weights in bytes on disk, used for forecasting volume storage allocations.
- **`parameters` (integer)**: Active model parameter count.
- **`sha256` (string)**: The SHA-256 checksum of the main model binary weight file to verify integrity during dynamic download.
- **`max_sequence_length` (integer)**: Context window length supported by the model (e.g. `512` or `1024`).
- **`vocab_size` (integer)**: Total vocabulary size of the model's tokenizer.
- **`license` (string)**: Model licensing details (e.g. `apache-2.0`, `mit`, `openrail`).
- **`source` (object)**:
  - **`registry` (string)**: Target weights registry (e.g. `huggingface`, `onnx-model-zoo`, `custom-s3`).
  - **`url` (string)**: Complete repository URL for downloading model weights.
- **`deployment` (object)**:
  - **`memory_limit_bytes` (integer)**: The minimum container memory size (in bytes) needed to run model loading and inference without triggering OOM-kills.
  - **`hardware_platforms` (list of strings)**: Runtime hardware devices supported (`cpu`, `cuda`, `mps`).
  - **`volume_mounts` (object)**:
    - **`host_path` (string)**: Host system volume path to mount.
    - **`container_path` (string)**: Container destination path to mount for Hugging Face cache persistence.
- **`metadata` (object)**:
  - **`owner` (string)**: Slack owner tag or team handles for package support.
  - **`description` (string)**: Detailed operational context and use cases for the registered model.


