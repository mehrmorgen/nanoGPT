# High-Performance Library Alternatives

<details>
<summary>Related documentation</summary>

- [ml_playground Developer Guidelines](../.dev-guidelines/Readme.md) – Advanced contributors working on the ml_playground module must follow these binding rules exactly.
- [Centralized Framework Utilities](./framework_utilities.md) – Describes shared utilities that standardize error handling, progress reporting, and file operations across experiments.

</details>

This guide catalogs performance-focused libraries that complement or extend our current stack. Each option is categorized by the workflow segment it accelerates, paired with fit considerations, and grounded in the practices mandated by the developer guidelines.

## Data Processing and Analytics

| Library                            | Primary Gains                                                           | Ideal Scenarios                                                                | Integration Notes                                                                        |
| ---------------------------------- | ----------------------------------------------------------------------- | ------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------- |
| **Polars**                         | Multi-threaded query engine, lazy execution, Arrow-native memory layout | Compute-heavy feature engineering, dataset statistics at preparation time      | Already integrated for token statistics; extend usage where columnar workloads dominate. |
| **Apache Arrow & PyArrow Compute** | Zero-copy interchange, vectorized kernels                               | Interop between Polars, pandas, and on-disk columnar formats (Parquet/Feather) | Use to keep data in Arrow buffers when moving across libraries or persisting artifacts.  |
| **Vaex**                           | Out-of-core DataFrame operations, memory-mapped storage                 | Feature pipelines that exceed RAM but fit on disk                              | Requires export to HDF5/FITS; ideal for read-heavy analytics without shuffling.          |
| **RAPIDS cuDF**                    | GPU-accelerated DataFrame operations                                    | NVIDIA GPU environments, large-scale preprocessing, joins, aggregations        | Mirrors pandas/Polars APIs; pair with Dask-cuDF for multi-GPU scaling.                   |
| **Modin (Ray or Dask backend)**    | Distributed DataFrame API with minimal code changes                     | Gradual scaling of pandas-style pipelines                                      | Swap pandas import for Modin to parallelize workloads when cluster resources exist.      |
| **Dask DataFrame**                 | Task-based parallelism, cluster execution                               | Custom pipelines that mix array, DataFrame, and graph workloads                | Gives fine-grained control; pair with Dask.delayed for bespoke compute graphs.           |
| **DuckDB**                         | Vectorized OLAP engine with embedded execution                          | Local analytics that need SQL expressiveness without spinning up a cluster     | Query Arrow/Parquet data in place; pipe results into Polars or PyTorch preprocessing.    |
| **DataFusion**                     | Rust-native query engine with Python bindings                           | When we need a portable, Arrow-based SQL engine for embedded workloads         | Shares Arrow memory with Polars; good for compiling repeatable query plans.              |

## Model Training and Distributed Optimization

| Library                     | Primary Gains                                            | Ideal Scenarios                                                              | Integration Notes                                                                                           |
| --------------------------- | -------------------------------------------------------- | ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| **PyTorch Lightning**       | Structured training loop abstraction, auto-acceleration  | Rapid prototyping with consistent training semantics                         | Already wired into configs; extend with callbacks/checkpointing for experiments needing rich logging.       |
| **Lightning Fabric**        | Lightweight orchestration without full Trainer           | Custom loops that still benefit from mixed precision, distributed setup      | Good bridge between handcrafted loops and full Lightning adoption.                                          |
| **DeepSpeed**               | ZeRO optimizer sharding, pipeline/tensor parallelism     | Training large language models that exceed single-node memory                | Requires DeepSpeed config files; integrate via PyTorch Lightning `DeepSpeedStrategy` or native engine.      |
| **Hugging Face Accelerate** | Simplified multi-GPU/TPU orchestration, device placement | Experiments where we keep manual training loops but need distributed support | Minimal API changes; wrap our training script with `accelerate launch`.                                     |
| **Ray Train**               | Elastic scaling, cluster fault tolerance                 | Running hyperparameter sweeps or elastic workloads on Ray clusters           | Works with Ray Tune for search; integrates with Lightning or vanilla PyTorch modules.                       |
| **Colossal-AI**             | Parallelism strategies, memory optimization primitives   | Scaling transformers with operator-level optimizations                       | Offers hybrid parallelism; evaluate when experimenting with model parallel architectures.                   |
| **JAX + Flax/Equinox**      | XLA compilation, TPU-first performance                   | Research projects needing jit/pmapped kernels and TPU utilization            | Requires porting model definitions; consider for experimental branches where JAX ecosystems offer benefits. |
| **Hidet**                   | Ahead-of-time kernel generation with PyTorch front end   | PyTorch 2.x training needing custom fusion beyond TorchInductor              | Wraps `torch.compile`; validate kernels with our quality gates before promotion.                            |
| **Alpa**                    | Automatic parallelization for massive models             | TPU/GPU clusters where manual parallel strategy design is a bottleneck       | Integrates with JAX programs; prototype in research branches before sharing configs.                        |

## Serving, Compilation, and Inference Tooling

| Library                             | Primary Gains                                                        | Ideal Scenarios                                                    | Integration Notes                                                                                         |
| ----------------------------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------- |
| **TorchInductor / `torch.compile`** | Graph capture, fused kernels, backend pluggability                   | PyTorch 2.x training/inference with repetitive kernels             | Activate via `torch.compile` in existing modules; verify numerical parity before rollout.                 |
| **ONNX Runtime**                    | Cross-framework inference acceleration, hardware execution providers | Deployment targets requiring CPU, GPU, or specialized accelerators | Export checkpoints to ONNX via PyTorch exporters; integrate with Triton for serving.                      |
| **TensorRT**                        | NVIDIA-optimized inference kernels, quantization                     | Latency-sensitive inference on NVIDIA GPUs                         | Combine with ONNX or Torch-TensorRT for conversion pipelines.                                             |
| **OpenVINO**                        | Intel-optimized inference, edge deployment                           | CPU-first inference, VPU/FPGA targets                              | Use ONNX export path; good for Intel hardware stacks.                                                     |
| **TVM / Relax**                     | Auto-tuned compilation, heterogeneous targets                        | When we need portable, optimized kernels across CPU/GPU/edge       | Requires ahead-of-time compilation; best for stable models needing maximum performance.                   |
| **NVIDIA Triton Inference Server**  | Production-grade serving with dynamic batching                       | Centralized model hosting with mixed hardware accelerators         | Pair with ONNX Runtime or TensorRT backends; keep configs versioned in TOML per guidelines.               |
| **vLLM**                            | Paged-attention serving optimized for LLMs                           | High-throughput text generation workloads                          | Export checkpoints via Hugging Face transformers; monitor memory footprints with our profiling playbooks. |

## Evaluation and Experiment Management

| Library                            | Primary Gains                                          | Ideal Scenarios                                             | Integration Notes                                                                             |
| ---------------------------------- | ------------------------------------------------------ | ----------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| **Weights & Biases Artifacts**     | Cached dataset/model versioning, streaming metrics     | Collaborative research where artifact lineage matters       | Works alongside Lightning's loggers; enables differential metric queries.                     |
| **Comet ML Offline Mode**          | Efficient local metric logging with later sync         | Air-gapped or resource-constrained experiments              | Provide drop-in experiment tracking with minimal overhead.                                    |
| **MLflow with Databricks Runtime** | Managed experiment tracking, scalable serving          | Enterprises running on Databricks with GPU/CPU clusters     | Supports Lightning autologging; pair with MLflow Model Registry.                              |
| **Aim**                            | Lightweight experiment tracker with performant UI      | Teams needing self-hosted, GPU-efficient experiment logging | Offers Lightning callback integration; align run metadata with our config naming conventions. |
| **Neptune**                        | Scalable metadata store with hierarchical organization | Long-running research programs with many derived artifacts  | Configure projects via environment overrides to stay within TOML-first policy.                |

## Profiling and Diagnostics

| Library                               | Primary Gains                                   | Ideal Scenarios                                                    | Integration Notes                                                                    |
| ------------------------------------- | ----------------------------------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------------------ |
| **PyTorch Profiler with TensorBoard** | Timeline analysis, operator-level hotspots      | Debugging regressions in new kernels or alternative training loops | Automate runs via Lightning callbacks; export traces alongside experiment artifacts. |
| **scalene**                           | CPU/GPU/memory sampling profiler                | Squeezing performance from Python-heavy preprocessing              | Run via `uv run scalene <script.py>` to respect the UV-only workflow.                |
| **viztracer**                         | Visualization of Python call stacks and timings | Diagnosing control-flow overhead before rewriting kernels          | Wrap CLI entrypoints with viztracer to confirm suspected bottlenecks.                |
| **nsys / nvprof**                     | Low-level GPU kernel profiling                  | CUDA-heavy pipelines with mysterious stalls                        | Capture traces within dedicated branches; attach summaries to experiment READMEs.    |

## Selecting the Right Path

1. **Define the bottleneck** — instrument the existing pipeline first to confirm whether we are CPU-, GPU-, or IO-bound.
1. **Check compatibility** — validate hardware availability, licensing, and alignment with our TOML-first configuration policy.
1. **Prototype in isolation** — branch from `main`, build a thin vertical slice using the alternative library, and keep commits runnable per the developer guidelines.
1. **Instrument and compare** — quantify speedups, resource utilization, and developer effort using the profiling tools above.
1. **Plan for rollout** — capture API differences, resume/rollback strategies, and configuration updates in experiment READMEs, then run `make quality` before requesting review.

Use this catalog to seed discovery tickets or spikes, keeping experiment documentation in sync so successful pilots can graduate into supported pathways.
