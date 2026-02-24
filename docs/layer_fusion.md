# Layer Fusion in PyTorch

Layer fusion combines multiple operations into one execution unit to reduce kernel launch overhead and memory traffic.
In practice, this is one of the most reliable ways to improve model throughput and latency.

## Motivation

- Reduce memory bandwidth pressure by keeping intermediate values in registers/shared memory instead of writing every step to device memory.
- Lower kernel launch overhead by replacing many small kernels with fewer larger kernels.
- Improve cache locality and execution efficiency on CPU, CUDA, and MPS backends.
- Create cleaner critical paths for latency-sensitive inference.

## What Gets Fused Most Often

- Linear/Convolution + Bias + Activation (for example, `Linear + GELU` or `Conv + BatchNorm` at inference).
- Elementwise chains such as `add + mul + activation`.
- Attention blocks where pointwise transforms around matmuls are merged (e.g., `Scaled Dot-Product Attention` or SDPA).
  - Modern attention (FlashAttention or memory-efficient attention) replaces $QK^T$, softmax, dropout, and matrix multiply with a single fused kernel.
- Normalization + affine + activation (backend-dependent).
- Quantization/Dequantization workflows (e.g., fetching int8/int4 weights and dequantizing them directly in the shared memory of a fused Linear kernel).

## Automatic vs Manual Fusion

- Automatic fusion:
  - Use `torch.compile(...)` and backend compilers (Inductor) to fuse supported graphs.
  - Lowest engineering cost, but fusion quality depends on graph shape and backend maturity.
  - Can be tuned via config values like `mode="reduce-overhead"` (best for launch-bound/small batches) or `mode="max-autotune"` (most aggressive fusion, compiles different Triton kernels to pick the best).
- Manual fusion:
  - Write fused module logic directly or use custom kernels (e.g., writing [OpenAI Triton](https://github.com/triton-lang/triton) kernels).
  - Higher engineering cost, but gives predictable behavior in hot paths when `torch.compile` falls short.

Use automatic fusion first, then manually fuse only confirmed bottlenecks.

## Manual Fusion: Practical Workflow

1. Profile first.
   - Identify true hot paths with PyTorch profiler and backend traces.
   - Confirm the candidate is launch-bound or memory-bound.
1. Choose safe candidates.
   - Prefer short, deterministic op chains with stable tensor shapes.
   - Avoid fusing sections that depend on dynamic control flow.
1. Re-express adjacent ops in one module/function.
   - Keep semantics identical to the unfused baseline.
1. Validate numerical parity.
   - Compare outputs with tolerance checks (`allclose` with dtype-aware tolerances).
1. Benchmark before/after.
   - Measure latency, tokens/s, memory, and compile overhead (if using `torch.compile`).
1. Keep a fallback.
   - Allow fast rollback to unfused/eager paths for debugging and backend regressions.

## Example 1: Fuse `Linear + GELU` into One Module Boundary

This does not create a custom kernel by itself, but it creates a fusion-friendly boundary and reduces Python-level graph
fragmentation.

```python
import torch
import torch.nn as nn
import torch.nn.functional as F


class FusedLinearGELU(nn.Module):
    def __init__(self, in_features: int, out_features: int, bias: bool = True) -> None:
        super().__init__()
        self.weight = nn.Parameter(torch.empty(out_features, in_features))
        self.bias = nn.Parameter(torch.empty(out_features)) if bias else None
        nn.init.xavier_uniform_(self.weight)
        if self.bias is not None:
            nn.init.zeros_(self.bias)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return F.gelu(F.linear(x, self.weight, self.bias))
```

Pair this with `torch.compile(model, backend="inductor")` to let the compiler fuse deeper where supported.

## Example 2: Inference-Time `Conv + BatchNorm` Folding

At inference, BatchNorm can be folded into Conv weights/bias. This removes one runtime layer.

```python
import torch


def fold_conv_bn_params(
    conv_w: torch.Tensor,
    conv_b: torch.Tensor | None,
    bn_weight: torch.Tensor,
    bn_bias: torch.Tensor,
    bn_running_mean: torch.Tensor,
    bn_running_var: torch.Tensor,
    bn_eps: float,
) -> tuple[torch.Tensor, torch.Tensor]:
    if conv_b is None:
        conv_b = torch.zeros_like(bn_running_mean)

    inv_std = torch.rsqrt(bn_running_var + bn_eps)
    scale = bn_weight * inv_std

    w_folded = conv_w * scale.reshape(-1, 1, 1, 1)
    b_folded = (conv_b - bn_running_mean) * scale + bn_bias
    return w_folded, b_folded
```

Use this only for evaluation/export paths unless you explicitly manage training-time BN behavior.

## Backend Notes (CPU, CUDA, MPS)

- CPU: fusion helps mostly by reducing memory movement and improving vectorized paths.
- CUDA: biggest wins often come from reducing kernel count and improving occupancy.
- MPS: fusion support has improved, but coverage is still evolving. Keep stable fallbacks and benchmark on target Apple hardware.

## Validation Checklist

- Functional parity:
  - Same logits/loss within tolerance on representative batches.
  - *Tip:* Validate numerical parity in the target mixed-precision dtype (e.g., `bfloat16`), as fusion can change the order of operations and therefore the precision bounds.
- Performance:
  - Median and tail latency improved for target batch sizes.
- Resource usage:
  - No unexpected memory spikes or compilation stalls.
- Operational safety:
  - Fallback path exists and is tested.

## Configuring for Maximum Performance

When experimenting with new architectures in `ml_playground`, layer fusion isn't just about code—it's heavily influenced by your configuration values (batch size, block size, vocab size).

### 1. Batch Size and Micro-batching
- **Launch-Bound:** If your batch size is very small (e.g., 1 or 2 during inference), the GPU spends more time launching kernels than executing them. Fusion is highly critical here to compress the launch overhead.
- **Memory/Compute-Bound:** If your batch size is large, you are bottlenecked by memory bandwidth or raw math. Fusion helps here by keeping data in SRAM. 
- **Tip:** If a fused kernel runs out of memory (OOM), prioritize shrinking the *micro-batch size* and using gradient accumulation rather than dropping the fusion.

### 2. Block Size (Context Length)
Attention mechanisms scale quadratically $O(N^2)$ with block size.
- Fusing attention (FlashAttention/SDPA) changes the memory complexity from $O(N^2)$ to $O(N)$, which is why it is the most critical fusion in modern architectures.
- Always configure your model to use SDPA if your block size exceeds ~512 tokens. 

### 3. Vocab Size and Tensor Core Padding
Matrix multiplications (like the final linear layer into the vocabulary projection) run drastically faster if the inner dimensions are aligned to hardware boundaries.
- **Rule of Thumb:** Always pad your vocabulary size to a multiple of 64 or 128 (e.g., pad GPT-2's `50257` to `50304`).
- Fused Cross-Entropy kernels assume the unpadded elements are masked out, allowing the Tensor Cores to operate at maximum efficiency without adding mathematical noise to the loss.

### 4. Calculating Model Flops Utilization (MFU)
To know if your configuration and fusions are actually working, calculate your MFU. MFU measures the percentage of the GPU's theoretical peak performance you are actually achieving.

```python
# Pseudo-code for calculating MFU
flops_per_token = 6 * number_of_parameters + (12 * layers * heads * head_dim * block_size)
flops_per_fwdbwd = flops_per_token * batch_size * block_size
tokens_per_sec = batch_size * block_size / time_per_iteration

observed_tflops = (flops_per_token * tokens_per_sec) / 1e12
mfu = observed_tflops / gpu_peak_tflops  # e.g., 312 TFLOPS for A100 bfloat16
```
Aim for >50% MFU on well-optimized, fused transformer architectures.

## Measuring and Profiling Fusion Performance

To accurately measure if a fusion was successful, observe these specific metrics using the PyTorch Profiler or `nsys` (NVIDIA Nsight Systems):

1. **Kernel Count:** The total number of kernels launched per forward/backward pass should decrease.
2. **Launch Overhead vs Execution Time:** 
   - A GPU is "launch-bound" when the time between kernel executions is longer than the execution itself. Fusion solves this.
   - Look at the *GPU Active Time* vs *CPU Launch Time*. 
3. **Memory Throughput:**
   - Look at DRAM read/write volume. A good fusion will dramatically drop intermediate memory writes.
4. **Effective TFLOPS & Throughput:**
   - Measure tokens/second or samples/second locally.
   - Use `torch.utils.benchmark` with warmup cycles rather than a simple `time.time()`.

```python
import torch.utils.benchmark as benchmark

t0 = benchmark.Timer(
    stmt='fused_module(x)',
    setup='from __main__ import fused_module, x',
    globals={'fused_module': fused_module, 'x': x}
)
print(t0.blocked_autorange(min_run_time=2.0))
```

## Common Failure Modes

- Fusing without profiling and optimizing cold paths.
- Ignoring numeric drift from changed op ordering/dtype behavior.
- Over-fusing dynamic sections that break compiler assumptions.
- Comparing only one batch size and missing regressions at production sizes.

## Recommended Pattern for `ml_playground`

- Keep baseline modules simple and readable.
- Add optional fused variants behind config flags for hot paths.
- Gate fused code paths with quick parity tests and micro-benchmarks.
- Prefer compiler-assisted fusion first, then selective manual fusion where profiling proves value.
