# Performance Review: Preparer, Trainer, and Sampler Workflows

This report highlights hotspots where the current implementations rely on Python-level loops or data wrangling that could be replaced by faster primitives from vectorized libraries (NumPy/PyTorch) or specialized tokenization utilities.

## Preparer Workflow

- **Vocabulary construction in `prepare_with_tokenizer`** – The char/word branches rebuild vocabularies with `sorted(set(...))` and dictionary comprehensions over Python iterables.【F:ml_playground/data_pipeline/transforms/tokenization.py†L39-L55】  On large corpora this becomes a Python-bound (O(n log n)) routine.  Replacing it with `numpy.unique`, `pandas.factorize`, or a tokenization library that surfaces built-in vocab extraction (e.g., Hugging Face `tokenizers`) would leverage optimized C/Rust back-ends and avoid repeated Python object allocations.

## Trainer Workflow

- **Random batch sampling path** – When `L <= block_size`, `_take_seq` builds each training example inside a Python list comprehension, invoking NumPy array creation `batch_size` times and performing wrap-around in Python.【F:ml_playground/data_pipeline/sampling/batches.py†L33-L51】  Even in the `L > block_size` branch we repeatedly slice in a comprehension.【F:ml_playground/data_pipeline/sampling/batches.py†L52-L62】  Both paths could be rewritten with vectorized indexing: `np.lib.stride_tricks.sliding_window_view` or `np.take` on a precomputed index matrix would let NumPy handle the heavy lifting in C.
- **Sequential sampler implementation** – `_get_sequential_batch` appends each sequence in a Python `for` loop, juggling multiple branches to handle wrap-around and concatenating fragments manually.【F:ml_playground/data_pipeline/sampling/batches.py†L125-L173】  Constructing an index grid with `np.arange` and reshaping or using `sliding_window_view` can eliminate the per-example Python loop, producing the full `bsz × T` batch in one vectorized call.
- **Gradient accumulation loop** – `_train_step` performs gradient accumulation with a pure-Python loop over `grad_accum_steps`.【F:ml_playground/training/loop/runner.py†L285-L305】  When accumulation steps are high, this loop becomes Python-bound.  Leveraging PyTorch utilities such as `torch.vmap`/`torch.compile` or fused gradient-accumulation kernels (e.g., through `accelerate` or custom CUDA ops) would reduce Python overhead per micro-step.

## Sampler Workflow

- **Prompt tensor construction** – Converting the starting prompt IDs with `torch.tensor(...)[None, ...]` materializes a new tensor from a Python list every run.【F:ml_playground/sampling/runner.py†L167-L176】  Using `torch.as_tensor` (which can share memory) or precomputing prompts as device tensors avoids repeated Python-to-C copies when sampling multiple prompts.
- **Decoding generated tokens** – Each sample calls `y[0].tolist()` before decoding, incurring a Python loop over every generated token.【F:ml_playground/sampling/runner.py†L180-L189】  If the tokenizer accepts NumPy arrays or PyTorch tensors, routing through `y[0].cpu().numpy()` or a vectorized decoder (from libraries like `tokenizers`) would shift the heavy conversion work into optimized code and cut Python overhead for long generations.

---
Prioritizing these areas should yield noticeable wins because they sit directly on the critical paths for dataset preparation, batch delivery during training, and text generation.
