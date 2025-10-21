import numpy as np

from ml_playground.core.tokenizer import create_tokenizer
from ml_playground.data_pipeline.transforms.tokenization import (
    compute_token_statistics,
    prepare_with_tokenizer,
)


def test_compute_token_statistics_includes_expected_keys():
    train = np.array([1, 2, 2, 3, 3, 3], dtype=np.uint16)
    val = np.array([2, 3, 4], dtype=np.uint16)

    stats = compute_token_statistics(train, val)
    assert "polars_token_stats" in stats
    summary = stats["polars_token_stats"]
    assert summary["train"]["count"] == train.size
    assert summary["val"]["count"] == val.size
    assert summary["shared_unique_tokens"] == 2
    assert summary["train"]["top_tokens"][0]["token"] == 3


def test_prepare_with_tokenizer_appends_polars_metadata():
    tokenizer = create_tokenizer("char")
    train_arr, val_arr, meta, _ = prepare_with_tokenizer("ababaabb", tokenizer, split=0.5)

    assert meta["polars_token_stats"]["train"]["count"] == train_arr.size
    assert meta["polars_token_stats"]["val"]["count"] == val_arr.size
