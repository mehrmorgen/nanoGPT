from pathlib import Path
from ml_playground.tools.utils.filesystem_utils import (
    RealFilesystemOperations,
    RealJsonOperations,
)


def test_real_filesystem_operations(tmp_path: Path):
    fs = RealFilesystemOperations()
    test_file = tmp_path / "test.txt"
    test_dir = tmp_path / "subdir"

    # Test mkdir
    fs.mkdir(test_dir)
    assert fs.exists(test_dir)
    assert fs.is_dir(test_dir)

    # Test write_text and exists
    fs.write_text(test_file, "hello world")
    assert fs.exists(test_file)
    assert fs.is_file(test_file)

    # Test read_text
    assert fs.read_text(test_file) == "hello world"

    # Test stat_size
    assert fs.stat_size(test_file) == len("hello world")

    # Test iterdir
    contents = fs.iterdir(tmp_path)
    assert test_file in contents
    assert test_dir in contents

    # Test glob
    glob_results = fs.glob(tmp_path, "*.txt")
    assert test_file in glob_results

    # Test rglob
    nested_file = test_dir / "nested.txt"
    fs.write_text(nested_file, "nested")
    rglob_results = fs.rglob(tmp_path, "*.txt")
    assert test_file in rglob_results
    assert nested_file in rglob_results

    # Test unlink
    fs.unlink(test_file)
    assert not fs.exists(test_file)
    # missing_ok=True test
    fs.unlink(test_file)

    # Test rmtree
    fs.rmtree(test_dir)
    assert not fs.exists(test_dir)


def test_real_json_operations(tmp_path: Path):
    js = RealJsonOperations()
    test_file = tmp_path / "test.json"
    data = {"key": "value", "nested": [1, 2, 3]}

    # Test dump
    js.dump(data, test_file)
    assert test_file.exists()

    # Test load
    loaded = js.load(test_file)
    assert loaded == data
