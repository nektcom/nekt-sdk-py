"""Reading a Delta table must not kill the process at exit.

deltalake 1.x's ``to_pyarrow_table`` scans through a Rust-backed filesystem from
pyarrow worker threads; the process then aborts at exit on Linux ("terminate
called without an active exception", exit 134) or hangs on macOS. The provider
scans on the calling thread instead. The check has to run in a child process:
the failure happens during interpreter shutdown.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest

pytest.importorskip("deltalake")


def _run(code: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "-c", textwrap.dedent(code)], capture_output=True, text=True, timeout=60)


def test_process_exits_cleanly_after_load(tmp_path):
    path = str(tmp_path / "t")
    result = _run(
        f"""
        import pyarrow as pa
        from deltalake import write_deltalake
        from nekt.provider.delta import DeltaProvider
        write_deltalake({path!r}, pa.table({{"id": [1, 2], "p": ["a", "b"]}}), partition_by=["p"])
        table = DeltaProvider().load({path!r})
        print(sorted(table.column_names), table.num_rows)
        """
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "['id', 'p'] 2"  # output survives: nothing lost to an abort


def test_load_keeps_to_pyarrow_table_options(tmp_path):
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake

    from nekt.provider.delta import DeltaProvider

    path = str(tmp_path / "t")
    write_deltalake(path, pa.table({"id": [1, 2, 3], "v": ["a", "b", "c"], "p": ["x", "x", "y"]}), partition_by=["p"])

    table = DeltaProvider().load(path, columns=["id", "v"], filters=[("p", "=", "x")])
    assert table.column_names == ["id", "v"]
    assert sorted(table["id"].to_pylist()) == [1, 2]
    # Same schema as the call it replaces.
    assert DeltaProvider().load(path).schema == DeltaTable(path).to_pyarrow_dataset().schema
