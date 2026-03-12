"""Data processing engines for the Nekt SDK."""

from nekt.engine.base import Engine, StubResponse

# PythonEngine is optional (requires pyarrow)
try:
    from nekt.engine.python import PythonEngine
except ImportError:
    PythonEngine = None  # type: ignore[assignment,misc]

__all__ = ["Engine", "PythonEngine", "StubResponse"]
