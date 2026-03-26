"""Data processing engines for the Nekt SDK."""

from nekt.engine.base import Engine

# PythonEngine is optional (requires pyarrow)
try:
    from nekt.engine.python import PythonEngine
except ImportError:
    PythonEngine = None  # type: ignore[assignment,misc]

# SparkEngine is optional (requires pyspark)
try:
    from nekt.engine.spark import SparkEngine
except ImportError:
    SparkEngine = None  # type: ignore[assignment,misc]

__all__ = ["Engine", "PythonEngine", "SparkEngine"]
