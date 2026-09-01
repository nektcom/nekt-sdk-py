import types


def test_import_nekt():
    import nekt
    assert hasattr(nekt, "__version__")


def test_version_string():
    import nekt
    assert isinstance(nekt.__version__, str)
    # Pinned to the packaged version rather than a literal: the two drifted
    # (0.7.0 vs 0.8.3) precisely because a literal here made it possible.
    from importlib.metadata import version

    assert nekt.__version__ == version("nekt-sdk")


def test_nekt_module_is_module_type():
    import nekt
    assert isinstance(nekt, types.ModuleType)


def test_nekt_has_all_attribute():
    import nekt
    assert hasattr(nekt, "__all__")
    assert isinstance(nekt.__all__, list)
    assert len(nekt.__all__) == 26
