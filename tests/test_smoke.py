import types


def test_import_nekt():
    import nekt
    assert hasattr(nekt, "__version__")


def test_version_string():
    import nekt
    assert isinstance(nekt.__version__, str)
    assert nekt.__version__ == "0.7.0"


def test_nekt_module_is_module_type():
    import nekt
    assert isinstance(nekt, types.ModuleType)


def test_nekt_has_all_attribute():
    import nekt
    assert hasattr(nekt, "__all__")
    assert isinstance(nekt.__all__, list)
    assert len(nekt.__all__) == 19
