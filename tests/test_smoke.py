def test_import_nekt():
    import nekt
    assert hasattr(nekt, "__version__")


def test_version_string():
    import nekt
    assert isinstance(nekt.__version__, str)
    assert nekt.__version__ == "0.4.0"
