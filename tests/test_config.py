import os
import pytest


def test_require_raises_runtime_error_when_var_missing(monkeypatch):
    monkeypatch.delenv("MISSING_VAR_XYZ", raising=False)
    import config as cfg
    with pytest.raises(RuntimeError, match="MISSING_VAR_XYZ"):
        cfg._require("MISSING_VAR_XYZ")


def test_require_returns_value_when_var_is_set(monkeypatch):
    monkeypatch.setenv("SOME_TEST_VAR", "hello")
    import config as cfg
    assert cfg._require("SOME_TEST_VAR") == "hello"
