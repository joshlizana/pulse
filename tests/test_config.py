import dataclasses

import pytest

from conftest import created
from pulse.config import Config


def test_data_dir_resolves_under_home(home):
    assert Config().data_dir == home / ".local" / "share" / "pulse"


def test_construction_creates_nothing(home):
    Config()
    assert created(home) == []


def test_paths_resolve_at_construction(home, monkeypatch, tmp_path_factory):
    # The module is already imported; a path fixed at import would miss this.
    elsewhere = tmp_path_factory.mktemp("elsewhere")
    monkeypatch.setenv("HOME", str(elsewhere))
    assert Config().data_dir.is_relative_to(elsewhere)


def test_fields_are_frozen(home):
    config = Config()
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.data_dir = home


def test_fields_override_by_keyword(tmp_path):
    assert Config(data_dir=tmp_path).data_dir == tmp_path


def test_fields_are_keyword_only(tmp_path):
    with pytest.raises(TypeError):
        Config(tmp_path)
