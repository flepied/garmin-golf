from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch

from garmin_golf.config import Settings, default_config_template, get_config_file


def test_settings_load_from_xdg_config(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    config_home = tmp_path / "xdg"
    config_dir = config_home / "garmin-golf"
    config_dir.mkdir(parents=True)
    (config_dir / "config.toml").write_text(
        f'data_dir = "{tmp_path / "data-from-file"}"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_home))

    settings = Settings()

    assert settings.data_dir == tmp_path / "data-from-file"


def test_settings_load_club_name_overrides(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    config_home = tmp_path / "xdg"
    config_dir = config_home / "garmin-golf"
    config_dir.mkdir(parents=True)
    (config_dir / "config.toml").write_text(
        '[club_name_overrides]\n"10400977" = "56 Wedge"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_home))

    settings = Settings()

    assert settings.club_name_overrides == {"10400977": "56 Wedge"}


def test_get_config_file_uses_override(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    config_file = tmp_path / "custom.toml"
    monkeypatch.setenv("GARMIN_GOLF_CONFIG_FILE", str(config_file))

    assert get_config_file() == config_file


def test_default_config_template_contains_expected_keys() -> None:
    template = default_config_template()

    assert 'data_dir = "/home/you/garmin-golf-data"' in template
    assert "[club_name_overrides]" in template
