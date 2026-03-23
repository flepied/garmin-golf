from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch

from garmin_golf.config import Settings, default_config_template, get_config_file


def test_settings_load_from_xdg_config(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    config_home = tmp_path / "xdg"
    config_dir = config_home / "garmin-golf"
    config_dir.mkdir(parents=True)
    (config_dir / "config.toml").write_text(
        'garmin_email = "file@example.com"\n'
        'garmin_password = "from-file"\n'
        f'data_dir = "{tmp_path / "data-from-file"}"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config_home))

    settings = Settings()

    assert settings.garmin_email == "file@example.com"
    assert settings.garmin_password == "from-file"
    assert settings.data_dir == tmp_path / "data-from-file"


def test_env_overrides_xdg_config(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        'garmin_email = "file@example.com"\n'
        'garmin_password = "from-file"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("GARMIN_GOLF_CONFIG_FILE", str(config_file))
    monkeypatch.setenv("GARMIN_GOLF_GARMIN_PASSWORD", "from-env")

    settings = Settings()

    assert settings.garmin_email == "file@example.com"
    assert settings.garmin_password == "from-env"


def test_get_config_file_uses_override(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    config_file = tmp_path / "custom.toml"
    monkeypatch.setenv("GARMIN_GOLF_CONFIG_FILE", str(config_file))

    assert get_config_file() == config_file


def test_default_config_template_contains_expected_keys() -> None:
    template = default_config_template()

    assert 'garmin_email = "you@example.com"' in template
    assert 'garmin_password = "replace-me"' in template


def test_settings_token_dir_uses_config_parent(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("GARMIN_GOLF_CONFIG_FILE", str(config_file))

    settings = Settings()

    assert settings.token_dir == tmp_path / "tokens"
