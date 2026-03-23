from __future__ import annotations

import os
from pathlib import Path

from pydantic import Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="GARMIN_GOLF_",
        env_file=".env",
        extra="ignore",
    )

    garmin_email: str | None = None
    garmin_password: str | None = None
    data_dir: Path = Field(default=Path("data"))
    raw_dir_name: str = "raw"
    parquet_dir_name: str = "parquet"
    token_dir_name: str = "tokens"
    rounds_table_name: str = "rounds"
    holes_table_name: str = "holes"
    shots_table_name: str = "shots"

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        config_settings = TomlConfigSettingsSource(settings_cls, toml_file=_default_config_file())
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            config_settings,
            file_secret_settings,
        )

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / self.raw_dir_name

    @property
    def parquet_dir(self) -> Path:
        return self.data_dir / self.parquet_dir_name

    @property
    def token_dir(self) -> Path:
        return get_config_file().parent / self.token_dir_name


def get_settings() -> Settings:
    return Settings()


def get_config_file() -> Path:
    return _default_config_file()


def _default_config_file() -> Path:
    config_override = os.getenv("GARMIN_GOLF_CONFIG_FILE")
    if config_override:
        return Path(config_override).expanduser()
    config_home = Path(os.getenv("XDG_CONFIG_HOME", "~/.config")).expanduser()
    return config_home / "garmin-golf" / "config.toml"


def default_config_template() -> str:
    return (
        "# Garmin Golf configuration\n"
        "# Environment variables still override values in this file.\n\n"
        'garmin_email = "you@example.com"\n'
        'garmin_password = "replace-me"\n'
        '# data_dir = "/home/you/garmin-golf-data"\n'
    )
