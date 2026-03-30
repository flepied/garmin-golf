from __future__ import annotations

import os
import re
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

    data_dir: Path = Field(default=Path("data"))
    club_name_overrides: dict[str, str] = Field(default_factory=dict)
    raw_dir_name: str = "raw"
    parquet_dir_name: str = "parquet"
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
        '# data_dir = "/home/you/garmin-golf-data"\n'
        '\n# Optional club-name overrides keyed by Garmin club_id.\n'
        '# Use this when Garmin club type inference does not match your actual bag.\n'
        '# [club_name_overrides]\n'
        '# "10400964" = "5 Wood"\n'
        '# "10400967" = "3 Hybrid"\n'
        '# "10400977" = "56 Wedge"\n'
    )


def set_club_name_override(config_file: Path, club_id: int, club_name: str) -> bool:
    content = config_file.read_text(encoding="utf-8") if config_file.exists() else ""
    lines = content.splitlines()

    section_header = "[club_name_overrides]"
    entry = f'"{club_id}" = "{_toml_basic_string(club_name)}"'
    section_index = next(
        (index for index, line in enumerate(lines) if line.strip() == section_header),
        None,
    )

    if section_index is None:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append(section_header)
        lines.append(entry)
        config_file.parent.mkdir(parents=True, exist_ok=True)
        config_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return False

    entry_pattern = re.compile(rf'^\s*"{club_id}"\s*=')
    insert_at = len(lines)
    for index in range(section_index + 1, len(lines)):
        stripped = lines[index].strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            insert_at = index
            break
        if entry_pattern.match(lines[index]):
            lines[index] = entry
            config_file.parent.mkdir(parents=True, exist_ok=True)
            config_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
            return True

    lines.insert(insert_at, entry)
    config_file.parent.mkdir(parents=True, exist_ok=True)
    config_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return False


def _toml_basic_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')
