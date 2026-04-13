# -*- coding: utf-8 -*-

import json
import pathlib

from httpx import BasicAuth
from pydantic_settings import BaseSettings, SettingsConfigDict

CONFIG_DIR = pathlib.Path("config")


class Settings(BaseSettings):
    CONNECTION_TIMEOUT: int = 180

    model_config = SettingsConfigDict(
        env_file=CONFIG_DIR / "efi_agent.ini",
    )


settings = Settings()


def get_credentials(prefix: str) -> dict:
    result = {}
    credentials_path = CONFIG_DIR / 'credentials.json'
    with credentials_path.open() as f:
        credentials = json.load(f)
    for creds in credentials:
        if creds['prefix'] == prefix:
            break
    else:
        raise RuntimeError(f"Did not find credentials for prefix {prefix}")
    base_url = creds['base_url']
    if base_url.endswith('/'):
        result['base_url'] = base_url
    else:
        result['base_url'] = f"{base_url}/"
    result['auth'] = BasicAuth(creds['username'], creds['password'])
    return result
