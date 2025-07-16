import os
from functools import lru_cache
from pydantic import BaseSettings
import hvac

class VaultSettings(BaseSettings):
    vault_addr: str
    vault_token: str
    service_name: str

    class Config:
        env_file = ".env"

@lru_cache()
def get_vault_settings() -> VaultSettings:
    return VaultSettings()

@lru_cache()
def get_config(settings: VaultSettings = get_vault_settings()):
    client = hvac.Client(url=settings.vault_addr, token=settings.vault_token)
    
    if not client.is_authenticated():
        raise Exception("Vault authentication failed")

    mount_point = 'secret'
    secret_path = f'platformq/{settings.service_name}'
    
    try:
        secret_response = client.secrets.kv.v2.read_secret_version(
            mount_point=mount_point,
            path=secret_path,
        )
        return secret_response['data']['data']
    except hvac.exceptions.InvalidPath:
        return {}

class Settings(BaseSettings):
    # This is where you would define your service's specific configuration
    # For example:
    # database_url: str
    
    class Config:
        # This will load the settings from the get_config function
        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            return (
                init_settings,
                get_config,
                env_settings,
                file_secret_settings,
            )

@lru_cache()
def get_settings() -> Settings:
    return Settings() 