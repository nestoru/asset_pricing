# asset_pricing/fx/spot/config.py

from pydantic import BaseModel, root_validator
from typing import List, Dict, Union, Optional
import json
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DatabaseConfig(BaseModel):
    host: str
    port: int
    database: str
    user: str
    password: str
    db_schema: str  # Renamed to avoid shadowing BaseModel.schema()

class SparkConfig(BaseModel):
    app_name: str
    master: str
    config: Dict[str, str]  # Additional Spark configurations

class RemoteCaching(BaseModel):
    storage_account_type: str  # e.g., "azure", "s3", "gcs"
    storage_account_name: str
    storage_account_key: str
    container_name: str
    blob_path: str  # e.g., "data/remote/spot"

class MockedFXSpotProfile(BaseModel):
    profile_name: str
    api_type: str
    tickers: List[str]
    since: datetime
    local_caching_path: Optional[str] = None
    remote_caching: Optional[RemoteCaching] = None
    seed_data_path: Optional[str] = None

class AlphaVantageFXSpotProfile(BaseModel):
    profile_name: str
    api_type: str
    tickers: List[str]
    since: datetime
    local_caching_path: Optional[str] = None
    remote_caching: Optional[RemoteCaching] = None
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    polling_interval: Optional[int] = None

SpotFXProfile = Union[MockedFXSpotProfile, AlphaVantageFXSpotProfile]

class SpotFXConfig(BaseModel):
    profiles: List[SpotFXProfile]

    @root_validator(pre=True)
    def check_caching(cls, values):
        profiles = values.get('profiles', [])
        for profile in profiles:
            has_local = 'local_caching_path' in profile and profile['local_caching_path']
            has_remote = 'remote_caching' in profile and profile['remote_caching']
            if has_local == has_remote:
                raise ValueError(f"Profile '{profile.get('profile_name')}' must have either local_caching_path or remote_caching, but not both.")
        return values

class FXConfig(BaseModel):
    spot: SpotFXConfig

class AssetPricingBatchConfig(BaseModel):
    fx: FXConfig

class Config(BaseModel):
    spark: SparkConfig
    asset_pricing_batch: AssetPricingBatchConfig
    database: DatabaseConfig

    def get_spot_profile(self, profile_name: str) -> SpotFXProfile:
        for profile in self.asset_pricing_batch.fx.spot.profiles:
            if profile.profile_name == profile_name:
                return profile
        raise ValueError(f"Profile '{profile_name}' not found.")

def load_config(config_file: str) -> Config:
    if not os.path.exists(config_file):
        logger.error(f"Configuration file {config_file} not found.")
        raise FileNotFoundError(f"Configuration file {config_file} not found.")
    with open(config_file, 'r') as f:
        config_dict = json.load(f)
    return Config(**config_dict)
