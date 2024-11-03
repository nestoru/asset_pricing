# asset_pricing/fx/spot/fetch.py

import sys
import os
import json
import logging
import random
from datetime import datetime, timedelta, time as datetime_time  # Add timedelta & rename time
from typing import Union

import requests
from azure.storage.blob import BlobServiceClient

from asset_pricing.fx.spot.config import (
    load_config,
    MockedFXSpotProfile,
    AlphaVantageFXSpotProfile,
)
from asset_pricing.fx.spot.models import FXSpotRate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_fx_spot_rates(config_file: str, profile_name: str) -> None:
    config = load_config(config_file)
    try:
        profile = config.get_spot_profile(profile_name)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    if isinstance(profile, MockedFXSpotProfile):
        run_mocked_fetch(profile)
    elif isinstance(profile, AlphaVantageFXSpotProfile):
        run_alphavantage_fetch(profile)
    else:
        logger.error(f"Unsupported profile type for '{profile_name}'")
        sys.exit(1)

def run_mocked_fetch(profile: MockedFXSpotProfile) -> None:
    # Set up storage
    if profile.local_caching_path:
        output_path = profile.local_caching_path
        os.makedirs(output_path, exist_ok=True)
    elif profile.remote_caching:
        remote = profile.remote_caching
        blob_service = BlobServiceClient(
            account_url=f"https://{remote.storage_account_name}.blob.core.windows.net",
            credential=remote.storage_account_key
        )
        container_client = blob_service.get_container_client(remote.container_name)
        try:
            container_client.create_container()
        except Exception as e:
            logger.warning(f"Container might already exist: {e}")
    else:
        logger.error("No caching configuration found.")
        sys.exit(1)

    # Load seed data
    if not os.path.exists(profile.seed_data_path):
        logger.error(f"Seed data file {profile.seed_data_path} not found.")
        sys.exit(1)
    
    with open(profile.seed_data_path, 'r') as f:
        seed_data = json.load(f)

    symbols = seed_data.get('symbols', profile.tickers)
    base_rates = seed_data.get('base_rates', {symbol: 1.0 for symbol in symbols})

    # Generate daily quotes from since date until today
    current_date = datetime.now()
    days_to_generate = (current_date.date() - profile.since.date()).days + 1
    
    quote_count = 0
    logger.info(f"Generating daily quotes from {profile.since.date()} to {current_date.date()}")
    
    for symbol in symbols:
        base_rate = base_rates.get(symbol, 1.0)
        last_rate = base_rate  # Track last rate for more realistic movements
        
        for day_offset in range(days_to_generate):
            quote_date = profile.since.date() + timedelta(days=day_offset)
            if quote_date > current_date.date():
                break

            # Skip weekends
            if quote_date.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
                continue

            # Generate a random daily variation (-0.5% to +0.5%) from previous rate
            daily_variation = random.uniform(-0.005, 0.005)
            rate = last_rate * (1 + daily_variation)
            last_rate = rate  # Update last rate for next iteration

            # Create quote at market open (9:00 AM)
            quote_datetime = datetime.combine(quote_date, datetime_time(9, 0))
            
            fx_rate = FXSpotRate(
                symbol=symbol,
                date=quote_datetime,
                rate=round(rate, 6)
            )

            data = fx_rate.dict()
            if profile.local_caching_path:
                file_name = f"{fx_rate.symbol.replace('/', '_')}_{fx_rate.date.strftime('%Y%m%d%H%M%S%f')}.json"
                file_path = os.path.join(profile.local_caching_path, file_name)
                with open(file_path, 'w') as f:
                    json.dump(data, f, default=str)
            elif profile.remote_caching:
                blob_path = f"{profile.remote_caching.blob_path}/{fx_rate.symbol.replace('/', '_')}_{fx_rate.date.strftime('%Y%m%d%H%M%S%f')}.json"
                blob_client = blob_service.get_blob_client(
                    container=profile.remote_caching.container_name, 
                    blob=blob_path
                )
                try:
                    blob_client.upload_blob(json.dumps(data, default=str), overwrite=True)
                except Exception as e:
                    logger.error(f"Failed to upload blob: {e}")
            
            quote_count += 1

    logger.info(f"Generated {quote_count} daily FX spot rate quotes.")

def run_alphavantage_fetch(profile: AlphaVantageFXSpotProfile) -> None:
    if profile.local_caching_path:
        output_path = profile.local_caching_path
        os.makedirs(output_path, exist_ok=True)
    elif profile.remote_caching:
        remote = profile.remote_caching
        blob_service = BlobServiceClient(
            account_url=f"https://{remote.storage_account_name}.blob.core.windows.net",
            credential=remote.storage_account_key
        )
        container_client = blob_service.get_container_client(remote.container_name)
        try:
            container_client.create_container()
        except Exception as e:
            logger.warning(f"Container might already exist: {e}")
    else:
        logger.error("No caching configuration found.")
        sys.exit(1)

    api_key = profile.api_key
    base_url = profile.base_url
    tickers = profile.tickers
    polling_interval = profile.polling_interval

    logger.info(f"Starting AlphaVantage polling for FX spot rates every {polling_interval} seconds.")
    try:
        while True:
            total_fetched = 0
            for pair in tickers:
                fx_rate = fetch_alphavantage_data(pair, api_key, base_url)
                if fx_rate:
                    data = fx_rate.dict()
                    if profile.local_caching_path:
                        file_name = f"{fx_rate.symbol.replace('/', '_')}_{fx_rate.date.strftime('%Y%m%d%H%M%S%f')}.json"
                        file_path = os.path.join(profile.local_caching_path, file_name)
                        with open(file_path, 'w') as f:
                            json.dump(data, f, default=str)
                    elif profile.remote_caching:
                        blob_path = f"{profile.remote_caching.blob_path}/{fx_rate.symbol.replace('/', '_')}_{fx_rate.date.strftime('%Y%m%d%H%M%S%f')}.json"
                        blob_client = blob_service.get_blob_client(
                            container=profile.remote_caching.container_name, 
                            blob=blob_path
                        )
                        try:
                            blob_client.upload_blob(json.dumps(data, default=str), overwrite=True)
                        except Exception as e:
                            logger.error(f"Failed to upload blob: {e}")
                    total_fetched += 1
            logger.info(f"Fetched and saved {total_fetched} FX spot rates.")
            time.sleep(polling_interval)
    except KeyboardInterrupt:
        logger.info("Polling stopped by user.")

def fetch_alphavantage_data(pair: str, api_key: str, base_url: str) -> Union[FXSpotRate, None]:
    from_currency, to_currency = pair.split('/')
    params = {
        "function": "CURRENCY_EXCHANGE_RATE",
        "from_currency": from_currency,
        "to_currency": to_currency,
        "apikey": api_key
    }
    try:
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            if "Realtime Currency Exchange Rate" in data:
                exchange_rate = data["Realtime Currency Exchange Rate"]
                fx_rate = FXSpotRate(
                    symbol=f"{from_currency}/{to_currency}",
                    date=datetime.now(),
                    rate=float(exchange_rate["5. Exchange Rate"])
                )
                logger.debug(f"Fetched FX spot rate: {fx_rate}")
                return fx_rate
            elif "Note" in data or "Information" in data:
              message = data.get("Note", data.get("Information"))
              logger.error(f"API Error: {message}")
              sys.exit(1)  # Exit on rate limit or other API errors
            else:
                logger.error(f"Unexpected response for {pair}: {data}")
                return None
        else:
            logger.error(f"HTTP error {response.status_code} fetching data for {pair}")
            return None
    except Exception as e:
        logger.error(f"Exception occurred while fetching data for {pair}: {e}")
        return None

def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: fetch-fx-spot <config_file> <profile_name>")
        sys.exit(1)
    config_file = sys.argv[1]
    profile_name = sys.argv[2]
    fetch_fx_spot_rates(config_file, profile_name)

if __name__ == "__main__":
    main()
