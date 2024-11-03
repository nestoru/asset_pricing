# asset_pricing/fx/spot/process.py

import sys
import os
import logging
from typing import Union
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from asset_pricing.fx.spot.config import (
    load_config,
    MockedFXSpotProfile,
    AlphaVantageFXSpotProfile,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark_session(spark_config) -> SparkSession:
    builder = SparkSession.builder.appName(spark_config.app_name)

    # Set master if specified
    if spark_config.master:
        builder = builder.master(spark_config.master)

    # Set other Spark configurations
    config_dict = spark_config.config
    for key, value in config_dict.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    return spark

def process_fx_spot_rates(config_file: str, profile_name: str) -> None:
    config = load_config(config_file)
    try:
        profile = config.get_spot_profile(profile_name)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    spark = get_spark_session(config.spark)

    try:
        if isinstance(profile, (MockedFXSpotProfile, AlphaVantageFXSpotProfile)):
            process_fx_data(spark, profile, config.database)
        else:
            logger.error(f"Unsupported profile type for '{profile_name}'")
            sys.exit(1)
    finally:
        spark.stop()

def process_fx_data(spark, profile, db_config) -> None:
    # Set up JDBC connection properties
    jdbc_url = f"jdbc:postgresql://{db_config.host}:{db_config.port}/{db_config.database}"
    connection_properties = {
        "user": db_config.user,
        "password": db_config.password,
        "driver": "org.postgresql.Driver"
    }

    # Get latest processed data from database
    latest_timestamps = spark.read.jdbc(
        url=jdbc_url,
        table=f"""(SELECT 
                    REPLACE(symbol, '/', '_') as symbol_key,
                    TO_CHAR(MAX(date), 'YYYYMMDDHH24MISSMS') as last_processed 
                  FROM {db_config.db_schema}.fx_spot_rates 
                  GROUP BY symbol) as latest""",
        properties=connection_properties
    ).collect()

    latest_by_symbol = {row['symbol_key']: row['last_processed'] for row in latest_timestamps}

    # Determine the data path
    if profile.local_caching_path:
        data_path = profile.local_caching_path
    elif profile.remote_caching:
        remote = profile.remote_caching
        if remote.storage_account_type.lower() == "azure":
            # Set up Azure storage configuration
            storage_account = remote.storage_account_name
            container = remote.container_name
            data_path = f"wasbs://{container}@{storage_account}.blob.core.windows.net/{remote.blob_path}"
            spark.conf.set(
                f"fs.azure.account.key.{storage_account}.blob.core.windows.net",
                remote.storage_account_key
            )
        else:
            logger.error(f"Unsupported storage_account_type: {remote.storage_account_type}")
            sys.exit(1)
    else:
        logger.error("No caching configuration found.")
        sys.exit(1)

    # Read all JSON files from the path
    df = spark.read.json(f"{data_path}/*.json")

    # Check if DataFrame is empty
    if df.rdd.isEmpty():
        logger.info(f"No data found in {data_path}")
        return

    # Cast columns to appropriate types
    df = df.withColumn("date", col("date").cast("timestamp"))
    df = df.withColumn("rate", col("rate").cast("double"))

    # Remove duplicates within the new data
    df = df.dropDuplicates(["symbol", "date"])

    # Read existing data from database for exact duplicate check
    existing_data_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"{db_config.db_schema}.fx_spot_rates",
        properties=connection_properties
    )

    # Anti-join to get only new records
    df_to_insert = df.join(
        existing_data_df,
        (df.symbol == existing_data_df.symbol) & 
        (df.date == existing_data_df.date),
        "left_anti"
    )

    # If nothing new to insert, return
    if df_to_insert.rdd.isEmpty():
        logger.info("No new unique records to insert")
        return

    try:
        record_count = df_to_insert.count()
        # Write to PostgreSQL using JDBC
        df_to_insert.write.jdbc(
            url=jdbc_url,
            table=f"{db_config.db_schema}.fx_spot_rates",
            mode="append",
            properties=connection_properties
        )
        logger.info(f"Successfully inserted {record_count} new records")
    except Exception as e:
        logger.error(f"Failed to write data to database: {e}")
        sys.exit(1)

def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: process-fx-spot <config_file> <profile_name>")
        sys.exit(1)
    
    config_file = sys.argv[1]
    profile_name = sys.argv[2]
    
    try:
        process_fx_spot_rates(config_file, profile_name)
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
