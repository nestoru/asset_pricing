"""
Portfolio pricing and valuation functionality.
"""

import sys
import logging
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when, expr

from asset_pricing.fx.spot.config import load_config
from asset_pricing.fx.spot.process import get_spark_session
from asset_pricing.investors.models import (
    DBHandler,
    FXSpotPosition,
    FXSpotValuationDetail,
    PortfolioValuation
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def quantize_decimal(value: Decimal, places: int = 8) -> Decimal:
    """Round decimal to specified number of places."""
    quantum = Decimal('0.{}'.format('0' * places))
    return Decimal(str(value)).quantize(quantum, rounding=ROUND_HALF_UP)

def setup_spark_session(config) -> SparkSession:
    """Initialize Spark session with proper logging configuration."""
    spark = get_spark_session(config.spark)
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def get_spark_positions_df(
    spark: SparkSession,
    db_config: Dict,
    investor_id: int
) -> DataFrame:
    """Get all FX spot positions for an investor."""
    jdbc_url = f"jdbc:postgresql://{db_config.host}:{db_config.port}/{db_config.database}"
    connection_properties = {
        "user": db_config.user,
        "password": db_config.password,
        "driver": "org.postgresql.Driver"
    }

    return spark.read.jdbc(
        url=jdbc_url,
        table=f"""(
            SELECT 
                p.id,
                p.investor_id,
                p.symbol,
                p.position_date,
                f.base_currency,
                f.quote_currency,
                f.position_amount,
                f.entry_rate
            FROM investors.position p
            JOIN investors.fx_spot_position f ON p.id = f.position_id
            WHERE p.investor_id = {investor_id}
            AND p.asset_type = 'FX_SPOT'
        ) as pos""",
        properties=connection_properties
    )

def get_latest_rates_df(
    spark: SparkSession,
    db_config: Dict,
    symbols: List[str],
    max_age_hours: int = 24
) -> DataFrame:
    """
    Get latest FX rates for all required symbols.
    Raises ValueError if current rates are not available for all symbols.
    """
    jdbc_url = f"jdbc:postgresql://{db_config.host}:{db_config.port}/{db_config.database}"
    connection_properties = {
        "user": db_config.user,
        "password": db_config.password,
        "driver": "org.postgresql.Driver"
    }

    symbols_str = "', '".join(symbols)
    
    # Query that includes rate age validation
    rates_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"""(
            WITH latest_dates AS (
                SELECT symbol, MAX(date) as max_date
                FROM {db_config.db_schema}.fx_spot_rates
                WHERE symbol IN ('{symbols_str}')
                GROUP BY symbol
            )
            SELECT 
                r.symbol, 
                r.rate, 
                r.date,
                EXTRACT(EPOCH FROM (NOW() - r.date))/3600 as hours_old
            FROM {db_config.db_schema}.fx_spot_rates r
            INNER JOIN latest_dates ld 
                ON r.symbol = ld.symbol 
                AND r.date = ld.max_date
        ) as rates""",
        properties=connection_properties
    )

    # Check if we have rates for all symbols
    available_symbols = set(row.symbol for row in rates_df.select("symbol").distinct().collect())
    missing_symbols = set(symbols) - available_symbols
    if missing_symbols:
        raise ValueError(f"No rates found for symbols: {', '.join(missing_symbols)}")

    # Check rate age and collect details for error message
    stale_rates = rates_df.filter(f"hours_old > {max_age_hours}").collect()
    if stale_rates:
        error_details = "\n".join(
            f"  {row.symbol}: {row.date} ({row.hours_old:.1f} hours old)"
            for row in stale_rates
        )
        raise ValueError(
            f"Stale rates found (older than {max_age_hours} hours):\n{error_details}"
        )

    return rates_df.select("symbol", "rate", "date")

def get_usd_conversion_rates_df(
    spark: SparkSession,
    db_config: Dict,
    currencies: List[str],
    max_age_hours: int = 24
) -> DataFrame:
    """
    Get USD conversion rates for all required currencies.
    Raises ValueError if current rates are not available.
    """
    if not currencies:
        return None

    currencies = [curr for curr in currencies if curr != "USD"]
    if not currencies:
        return spark.createDataFrame([("USD", 1.0)], ["currency", "usd_rate"])

    jdbc_url = f"jdbc:postgresql://{db_config.host}:{db_config.port}/{db_config.database}"
    connection_properties = {
        "user": db_config.user,
        "password": db_config.password,
        "driver": "org.postgresql.Driver"
    }

    # Create direct and inverse symbol lists
    direct_symbols = [f"{curr}/USD" for curr in currencies]
    inverse_symbols = [f"USD/{curr}" for curr in currencies]
    all_symbols = direct_symbols + inverse_symbols
    symbols_str = "', '".join(all_symbols)

    # Query with rate age validation
    rates_df = spark.read.jdbc(
        url=jdbc_url,
        table=f"""(
            WITH latest_rates AS (
                SELECT r.symbol, r.rate, r.date,
                       EXTRACT(EPOCH FROM (NOW() - r.date))/3600 as hours_old
                FROM {db_config.db_schema}.fx_spot_rates r
                INNER JOIN (
                    SELECT symbol, MAX(date) as max_date
                    FROM {db_config.db_schema}.fx_spot_rates
                    WHERE symbol IN ('{symbols_str}')
                    GROUP BY symbol
                ) ld ON r.symbol = ld.symbol AND r.date = ld.max_date
            )
            SELECT * FROM latest_rates
        ) as rates""",
        properties=connection_properties
    )

    # Check for missing conversion rates
    available_rates = set(
        symbol.split('/')[0] if '/USD' in symbol else symbol.split('/')[1]
        for symbol in [row.symbol for row in rates_df.select("symbol").distinct().collect()]
    )
    missing_currencies = set(currencies) - available_rates
    if missing_currencies:
        raise ValueError(
            f"No USD conversion rates found for currencies: {', '.join(missing_currencies)}"
        )

    # Check rate age
    stale_rates = rates_df.filter(f"hours_old > {max_age_hours}").collect()
    if stale_rates:
        error_details = "\n".join(
            f"  {row.symbol}: {row.date} ({row.hours_old:.1f} hours old)"
            for row in stale_rates
        )
        raise ValueError(
            f"Stale USD conversion rates found (older than {max_age_hours} hours):\n{error_details}"
        )

    # Transform rates into USD conversion rates
    conversion_rates = (rates_df
        .withColumn("currency", 
            when(col("symbol").contains("/USD"), 
                expr("split(symbol, '/')[0]"))
            .otherwise(expr("split(symbol, '/')[1]")))
        .withColumn("usd_rate",
            when(col("symbol").contains("/USD"), col("rate"))
            .otherwise(1/col("rate")))
        .select("currency", "usd_rate")
        .union(spark.createDataFrame([("USD", 1.0)], ["currency", "usd_rate"]))
    )

    return conversion_rates

def calculate_position_values(row: Dict) -> FXSpotValuationDetail:
    """Calculate position values with proper decimal precision."""
    # Convert all values to Decimal for precise calculation
    position_amount = Decimal(str(row.position_amount))
    current_rate = Decimal(str(row.current_rate))
    entry_rate = Decimal(str(row.entry_rate))
    usd_rate = Decimal(str(row.usd_rate))

    # Calculate current value
    position_value = position_amount * current_rate
    position_value_usd = position_value * usd_rate

    # Calculate unrealized P&L
    # P&L = Amount * (Current Rate - Entry Rate)
    unrealized_pnl = position_amount * (current_rate - entry_rate)
    unrealized_pnl_usd = unrealized_pnl * usd_rate

    logger.debug(
        f"Position {row.id} calculation:\n"
        f"  Amount: {position_amount:,.2f} {row.base_currency}\n"
        f"  Entry Rate: {entry_rate:,.4f}\n"
        f"  Current Rate: {current_rate:,.4f}\n"
        f"  USD Rate: {usd_rate:,.4f}\n"
        f"  Value: {position_value_usd:,.2f} USD\n"
        f"  P&L: {unrealized_pnl_usd:,.2f} USD"
    )

    return FXSpotValuationDetail(
        position_id=row.id,
        current_rate=quantize_decimal(current_rate),
        market_value_usd=quantize_decimal(position_value_usd),
        unrealized_pnl_usd=quantize_decimal(unrealized_pnl_usd)
    )

def value_portfolio(config_file: str, investor_name: str) -> None:
    """Calculate and store portfolio valuation."""
    config = load_config(config_file)
    db_handler = DBHandler(config)
    valuation_date = datetime.now(timezone.utc)
    
    try:
        # Verify investor exists
        investor = db_handler.verify_investor_exists(investor_name)
        logger.info(f"Valuing portfolio for {investor_name} at {valuation_date}")
        
        # Initialize Spark with proper logging
        spark = setup_spark_session(config)
        try:
            # Get positions
            positions_df = get_spark_positions_df(spark, config.database, investor.id)
            
            if positions_df.rdd.isEmpty():
                logger.info(f"No positions found for investor {investor_name}")
                return

            symbols = [row.symbol for row in positions_df.select("symbol").distinct().collect()]
            quote_currencies = [row.quote_currency for row in positions_df.select("quote_currency").distinct().collect()]

            try:
                # Get and validate current rates
                logger.info("Fetching current rates...")
                rates_df = get_latest_rates_df(spark, config.database, symbols)
                
                # Get and validate USD conversion rates
                logger.info("Fetching USD conversion rates...")
                usd_rates_df = get_usd_conversion_rates_df(spark, config.database, quote_currencies)

            except ValueError as e:
                logger.error(f"Cannot value portfolio: {str(e)}")
                return

            # Join data and collect for processing
            valued_positions = (positions_df
                .join(rates_df, "symbol")
                .withColumnRenamed("rate", "current_rate")
                .join(usd_rates_df, 
                    positions_df.quote_currency == usd_rates_df.currency)
            ).collect()

            # Calculate position values with proper decimal handling
            position_valuations = []
            total_value = Decimal('0')

            logger.info("\nPosition details:")
            for row in valued_positions:
                valuation = calculate_position_values(row)
                position_valuations.append(valuation)
                total_value += valuation.market_value_usd

                logger.info(
                    f"{row.symbol}: Amount {row.position_amount:,.2f} "
                    f"Entry: {row.entry_rate:,.4f} Current: {valuation.current_rate:,.4f} "
                    f"Value: ${valuation.market_value_usd:,.2f} USD "
                    f"P&L: ${valuation.unrealized_pnl_usd:,.2f} USD "
                    f"({valuation.unrealized_pnl_usd / valuation.market_value_usd * 100:,.2f}%)"
                )

            # Create and save portfolio valuation
            portfolio_valuation = PortfolioValuation(
                investor_id=investor.id,
                valuation_date=valuation_date,
                total_value_usd=quantize_decimal(total_value),
                position_valuations=position_valuations
            )

            db_handler.save_portfolio_valuation(portfolio_valuation)

            logger.info(f"\nPortfolio Summary for {investor_name}:")
            logger.info(f"Total Value: ${portfolio_valuation.total_value_usd:,.2f} USD")
            total_pnl = sum(v.unrealized_pnl_usd for v in position_valuations)
            logger.info(f"Total Unrealized P&L: ${total_pnl:,.2f} USD ({total_pnl / total_value * 100:,.2f}%)")

        finally:
            spark.stop()

    except Exception as e:
        logger.error(f"Failed to value portfolio: {str(e)}")
        sys.exit(1)

def main():
    if len(sys.argv) != 3:
        print("Usage: investor-portfolio-pricing <config_file> <investor_full_name>")
        sys.exit(1)
    
    value_portfolio(sys.argv[1], sys.argv[2])

if __name__ == "__main__":
    main()
