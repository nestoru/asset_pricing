"""
Mock data generation for investor positions.
"""

import sys
import logging
from datetime import datetime, timezone, timedelta
import random
from decimal import Decimal
from typing import Dict, List, Tuple

from asset_pricing.fx.spot.config import load_config
from asset_pricing.investors.models import DBHandler, FXSpotPosition

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_historical_rates(db_handler: DBHandler, symbol: str, num_points: int = 3) -> List[Tuple[Decimal, datetime]]:
    """Get historical rates for a symbol, ordered by date descending."""
    with db_handler.get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT rate, date
                FROM asset_pricing.fx_spot_rates
                WHERE symbol = %s
                ORDER BY date DESC
                LIMIT %s
            """, (symbol, num_points))
            return [(Decimal(str(rate)), date) for rate, date in cur.fetchall()]

def generate_mock_positions(
    db_handler: DBHandler,
    investor_id: int,
    fx_pairs: Dict[str, Dict[str, str]],
    num_pairs: int = 2,          # Number of different FX pairs to use
    positions_per_pair: int = 3   # Number of positions to create for each pair
) -> None:
    """Generate random FX spot positions for an investor."""
    try:
        # First delete existing positions
        db_handler.delete_investor_positions(investor_id)
        
        # Select random pairs to work with
        symbols = list(fx_pairs.keys())
        selected_pairs = random.sample(symbols, min(num_pairs, len(symbols)))
        
        for symbol in selected_pairs:
            pair_info = fx_pairs[symbol]
            
            # Get historical rates for this pair
            historical_rates = get_historical_rates(db_handler, symbol, positions_per_pair)
            
            if not historical_rates:
                logger.warning(f"No rate data found for {symbol}, skipping")
                continue
            
            for rate_data in historical_rates:
                rate, date = rate_data
                
                # Generate a random position amount between 10,000 and 1,000,000
                position_amount = Decimal(str(round(random.uniform(10000, 1000000), 2)))
                
                # Create position using Pydantic model
                position = FXSpotPosition(
                    investor_id=investor_id,
                    symbol=symbol,
                    position_date=date,
                    base_currency=pair_info['base_currency'],
                    quote_currency=pair_info['quote_currency'],
                    position_amount=position_amount,
                    entry_rate=rate
                )
                
                # Save position to database
                try:
                    db_handler.create_fx_spot_position(position)
                    logger.info(
                        f"Created position for {symbol}: {position_amount:,.2f} "
                        f"{pair_info['base_currency']} at {rate:,.4f} on {date}"
                    )
                except ValueError as e:
                    logger.error(f"Failed to create position for {symbol}: {str(e)}")
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error creating position for {symbol}: {str(e)}")
                    continue
            
    except Exception as e:
        logger.error(f"Failed to generate mock positions: {str(e)}")
        raise

def create_mock_investments(config_file: str, investor_name: str) -> None:
    """Generate mock FX spot positions for an investor."""
    config = load_config(config_file)
    db_handler = DBHandler(config)
    
    try:
        # Get or create investor
        investor = db_handler.get_or_create_investor(investor_name)
        logger.info(f"Working with investor: {investor.full_name} (ID: {investor.id})")
        
        # Get available FX pairs
        fx_pairs = db_handler.get_supported_fx_pairs()
        if not fx_pairs:
            logger.error("No FX pairs available in the database")
            sys.exit(1)
        
        logger.info(f"Found {len(fx_pairs)} available FX pairs")
        
        # Generate mock positions
        generate_mock_positions(
            db_handler, 
            investor.id, 
            fx_pairs,
            num_pairs=2,           # Will create positions for 2 different pairs
            positions_per_pair=3    # Will create 3 positions for each pair
        )
        
        # Log summary of created positions
        with db_handler.get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        p.symbol,
                        COUNT(*) as position_count,
                        SUM(f.position_amount) as total_amount,
                        MIN(p.position_date) as earliest_date,
                        MAX(p.position_date) as latest_date
                    FROM investors.position p
                    JOIN investors.fx_spot_position f ON p.id = f.position_id
                    WHERE p.investor_id = %s
                    GROUP BY p.symbol
                """, (investor.id,))
                
                logger.info(f"\nPosition summary for {investor_name}:")
                for row in cur.fetchall():
                    logger.info(
                        f"  {row[0]}: {row[1]} positions, "
                        f"total amount: {row[2]:,.2f}, "
                        f"date range: {row[3].date()} to {row[4].date()}"
                    )
        
    except Exception as e:
        logger.error(f"Failed to generate mock positions: {str(e)}")
        sys.exit(1)

def main():
    if len(sys.argv) != 3:
        print("Usage: mock-investor-investments <config_file> <investor_full_name>")
        sys.exit(1)
    
    create_mock_investments(sys.argv[1], sys.argv[2])

if __name__ == "__main__":
    main()
