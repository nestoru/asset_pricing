"""
Pydantic models for investor data management.

Provides data models for:
- Investor records
- FX Spot positions
- Portfolio valuations
"""

from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional, List
import logging
import psycopg2
from psycopg2 import Error, errors
from psycopg2.extensions import connection as psycopg2_connection
from psycopg2.extras import DictCursor
from pydantic import BaseModel, Field, validator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InvestorModel(BaseModel):
    id: Optional[int] = None
    full_name: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class FXSpotPosition(BaseModel):
    id: Optional[int] = None
    investor_id: int
    symbol: str
    position_date: datetime
    base_currency: str
    quote_currency: str
    position_amount: Decimal = Field(decimal_places=8)
    entry_rate: Decimal = Field(decimal_places=8)

    @validator('symbol')
    def validate_symbol(cls, v, values):
        """Ensure symbol matches base/quote currency format."""
        if 'base_currency' in values and 'quote_currency' in values:
            expected = f"{values['base_currency']}/{values['quote_currency']}"
            if v != expected:
                raise ValueError(f"Symbol {v} doesn't match expected format {expected}")
        return v

    @validator('base_currency', 'quote_currency')
    def validate_currency_length(cls, v):
        """Ensure currency codes are 3 characters."""
        if len(v) != 3:
            raise ValueError("Currency codes must be exactly 3 characters")
        return v.upper()

class FXSpotValuationDetail(BaseModel):
    position_id: int
    current_rate: Decimal = Field(decimal_places=8)
    market_value_usd: Decimal = Field(decimal_places=8)
    unrealized_pnl_usd: Decimal = Field(decimal_places=8)

class PortfolioValuation(BaseModel):
    id: Optional[int] = None
    investor_id: int
    valuation_date: datetime
    total_value_usd: Decimal = Field(decimal_places=8)
    position_valuations: List[FXSpotValuationDetail] = []

class DBHandler:
    """Database operations for investor data using Pydantic models."""
    
    def __init__(self, config):
        """Initialize with database configuration."""
        self.config = config
    
    def get_db_connection(self) -> psycopg2_connection:
        """Create a database connection using configuration."""
        return psycopg2.connect(
            host=self.config.database.host,
            port=self.config.database.port,
            database=self.config.database.database,
            user=self.config.database.user,
            password=self.config.database.password
        )

    def delete_investor_positions(self, investor_id: int) -> None:
        """Delete all FX spot positions for an investor."""
        with self.get_db_connection() as conn:
            try:
                with conn.cursor() as cur:
                    # First delete from fx_spot_position due to foreign key constraint
                    cur.execute("""
                        DELETE FROM investors.fx_spot_position 
                        WHERE position_id IN (
                            SELECT id FROM investors.position 
                            WHERE investor_id = %s AND asset_type = 'FX_SPOT'
                        )
                    """, (investor_id,))
                    
                    # Then delete from position table
                    cur.execute("""
                        DELETE FROM investors.position 
                        WHERE investor_id = %s AND asset_type = 'FX_SPOT'
                    """, (investor_id,))
                    
                    conn.commit()
                    logger.info(f"Successfully deleted existing positions for investor {investor_id}")
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to delete investor positions: {str(e)}")
                raise

    def get_or_create_investor(self, full_name: str) -> InvestorModel:
        """Get investor by name or create new investor record."""
        with self.get_db_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                # Try to get existing investor
                cur.execute("""
                    SELECT id, full_name, created_at, updated_at 
                    FROM investors.investor 
                    WHERE full_name = %s
                """, (full_name,))
                result = cur.fetchone()
                
                if result:
                    return InvestorModel(**dict(result))
                
                # Create new investor
                cur.execute("""
                    INSERT INTO investors.investor (full_name) 
                    VALUES (%s) 
                    RETURNING id, full_name, created_at, updated_at
                """, (full_name,))
                conn.commit()
                return InvestorModel(**dict(cur.fetchone()))

    def get_supported_fx_pairs(self) -> Dict[str, Dict[str, str]]:
        """Get dictionary of supported FX pairs with their currencies."""
        with self.get_db_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute("""
                    SELECT DISTINCT 
                        symbol,
                        SPLIT_PART(symbol, '/', 1) as base_currency,
                        SPLIT_PART(symbol, '/', 2) as quote_currency
                    FROM asset_pricing.fx_spot_rates
                """)
                return {
                    row['symbol']: {
                        'base_currency': row['base_currency'],
                        'quote_currency': row['quote_currency']
                    }
                    for row in cur.fetchall()
                }

    def create_fx_spot_position(self, position: FXSpotPosition) -> FXSpotPosition:
        """Create a new FX spot position."""
        with self.get_db_connection() as conn:
            try:
                with conn.cursor() as cur:
                    # Create base position record
                    cur.execute("""
                        INSERT INTO investors.position (
                            investor_id, asset_type, symbol, position_date
                        ) VALUES (%s, 'FX_SPOT', %s, %s)
                        RETURNING id
                    """, (
                        position.investor_id,
                        position.symbol,
                        position.position_date
                    ))
                    position.id = cur.fetchone()[0]

                    # Create FX spot position details
                    try:
                        cur.execute("""
                            INSERT INTO investors.fx_spot_position (
                                position_id, base_currency, quote_currency,
                                position_amount, entry_rate
                            ) VALUES (%s, %s, %s, %s, %s)
                        """, (
                            position.id,
                            position.base_currency,
                            position.quote_currency,
                            position.position_amount,
                            position.entry_rate
                        ))
                    except errors.RaiseException as e:
                        # Handle trigger-raised exceptions
                        if "does not match the format" in str(e):
                            conn.rollback()
                            raise ValueError(f"Invalid position data: {str(e)}")
                        raise

                    conn.commit()
                    return position
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to create FX spot position: {str(e)}")
                raise

    def verify_investor_exists(self, full_name: str) -> InvestorModel:
        """Verify investor exists and return their data."""
        with self.get_db_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute("""
                    SELECT id, full_name, created_at, updated_at 
                    FROM investors.investor 
                    WHERE full_name = %s
                """, (full_name,))
                result = cur.fetchone()
                if not result:
                    raise ValueError(f"Investor {full_name} not found")
                return InvestorModel(**dict(result))

    def save_portfolio_valuation(self, valuation: PortfolioValuation) -> PortfolioValuation:
        """Save portfolio valuation and position details."""
        with self.get_db_connection() as conn:
            try:
                with conn.cursor() as cur:
                    # Insert portfolio valuation
                    cur.execute("""
                        INSERT INTO investors.portfolio_valuation 
                            (investor_id, valuation_date, total_value_usd)
                        VALUES (%s, %s, %s)
                        RETURNING id
                    """, (
                        valuation.investor_id,
                        valuation.valuation_date,
                        valuation.total_value_usd
                    ))
                    valuation.id = cur.fetchone()[0]

                    # Insert position valuations
                    for pos_val in valuation.position_valuations:
                        cur.execute("""
                            INSERT INTO investors.position_valuation 
                                (portfolio_valuation_id, position_id, 
                                 market_value_usd, unrealized_pnl_usd)
                            VALUES (%s, %s, %s, %s)
                            RETURNING id
                        """, (
                            valuation.id,
                            pos_val.position_id,
                            pos_val.market_value_usd,
                            pos_val.unrealized_pnl_usd
                        ))
                        
                        valuation_id = cur.fetchone()[0]
                        
                        cur.execute("""
                            INSERT INTO investors.fx_spot_valuation_detail
                                (position_valuation_id, current_rate)
                            VALUES (%s, %s)
                        """, (valuation_id, pos_val.current_rate))

                    conn.commit()
                    return valuation
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to save portfolio valuation: {str(e)}")
                raise
