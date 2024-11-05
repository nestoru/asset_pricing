-- Create investors schema
CREATE SCHEMA IF NOT EXISTS investors;

-- Create asset type enum - will grow as we add more asset types
CREATE TYPE investors.asset_type AS ENUM (
    'FX_SPOT'  -- Start with just FX_SPOT
);

-- Create investors table
CREATE TABLE IF NOT EXISTS investors.investor (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create base position table
CREATE TABLE IF NOT EXISTS investors.position (
    id SERIAL PRIMARY KEY,
    investor_id INTEGER NOT NULL REFERENCES investors.investor(id),
    asset_type investors.asset_type NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    position_date TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create portfolio valuation table
CREATE TABLE IF NOT EXISTS investors.portfolio_valuation (
    id SERIAL PRIMARY KEY,
    investor_id INTEGER NOT NULL REFERENCES investors.investor(id),
    valuation_date TIMESTAMP WITH TIME ZONE NOT NULL,
    total_value_usd DECIMAL(20,8) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (investor_id, valuation_date)
);

-- Create base position valuation table
CREATE TABLE IF NOT EXISTS investors.position_valuation (
    id SERIAL PRIMARY KEY,
    portfolio_valuation_id INTEGER NOT NULL REFERENCES investors.portfolio_valuation(id),
    position_id INTEGER NOT NULL REFERENCES investors.position(id),
    market_value_usd DECIMAL(20,8) NOT NULL,
    unrealized_pnl_usd DECIMAL(20,8) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (portfolio_valuation_id, position_id)
);

-- Add triggers to update updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_investor_updated_at
    BEFORE UPDATE ON investors.investor
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_position_updated_at
    BEFORE UPDATE ON investors.position
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
