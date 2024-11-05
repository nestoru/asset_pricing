-- FX Spot specific position details
CREATE TABLE IF NOT EXISTS investors.fx_spot_position (
    position_id INTEGER PRIMARY KEY REFERENCES investors.position(id),
    base_currency VARCHAR(3) NOT NULL,
    quote_currency VARCHAR(3) NOT NULL,
    position_amount DECIMAL(20,8) NOT NULL,  -- Amount in base currency
    entry_rate DECIMAL(20,8) NOT NULL
);

-- Create trigger function to validate symbol format
CREATE OR REPLACE FUNCTION investors.validate_fx_spot_symbol()
RETURNS TRIGGER AS $$
DECLARE
    position_symbol VARCHAR;
BEGIN
    SELECT symbol INTO position_symbol
    FROM investors.position
    WHERE id = NEW.position_id;

    IF position_symbol != CONCAT(NEW.base_currency, '/', NEW.quote_currency) THEN
        RAISE EXCEPTION 'Symbol % does not match the format %/%', 
            position_symbol, NEW.base_currency, NEW.quote_currency;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
DROP TRIGGER IF EXISTS validate_fx_spot_symbol_trigger ON investors.fx_spot_position;
CREATE TRIGGER validate_fx_spot_symbol_trigger
    BEFORE INSERT OR UPDATE ON investors.fx_spot_position
    FOR EACH ROW
    EXECUTE FUNCTION investors.validate_fx_spot_symbol();

-- Create FX Spot specific valuation details
CREATE TABLE IF NOT EXISTS investors.fx_spot_valuation_detail (
    position_valuation_id INTEGER PRIMARY KEY REFERENCES investors.position_valuation(id),
    current_rate DECIMAL(20,8) NOT NULL
);

-- Create convenience view for FX Spot positions
CREATE OR REPLACE VIEW investors.v_fx_spot_positions AS
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
WHERE p.asset_type = 'FX_SPOT';

-- Create convenience view for latest FX Spot valuations
CREATE OR REPLACE VIEW investors.v_fx_spot_latest_valuations AS
SELECT 
    pv.id as valuation_id,
    p.investor_id,
    p.symbol,
    pval.valuation_date,
    f.base_currency,
    f.quote_currency,
    f.position_amount,
    fd.current_rate,
    pv.market_value_usd,
    pv.unrealized_pnl_usd
FROM investors.position_valuation pv
JOIN investors.position p ON pv.position_id = p.id
JOIN investors.fx_spot_position f ON p.id = f.position_id
JOIN investors.fx_spot_valuation_detail fd ON pv.id = fd.position_valuation_id
JOIN investors.portfolio_valuation pval ON pv.portfolio_valuation_id = pval.id
WHERE p.asset_type = 'FX_SPOT'
AND pval.valuation_date = (
    SELECT MAX(valuation_date) 
    FROM investors.portfolio_valuation 
    WHERE investor_id = p.investor_id
);
