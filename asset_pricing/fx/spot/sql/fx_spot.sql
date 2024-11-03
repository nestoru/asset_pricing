CREATE SCHEMA IF NOT EXISTS asset_pricing;

CREATE TABLE IF NOT EXISTS asset_pricing.fx_spot_rates (
    symbol VARCHAR(10),
    date TIMESTAMP,
    rate DOUBLE PRECISION,
    PRIMARY KEY (symbol, date)
);

