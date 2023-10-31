CREATE TABLE gaby_delgado11_coderhouse.crypto_currency (
    crypto_name VARCHAR(255),
    symbol VARCHAR(255),
    currency_symbol VARCHAR(255),
    type VARCHAR(255),
    rate_usd DOUBLE PRECISION,
    update_time TIMESTAMP
) sortkey(update_time);
