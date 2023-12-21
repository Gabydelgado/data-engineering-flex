CREATE TABLE IF NOT EXISTS gaby_delgado11_coderhouse.crypto_currency (
    crypto_name VARCHAR(255) distkey,
    symbol VARCHAR(5),
    currency_symbol VARCHAR(7),
    type VARCHAR(6),
    rate_usd DOUBLE PRECISION,
    update_time TIMESTAMP,
    PRIMARY KEY (crypto_name, update_time)
) sortkey(update_time);
