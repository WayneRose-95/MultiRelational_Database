ALTER TABLE dim_currency
ADD CONSTRAINT fk_dim_currency_conversion
FOREIGN KEY (currency_conversion_key)
REFERENCES dim_currency_conversion (currency_conversion_key);