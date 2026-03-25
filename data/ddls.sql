CREATE TABLE market.symbol_ref (
  ticker STRING,
  company_name STRING -- nullable,
  sector_etf STRING -- nullable,
  market_cap_tier STRING -- nullable,
  sp500_flag BOOLEAN -- nullable
);

CREATE TABLE market.daily_prices (
  ticker STRING,
  trade_date DATE,
  open DOUBLE -- nullable,
  high DOUBLE -- nullable,
  low DOUBLE -- nullable,
  close DOUBLE -- nullable,
  adj_close DOUBLE -- nullable,
  volume BIGINT -- nullable
);

CREATE TABLE market.sector_etfs (
  etf_ticker STRING,
  trade_date DATE,
  open DOUBLE -- nullable,
  high DOUBLE -- nullable,
  low DOUBLE -- nullable,
  close DOUBLE -- nullable,
  adj_close DOUBLE -- nullable,
  volume BIGINT -- nullable
);

CREATE TABLE options.options_chain (
  contract_id STRING,
  ticker STRING,
  trade_date DATE,
  expiration_date DATE -- nullable,
  strike_price DOUBLE -- nullable,
  option_type STRING -- nullable,
  delta DOUBLE -- nullable,
  iv DOUBLE -- nullable,
  pop DOUBLE -- nullable,
  dte INT -- nullable
);

CREATE TABLE macro.macro_indicators (
  indicator_code STRING,
  announcement_date DATE,
  period_end_date DATE -- nullable,
  value DOUBLE -- nullable,
  prior_value DOUBLE -- nullable
);

CREATE TABLE governance.join_relationships (
  table_a STRING,
  col_a STRING,
  table_b STRING,
  col_b STRING,
  confidence STRING -- nullable,
  frequency INT -- nullable,
  reasoning STRING -- nullable,
  warning STRING -- nullable
);

