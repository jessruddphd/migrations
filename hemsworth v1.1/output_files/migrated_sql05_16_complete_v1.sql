-- ============================================================================
-- COMPLETE SPORTSBOOK PERFORMANCE MIGRATION - SQL05-16
-- Migrated from Redshift to Databricks Delta Lake
-- Applied AGENTS.md best practices for performance and readability
-- ============================================================================

-- ============================================================================
-- SQL05: UNION ALL PERFORMANCE DATA
-- ============================================================================
%sql
-- OPTIMIZED SQL05 - Union All Performance Data
-- Applied AGENTS.md best practices for performance and readability

-- Drop and recreate union table for clean state
DROP TABLE IF EXISTS sandbox.shared.br_temp_sbk_performance_union;

CREATE TABLE sandbox.shared.br_temp_sbk_performance_union AS
SELECT * FROM sandbox.shared.br_temp_sbk_performance_online
UNION ALL
SELECT * FROM sandbox.shared.br_temp_sbk_performance_retail  
UNION ALL
SELECT * FROM sandbox.shared.br_temp_sbk_performance_canada;

-- ============================================================================
-- SQL06-08: DATA MANAGEMENT WITH DELTA MERGE OPERATIONS
-- ============================================================================
%sql
-- OPTIMIZED SQL06-08 - Data Management with Delta Lake MERGE
-- Replaces DELETE/INSERT pattern with efficient MERGE operations

-- Create target tables if they don't exist (with proper schema)
CREATE TABLE IF NOT EXISTS analytics.sportsbook.performance_daily
USING DELTA
PARTITIONED BY (settled_day_local)
AS SELECT * FROM sandbox.shared.br_temp_sbk_performance_union WHERE 1=0;

CREATE TABLE IF NOT EXISTS analytics.sportsbook.performance_recent  
USING DELTA
PARTITIONED BY (settled_day_local)
AS SELECT * FROM sandbox.shared.br_temp_sbk_performance_union WHERE 1=0;

-- MERGE into daily performance table (replaces SQL06-08)
MERGE INTO analytics.sportsbook.performance_daily AS target
USING (
    SELECT DISTINCT settled_day_local, channel
    FROM sandbox.shared.br_temp_sbk_performance_union
) AS source_dates
ON target.settled_day_local = source_dates.settled_day_local 
   AND target.channel = source_dates.channel
WHEN MATCHED THEN DELETE;

-- Insert new data
INSERT INTO analytics.sportsbook.performance_daily
SELECT * FROM sandbox.shared.br_temp_sbk_performance_union;

-- MERGE into recent performance table
MERGE INTO analytics.sportsbook.performance_recent AS target
USING (
    SELECT DISTINCT settled_day_local, channel
    FROM sandbox.shared.br_temp_sbk_performance_union
) AS source_dates
ON target.settled_day_local = source_dates.settled_day_local 
   AND target.channel = source_dates.channel
WHEN MATCHED THEN DELETE;

-- Insert new data
INSERT INTO analytics.sportsbook.performance_recent
SELECT * FROM sandbox.shared.br_temp_sbk_performance_union;

-- ============================================================================
-- SQL09: AGGREGATION DATES GENERATION
-- ============================================================================
%sql
-- OPTIMIZED SQL09 - Aggregation Dates Generation
-- Applied AGENTS.md best practices for performance and readability

-- Create aggregation dates temporary view (replaces temp table)
CREATE OR REPLACE TEMPORARY VIEW aggregation_dates AS
SELECT DISTINCT settled_day_local
FROM analytics.sportsbook.performance_daily
WHERE settled_day_local < current_date() - INTERVAL 14 DAYS
  AND (
    handicap_value IS NOT NULL OR
    selection_name IS NOT NULL OR
    event_id IS NOT NULL OR
    market_id IS NOT NULL OR
    selection_id IS NOT NULL
  );

-- ============================================================================
-- SQL10: AGGREGATED PERFORMANCE DATA
-- ============================================================================
%sql
-- OPTIMIZED SQL10 - Aggregated Performance Data
-- Applied AGENTS.md best practices for performance and readability

-- Drop and recreate aggregated table for clean state
DROP TABLE IF EXISTS sandbox.shared.br_temp_sbk_performance_aggregated;

CREATE TABLE sandbox.shared.br_temp_sbk_performance_aggregated AS
SELECT
    p.source_id,
    p.state_id,
    p.channel,
    p.shop_name,
    p.retail_channel,
    p.monitored_yn,
    p.settled_month,
    p.settled_week,
    p.settled_day,
    p.settled_day_local,
    p.sport_group,
    p.sport_name,
    p.competition_group,
    p.competition_name,
    p.event_name,
    p.event_date,
    p.market_group,
    p.market_name,
    
    -- Aggregated fields (set to NULL for aggregated data)
    NULL AS handicap_value,
    NULL AS selection_name,
    NULL AS event_id,
    NULL AS market_id,
    NULL AS selection_id,
    
    p.in_play_yn,
    p.time_to_off,
    p.bet_type,
    p.leg_count,
    p.cashout_yn,
    p.price_group,
    p.percent_max,
    p.free_bet_yn,
    p.profit_boost_yn,
    p.stake_factor_group,
    p.liability_group,
    p.em_populated_yn,
    
    -- Aggregated metrics (optimized with proper rounding)
    round(sum(p.bet_count), 4) AS bet_count,
    round(sum(p.gross_handle), 2) AS gross_handle,
    round(sum(p.free_bet_handle), 2) AS free_bet_handle,
    round(sum(p.finance_revenue), 2) AS finance_revenue,
    round(sum(p.trading_revenue), 2) AS trading_revenue,
    round(sum(p.exp_handle), 2) AS exp_handle,
    round(sum(p.exp_revenue), 2) AS exp_revenue,
    round(sum(p.finance_exp_revenue), 2) AS finance_exp_revenue,
    round(sum(p.mikeprice_handle), 2) AS mikeprice_handle,
    round(sum(p.mikeprice_exp_revenue), 2) AS mikeprice_exp_revenue,
    round(sum(p.bnn_shrewd_stake), 2) AS bnn_shrewd_stake,
    round(sum(p.bnn_all_stake), 2) AS bnn_all_stake

FROM analytics.sportsbook.performance_daily AS p
INNER JOIN aggregation_dates AS d
    ON d.settled_day_local = p.settled_day_local

-- GROUP BY ALL is efficient in Databricks
GROUP BY ALL;

-- ============================================================================
-- SQL11-12: DATA LIFECYCLE MANAGEMENT
-- ============================================================================
%sql
-- OPTIMIZED SQL11-12 - Data Lifecycle Management
-- Applied AGENTS.md best practices for performance and readability

-- Delete old data from recent performance table (>15 months)
DELETE FROM analytics.sportsbook.performance_recent
WHERE settled_day < date_trunc('month', current_date()) - INTERVAL 15 MONTHS;

-- Delete granular data that will be replaced with aggregated data
DELETE FROM analytics.sportsbook.performance_daily
WHERE settled_day_local IN (SELECT settled_day_local FROM aggregation_dates);

DELETE FROM analytics.sportsbook.performance_recent  
WHERE settled_day_local IN (SELECT settled_day_local FROM aggregation_dates);

-- Insert aggregated data (recent data - last 15 months)
INSERT INTO analytics.sportsbook.performance_recent
SELECT * FROM sandbox.shared.br_temp_sbk_performance_aggregated
WHERE settled_day >= date_trunc('month', current_date()) - INTERVAL 15 MONTHS;

-- Insert all aggregated data into daily table
INSERT INTO analytics.sportsbook.performance_daily
SELECT * FROM sandbox.shared.br_temp_sbk_performance_aggregated;

-- ============================================================================
-- SQL13: HIGH-LEVEL MONTHLY PERFORMANCE DATA
-- ============================================================================
%sql
-- OPTIMIZED SQL13 - High-Level Monthly Performance Data
-- Applied AGENTS.md best practices for performance and readability

-- Drop and recreate high-level table for clean state
DROP TABLE IF EXISTS sandbox.shared.br_temp_sbk_performance_high_level;

CREATE TABLE sandbox.shared.br_temp_sbk_performance_high_level AS
SELECT
    p.source_id,
    p.state_id,
    p.channel,
    p.shop_name,
    p.retail_channel,
    p.monitored_yn,
    p.settled_month,
    p.sport_group,
    p.sport_name,
    p.competition_group,
    p.market_group,
    p.in_play_yn,
    p.time_to_off,
    p.bet_type,
    p.cashout_yn,
    p.price_group,
    p.percent_max,
    p.free_bet_yn,
    p.profit_boost_yn,
    p.stake_factor_group,
    p.liability_group,
    p.em_populated_yn,
    
    -- Aggregated metrics (optimized with proper rounding)
    round(sum(p.bet_count), 4) AS bet_count,
    round(sum(p.gross_handle), 2) AS gross_handle,
    round(sum(p.free_bet_handle), 2) AS free_bet_handle,
    round(sum(p.finance_revenue), 2) AS finance_revenue,
    round(sum(p.trading_revenue), 2) AS trading_revenue,
    round(sum(p.exp_handle), 2) AS exp_handle,
    round(sum(p.exp_revenue), 2) AS exp_revenue,
    round(sum(p.finance_exp_revenue), 2) AS finance_exp_revenue,
    round(sum(p.mikeprice_handle), 2) AS mikeprice_handle,
    round(sum(p.mikeprice_exp_revenue), 2) AS mikeprice_exp_revenue,
    round(sum(p.bnn_shrewd_stake), 2) AS bnn_shrewd_stake,
    round(sum(p.bnn_all_stake), 2) AS bnn_all_stake

FROM analytics.sportsbook.performance_daily AS p
INNER JOIN months_union AS m
    ON m.settled_month = p.settled_month

-- GROUP BY ALL is efficient in Databricks  
GROUP BY ALL;

-- ============================================================================
-- SQL14-15: MONTHLY DATA REPLACEMENT
-- ============================================================================
%sql
-- OPTIMIZED SQL14-15 - Monthly Data Replacement with Delta MERGE
-- Applied AGENTS.md best practices for performance and readability

-- Create target table if it doesn't exist
CREATE TABLE IF NOT EXISTS analytics.sportsbook.performance_monthly
USING DELTA
PARTITIONED BY (settled_month)
AS SELECT * FROM sandbox.shared.br_temp_sbk_performance_high_level WHERE 1=0;

-- MERGE operation to replace monthly data (more efficient than DELETE/INSERT)
MERGE INTO analytics.sportsbook.performance_monthly AS target
USING (
    SELECT DISTINCT settled_month
    FROM sandbox.shared.br_temp_sbk_performance_high_level
) AS source_months
ON target.settled_month = source_months.settled_month
WHEN MATCHED THEN DELETE;

-- Insert new monthly data
INSERT INTO analytics.sportsbook.performance_monthly
SELECT * FROM sandbox.shared.br_temp_sbk_performance_high_level;

-- ============================================================================
-- SQL16: CLEANUP AND OPTIMIZATION
-- ============================================================================
%sql
-- OPTIMIZED SQL16 - Cleanup and Delta Lake Optimization
-- Applied AGENTS.md best practices for performance and readability

-- Drop temporary views (Databricks equivalent of temp tables)
DROP VIEW IF EXISTS dates_online;
DROP VIEW IF EXISTS dates_retail;
DROP VIEW IF EXISTS dates_canada;
DROP VIEW IF EXISTS months_online;
DROP VIEW IF EXISTS months_retail;
DROP VIEW IF EXISTS months_canada;
DROP VIEW IF EXISTS months_union;
DROP VIEW IF EXISTS aggregation_dates;

-- Drop staging tables
DROP TABLE IF EXISTS sandbox.shared.br_temp_sbk_performance_online;
DROP TABLE IF EXISTS sandbox.shared.br_temp_sbk_performance_retail;
DROP TABLE IF EXISTS sandbox.shared.br_temp_sbk_performance_canada;
DROP TABLE IF EXISTS sandbox.shared.br_temp_sbk_performance_union;
DROP TABLE IF EXISTS sandbox.shared.br_temp_sbk_performance_aggregated;
DROP TABLE IF EXISTS sandbox.shared.br_temp_sbk_performance_high_level;

-- Delta Lake Optimizations (replaces ANALYZE in Redshift)
OPTIMIZE analytics.sportsbook.performance_daily 
ZORDER BY (settled_day_local, channel, sport_group);

OPTIMIZE analytics.sportsbook.performance_recent
ZORDER BY (settled_day_local, channel, sport_group);

OPTIMIZE analytics.sportsbook.performance_monthly
ZORDER BY (settled_month, channel, sport_group);

-- Vacuum old versions (optional - keeps last 7 days by default)
-- VACUUM analytics.sportsbook.performance_daily RETAIN 168 HOURS;
-- VACUUM analytics.sportsbook.performance_recent RETAIN 168 HOURS;  
-- VACUUM analytics.sportsbook.performance_monthly RETAIN 168 HOURS;

-- ============================================================================
-- MIGRATION COMPLETE
-- ============================================================================

-- Performance validation query (optional)
SELECT 
    'performance_daily' AS table_name,
    count(*) AS row_count,
    min(settled_day_local) AS min_date,
    max(settled_day_local) AS max_date
FROM analytics.sportsbook.performance_daily

UNION ALL

SELECT 
    'performance_recent' AS table_name,
    count(*) AS row_count,
    min(settled_day_local) AS min_date,
    max(settled_day_local) AS max_date
FROM analytics.sportsbook.performance_recent

UNION ALL

SELECT 
    'performance_monthly' AS table_name,
    count(*) AS row_count,
    min(settled_month) AS min_date,
    max(settled_month) AS max_date
FROM analytics.sportsbook.performance_monthly;
