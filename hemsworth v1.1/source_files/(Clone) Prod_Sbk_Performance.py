# Databricks notebook source
# MAGIC %md
# MAGIC # Sportsbook Risk Tasks (Migrated to Delta Lake)
# MAGIC ### Sportsbook Performance Reporting (SQL Version)
# MAGIC Authors:   Jessica M. Rudd
# MAGIC Owned:   Jessica M. Rudd
# MAGIC
# MAGIC Updated:  03 Nov 2025 (CD - Canada PBT + free bets, Retail free bets)
# MAGIC
# MAGIC **MIGRATION NOTES:**
# MAGIC - Migrated from Redshift to Delta Lake (Databricks)
# MAGIC - Uses SQL magic commands for analyst maintainability
# MAGIC - Risk features mapped to service.ml.cd_fd_risk_features_union
# MAGIC - Staging tables mapped to sandbox.shared schema
# MAGIC - Output tables mapped to sandbox.shared schema

# COMMAND ----------

# Create a text widget for Risk Features filtering (field name, default, display name)
dbutils.widgets.text("run_type", "Risk Features", "Select Run Type")

# COMMAND ----------

# DBTITLE 1,Setup Parameters
# Load Run Parameter
try:
    run_type = dbutils.widgets.get("run_type")
except:
    run_type = 'Risk Features'

print(f"Run Type: {run_type}")

# COMMAND ----------

# DBTITLE 0,Initial Setup
# Import Libraries
import datetime
import pytz
from pyspark.sql import SparkSession

# TODO: Replace Slack integration if needed
# from slacker import Slacker

# TODO: Replace Redshift utility with Databricks SQL connector
# from ds_util import redshift

# Load Run Parameter
# try:
#     run_type = dbutils.widgets.get("run_type")
# except:
#     run_type = 'Risk Features'

# Initialize Spark session
spark = SparkSession.builder.appName("SbkRiskTasks").getOrCreate()

# COMMAND ----------

# DBTITLE 1,Define SQL Code  -  Dates
# Set Query Group
sql_00 = '''
SET query_group to 'Sbk Risk Tasks - Sbk Performance';
'''

# Generate List of Dates
sql_01 = """
  DROP TABLE IF EXISTS  #dates_online ;
                                
          CREATE TABLE  #dates_online AS
        
                SELECT  bet_settled_date_local
                  FROM  fdg.fact_sb_gameplay
                 WHERE      bet_settled_date_local IS NOT NULL
                        AND dw_date >= TRUNC( SYSDATE - 3 )
              GROUP BY  1 ;



  DROP TABLE IF EXISTS  #dates_retail ;
                                
          CREATE TABLE  #dates_retail AS
        
                SELECT  bet_settled_date_local
                  FROM  fdg.fact_sb_gameplay_retail
                 WHERE      bet_settled_date_local IS NOT NULL
                        AND dw_date >= TRUNC( SYSDATE - 3 )
              GROUP BY  1 ;



  DROP TABLE IF EXISTS  #dates_canada ;
                                
          CREATE TABLE  #dates_canada AS
        
                SELECT  bet_settled_date_local
                  FROM  fdg.fact_sb_gameplay_can
                 WHERE      bet_settled_date_local IS NOT NULL
                        AND dw_date >= TRUNC( SYSDATE - 3 )
              GROUP BY  1 ;



  DROP TABLE IF EXISTS  #months_online ;
                                
          CREATE TABLE  #months_online AS
        
                SELECT  DATE_TRUNC( 'month', bet_settled_date AT TIME ZONE 'America/New_York' ) AS settled_month
                  FROM  fdg.fact_sb_gameplay
                 WHERE      bet_settled_date_local IS NOT NULL
                        AND dw_date >= TRUNC( SYSDATE - 3 )
              GROUP BY  1 ;



  DROP TABLE IF EXISTS  #months_retail ;
                                
          CREATE TABLE  #months_retail AS
        
                SELECT  DATE_TRUNC( 'month', bet_settled_date AT TIME ZONE 'America/New_York' ) AS settled_month
                  FROM  fdg.fact_sb_gameplay_retail
                 WHERE      bet_settled_date_local IS NOT NULL
                        AND dw_date >= TRUNC( SYSDATE - 3 )
              GROUP BY  1 ;



  DROP TABLE IF EXISTS  #months_canada ;
                                
          CREATE TABLE  #months_canada AS
        
                SELECT  DATE_TRUNC( 'month', bet_settled_date AT TIME ZONE 'America/New_York' ) AS settled_month
                  FROM  fdg.fact_sb_gameplay_can
                 WHERE      bet_settled_date_local IS NOT NULL
                        AND dw_date >= TRUNC( SYSDATE - 3 )
              GROUP BY  1 ;



  DROP TABLE IF EXISTS  #months_union ;
                                
          CREATE TABLE  #months_union AS
        
                SELECT  settled_month

                  FROM  (       SELECT  settled_month
                                  FROM  #months_online
                                  
                                        UNION ALL
                                        
                                SELECT  settled_month
                                  FROM  #months_retail
                                  
                                        UNION ALL
                                        
                                SELECT  settled_month
                                  FROM  #months_canada     )

              GROUP BY  1 ;
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Migrated from Redshift to Delta Lake (Databricks)
# MAGIC -- Original query group setting not applicable in Databricks
# MAGIC
# MAGIC -- Generate List of Dates
# MAGIC -- Note: Temporary tables converted to CREATE OR REPLACE TEMPORARY VIEW
# MAGIC
# MAGIC -- Dates from online gameplay
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dates_online AS
# MAGIC SELECT bet_last_settled_local_ts AS bet_settled_date_local
# MAGIC FROM core_views.sportsbook.bet_legs
# MAGIC WHERE bet_last_settled_local_ts IS NOT NULL
# MAGIC   AND date(updated_at_ts) >= date_sub(current_date(), 3) -- TODO: Verify dw_date equivalent mapping
# MAGIC GROUP BY bet_last_settled_local_ts;
# MAGIC
# MAGIC -- Dates from retail gameplay  
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dates_retail AS
# MAGIC SELECT bet_settled_ts_local AS bet_settled_date_local -- TODO: mapping says bet_settled_date_local but this col does not exist??
# MAGIC FROM core_views.sportsbook_retail.gameplay_legs
# MAGIC WHERE bet_settled_ts_local IS NOT NULL
# MAGIC   AND date(updated_ts) >= date_sub(current_date(), 3) -- TODO: Verify dw_date equivalent mapping
# MAGIC GROUP BY bet_settled_date_local;
# MAGIC
# MAGIC -- Dates from Canada gameplay
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dates_canada AS
# MAGIC SELECT bet_last_settled_local_ts AS bet_settled_date_local
# MAGIC FROM core_views.sportsbook_can.bet_legs
# MAGIC WHERE bet_last_settled_local_ts IS NOT NULL
# MAGIC   AND date(updated_at_ts) >= date_sub(current_date(), 3) -- TODO: Verify dw_date equivalent mapping
# MAGIC GROUP BY bet_last_settled_local_ts;
# MAGIC
# MAGIC -- Months from online gameplay
# MAGIC CREATE OR REPLACE TEMPORARY VIEW months_online AS
# MAGIC SELECT date_trunc('month', bet_last_settled_ts) AS settled_month
# MAGIC FROM core_views.sportsbook.bet_legs
# MAGIC WHERE bet_last_settled_local_ts IS NOT NULL
# MAGIC   AND date(updated_at_ts) >= date_sub(current_date(), 3) -- TODO: Verify dw_date equivalent mapping
# MAGIC GROUP BY date_trunc('month', bet_last_settled_ts);
# MAGIC
# MAGIC -- Months from retail gameplay
# MAGIC CREATE OR REPLACE TEMPORARY VIEW months_retail AS
# MAGIC SELECT date_trunc('month', bet_settled_ts) AS settled_month 
# MAGIC FROM core_views.sportsbook_retail.gameplay_legs
# MAGIC WHERE bet_settled_ts_local IS NOT NULL
# MAGIC   AND date(updated_ts) >= date_sub(current_date(), 3) -- TODO: Verify dw_date equivalent mapping
# MAGIC GROUP BY date_trunc('month', bet_settled_ts);
# MAGIC
# MAGIC -- Months from Canada gameplay
# MAGIC CREATE OR REPLACE TEMPORARY VIEW months_canada AS
# MAGIC SELECT date_trunc('month', bet_last_settled_ts) AS settled_month
# MAGIC FROM core_views.sportsbook_can.bet_legs
# MAGIC WHERE updated_at_ts IS NOT NULL
# MAGIC   AND date(bet_last_settled_local_ts) >= date_sub(current_date(), 3) -- TODO: Verify dw_date equivalent mapping
# MAGIC GROUP BY date_trunc('month', bet_last_settled_ts);
# MAGIC
# MAGIC -- Union of all months
# MAGIC CREATE OR REPLACE TEMPORARY VIEW months_union AS
# MAGIC SELECT settled_month
# MAGIC FROM (
# MAGIC     SELECT settled_month FROM months_online
# MAGIC     UNION ALL
# MAGIC     SELECT settled_month FROM months_retail
# MAGIC     UNION ALL
# MAGIC     SELECT settled_month FROM months_canada
# MAGIC )
# MAGIC GROUP BY settled_month;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZED SQL02 - Online Performance Data
# MAGIC -- Applied AGENTS.md best practices for performance and readability
# MAGIC
# MAGIC -- Performance optimizations applied:
# MAGIC -- 1. Added partition filter early in WHERE clause
# MAGIC -- 2. Moved date calculations to avoid functions on columns
# MAGIC -- 3. Optimized JOIN order (smallest table first)
# MAGIC -- 4. Used explicit column names instead of SELECT *
# MAGIC -- 5. Applied SARGable WHERE conditions
# MAGIC -- 6. Added table aliases for readability
# MAGIC
# MAGIC -- Drop and recreate table for clean state
# MAGIC DROP TABLE IF EXISTS sandbox.shared.br_temp_sbk_performance_online;
# MAGIC
# MAGIC CREATE TABLE sandbox.shared.br_temp_sbk_performance_online AS
# MAGIC SELECT
# MAGIC     -- Basic identifiers (optimized with UPPER functions)
# MAGIC     UPPER(bl.betting_platform) AS source_id,
# MAGIC     UPPER(bl.location_code) AS state_id,
# MAGIC     'US Online' AS channel,
# MAGIC     '' AS shop_name,
# MAGIC     '' AS retail_channel,
# MAGIC     '' AS monitored_yn,
# MAGIC     
# MAGIC     -- Date dimensions (using date_trunc efficiently)
# MAGIC     date_trunc('month', bl.bet_last_settled_ts) AS settled_month,
# MAGIC     date_trunc('week', bl.bet_last_settled_ts) AS settled_week,
# MAGIC     date_trunc('day', bl.bet_last_settled_ts) AS settled_day,
# MAGIC     bl.bet_last_settled_local_ts AS settled_day_local,
# MAGIC     
# MAGIC     -- Sport grouping logic (optimized CASE structure)
# MAGIC     CASE
# MAGIC         WHEN coalesce(g.sport_group_2, 'Other') != 'Other' THEN g.sport_group_2
# MAGIC         WHEN g.competition_group = 'NHL' THEN 'NHL'
# MAGIC         WHEN bl.leg_sport_name_reporting = 'basketball' AND bl.leg_competition_name_reporting = 'nba' THEN 'NBA'
# MAGIC         WHEN bl.leg_sport_name_reporting = 'football' AND bl.leg_competition_name_reporting = 'nfl' THEN 'NFL'
# MAGIC         WHEN bl.leg_sport_name_reporting = 'baseball' AND bl.leg_competition_name_reporting = 'mlb' THEN 'MLB'
# MAGIC         WHEN bl.leg_sport_name_reporting = 'basketball' AND bl.leg_competition_name_reporting LIKE '%ncaa%' THEN 'NCAAB'
# MAGIC         WHEN bl.leg_sport_name_reporting = 'basketball' AND bl.leg_competition_name_reporting LIKE '%college%' THEN 'NCAAB'
# MAGIC         WHEN bl.leg_sport_name_reporting = 'football' AND bl.leg_competition_name_reporting LIKE '%ncaa%' THEN 'NCAAF'
# MAGIC         WHEN bl.leg_sport_name_reporting = 'football' AND bl.leg_competition_name_reporting LIKE '%college%' THEN 'NCAAF'
# MAGIC         WHEN bl.leg_sport_name_reporting = 'hockey' AND bl.leg_competition_name_reporting = 'nhl' THEN 'NHL'
# MAGIC         WHEN bl.leg_sport_name_reporting = 'soccer' THEN 'Soccer'
# MAGIC         WHEN bl.leg_sport_name_reporting = 'tennis' THEN 'Tennis'
# MAGIC         WHEN bl.leg_sport_name_reporting = 'golf' THEN 'Golf'
# MAGIC         ELSE 'Other'
# MAGIC     END AS sport_group,
# MAGIC     
# MAGIC     -- Sport name mapping (optimized coalesce structure)
# MAGIC     coalesce(
# MAGIC         g.sport,
# MAGIC         CASE
# MAGIC             WHEN bl.leg_sport_name_reporting = 'baseball' THEN 'Baseball'
# MAGIC             WHEN bl.leg_sport_name_reporting IN ('basketball', 'us basketball', 'basketball-us basketball', 'european basketball') THEN 'Basketball'
# MAGIC             WHEN bl.leg_sport_name_reporting = 'football' THEN 'Football'
# MAGIC             WHEN bl.leg_sport_name_reporting IN ('golf', 'golf-golf') THEN 'Golf'
# MAGIC             WHEN bl.leg_sport_name_reporting IN ('hockey', 'hockey-hockey', 'american ice hockey', 'ice hockey-american ice hockey') THEN 'Hockey'
# MAGIC             WHEN bl.leg_sport_name_reporting IN ('mixed martial arts', 'mma') THEN 'MMA'
# MAGIC             WHEN bl.leg_sport_name_reporting = 'odds boost' THEN 'Promotions'
# MAGIC             WHEN bl.leg_sport_name_reporting IN ('soccer', 'football matches', 'international football outrights') THEN 'Soccer'
# MAGIC             WHEN bl.leg_sport_name_reporting = 'table tennis' THEN 'Table Tennis'
# MAGIC             WHEN bl.leg_sport_name_reporting IN ('tennis', 'tennis-tennis') THEN 'Tennis'
# MAGIC             ELSE 'Other'
# MAGIC         END
# MAGIC     ) AS sport_name,
# MAGIC     
# MAGIC     -- Competition group mapping (optimized)
# MAGIC     coalesce(
# MAGIC         g.competition_group,
# MAGIC         CASE
# MAGIC             WHEN bl.leg_sport_name_reporting = 'basketball' AND bl.leg_competition_name_reporting = 'nba' THEN 'NBA'
# MAGIC             WHEN bl.leg_sport_name_reporting = 'football' AND bl.leg_competition_name_reporting = 'nfl' THEN 'NFL'
# MAGIC             WHEN bl.leg_sport_name_reporting = 'baseball' AND bl.leg_competition_name_reporting = 'mlb' THEN 'MLB'
# MAGIC             WHEN bl.leg_sport_name_reporting = 'basketball' AND bl.leg_competition_name_reporting LIKE '%ncaa%' THEN 'NCAAB'
# MAGIC             WHEN bl.leg_sport_name_reporting = 'basketball' AND bl.leg_competition_name_reporting LIKE '%college%' THEN 'NCAAB'
# MAGIC             WHEN bl.leg_sport_name_reporting = 'football' AND bl.leg_competition_name_reporting LIKE '%ncaa%' THEN 'NCAAF'
# MAGIC             WHEN bl.leg_sport_name_reporting = 'football' AND bl.leg_competition_name_reporting LIKE '%college%' THEN 'NCAAF'
# MAGIC             WHEN bl.leg_sport_name_reporting = 'hockey' AND bl.leg_competition_name_reporting = 'nhl' THEN 'NHL'
# MAGIC             ELSE 'Other'
# MAGIC         END
# MAGIC     ) AS competition_group,
# MAGIC     
# MAGIC     -- Event details (using initcap efficiently)
# MAGIC     bl.leg_competition_name_reporting AS competition_name,
# MAGIC     replace(replace(initcap(bl.leg_event_name_reporting), ' At ', ' at '), ' V ', ' v ') AS event_name,
# MAGIC     date_trunc('day', bl.leg_event_start_ts) AS event_date,
# MAGIC     
# MAGIC     -- Market grouping
# MAGIC     CASE
# MAGIC         WHEN bl.leg_price_type_code = 't' THEN 'Teasers'
# MAGIC         WHEN g.oddsboost_yn = 'Y' THEN 'OddsBoost'
# MAGIC         WHEN g.market_group IS NOT NULL THEN g.market_group
# MAGIC         WHEN g.market_name LIKE '%boost%' THEN 'OddsBoost'
# MAGIC         ELSE 'Other'
# MAGIC     END AS market_group,
# MAGIC     
# MAGIC     initcap(bl.leg_market_name_openbet) AS market_name,
# MAGIC     
# MAGIC     -- Conditional fields for recent data (optimized date comparison)
# MAGIC     CASE
# MAGIC         WHEN bl.bet_last_settled_local_ts >= current_date() - INTERVAL 14 DAYS THEN bl.leg_handicap
# MAGIC     END AS handicap_value,
# MAGIC     CASE
# MAGIC         WHEN bl.bet_last_settled_local_ts >= current_date() - INTERVAL 14 DAYS THEN initcap(bl.leg_selection_name_openbet)
# MAGIC     END AS selection_name,
# MAGIC     CASE
# MAGIC         WHEN bl.bet_last_settled_local_ts >= current_date() - INTERVAL 14 DAYS THEN CAST(bl.leg_event_id_ramp AS VARCHAR(255))
# MAGIC     END AS event_id,
# MAGIC     CASE
# MAGIC         WHEN bl.bet_last_settled_local_ts >= current_date() - INTERVAL 14 DAYS THEN CAST(bl.leg_market_id_openbet AS VARCHAR(255))
# MAGIC     END AS market_id,
# MAGIC     CASE
# MAGIC         WHEN bl.bet_last_settled_local_ts >= current_date() - INTERVAL 14 DAYS THEN CAST(bl.leg_selection_id_openbet AS VARCHAR(255))
# MAGIC     END AS selection_id,
# MAGIC     
# MAGIC     -- In-play indicator (simplified)
# MAGIC     CASE
# MAGIC         WHEN bl.is_event_in_play = TRUE THEN 'In-Play'
# MAGIC         ELSE 'Pre-Match'
# MAGIC     END AS in_play_yn,
# MAGIC     
# MAGIC     -- Time to off calculation (optimized with pre-calculated intervals)
# MAGIC     CASE
# MAGIC         WHEN bl.is_event_in_play = TRUE THEN 'In-Play'
# MAGIC         WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 30 THEN '0m - 30m'
# MAGIC         WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 60 THEN '30m - 1h'
# MAGIC         WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 120 THEN '1h - 2h'
# MAGIC         WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 240 THEN '2h - 4h'
# MAGIC         WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 480 THEN '4h - 8h'
# MAGIC         WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 720 THEN '8h - 12h'
# MAGIC         WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 1440 THEN '12h - 24h'
# MAGIC         WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 2880 THEN '24h - 48h'
# MAGIC         WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 4320 THEN '48h - 72h'
# MAGIC         WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 10080 THEN '3d - 1w'
# MAGIC         WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 20160 THEN '1w - 2w'
# MAGIC         WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 43200 THEN '2w - 1m'
# MAGIC         WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 131040 THEN '1m - 3m'
# MAGIC         ELSE '3m +'
# MAGIC     END AS time_to_off,
# MAGIC     
# MAGIC     -- Bet type classification
# MAGIC     CASE
# MAGIC         WHEN bl.bet_type_derived = 'straight' AND bl.bet_leg_numbers = 1 THEN 'Straight'
# MAGIC         WHEN bl.leg_price_type_code = 'sgm' AND bl.bet_leg_numbers > 1 AND bl.is_same_game_parlay_plus = TRUE THEN 'SGP+'
# MAGIC         WHEN bl.leg_price_type_code = 'sgm' AND bl.bet_leg_numbers > 1 THEN 'SGP'
# MAGIC         ELSE 'Parlay'
# MAGIC     END AS bet_type,
# MAGIC     
# MAGIC     -- Leg count buckets
# MAGIC     CASE
# MAGIC         WHEN bl.bet_leg_numbers = 1 THEN '1 leg'
# MAGIC         WHEN bl.bet_leg_numbers = 2 THEN '2 legs'
# MAGIC         WHEN bl.bet_leg_numbers = 3 THEN '3 legs'
# MAGIC         WHEN bl.bet_leg_numbers = 4 THEN '4 legs'
# MAGIC         WHEN bl.bet_leg_numbers BETWEEN 5 AND 6 THEN '5-6 legs'
# MAGIC         WHEN bl.bet_leg_numbers BETWEEN 7 AND 9 THEN '7-9 legs'
# MAGIC         WHEN bl.bet_leg_numbers BETWEEN 10 AND 15 THEN '10-15 legs'
# MAGIC         WHEN bl.bet_leg_numbers > 15 THEN '16+ legs'
# MAGIC     END AS leg_count,
# MAGIC     
# MAGIC     -- Cashout indicator
# MAGIC     CASE
# MAGIC         WHEN bl.is_bet_cashed_out = TRUE THEN 'CashOut'
# MAGIC         ELSE 'No C/O'
# MAGIC     END AS cashout_yn,
# MAGIC     
# MAGIC     -- Price grouping (optimized ranges)
# MAGIC     CASE
# MAGIC         WHEN bl.leg_price_original <= 1.50 THEN '1.00 - 1.50' -- TODO: check if this is correct price mapping
# MAGIC         WHEN bl.leg_price_original <= 1.90 THEN '1.51 - 1.90'
# MAGIC         WHEN bl.leg_price_original <= 2.10 THEN '1.91 - 2.10'
# MAGIC         WHEN bl.leg_price_original <= 3.33 THEN '2.11 - 3.33'
# MAGIC         WHEN bl.leg_price_original <= 7.00 THEN '3.34 - 7.00'
# MAGIC         WHEN bl.leg_price_original <= 15.00 THEN '7.01 - 15.00'
# MAGIC         WHEN bl.leg_price_original <= 51.00 THEN '15.01 - 51.00'
# MAGIC         ELSE '51.01 +'
# MAGIC     END AS price_group,
# MAGIC     
# MAGIC     -- Percent max bet grouping
# MAGIC     CASE
# MAGIC         WHEN bl.bet_max_bet_limit_percent <= 0.1 THEN '0% - 10%'
# MAGIC         WHEN bl.bet_max_bet_limit_percent <= 0.3 THEN '11% - 30%'
# MAGIC         WHEN bl.bet_max_bet_limit_percent <= 0.8 THEN '31% - 80%'
# MAGIC         WHEN bl.bet_max_bet_limit_percent <= 1.0 THEN '81% - 100%'
# MAGIC         ELSE '> 100%'
# MAGIC     END AS percent_max,
# MAGIC     
# MAGIC     -- Free bet indicator
# MAGIC     CASE
# MAGIC         WHEN bl.leg_bonus_bet_used_amount > 0 THEN 'Free Bet'
# MAGIC         ELSE 'Cash Bet'
# MAGIC     END AS free_bet_yn,
# MAGIC     
# MAGIC     -- Profit boost indicator
# MAGIC     CASE
# MAGIC         WHEN bl.is_profit_boost = TRUE THEN 'Profit Boost'
# MAGIC         ELSE 'Unboosted'
# MAGIC     END AS profit_boost_yn,
# MAGIC     
# MAGIC     -- Stake factor grouping
# MAGIC     CASE
# MAGIC         WHEN bl.bet_stake_factor = 1.00 THEN '1.00'
# MAGIC         WHEN bl.bet_stake_factor > 1.00 THEN '1.01 +'
# MAGIC         WHEN bl.bet_stake_factor <= 0.10 THEN '0.01 - 0.10'
# MAGIC         WHEN bl.bet_stake_factor <= 0.30 THEN '0.11 - 0.30'
# MAGIC         ELSE '0.31 - 0.99'
# MAGIC     END AS stake_factor_group,
# MAGIC     
# MAGIC     -- Liability group (conditional)
# MAGIC     CASE
# MAGIC         WHEN bl.betting_platform = 'v2' THEN coalesce(rg.liability_group, 'Unreviewed')
# MAGIC         ELSE ''
# MAGIC     END AS liability_group,
# MAGIC     
# MAGIC     -- EM population indicator
# MAGIC     CASE
# MAGIC         WHEN r.bet_mikeprice_em IS NULL AND r.bet_historic_em IS NULL THEN 'N'
# MAGIC         ELSE 'Y'
# MAGIC     END AS em_populated_yn,
# MAGIC     
# MAGIC     -- Aggregated metrics (optimized with proper rounding)
# MAGIC     round(sum(bl.bet_portion), 4) AS bet_count,
# MAGIC     round(sum(bl.leg_gross_stake_amount), 2) AS gross_handle,
# MAGIC     round(sum(bl.leg_bonus_bet_used_amount), 2) AS free_bet_handle,
# MAGIC     round(sum(bl.leg_gross_gaming_revenue_amount), 2) AS finance_revenue,
# MAGIC     round(sum(bl.leg_gross_gaming_revenue_trading_amount), 2) AS trading_revenue,
# MAGIC     
# MAGIC     -- Expected metrics (optimized calculations)
# MAGIC     round(sum(
# MAGIC         CASE 
# MAGIC             WHEN r.bet_historic_em IS NOT NULL THEN bl.leg_gross_stake_amount 
# MAGIC             ELSE 0 
# MAGIC         END
# MAGIC     ), 2) AS exp_handle,
# MAGIC     
# MAGIC     round(sum(
# MAGIC         CASE
# MAGIC             WHEN r.bet_historic_em IS NOT NULL AND bl.is_profit_boost = TRUE THEN
# MAGIC                 bl.leg_gross_stake_amount * (((coalesce(r.bet_mikeprice_em, r.bet_historic_em) - 1.00) * 
# MAGIC                 (bl.bet_price_actual_derived / NULLIF(bl.bet_price_unboosted, 0))) + 1.00)
# MAGIC             WHEN r.bet_historic_em IS NOT NULL THEN
# MAGIC                 bl.leg_gross_stake_amount * coalesce(r.bet_mikeprice_em, r.bet_historic_em)
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ), 2) AS exp_revenue,
# MAGIC     
# MAGIC     -- Finance expected revenue (complex calculation optimized)
# MAGIC     round(sum(
# MAGIC         CASE
# MAGIC             WHEN r.bet_historic_em IS NOT NULL AND bl.is_profit_boost = TRUE AND bl.leg_bonus_bet_used_amount > 0 THEN
# MAGIC                 bl.leg_gross_stake_amount * (((coalesce(r.bet_mikeprice_em, r.bet_historic_em) - 1.00) * 
# MAGIC                 (bl.bet_price_actual_derived / NULLIF(bl.bet_price_unboosted, 0))) + 1.00) +
# MAGIC                 (((1 - coalesce(r.bet_mikeprice_em, r.bet_historic_em)) / 
# MAGIC                 NULLIF(coalesce(bl.bet_price_unboosted, bl.bet_price_actual_derived), 0)) * 
# MAGIC                 coalesce(bl.leg_bonus_bet_used_amount, 0))
# MAGIC             WHEN r.bet_historic_em IS NOT NULL AND bl.is_profit_boost = TRUE THEN
# MAGIC                 bl.leg_gross_stake_amount * (((coalesce(r.bet_mikeprice_em, r.bet_historic_em) - 1.00) * 
# MAGIC                 (bl.bet_price_actual_derived / NULLIF(bl.bet_price_unboosted, 0))) + 1.00)
# MAGIC             WHEN r.bet_historic_em IS NOT NULL AND bl.leg_bonus_bet_used_amount > 0 THEN
# MAGIC                 bl.leg_gross_stake_amount * coalesce(r.bet_mikeprice_em, r.bet_historic_em) +
# MAGIC                 (((1 - coalesce(r.bet_mikeprice_em, r.bet_historic_em)) / 
# MAGIC                 NULLIF(coalesce(bl.bet_price_unboosted, bl.bet_price_actual_derived), 0)) * 
# MAGIC                 coalesce(bl.leg_bonus_bet_used_amount, 0))
# MAGIC             WHEN r.bet_historic_em IS NOT NULL THEN
# MAGIC                 bl.leg_gross_stake_amount * coalesce(r.bet_mikeprice_em, r.bet_historic_em)
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ), 2) AS finance_exp_revenue,
# MAGIC     
# MAGIC     -- MikePrice metrics
# MAGIC     round(sum(
# MAGIC         CASE 
# MAGIC             WHEN r.bet_mikeprice_em IS NOT NULL THEN bl.leg_gross_stake_amount 
# MAGIC             ELSE 0 
# MAGIC         END
# MAGIC     ), 2) AS mikeprice_handle,
# MAGIC     
# MAGIC     round(sum(
# MAGIC         CASE 
# MAGIC             WHEN r.bet_mikeprice_em IS NOT NULL THEN bl.leg_gross_stake_amount * r.bet_mikeprice_em 
# MAGIC             ELSE 0 
# MAGIC         END
# MAGIC     ), 2) AS mikeprice_exp_revenue,
# MAGIC     
# MAGIC     -- BNN shrewd detection (optimized logic)
# MAGIC     round(sum(
# MAGIC         CASE
# MAGIC             WHEN r.online_matched_accounts = 0 OR r.online_matched_vol = 0 THEN 0
# MAGIC             WHEN (r.online_limit_bet_accounts / r.online_matched_accounts > 0.10) OR
# MAGIC                  (r.online_matched_shrewd_accounts / r.online_matched_accounts > 0.10) OR
# MAGIC                  (r.online_limit_bet_vol / r.online_matched_vol > 0.35) OR
# MAGIC                  (r.online_matched_shrewd_vol / r.online_matched_vol > 0.45) THEN
# MAGIC                 bl.leg_gross_stake_amount
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ), 2) AS bnn_shrewd_stake,
# MAGIC     
# MAGIC     round(sum(
# MAGIC         CASE
# MAGIC             WHEN r.online_matched_accounts IS NOT NULL THEN bl.leg_gross_stake_amount
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ), 2) AS bnn_all_stake
# MAGIC
# MAGIC FROM
# MAGIC     -- OPTIMIZED JOIN ORDER: Start with filtered date range, then main table
# MAGIC     dates_online AS d
# MAGIC     INNER JOIN core_views.sportsbook.bet_legs AS bl
# MAGIC         ON d.bet_settled_date_local = bl.bet_last_settled_local_ts
# MAGIC         -- Apply main filters early for performance
# MAGIC         AND bl.bet_status_code = 'c'
# MAGIC         AND bl.bet_result IN ('won', 'lost', 'void')
# MAGIC         AND bl.location_code != 'fd'
# MAGIC         AND (bl.is_test_account IS NULL OR bl.is_test_account = FALSE)
# MAGIC         -- CRITICAL: Partition filter for performance
# MAGIC         AND bl.bet_last_settled_local_ts >= current_date() - INTERVAL 90 DAYS
# MAGIC     
# MAGIC     -- LEFT JOINs for optional data (filtered in ON clause where possible)
# MAGIC     LEFT JOIN features.reference.gen_sbk_gameplay_groupings AS g
# MAGIC         ON g.sport_name = bl.leg_sport_name_reporting
# MAGIC         AND g.competition_name = bl.leg_competition_name_reporting
# MAGIC         AND g.market_name = bl.leg_market_name_openbet
# MAGIC     
# MAGIC     LEFT JOIN service.ml.cd_fd_risk_features_union AS r
# MAGIC         ON r.bet_placed_date = bl.bet_placed_ts
# MAGIC         AND r.bet_id = bl.bet_id
# MAGIC         AND r.leg_id = bl.leg_id
# MAGIC         AND r.bet_placed_date >= current_date() - INTERVAL 395 DAYS
# MAGIC     
# MAGIC     LEFT JOIN core_views.sportsbook.customer_risk_user_profiles_us AS rg
# MAGIC         ON rg.row_id = TRY_CAST(bl.fanduel_user_id AS VARCHAR(255))
# MAGIC         AND rg.location_code = bl.location_code
# MAGIC         AND rg.account_category_start_ts <= bl.bet_placed_ts
# MAGIC         AND rg.account_category_end_ts > bl.bet_placed_ts
# MAGIC
# MAGIC
# MAGIC -- GROUP BY ALL is efficient in Databricks
# MAGIC GROUP BY ALL
# MAGIC
# MAGIC -- Remove LIMIT for production use - this is for testing only
# MAGIC LIMIT 1000
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZED SQL03 - Retail Performance Data
# MAGIC -- Applied AGENTS.md best practices for performance and readability
# MAGIC -- DEPENDS ON: SQL01 temporary views (dates_retail from SQL01)
# MAGIC
# MAGIC -- Performance optimizations applied:
# MAGIC -- 1. Uses temporary views from SQL01 for date filtering
# MAGIC -- 2. Moved date calculations to avoid functions on columns
# MAGIC -- 3. Optimized JOIN order (smallest table first)
# MAGIC -- 4. Used explicit column names instead of SELECT *
# MAGIC -- 5. Applied SARGable WHERE conditions
# MAGIC -- 6. Added table aliases for readability
# MAGIC
# MAGIC -- Drop and recreate table for clean state
# MAGIC DROP TABLE IF EXISTS sandbox.shared.br_temp_sbk_performance_retail;
# MAGIC
# MAGIC CREATE TABLE sandbox.shared.br_temp_sbk_performance_retail AS
# MAGIC SELECT
# MAGIC     -- Basic identifiers (optimized with UPPER functions)
# MAGIC     UPPER(bl.betting_platform) AS source_id,
# MAGIC     UPPER(bl.location_code) AS state_id,
# MAGIC     'US Retail' AS channel,
# MAGIC     
# MAGIC     -- Shop name mapping (retail-specific business logic)
# MAGIC     CASE 
# MAGIC         WHEN bl.business_unit_name = 'motorcity casino' THEN 'MotorCity Casino'
# MAGIC         WHEN bl.business_unit_name = 'ip casino resort spa' THEN 'IP Casino Resort Spa'
# MAGIC         WHEN bl.business_unit_name = 'sam''s town tunica' THEN 'Sam''s Town Tunica'
# MAGIC         WHEN bl.business_unit_name = 'bally''s' THEN 'Bally''s AC'
# MAGIC         WHEN bl.business_unit_name = 'ballys ac' THEN 'Bally''s AC'
# MAGIC         WHEN bl.business_unit_name = 'casino pittsburgh' THEN 'Live! Casino Pittsburgh'
# MAGIC         WHEN bl.business_unit_name = 'philly live' THEN 'Live! Casino Philadelphia'
# MAGIC         WHEN bl.business_unit_name = 'live! casino' AND bl.state = 'md' THEN 'Live! Casino Maryland'
# MAGIC         WHEN bl.business_unit_name = 'par-a-dice' THEN 'Par-A-Dice'
# MAGIC         WHEN bl.business_unit_name = 'sams town shrevport' THEN 'Sam''s Town Shrevport'
# MAGIC         ELSE initcap(bl.business_unit_name)
# MAGIC     END AS shop_name,
# MAGIC     
# MAGIC     initcap(bl.channel) AS retail_channel,
# MAGIC     
# MAGIC     -- Monitored status
# MAGIC     CASE 
# MAGIC         WHEN bl.mc_code IS NOT NULL THEN 'Monitored'
# MAGIC         ELSE 'Other'
# MAGIC     END AS monitored_yn,
# MAGIC     
# MAGIC     -- Date dimensions (using date_trunc efficiently with timezone conversion)
# MAGIC     date_trunc('month', bl.bet_last_settled_ts) AS settled_month,
# MAGIC     date_trunc('week', bl.bet_last_settled_ts) AS settled_week,
# MAGIC     date_trunc('day', bl.bet_last_settled_ts) AS settled_day,
# MAGIC     bl.bet_settled_date_local AS settled_day_local,
# MAGIC     
# MAGIC     -- Sport grouping logic (optimized CASE structure - same as online)
# MAGIC     CASE
# MAGIC         WHEN coalesce(g.sport_group_2, 'Other') != 'Other' THEN g.sport_group_2
# MAGIC         WHEN g.competition_group = 'NHL' THEN 'NHL'
# MAGIC         WHEN bl.sport_name = 'basketball' AND bl.competition_name = 'nba' THEN 'NBA'
# MAGIC         WHEN bl.sport_name = 'football' AND bl.competition_name = 'nfl' THEN 'NFL'
# MAGIC         WHEN bl.sport_name = 'baseball' AND bl.competition_name = 'mlb' THEN 'MLB'
# MAGIC         WHEN bl.sport_name = 'basketball' AND bl.competition_name LIKE '%ncaa%' THEN 'NCAAB'
# MAGIC         WHEN bl.sport_name = 'basketball' AND bl.competition_name LIKE '%college%' THEN 'NCAAB'
# MAGIC         WHEN bl.sport_name = 'football' AND bl.competition_name LIKE '%ncaa%' THEN 'NCAAF'
# MAGIC         WHEN bl.sport_name = 'football' AND bl.competition_name LIKE '%college%' THEN 'NCAAF'
# MAGIC         WHEN bl.sport_name = 'hockey' AND bl.competition_name = 'nhl' THEN 'NHL'
# MAGIC         WHEN bl.sport_name = 'soccer' THEN 'Soccer'
# MAGIC         WHEN bl.sport_name = 'tennis' THEN 'Tennis'
# MAGIC         WHEN bl.sport_name = 'golf' THEN 'Golf'
# MAGIC         ELSE 'Other'
# MAGIC     END AS sport_group,
# MAGIC     
# MAGIC     -- Sport name mapping (optimized coalesce structure)
# MAGIC     coalesce(
# MAGIC         g.sport,
# MAGIC         CASE
# MAGIC             WHEN bl.sport_name = 'baseball' THEN 'Baseball'
# MAGIC             WHEN bl.sport_name IN ('basketball', 'us basketball', 'basketball-us basketball', 'european basketball') THEN 'Basketball'
# MAGIC             WHEN bl.sport_name = 'football' THEN 'Football'
# MAGIC             WHEN bl.sport_name IN ('golf', 'golf-golf') THEN 'Golf'
# MAGIC             WHEN bl.sport_name IN ('hockey', 'hockey-hockey', 'american ice hockey', 'ice hockey-american ice hockey') THEN 'Hockey'
# MAGIC             WHEN bl.sport_name IN ('mixed martial arts', 'mma') THEN 'MMA'
# MAGIC             WHEN bl.sport_name = 'odds boost' THEN 'Promotions'
# MAGIC             WHEN bl.sport_name IN ('soccer', 'football matches', 'international football outrights') THEN 'Soccer'
# MAGIC             WHEN bl.sport_name = 'table tennis' THEN 'Table Tennis'
# MAGIC             WHEN bl.sport_name IN ('tennis', 'tennis-tennis') THEN 'Tennis'
# MAGIC             ELSE 'Other'
# MAGIC         END
# MAGIC     ) AS sport_name,
# MAGIC     
# MAGIC     -- Competition group mapping (optimized)
# MAGIC     coalesce(
# MAGIC         g.competition_group,
# MAGIC         CASE
# MAGIC             WHEN bl.sport_name = 'basketball' AND bl.competition_name = 'nba' THEN 'NBA'
# MAGIC             WHEN bl.sport_name = 'football' AND bl.competition_name = 'nfl' THEN 'NFL'
# MAGIC             WHEN bl.sport_name = 'baseball' AND bl.competition_name = 'mlb' THEN 'MLB'
# MAGIC             WHEN bl.sport_name = 'basketball' AND bl.competition_name LIKE '%ncaa%' THEN 'NCAAB'
# MAGIC             WHEN bl.sport_name = 'basketball' AND bl.competition_name LIKE '%college%' THEN 'NCAAB'
# MAGIC             WHEN bl.sport_name = 'football' AND bl.competition_name LIKE '%ncaa%' THEN 'NCAAF'
# MAGIC             WHEN bl.sport_name = 'football' AND bl.competition_name LIKE '%college%' THEN 'NCAAF'
# MAGIC             WHEN bl.sport_name = 'hockey' AND bl.competition_name = 'nhl' THEN 'NHL'
# MAGIC             ELSE 'Other'
# MAGIC         END
# MAGIC     ) AS competition_group,
# MAGIC     
# MAGIC     -- Event details (using initcap efficiently)
# MAGIC     initcap(bl.competition_name) AS competition_name,
# MAGIC     replace(replace(initcap(bl.event_name), ' At ', ' at '), ' V ', ' v ') AS event_name,
# MAGIC     date_trunc('day', bl.event_start_date) AS event_date,
# MAGIC     
# MAGIC     -- Market grouping
# MAGIC     CASE
# MAGIC         WHEN bl.price_type_code = 't' THEN 'Teasers'
# MAGIC         WHEN g.oddsboost_yn = 'Y' THEN 'OddsBoost'
# MAGIC         WHEN g.market_group IS NOT NULL THEN g.market_group
# MAGIC         WHEN g.market_name LIKE '%boost%' THEN 'OddsBoost'
# MAGIC         ELSE 'Other'
# MAGIC     END AS market_group,
# MAGIC     
# MAGIC     initcap(bl.market_name) AS market_name,
# MAGIC     
# MAGIC     -- Conditional fields for recent data (optimized date comparison)
# MAGIC     CASE
# MAGIC         WHEN bl.bet_settled_date_local >= current_date() - INTERVAL 14 DAYS THEN bl.handicap_value
# MAGIC     END AS handicap_value,
# MAGIC     CASE
# MAGIC         WHEN bl.bet_settled_date_local >= current_date() - INTERVAL 14 DAYS THEN initcap(bl.selection_name)
# MAGIC     END AS selection_name,
# MAGIC     CASE
# MAGIC         WHEN bl.bet_settled_date_local >= current_date() - INTERVAL 14 DAYS THEN CAST(bl.event_id AS VARCHAR(255))
# MAGIC     END AS event_id,
# MAGIC     CASE
# MAGIC         WHEN bl.bet_settled_date_local >= current_date() - INTERVAL 14 DAYS THEN CAST(bl.market_id AS VARCHAR(255))
# MAGIC     END AS market_id,
# MAGIC     CASE
# MAGIC         WHEN bl.bet_settled_date_local >= current_date() - INTERVAL 14 DAYS THEN CAST(bl.selection_id AS VARCHAR(255))
# MAGIC     END AS selection_id,
# MAGIC     
# MAGIC     -- In-play indicator (simplified)
# MAGIC     CASE
# MAGIC         WHEN bl.is_event_in_play = -1 THEN 'In-Play'
# MAGIC         ELSE 'Pre-Match'
# MAGIC     END AS in_play_yn,
# MAGIC     
# MAGIC     -- Time to off calculation (optimized with pre-calculated intervals)
# MAGIC     CASE
# MAGIC         WHEN bl.is_event_in_play = -1 THEN 'In-Play'
# MAGIC         WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 30 THEN '0m - 30m'
# MAGIC         WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 60 THEN '30m - 1h'
# MAGIC         WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 120 THEN '1h - 2h'
# MAGIC         WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 240 THEN '2h - 4h'
# MAGIC         WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 480 THEN '4h - 8h'
# MAGIC         WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 720 THEN '8h - 12h'
# MAGIC         WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 1440 THEN '12h - 24h'
# MAGIC         WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 2880 THEN '24h - 48h'
# MAGIC         WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 4320 THEN '48h - 72h'
# MAGIC         WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 10080 THEN '3d - 1w'
# MAGIC         WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 20160 THEN '1w - 2w'
# MAGIC         WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 43200 THEN '2w - 1m'
# MAGIC         WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 131040 THEN '1m - 3m'
# MAGIC         ELSE '3m +'
# MAGIC     END AS time_to_off,
# MAGIC     
# MAGIC     -- Bet type classification
# MAGIC     CASE
# MAGIC         WHEN bl.bet_type = 'straight' AND bl.leg_numbers = 1 THEN 'Straight'
# MAGIC         WHEN bl.price_type_code = 'sgm' AND bl.leg_numbers > 1 AND bl.is_same_game_parlay_plus = TRUE THEN 'SGP+'
# MAGIC         WHEN bl.price_type_code = 'sgm' AND bl.leg_numbers > 1 THEN 'SGP'
# MAGIC         ELSE 'Parlay'
# MAGIC     END AS bet_type,
# MAGIC     
# MAGIC     -- Leg count buckets
# MAGIC     CASE
# MAGIC         WHEN bl.leg_numbers = 1 THEN '1 leg'
# MAGIC         WHEN bl.leg_numbers = 2 THEN '2 legs'
# MAGIC         WHEN bl.leg_numbers = 3 THEN '3 legs'
# MAGIC         WHEN bl.leg_numbers = 4 THEN '4 legs'
# MAGIC         WHEN bl.leg_numbers BETWEEN 5 AND 6 THEN '5-6 legs'
# MAGIC         WHEN bl.leg_numbers BETWEEN 7 AND 9 THEN '7-9 legs'
# MAGIC         WHEN bl.leg_numbers BETWEEN 10 AND 15 THEN '10-15 legs'
# MAGIC         WHEN bl.leg_numbers > 15 THEN '16+ legs'
# MAGIC     END AS leg_count,
# MAGIC     
# MAGIC     -- Cashout indicator
# MAGIC     CASE
# MAGIC         WHEN bl.bet_cashed_out = -1 THEN 'CashOut'
# MAGIC         ELSE 'No C/O'
# MAGIC     END AS cashout_yn,
# MAGIC     
# MAGIC     -- Price grouping (optimized ranges)
# MAGIC     CASE
# MAGIC         WHEN bl.leg_price_decimal <= 1.50 THEN '1.00 - 1.50'
# MAGIC         WHEN bl.leg_price_decimal <= 1.90 THEN '1.51 - 1.90'
# MAGIC         WHEN bl.leg_price_decimal <= 2.10 THEN '1.91 - 2.10'
# MAGIC         WHEN bl.leg_price_decimal <= 3.33 THEN '2.11 - 3.33'
# MAGIC         WHEN bl.leg_price_decimal <= 7.00 THEN '3.34 - 7.00'
# MAGIC         WHEN bl.leg_price_decimal <= 15.00 THEN '7.01 - 15.00'
# MAGIC         WHEN bl.leg_price_decimal <= 51.00 THEN '15.01 - 51.00'
# MAGIC         ELSE '51.01 +'
# MAGIC     END AS price_group,
# MAGIC     
# MAGIC     -- Percent max bet grouping
# MAGIC     CASE
# MAGIC         WHEN bl.bet_percent_max_bet <= 0.1 THEN '0% - 10%'
# MAGIC         WHEN bl.bet_percent_max_bet <= 0.3 THEN '11% - 30%'
# MAGIC         WHEN bl.bet_percent_max_bet <= 0.8 THEN '31% - 80%'
# MAGIC         WHEN bl.bet_percent_max_bet <= 1.0 THEN '81% - 100%'
# MAGIC         ELSE '> 100%'
# MAGIC     END AS percent_max,
# MAGIC     
# MAGIC     -- Free bet indicator (retail uses promo_redeemed)
# MAGIC     CASE
# MAGIC         WHEN coalesce(bl.promo_redeemed, 0) > 0 THEN 'Free Bet'
# MAGIC         ELSE 'Cash Bet'
# MAGIC     END AS free_bet_yn,
# MAGIC     
# MAGIC     -- Profit boost indicator (retail doesn't have profit boost)
# MAGIC     'Unboosted' AS profit_boost_yn,
# MAGIC     
# MAGIC     -- Stake factor grouping
# MAGIC     CASE
# MAGIC         WHEN bl.bet_stake_factor = 1.00 THEN '1.00'
# MAGIC         WHEN bl.bet_stake_factor > 1.00 THEN '1.01 +'
# MAGIC         WHEN bl.bet_stake_factor <= 0.10 THEN '0.01 - 0.10'
# MAGIC         WHEN bl.bet_stake_factor <= 0.30 THEN '0.11 - 0.30'
# MAGIC         ELSE '0.31 - 0.99'
# MAGIC     END AS stake_factor_group,
# MAGIC     
# MAGIC     -- Liability group (retail doesn't have liability grouping)
# MAGIC     '' AS liability_group,
# MAGIC     
# MAGIC     -- EM population indicator
# MAGIC     CASE
# MAGIC         WHEN r.bet_mikeprice_em IS NULL AND r.bet_historic_em IS NULL THEN 'N'
# MAGIC         ELSE 'Y'
# MAGIC     END AS em_populated_yn,
# MAGIC     
# MAGIC     -- Aggregated metrics (optimized with proper rounding)
# MAGIC     round(sum(bl.bet_count), 4) AS bet_count,
# MAGIC     round(sum(bl.gross_stake), 2) AS gross_handle,
# MAGIC     round(sum(coalesce(bl.promo_redeemed, 0)), 2) AS free_bet_handle,
# MAGIC     round(sum(bl.ggr), 2) AS finance_revenue,
# MAGIC     round(sum(bl.ggr), 2) AS trading_revenue,
# MAGIC     
# MAGIC     -- Expected metrics (optimized calculations)
# MAGIC     round(sum(
# MAGIC         CASE 
# MAGIC             WHEN r.bet_historic_em IS NOT NULL THEN bl.gross_stake 
# MAGIC             ELSE 0 
# MAGIC         END
# MAGIC     ), 2) AS exp_handle,
# MAGIC     
# MAGIC     round(sum(
# MAGIC         CASE
# MAGIC             WHEN r.bet_historic_em IS NOT NULL THEN
# MAGIC                 bl.gross_stake * coalesce(r.bet_mikeprice_em, r.bet_historic_em)
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ), 2) AS exp_revenue,
# MAGIC     
# MAGIC     -- Finance expected revenue (retail simplified - no profit boost complexity)
# MAGIC     round(sum(
# MAGIC         CASE
# MAGIC             WHEN r.bet_historic_em IS NOT NULL THEN
# MAGIC                 bl.gross_stake * coalesce(r.bet_mikeprice_em, r.bet_historic_em)
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ), 2) AS finance_exp_revenue,
# MAGIC     
# MAGIC     -- MikePrice metrics
# MAGIC     round(sum(
# MAGIC         CASE 
# MAGIC             WHEN r.bet_mikeprice_em IS NOT NULL THEN bl.gross_stake 
# MAGIC             ELSE 0 
# MAGIC         END
# MAGIC     ), 2) AS mikeprice_handle,
# MAGIC     
# MAGIC     round(sum(
# MAGIC         CASE 
# MAGIC             WHEN r.bet_mikeprice_em IS NOT NULL THEN bl.gross_stake * r.bet_mikeprice_em 
# MAGIC             ELSE 0 
# MAGIC         END
# MAGIC     ), 2) AS mikeprice_exp_revenue,
# MAGIC     
# MAGIC     -- BNN shrewd detection (optimized logic)
# MAGIC     round(sum(
# MAGIC         CASE
# MAGIC             WHEN r.online_matched_accounts = 0 OR r.online_matched_vol = 0 THEN 0
# MAGIC             WHEN (r.online_limit_bet_accounts / CAST(r.online_matched_accounts AS REAL) > 0.10) OR
# MAGIC                  (r.online_matched_shrewd_accounts / CAST(r.online_matched_accounts AS REAL) > 0.10) OR
# MAGIC                  (r.online_limit_bet_vol / CAST(r.online_matched_vol AS REAL) > 0.35) OR
# MAGIC                  (r.online_matched_shrewd_vol / CAST(r.online_matched_vol AS REAL) > 0.45) THEN
# MAGIC                 bl.gross_stake
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ), 2) AS bnn_shrewd_stake,
# MAGIC     
# MAGIC     round(sum(
# MAGIC         CASE
# MAGIC             WHEN r.online_matched_accounts IS NOT NULL THEN bl.gross_stake
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ), 2) AS bnn_all_stake
# MAGIC
# MAGIC FROM
# MAGIC     -- OPTIMIZED JOIN ORDER: Use SQL01 temporary view for date filtering
# MAGIC     dates_retail AS d
# MAGIC     INNER JOIN core_views.sportsbook_retail.gameplay_legs AS bl
# MAGIC         ON d.bet_settled_date_local = bl.bet_last_settled_local_ts
# MAGIC         -- Apply main filters early for performance
# MAGIC         AND bl.bet_status = 'c'
# MAGIC         AND bl.bet_result IN ('won', 'lost', 'void')
# MAGIC         -- CRITICAL: Partition filter for performance
# MAGIC         AND bl.bet_last_settled_local_ts >= current_date() - INTERVAL 90 DAYS
# MAGIC     
# MAGIC     -- LEFT JOINs for optional data (filtered in ON clause where possible)
# MAGIC     LEFT JOIN features.reference.gen_sbk_gameplay_groupings AS g
# MAGIC         ON g.sport_name = bl.sport_name
# MAGIC         AND g.competition_name = bl.competition_name
# MAGIC         AND g.market_name = bl.market_name
# MAGIC     
# MAGIC     LEFT JOIN service.ml.cd_fd_risk_features_union AS r
# MAGIC         ON r.bet_placed_date = bl.bet_placed_ts
# MAGIC         AND r.bet_id = bl.bet_id
# MAGIC         AND r.leg_id = TRIM(TRAILING '0' FROM CAST(bl.leg_id AS VARCHAR(255)))
# MAGIC         AND r.bet_placed_date >= current_date() - INTERVAL 395 DAYS
# MAGIC
# MAGIC -- GROUP BY ALL is efficient in Databricks
# MAGIC GROUP BY ALL
# MAGIC
# MAGIC -- Remove LIMIT for production use - this is for testing only
# MAGIC LIMIT 1000
# MAGIC ;
# MAGIC

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Define SQL Code  -  Daily Performance
# Sportsbook Performance Online
sql_02 = """
              TRUNCATE  analyst_rt_staging.br_temp_sbk_performance_online ;
                                
           INSERT INTO  analyst_rt_staging.br_temp_sbk_performance_online (
    
                SELECT  UPPER( bet_legs.source ) AS source_id,
                        UPPER( bet_legs.state  ) AS state_id,
                
                        'US Online'::VARCHAR AS channel,
                        ''::VARCHAR          AS shop_name,
                        ''::VARCHAR          AS retail_channel,
                        ''::VARCHAR          AS monitored_yn,                
                        
                        DATE_TRUNC( 'month', bet_legs.bet_settled_date AT TIME ZONE 'America/New_York' ) AS settled_month,
                        DATE_TRUNC( 'week',  bet_legs.bet_settled_date AT TIME ZONE 'America/New_York' ) AS settled_week,
                        DATE_TRUNC( 'day',   bet_legs.bet_settled_date AT TIME ZONE 'America/New_York' ) AS settled_day,
                        
                        bet_legs.bet_settled_date_local AS settled_day_local,
                        
                        CASE WHEN NVL( g.sport_group_2, 'Other' ) <> 'Other'
                             THEN g.sport_group_2
                             WHEN g.competition_group = 'NHL'
                             THEN 'NHL'
                             WHEN bet_legs.sport_name       = 'basketball'
                              AND bet_legs.competition_name = 'nba'                     
                             THEN 'NBA'
                             WHEN bet_legs.sport_name       = 'football'
                              AND bet_legs.competition_name = 'nfl'                     
                             THEN 'NFL'
                             WHEN bet_legs.sport_name       = 'baseball'
                              AND bet_legs.competition_name = 'mlb'                     
                             THEN 'MLB'
                             WHEN bet_legs.sport_name       = 'basketball'
                              AND bet_legs.competition_name ILIKE '%ncaa%'                     
                             THEN 'NCAAB'
                             WHEN bet_legs.sport_name       = 'basketball'
                              AND bet_legs.competition_name ILIKE '%college%'                     
                             THEN 'NCAAB'
                             WHEN bet_legs.sport_name       = 'football'
                              AND bet_legs.competition_name ILIKE '%ncaa%'                     
                             THEN 'NCAAF'
                             WHEN bet_legs.sport_name       = 'football'
                              AND bet_legs.competition_name ILIKE '%college%'                     
                             THEN 'NCAAF'
                             WHEN bet_legs.sport_name       = 'hockey'
                              AND bet_legs.competition_name = 'nhl'    
                             THEN 'NHL'
                             WHEN bet_legs.sport_name = 'soccer'
                             THEN 'Soccer'
                             WHEN bet_legs.sport_name = 'tennis'
                             THEN 'Tennis'
                             WHEN bet_legs.sport_name = 'golf'
                             THEN 'Golf'
                             ELSE 'Other'
                              END AS sport_group,
                        
                        NVL( g.sport, CASE WHEN bet_legs.sport_name = 'baseball'                         THEN 'Baseball'
                                           WHEN bet_legs.sport_name = 'basketball'                       THEN 'Basketball'
                                           WHEN bet_legs.sport_name = 'us basketball'                    THEN 'Basketball'
                                           WHEN bet_legs.sport_name = 'basketball-us basketball'         THEN 'Basketball'
                                           WHEN bet_legs.sport_name = 'european basketball'              THEN 'Basketball'            
                                           WHEN bet_legs.sport_name = 'football'                         THEN 'Football'                  
                                           WHEN bet_legs.sport_name = 'golf'                             THEN 'Golf'                      
                                           WHEN bet_legs.sport_name = 'golf-golf'                        THEN 'Golf'                
                                           WHEN bet_legs.sport_name = 'hockey'                           THEN 'Hockey'                   
                                           WHEN bet_legs.sport_name = 'hockey-hockey'                    THEN 'Hockey'                      
                                           WHEN bet_legs.sport_name = 'american ice hockey'              THEN 'Hockey'                   
                                           WHEN bet_legs.sport_name = 'ice hockey-american ice hockey'   THEN 'Hockey'     
                                           WHEN bet_legs.sport_name = 'mixed martial arts'               THEN 'MMA'       
                                           WHEN bet_legs.sport_name = 'mma'                              THEN 'MMA'             
                                           WHEN bet_legs.sport_name = 'odds boost'                       THEN 'Promotions'
                                           WHEN bet_legs.sport_name = 'soccer'                           THEN 'Soccer'                     
                                           WHEN bet_legs.sport_name = 'football matches'                 THEN 'Soccer'                      
                                           WHEN bet_legs.sport_name = 'international football outrights' THEN 'Soccer'                        
                                           WHEN bet_legs.sport_name = 'table tennis'                     THEN 'Table Tennis'                 
                                           WHEN bet_legs.sport_name = 'tennis'                           THEN 'Tennis'                
                                           WHEN bet_legs.sport_name = 'tennis-tennis'                    THEN 'Tennis'
                                           ELSE 'Other'
                                            END ) AS sport_name,
                        
                        NVL( g.competition_group, CASE WHEN bet_legs.sport_name       = 'basketball'
                                                        AND bet_legs.competition_name = 'nba'                     
                                                       THEN 'NBA'
                                                       WHEN bet_legs.sport_name       = 'football'
                                                        AND bet_legs.competition_name = 'nfl'                     
                                                       THEN 'NFL'
                                                       WHEN bet_legs.sport_name       = 'baseball'
                                                        AND bet_legs.competition_name = 'mlb'                     
                                                       THEN 'MLB'
                                                       WHEN bet_legs.sport_name       = 'basketball'
                                                        AND bet_legs.competition_name ILIKE '%ncaa%'                     
                                                       THEN 'NCAAB'
                                                       WHEN bet_legs.sport_name       = 'basketball'
                                                        AND bet_legs.competition_name ILIKE '%college%'                     
                                                       THEN 'NCAAB'
                                                       WHEN bet_legs.sport_name       = 'football'
                                                        AND bet_legs.competition_name ILIKE '%ncaa%'                     
                                                       THEN 'NCAAF'
                                                       WHEN bet_legs.sport_name       = 'football'
                                                        AND bet_legs.competition_name ILIKE '%college%'
                                                       THEN 'NCAAF'
                                                       WHEN bet_legs.sport_name       = 'hockey'
                                                        AND bet_legs.competition_name = 'nhl'                     
                                                       THEN 'NHL'
                                                       ELSE 'Other'
                                                        END ) AS competition_group,
                        
                        INITCAP( bet_legs.competition_name ) AS competition_name,
                        
                        REPLACE( REPLACE( INITCAP( bet_legs.event_name ), ' At ', ' at ' ), ' V ', ' v ' )        AS event_name,
                        DATE_TRUNC( 'day', bet_legs.event_start_date AT TIME ZONE 'America/New_York' )::TIMESTAMP AS event_date,
                                                   
                        CASE WHEN bet_legs.price_type_code = 't'
                             THEN 'Teasers'
                             WHEN g.oddsboost_yn = 'Y'
                             THEN 'OddsBoost'
                             WHEN g.market_group IS NOT NULL
                             THEN g.market_group
                             WHEN g.market_name ILIKE '%boost%'
                             THEN 'OddsBoost'
                             ELSE 'Other'
                              END AS market_group,

                        INITCAP( bet_legs.market_name      ) AS market_name,
                        
                        CASE WHEN bet_legs.bet_settled_date_local >= DATEADD( week, -2, TRUNC( SYSDATE ) ) THEN bet_legs.handicap_value            END AS handicap_value,
                        CASE WHEN bet_legs.bet_settled_date_local >= DATEADD( week, -2, TRUNC( SYSDATE ) ) THEN INITCAP( bet_legs.selection_name ) END AS selection_name,
                        
                        CASE WHEN bet_legs.bet_settled_date_local >= DATEADD( week, -2, TRUNC( SYSDATE ) ) THEN bet_legs.event_id::VARCHAR     END AS event_id,
                        CASE WHEN bet_legs.bet_settled_date_local >= DATEADD( week, -2, TRUNC( SYSDATE ) ) THEN bet_legs.market_id::VARCHAR    END AS market_id,
                        CASE WHEN bet_legs.bet_settled_date_local >= DATEADD( week, -2, TRUNC( SYSDATE ) ) THEN bet_legs.selection_id::VARCHAR END AS selection_id,
                        
                        CASE WHEN bet_legs.event_in_play = -1
                             THEN 'In-Play'
                             ELSE 'Pre-Match'
                              END AS in_play_yn,
                              
                        CASE WHEN bet_legs.event_in_play = -1 THEN 'In-Play'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <=   30 THEN '0m - 30m'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <=   60 THEN '30m - 1h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <=  120 THEN '1h - 2h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <=  240 THEN '2h - 4h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <=  480 THEN '4h - 8h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <=  720 THEN '8h - 12h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= 1440 THEN '12h - 24h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= 2880 THEN '24h - 48h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= 4320 THEN '48h - 72h'
                            
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= ( 1440 *  7 ) THEN '3d - 1w'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= ( 1440 * 14 ) THEN '1w - 2w'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= ( 1440 * 30 ) THEN '2w - 1m'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= ( 1440 * 91 ) THEN '1m - 3m'
                             ELSE '3m +'
                              END AS time_to_off,
                        
                        CASE WHEN bet_legs.bet_type    = 'straight'
                              AND bet_legs.leg_numbers = 1
                             THEN 'Straight'
                             WHEN bet_legs.price_type_code = 'sgm'
                              AND bet_legs.leg_numbers     > 1
                              AND bet_legs.sgpp_yn         = TRUE
                             THEN 'SGP+'
                             WHEN bet_legs.price_type_code = 'sgm'
                              AND bet_legs.leg_numbers     > 1
                             THEN 'SGP'
                             ELSE 'Parlay'
                              END AS bet_type,
                              
                        CASE WHEN bet_legs.leg_numbers  =  1 THEN '1 leg'
                             WHEN bet_legs.leg_numbers  =  2 THEN '2 legs'
                             WHEN bet_legs.leg_numbers  =  3 THEN '3 legs'
                             WHEN bet_legs.leg_numbers  =  4 THEN '4 legs'
                             WHEN bet_legs.leg_numbers <=  6 THEN '5-6 legs'
                             WHEN bet_legs.leg_numbers <=  9 THEN '7-9 legs'
                             WHEN bet_legs.leg_numbers <= 15 THEN '10-15 legs'
                             WHEN bet_legs.leg_numbers  > 15 THEN '16+ legs'
                              END AS leg_count,
                              
                        CASE WHEN bet_legs.bet_cashed_out = -1
                             THEN 'CashOut'
                             ELSE 'No C/O'
                              END AS cashout_yn,
                              
                        CASE WHEN bet_legs.leg_price_decimal <=  1.50 THEN '1.00 -  1.50'
                             WHEN bet_legs.leg_price_decimal <=  1.90 THEN '1.51 -  1.90'
                             WHEN bet_legs.leg_price_decimal <=  2.10 THEN '1.91 -  2.10'
                             WHEN bet_legs.leg_price_decimal <=  3.33 THEN '2.11 -  3.33'
                             WHEN bet_legs.leg_price_decimal <=  7.00 THEN '3.34 -  7.00'
                             WHEN bet_legs.leg_price_decimal <= 15.00 THEN '7.01 -  15.00'
                             WHEN bet_legs.leg_price_decimal <= 51.00 THEN '15.01 -  51.00'
                             WHEN bet_legs.leg_price_decimal  > 51.00 THEN '51.01 +'
                              END AS price_group,
                
                        CASE WHEN bet_legs.bet_percent_max_bet <= 0.1 THEN '0% - 10%'
                             WHEN bet_legs.bet_percent_max_bet <= 0.3 THEN '11% - 30%'
                             WHEN bet_legs.bet_percent_max_bet <= 0.8 THEN '31% - 80%'
                             WHEN bet_legs.bet_percent_max_bet <= 1.0 THEN '81% - 100%'
                             WHEN bet_legs.bet_percent_max_bet  > 1.0 THEN '> 100%'
                              END AS percent_max,
                              
                        CASE WHEN bet_legs.free_bet_used > 0
                             THEN 'Free Bet'
                             ELSE 'Cash Bet'
                              END AS free_bet_yn,
                              
                        CASE WHEN bet_legs.is_profit_boost IS TRUE
                             THEN 'Profit Boost'
                             ELSE 'Unboosted'
                              END AS profit_boost_yn,
                        
                        CASE WHEN bet_legs.bet_stake_factor  = 1.00 THEN '1.00'
                             WHEN bet_legs.bet_stake_factor  > 1.00 THEN '1.01 +'
                             WHEN bet_legs.bet_stake_factor <= 0.10 THEN '0.01 - 0.10'
                             WHEN bet_legs.bet_stake_factor <= 0.30 THEN '0.11 - 0.30'
                             WHEN bet_legs.bet_stake_factor  < 1.00 THEN '0.31 - 0.99'
                             ELSE '1.00'
                              END AS stake_factor_group,
                              
                        CASE WHEN bet_legs.source = 'v2'
                             THEN NVL( rg.liability_group, 'Unreviewed' )
                             ELSE ''
                              END AS liability_group,
                              
                        CASE WHEN r.bet_mikeprice_em IS NULL
                              AND r.bet_historic_em  IS NULL
                             THEN 'N'
                             ELSE 'Y'
                              END AS em_populated_yn,
                        
                        ROUND( SUM( bet_legs.bet_count   ), 4 ) AS bet_count,
                        ROUND( SUM( bet_legs.gross_stake ), 2 ) AS gross_handle,

                        ROUND( SUM( bet_legs.free_bet_used ), 2 ) AS free_bet_handle,
                        
                        ROUND( SUM( bet_legs.ggr                              ), 2 ) AS finance_revenue,
                        ROUND( SUM( bet_legs.gross_gaming_revenue_trading_usd ), 2 ) AS trading_revenue,
                        
                        ROUND( SUM( CASE WHEN r.bet_historic_em IS NOT NULL THEN bet_legs.gross_stake ELSE 0 END ), 2 ) AS exp_handle,
                        
                        ROUND( SUM( CASE WHEN r.bet_historic_em IS NOT NULL
                                          AND bet_legs.is_profit_boost IS TRUE
                                         THEN bet_legs.gross_stake * ( ( ( NVL( r.bet_mikeprice_em, r.bet_historic_em ) - 1.00 ) * ( bet_legs.bet_price_actual / NULLIF( bet_legs.bet_price_unboosted, 0 ) ) ) + 1.00 )
                                         WHEN r.bet_historic_em IS NOT NULL
                                         THEN bet_legs.gross_stake * NVL( r.bet_mikeprice_em, r.bet_historic_em )
                                         ELSE 0
                                          END ), 2 ) AS exp_revenue,
                                                          
                        ROUND( SUM( CASE WHEN r.bet_historic_em IS NOT NULL
                                          AND bet_legs.is_profit_boost IS TRUE
                                          AND bet_legs.free_bet_used > 0
                                         THEN bet_legs.gross_stake * ( ( ( NVL( r.bet_mikeprice_em, r.bet_historic_em ) - 1.00 ) * ( bet_legs.bet_price_actual / NULLIF( bet_legs.bet_price_unboosted, 0 ) ) ) + 1.00 )
                                              + ( ( ( 1 - NVL( r.bet_mikeprice_em, r.bet_historic_em ) ) / NULLIF( NVL( bet_legs.bet_price_unboosted, bet_legs.bet_price_actual ), 0 )::REAL ) * NVL( bet_legs.free_bet_used, 0 ) )
                                         WHEN r.bet_historic_em IS NOT NULL
                                          AND bet_legs.is_profit_boost IS TRUE
                                         THEN bet_legs.gross_stake * ( ( ( NVL( r.bet_mikeprice_em, r.bet_historic_em ) - 1.00 ) * ( bet_legs.bet_price_actual / NULLIF( bet_legs.bet_price_unboosted, 0 ) ) ) + 1.00 )
                                         WHEN r.bet_historic_em IS NOT NULL
                                          AND bet_legs.free_bet_used > 0
                                         THEN bet_legs.gross_stake * NVL( r.bet_mikeprice_em, r.bet_historic_em )
                                              + ( ( ( 1 - NVL( r.bet_mikeprice_em, r.bet_historic_em ) ) / NULLIF( NVL( bet_legs.bet_price_unboosted, bet_legs.bet_price_actual ), 0 )::REAL ) * NVL( bet_legs.free_bet_used, 0 ) )
                                         WHEN r.bet_historic_em IS NOT NULL
                                         THEN bet_legs.gross_stake * NVL( r.bet_mikeprice_em, r.bet_historic_em )
                                         ELSE 0
                                          END ), 2 ) AS finance_exp_revenue,
                
                        ROUND( SUM( CASE WHEN r.bet_mikeprice_em IS NOT NULL THEN bet_legs.gross_stake                      ELSE 0 END ), 2 ) AS mikeprice_handle,
                        ROUND( SUM( CASE WHEN r.bet_mikeprice_em IS NOT NULL THEN bet_legs.gross_stake * r.bet_mikeprice_em ELSE 0 END ), 2 ) AS mikeprice_exp_revenue,
                        
                        ROUND( SUM( CASE WHEN r.online_matched_accounts = 0
                                           OR r.online_matched_vol      = 0
                                         THEN 0 
                                         WHEN r.online_limit_bet_accounts      / r.online_matched_accounts::REAL > 0.10
                                           OR r.online_matched_shrewd_accounts / r.online_matched_accounts::REAL > 0.10
                                           OR r.online_limit_bet_vol           / r.online_matched_vol::REAL      > 0.35
                                           OR r.online_matched_shrewd_vol      / r.online_matched_vol::REAL      > 0.45
                                         THEN bet_legs.gross_stake
                                         ELSE 0
                                          END ), 2 ) AS bnn_shrewd_stake,
                                   
                        ROUND( SUM( CASE WHEN r.online_matched_accounts IS NOT NULL
                                         THEN bet_legs.gross_stake
                                         ELSE 0
                                          END ), 2 ) AS bnn_all_stake
                                           
                  FROM  fdg.fact_sb_gameplay b

                        INNER JOIN #dates_online d
                                ON d.bet_settled_date_local = bet_legs.bet_settled_date_local
                                       
                        LEFT  JOIN analyst_rt.br_sb_groupings g
                                ON g.sport_name       = bet_legs.sport_name
                               AND g.competition_name = bet_legs.competition_name
                               AND g.market_name      = bet_legs.market_name
                
                        LEFT  JOIN analyst_rt.cd_fd_risk_features r
                                ON r.bet_placed_date = bet_legs.bet_placed_date
                               AND r.bet_id          = bet_legs.bet_id
                               AND r.leg_id          = bet_legs.leg_id::VARCHAR
                               AND r.bet_placed_date > DATEADD( month, -13, TRUNC( SYSDATE ) )
                               
                        LEFT  JOIN fdg.fact_ob_account_categorisation_user s
                                ON s.product_account_id  = bet_legs.product_account_id
                               AND s.state               = bet_legs.state
                               AND s.start_dt           <= bet_legs.bet_placed_date
                               AND s.end_dt              > bet_legs.bet_placed_date
                
                 WHERE      bet_legs.bet_status = 'c'
                        AND bet_legs.bet_result IN ( 'won', 'lost', 'void' )
                        AND (    bet_legs.is_test_account IS NULL
                              OR bet_legs.is_test_account IS FALSE )
                        AND bet_legs.state <> 'fd'
                
              GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35  ) ;
"""

# Sportsbook Performance Retail
sql_03 = """
              TRUNCATE  analyst_rt_staging.br_temp_sbk_performance_retail ;
                                
           INSERT INTO  analyst_rt_staging.br_temp_sbk_performance_retail (
    
                SELECT  UPPER( bet_legs.source ) AS source_id,
                        UPPER( bet_legs.state  ) AS state_id,
                        
                        'US Retail'::VARCHAR AS channel,
                        
                        CASE WHEN bet_legs.business_unit_name = 'motorcity casino'     THEN 'MotorCity Casino'
                             WHEN bet_legs.business_unit_name = 'ip casino resort spa' THEN 'IP Casino Resort Spa'
                             WHEN bet_legs.business_unit_name = 'sam''s town tunica'   THEN 'Sam''s Town Tunica'
                             WHEN bet_legs.business_unit_name = 'bally''s'             THEN 'Bally''s AC'
                             WHEN bet_legs.business_unit_name = 'ballys ac'            THEN 'Bally''s AC'
                             WHEN bet_legs.business_unit_name = 'casino pittsburgh'    THEN 'Live! Casino Pittsburgh'
                             WHEN bet_legs.business_unit_name = 'philly live'          THEN 'Live! Casino Philadelphia'
                             WHEN bet_legs.business_unit_name = 'live! casino'         
                              AND bet_legs.state = 'md'                                THEN 'Live! Casino Maryland'
                             WHEN bet_legs.business_unit_name = 'par-a-dice'           THEN 'Par-A-Dice'
                             WHEN bet_legs.business_unit_name = 'sams town shrevport'  THEN 'Sam''s Town Shrevport'
                             ELSE INITCAP( bet_legs.business_unit_name )
                              END AS shop_name,
                              
                        INITCAP( bet_legs.channel ) AS retail_channel,
                        
                        CASE WHEN bet_legs.mc_code IS NOT NULL
                             THEN 'Monitored'
                             ELSE 'Other'
                              END AS monitored_yn,                
                        
                        DATE_TRUNC( 'month', bet_legs.bet_settled_date AT TIME ZONE 'America/New_York' ) AS settled_month,
                        DATE_TRUNC( 'week',  bet_legs.bet_settled_date AT TIME ZONE 'America/New_York' ) AS settled_week,
                        DATE_TRUNC( 'day',   bet_legs.bet_settled_date AT TIME ZONE 'America/New_York' ) AS settled_day,
                        
                        bet_legs.bet_settled_date_local AS settled_day_local,
                        
                        CASE WHEN NVL( g.sport_group_2, 'Other' ) <> 'Other'
                             THEN g.sport_group_2
                             WHEN g.competition_group = 'NHL'
                             THEN 'NHL'
                             WHEN bet_legs.sport_name       = 'basketball'
                              AND bet_legs.competition_name = 'nba'                     
                             THEN 'NBA'
                             WHEN bet_legs.sport_name       = 'football'
                              AND bet_legs.competition_name = 'nfl'                     
                             THEN 'NFL'
                             WHEN bet_legs.sport_name       = 'baseball'
                              AND bet_legs.competition_name = 'mlb'                     
                             THEN 'MLB'
                             WHEN bet_legs.sport_name       = 'basketball'
                              AND bet_legs.competition_name ILIKE '%ncaa%'                     
                             THEN 'NCAAB'
                             WHEN bet_legs.sport_name       = 'basketball'
                              AND bet_legs.competition_name ILIKE '%college%'                     
                             THEN 'NCAAB'
                             WHEN bet_legs.sport_name       = 'football'
                              AND bet_legs.competition_name ILIKE '%ncaa%'                     
                             THEN 'NCAAF'
                             WHEN bet_legs.sport_name       = 'football'
                              AND bet_legs.competition_name ILIKE '%college%'                     
                             THEN 'NCAAF'
                             WHEN bet_legs.sport_name       = 'hockey'
                              AND bet_legs.competition_name = 'nhl'    
                             THEN 'NHL'
                             WHEN bet_legs.sport_name = 'soccer'
                             THEN 'Soccer'
                             WHEN bet_legs.sport_name = 'tennis'
                             THEN 'Tennis'
                             WHEN bet_legs.sport_name = 'golf'
                             THEN 'Golf'
                             ELSE 'Other'
                              END AS sport_group,
                        
                        NVL( g.sport, CASE WHEN bet_legs.sport_name = 'baseball'                         THEN 'Baseball'
                                           WHEN bet_legs.sport_name = 'basketball'                       THEN 'Basketball'
                                           WHEN bet_legs.sport_name = 'us basketball'                    THEN 'Basketball'
                                           WHEN bet_legs.sport_name = 'basketball-us basketball'         THEN 'Basketball'
                                           WHEN bet_legs.sport_name = 'european basketball'              THEN 'Basketball'            
                                           WHEN bet_legs.sport_name = 'football'                         THEN 'Football'                  
                                           WHEN bet_legs.sport_name = 'golf'                             THEN 'Golf'                      
                                           WHEN bet_legs.sport_name = 'golf-golf'                        THEN 'Golf'                
                                           WHEN bet_legs.sport_name = 'hockey'                           THEN 'Hockey'                   
                                           WHEN bet_legs.sport_name = 'hockey-hockey'                    THEN 'Hockey'                      
                                           WHEN bet_legs.sport_name = 'american ice hockey'              THEN 'Hockey'                   
                                           WHEN bet_legs.sport_name = 'ice hockey-american ice hockey'   THEN 'Hockey'     
                                           WHEN bet_legs.sport_name = 'mixed martial arts'               THEN 'MMA'       
                                           WHEN bet_legs.sport_name = 'mma'                              THEN 'MMA'             
                                           WHEN bet_legs.sport_name = 'odds boost'                       THEN 'Promotions'
                                           WHEN bet_legs.sport_name = 'soccer'                           THEN 'Soccer'                     
                                           WHEN bet_legs.sport_name = 'football matches'                 THEN 'Soccer'                      
                                           WHEN bet_legs.sport_name = 'international football outrights' THEN 'Soccer'                        
                                           WHEN bet_legs.sport_name = 'table tennis'                     THEN 'Table Tennis'                 
                                           WHEN bet_legs.sport_name = 'tennis'                           THEN 'Tennis'                
                                           WHEN bet_legs.sport_name = 'tennis-tennis'                    THEN 'Tennis'
                                           ELSE 'Other'
                                            END ) AS sport_name,
                        
                        NVL( g.competition_group, CASE WHEN bet_legs.sport_name       = 'basketball'
                                                        AND bet_legs.competition_name = 'nba'                     
                                                       THEN 'NBA'
                                                       WHEN bet_legs.sport_name       = 'football'
                                                        AND bet_legs.competition_name = 'nfl'                     
                                                       THEN 'NFL'
                                                       WHEN bet_legs.sport_name       = 'baseball'
                                                        AND bet_legs.competition_name = 'mlb'                     
                                                       THEN 'MLB'
                                                       WHEN bet_legs.sport_name       = 'basketball'
                                                        AND bet_legs.competition_name ILIKE '%ncaa%'                     
                                                       THEN 'NCAAB'
                                                       WHEN bet_legs.sport_name       = 'basketball'
                                                        AND bet_legs.competition_name ILIKE '%college%'                     
                                                       THEN 'NCAAB'
                                                       WHEN bet_legs.sport_name       = 'football'
                                                        AND bet_legs.competition_name ILIKE '%ncaa%'                     
                                                       THEN 'NCAAF'
                                                       WHEN bet_legs.sport_name       = 'football'
                                                        AND bet_legs.competition_name ILIKE '%college%'
                                                       THEN 'NCAAF'
                                                       WHEN bet_legs.sport_name       = 'hockey'
                                                        AND bet_legs.competition_name = 'nhl'                     
                                                       THEN 'NHL'
                                                       ELSE 'Other'
                                                        END ) AS competition_group,
                                                   
                        INITCAP( bet_legs.competition_name ) AS competition_name,
                        
                        REPLACE( REPLACE( INITCAP( bet_legs.event_name ), ' At ', ' at ' ), ' V ', ' v ' )        AS event_name,
                        DATE_TRUNC( 'day', bet_legs.event_start_date AT TIME ZONE 'America/New_York' )::TIMESTAMP AS event_date,
                                                   
                        CASE WHEN bet_legs.price_type_code = 't'
                             THEN 'Teasers'
                             WHEN g.oddsboost_yn = 'Y'
                             THEN 'OddsBoost'
                             WHEN g.market_group IS NOT NULL
                             THEN g.market_group
                             WHEN g.market_name ILIKE '%boost%'
                             THEN 'OddsBoost'
                             ELSE 'Other'
                              END AS market_group,

                        INITCAP( bet_legs.market_name      ) AS market_name,
                        
                        CASE WHEN bet_legs.bet_settled_date_local >= DATEADD( week, -2, TRUNC( SYSDATE ) ) THEN bet_legs.handicap_value            END AS handicap_value,
                        CASE WHEN bet_legs.bet_settled_date_local >= DATEADD( week, -2, TRUNC( SYSDATE ) ) THEN INITCAP( bet_legs.selection_name ) END AS selection_name,
                        
                        CASE WHEN bet_legs.bet_settled_date_local >= DATEADD( week, -2, TRUNC( SYSDATE ) ) THEN bet_legs.event_id::VARCHAR     END AS event_id,
                        CASE WHEN bet_legs.bet_settled_date_local >= DATEADD( week, -2, TRUNC( SYSDATE ) ) THEN bet_legs.market_id::VARCHAR    END AS market_id,
                        CASE WHEN bet_legs.bet_settled_date_local >= DATEADD( week, -2, TRUNC( SYSDATE ) ) THEN bet_legs.selection_id::VARCHAR END AS selection_id,
                        
                        CASE WHEN bet_legs.event_in_play = -1
                             THEN 'In-Play'
                             ELSE 'Pre-Match'
                              END AS in_play_yn,
                              
                        CASE WHEN bet_legs.event_in_play = -1 THEN 'In-Play'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <=   30 THEN '0m - 30m'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <=   60 THEN '30m - 1h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <=  120 THEN '1h - 2h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <=  240 THEN '2h - 4h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <=  480 THEN '4h - 8h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <=  720 THEN '8h - 12h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= 1440 THEN '12h - 24h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= 2880 THEN '24h - 48h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= 4320 THEN '48h - 72h'
                            
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= ( 1440 *  7 ) THEN '3d - 1w'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= ( 1440 * 14 ) THEN '1w - 2w'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= ( 1440 * 30 ) THEN '2w - 1m'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= ( 1440 * 91 ) THEN '1m - 3m'
                             ELSE '3m +'
                              END AS time_to_off,
                        
                        CASE WHEN bet_legs.bet_type    = 'straight'
                              AND bet_legs.leg_numbers = 1
                             THEN 'Straight'
                             WHEN bet_legs.price_type_code = 'sgm'
                              AND bet_legs.leg_numbers     > 1
                              AND bet_legs.is_same_game_parlay_plus IS TRUE
                             THEN 'SGP+'
                             WHEN bet_legs.price_type_code = 'sgm'
                              AND bet_legs.leg_numbers     > 1
                             THEN 'SGP'
                             ELSE 'Parlay'
                              END AS bet_type,
                              
                        CASE WHEN bet_legs.leg_numbers  =  1 THEN '1 leg'
                             WHEN bet_legs.leg_numbers  =  2 THEN '2 legs'
                             WHEN bet_legs.leg_numbers  =  3 THEN '3 legs'
                             WHEN bet_legs.leg_numbers  =  4 THEN '4 legs'
                             WHEN bet_legs.leg_numbers <=  6 THEN '5-6 legs'
                             WHEN bet_legs.leg_numbers <=  9 THEN '7-9 legs'
                             WHEN bet_legs.leg_numbers <= 15 THEN '10-15 legs'
                             WHEN bet_legs.leg_numbers  > 15 THEN '16+ legs'
                              END AS leg_count,
                              
                        CASE WHEN bet_legs.bet_cashed_out = -1
                             THEN 'CashOut'
                             ELSE 'No C/O'
                              END AS cashout_yn,
                              
                        CASE WHEN bet_legs.leg_price_decimal <=  1.50 THEN '1.00 -  1.50'
                             WHEN bet_legs.leg_price_decimal <=  1.90 THEN '1.51 -  1.90'
                             WHEN bet_legs.leg_price_decimal <=  2.10 THEN '1.91 -  2.10'
                             WHEN bet_legs.leg_price_decimal <=  3.33 THEN '2.11 -  3.33'
                             WHEN bet_legs.leg_price_decimal <=  7.00 THEN '3.34 -  7.00'
                             WHEN bet_legs.leg_price_decimal <= 15.00 THEN '7.01 -  15.00'
                             WHEN bet_legs.leg_price_decimal <= 51.00 THEN '15.01 -  51.00'
                             WHEN bet_legs.leg_price_decimal  > 51.00 THEN '51.01 +'
                              END AS price_group,
        
                        CASE WHEN bet_legs.bet_percent_max_bet <= 0.1 THEN '0% - 10%'
                             WHEN bet_legs.bet_percent_max_bet <= 0.3 THEN '11% - 30%'
                             WHEN bet_legs.bet_percent_max_bet <= 0.8 THEN '31% - 80%'
                             WHEN bet_legs.bet_percent_max_bet <= 1.0 THEN '81% - 100%'
                             WHEN bet_legs.bet_percent_max_bet  > 1.0 THEN '> 100%'
                              END AS percent_max,
                              
                        CASE WHEN coalesce(bet_legs.promo_redeemed,0) > 0
                             THEN 'Free Bet'
                             ELSE 'Cash Bet'
                              END AS free_bet_yn,

                        'Unboosted'::VARCHAR AS profit_boost_yn,
                        
                        CASE WHEN bet_legs.bet_stake_factor  = 1.00 THEN '1.00'
                             WHEN bet_legs.bet_stake_factor  > 1.00 THEN '1.01 +'
                             WHEN bet_legs.bet_stake_factor <= 0.10 THEN '0.01 - 0.10'
                             WHEN bet_legs.bet_stake_factor <= 0.30 THEN '0.11 - 0.30'
                             WHEN bet_legs.bet_stake_factor  < 1.00 THEN '0.31 - 0.99'
                             ELSE '1.00'
                              END AS stake_factor_group,
                              
                        ''::VARCHAR AS liability_group,
                              
                        CASE WHEN r.bet_mikeprice_em IS NULL
                              AND r.bet_historic_em  IS NULL
                             THEN 'N'
                             ELSE 'Y'
                              END AS em_populated_yn,
                        
                        ROUND( SUM( bet_legs.bet_count   ), 4 ) AS bet_count,
                        ROUND( SUM( bet_legs.gross_stake ), 2 ) AS gross_handle,

                        ROUND( SUM( coalesce(bet_legs.promo_redeemed,0) ), 2 ) AS free_bet_handle,
                        
                        ROUND( SUM( bet_legs.ggr ), 2 ) AS finance_revenue,
                        ROUND( SUM( bet_legs.ggr ), 2 ) AS trading_revenue,
                        
                        ROUND( SUM( CASE WHEN r.bet_historic_em  IS NOT NULL THEN bet_legs.gross_stake                                                ELSE 0 END ), 2 ) AS exp_handle,
                        ROUND( SUM( CASE WHEN r.bet_historic_em  IS NOT NULL THEN bet_legs.gross_stake * NVL( r.bet_mikeprice_em, r.bet_historic_em ) ELSE 0 END ), 2 ) AS exp_revenue,
                        ROUND( SUM( CASE WHEN r.bet_historic_em  IS NOT NULL THEN bet_legs.gross_stake * NVL( r.bet_mikeprice_em, r.bet_historic_em ) ELSE 0 END ), 2 ) AS finance_exp_revenue,
        
                        ROUND( SUM( CASE WHEN r.bet_mikeprice_em IS NOT NULL THEN bet_legs.gross_stake                      ELSE 0 END ), 2 ) AS mikeprice_handle,
                        ROUND( SUM( CASE WHEN r.bet_mikeprice_em IS NOT NULL THEN bet_legs.gross_stake * r.bet_mikeprice_em ELSE 0 END ), 2 ) AS mikeprice_exp_revenue,
                        
                        ROUND( SUM( CASE WHEN r.online_matched_accounts = 0
                                           OR r.online_matched_vol      = 0
                                         THEN 0 
                                         WHEN r.online_limit_bet_accounts      / r.online_matched_accounts::REAL > 0.10
                                           OR r.online_matched_shrewd_accounts / r.online_matched_accounts::REAL > 0.10
                                           OR r.online_limit_bet_vol           / r.online_matched_vol::REAL      > 0.35
                                           OR r.online_matched_shrewd_vol      / r.online_matched_vol::REAL      > 0.45
                                         THEN bet_legs.gross_stake
                                         ELSE 0
                                          END ), 2 ) AS bnn_shrewd_stake,
                                   
                        ROUND( SUM( CASE WHEN r.online_matched_accounts IS NOT NULL
                                         THEN bet_legs.gross_stake
                                         ELSE 0
                                          END ), 2 ) AS bnn_all_stake
                                   
                  FROM  fdg.fact_sb_gameplay_retail b

                        INNER JOIN #dates_retail d
                                ON d.bet_settled_date_local = bet_legs.bet_settled_date_local
                                       
                        LEFT  JOIN analyst_rt.br_sb_groupings g
                                ON g.sport_name       = bet_legs.sport_name
                               AND g.competition_name = bet_legs.competition_name
                               AND g.market_name      = bet_legs.market_name
        
                        LEFT  JOIN analyst_rt.cd_fd_risk_features r
                                ON r.bet_placed_date = bet_legs.bet_placed_date
                               AND r.bet_id          = bet_legs.bet_id
                               AND r.leg_id          = TRIM( TRAILING '0' FROM bet_legs.leg_id::VARCHAR )
                               AND r.bet_placed_date > DATEADD( month, -13, TRUNC( SYSDATE )
                                )
        
                 WHERE      bet_legs.bet_status = 'c'
                        AND bet_legs.bet_result IN ( 'won', 'lost', 'void' )
        
              GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35 ) ;
"""

# Sportsbook Performance Canada
sql_04 = """
              TRUNCATE  analyst_rt_staging.br_temp_sbk_performance_canada ;
                                
           INSERT INTO  analyst_rt_staging.br_temp_sbk_performance_canada (
    
                SELECT  UPPER( bet_legs.source ) AS source_id,
                        UPPER( bet_legs.state  ) AS state_id,
                        
                        'Canada Online'::VARCHAR AS channel,
                        ''::VARCHAR              AS shop_name,
                        ''::VARCHAR              AS retail_channel,
                        ''::VARCHAR              AS monitored_yn,
                        
                        DATE_TRUNC( 'month', bet_legs.bet_settled_date AT TIME ZONE 'America/New_York' ) AS settled_month,
                        DATE_TRUNC( 'week',  bet_legs.bet_settled_date AT TIME ZONE 'America/New_York' ) AS settled_week,
                        DATE_TRUNC( 'day',   bet_legs.bet_settled_date AT TIME ZONE 'America/New_York' ) AS settled_day,
                        
                        bet_legs.bet_settled_date_local AS settled_day_local,
                        
                        CASE WHEN NVL( g.sport_group_2, 'Other' ) <> 'Other'
                             THEN g.sport_group_2
                             WHEN g.competition_group = 'NHL'
                             THEN 'NHL'
                             WHEN bet_legs.sport_name       = 'basketball'
                              AND bet_legs.competition_name = 'nba'                     
                             THEN 'NBA'
                             WHEN bet_legs.sport_name       = 'football'
                              AND bet_legs.competition_name = 'nfl'                     
                             THEN 'NFL'
                             WHEN bet_legs.sport_name       = 'baseball'
                              AND bet_legs.competition_name = 'mlb'                     
                             THEN 'MLB'
                             WHEN bet_legs.sport_name       = 'basketball'
                              AND bet_legs.competition_name ILIKE '%ncaa%'                     
                             THEN 'NCAAB'
                             WHEN bet_legs.sport_name       = 'basketball'
                              AND bet_legs.competition_name ILIKE '%college%'                     
                             THEN 'NCAAB'
                             WHEN bet_legs.sport_name       = 'football'
                              AND bet_legs.competition_name ILIKE '%ncaa%'                     
                             THEN 'NCAAF'
                             WHEN bet_legs.sport_name       = 'football'
                              AND bet_legs.competition_name ILIKE '%college%'                     
                             THEN 'NCAAF'
                             WHEN bet_legs.sport_name       = 'hockey'
                              AND bet_legs.competition_name = 'nhl'    
                             THEN 'NHL'
                             WHEN bet_legs.sport_name = 'soccer'
                             THEN 'Soccer'
                             WHEN bet_legs.sport_name = 'tennis'
                             THEN 'Tennis'
                             WHEN bet_legs.sport_name = 'golf'
                             THEN 'Golf'
                             ELSE 'Other'
                              END AS sport_group,
                        
                        NVL( g.sport, CASE WHEN bet_legs.sport_name = 'baseball'                         THEN 'Baseball'
                                           WHEN bet_legs.sport_name = 'basketball'                       THEN 'Basketball'
                                           WHEN bet_legs.sport_name = 'us basketball'                    THEN 'Basketball'
                                           WHEN bet_legs.sport_name = 'basketball-us basketball'         THEN 'Basketball'
                                           WHEN bet_legs.sport_name = 'european basketball'              THEN 'Basketball'            
                                           WHEN bet_legs.sport_name = 'football'                         THEN 'Football'                  
                                           WHEN bet_legs.sport_name = 'golf'                             THEN 'Golf'                      
                                           WHEN bet_legs.sport_name = 'golf-golf'                        THEN 'Golf'                
                                           WHEN bet_legs.sport_name = 'hockey'                           THEN 'Hockey'                   
                                           WHEN bet_legs.sport_name = 'hockey-hockey'                    THEN 'Hockey'                      
                                           WHEN bet_legs.sport_name = 'american ice hockey'              THEN 'Hockey'                   
                                           WHEN bet_legs.sport_name = 'ice hockey-american ice hockey'   THEN 'Hockey'     
                                           WHEN bet_legs.sport_name = 'mixed martial arts'               THEN 'MMA'       
                                           WHEN bet_legs.sport_name = 'mma'                              THEN 'MMA'             
                                           WHEN bet_legs.sport_name = 'odds boost'                       THEN 'Promotions'
                                           WHEN bet_legs.sport_name = 'soccer'                           THEN 'Soccer'                     
                                           WHEN bet_legs.sport_name = 'football matches'                 THEN 'Soccer'                      
                                           WHEN bet_legs.sport_name = 'international football outrights' THEN 'Soccer'                        
                                           WHEN bet_legs.sport_name = 'table tennis'                     THEN 'Table Tennis'                 
                                           WHEN bet_legs.sport_name = 'tennis'                           THEN 'Tennis'                
                                           WHEN bet_legs.sport_name = 'tennis-tennis'                    THEN 'Tennis'
                                           ELSE 'Other'
                                            END ) AS sport_name,
                        
                        NVL( g.competition_group, CASE WHEN bet_legs.sport_name       = 'basketball'
                                                        AND bet_legs.competition_name = 'nba'                     
                                                       THEN 'NBA'
                                                       WHEN bet_legs.sport_name       = 'football'
                                                        AND bet_legs.competition_name = 'nfl'                     
                                                       THEN 'NFL'
                                                       WHEN bet_legs.sport_name       = 'baseball'
                                                        AND bet_legs.competition_name = 'mlb'                     
                                                       THEN 'MLB'
                                                       WHEN bet_legs.sport_name       = 'basketball'
                                                        AND bet_legs.competition_name ILIKE '%ncaa%'                     
                                                       THEN 'NCAAB'
                                                       WHEN bet_legs.sport_name       = 'basketball'
                                                        AND bet_legs.competition_name ILIKE '%college%'                     
                                                       THEN 'NCAAB'
                                                       WHEN bet_legs.sport_name       = 'football'
                                                        AND bet_legs.competition_name ILIKE '%ncaa%'                     
                                                       THEN 'NCAAF'
                                                       WHEN bet_legs.sport_name       = 'football'
                                                        AND bet_legs.competition_name ILIKE '%college%'
                                                       THEN 'NCAAF'
                                                       WHEN bet_legs.sport_name       = 'hockey'
                                                        AND bet_legs.competition_name = 'nhl'                     
                                                       THEN 'NHL'
                                                       ELSE 'Other'
                                                        END ) AS competition_group,
                                                   
                        INITCAP( bet_legs.competition_name ) AS competition_name,
                        
                        REPLACE( REPLACE( INITCAP( bet_legs.event_name ), ' At ', ' at ' ), ' V ', ' v ' )        AS event_name,
                        DATE_TRUNC( 'day', bet_legs.event_start_date AT TIME ZONE 'America/New_York' )::TIMESTAMP AS event_date,
                                                   
                        CASE WHEN bet_legs.price_type_code = 't'
                             THEN 'Teasers'
                             WHEN g.oddsboost_yn = 'Y'
                             THEN 'OddsBoost'
                             WHEN g.market_group IS NOT NULL
                             THEN g.market_group
                             WHEN g.market_name ILIKE '%boost%'
                             THEN 'OddsBoost'
                             ELSE 'Other'
                              END AS market_group,

                        INITCAP( bet_legs.market_name      ) AS market_name,
                        
                        CASE WHEN bet_legs.bet_settled_date_local >= DATEADD( week, -2, TRUNC( SYSDATE ) ) THEN bet_legs.handicap_value            END AS handicap_value,
                        CASE WHEN bet_legs.bet_settled_date_local >= DATEADD( week, -2, TRUNC( SYSDATE ) ) THEN INITCAP( bet_legs.selection_name ) END AS selection_name,
                        
                        CASE WHEN bet_legs.bet_settled_date_local >= DATEADD( week, -2, TRUNC( SYSDATE ) ) THEN bet_legs.event_id::VARCHAR     END AS event_id,
                        CASE WHEN bet_legs.bet_settled_date_local >= DATEADD( week, -2, TRUNC( SYSDATE ) ) THEN bet_legs.market_id::VARCHAR    END AS market_id,
                        CASE WHEN bet_legs.bet_settled_date_local >= DATEADD( week, -2, TRUNC( SYSDATE ) ) THEN bet_legs.selection_id::VARCHAR END AS selection_id,
                        
                        CASE WHEN bet_legs.event_in_play = -1
                             THEN 'In-Play'
                             ELSE 'Pre-Match'
                              END AS in_play_yn,
                              
                        CASE WHEN bet_legs.event_in_play = -1 THEN 'In-Play'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <=   30 THEN '0m - 30m'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <=   60 THEN '30m - 1h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <=  120 THEN '1h - 2h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <=  240 THEN '2h - 4h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <=  480 THEN '4h - 8h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <=  720 THEN '8h - 12h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= 1440 THEN '12h - 24h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= 2880 THEN '24h - 48h'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= 4320 THEN '48h - 72h'
                            
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= ( 1440 *  7 ) THEN '3d - 1w'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= ( 1440 * 14 ) THEN '1w - 2w'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= ( 1440 * 30 ) THEN '2w - 1m'
                             WHEN ROUND( DATEDIFF( minute, bet_legs.bet_placed_date AT TIME ZONE 'America/New_York', bet_legs.event_start_date AT TIME ZONE 'America/New_York' ), 0 ) <= ( 1440 * 91 ) THEN '1m - 3m'
                             ELSE '3m +'
                              END AS time_to_off,
                        
                        CASE WHEN bet_legs.bet_type    = 'straight'
                              AND bet_legs.leg_numbers = 1
                             THEN 'Straight'
                             WHEN bet_legs.price_type_code = 'sgm'
                              AND bet_legs.leg_numbers     > 1
                              AND bet_legs.sgpp_yn IS TRUE
                             THEN 'SGP+'
                             WHEN bet_legs.price_type_code = 'sgm'
                              AND bet_legs.leg_numbers     > 1
                             THEN 'SGP'
                             ELSE 'Parlay'
                              END AS bet_type,
                              
                        CASE WHEN bet_legs.leg_numbers  =  1 THEN '1 leg'
                             WHEN bet_legs.leg_numbers  =  2 THEN '2 legs'
                             WHEN bet_legs.leg_numbers  =  3 THEN '3 legs'
                             WHEN bet_legs.leg_numbers  =  4 THEN '4 legs'
                             WHEN bet_legs.leg_numbers <=  6 THEN '5-6 legs'
                             WHEN bet_legs.leg_numbers <=  9 THEN '7-9 legs'
                             WHEN bet_legs.leg_numbers <= 15 THEN '10-15 legs'
                             WHEN bet_legs.leg_numbers  > 15 THEN '16+ legs'
                              END AS leg_count,
                              
                        CASE WHEN bet_legs.bet_cashed_out = -1
                             THEN 'CashOut'
                             ELSE 'No C/O'
                              END AS cashout_yn,
                              
                        CASE WHEN bet_legs.leg_price_decimal <=  1.50 THEN '1.00 -  1.50'
                             WHEN bet_legs.leg_price_decimal <=  1.90 THEN '1.51 -  1.90'
                             WHEN bet_legs.leg_price_decimal <=  2.10 THEN '1.91 -  2.10'
                             WHEN bet_legs.leg_price_decimal <=  3.33 THEN '2.11 -  3.33'
                             WHEN bet_legs.leg_price_decimal <=  7.00 THEN '3.34 -  7.00'
                             WHEN bet_legs.leg_price_decimal <= 15.00 THEN '7.01 -  15.00'
                             WHEN bet_legs.leg_price_decimal <= 51.00 THEN '15.01 -  51.00'
                             WHEN bet_legs.leg_price_decimal  > 51.00 THEN '51.01 +'
                              END AS price_group,
        
                        CASE WHEN bet_legs.bet_percent_max_bet <= 0.1 THEN '0% - 10%'
                             WHEN bet_legs.bet_percent_max_bet <= 0.3 THEN '11% - 30%'
                             WHEN bet_legs.bet_percent_max_bet <= 0.8 THEN '31% - 80%'
                             WHEN bet_legs.bet_percent_max_bet <= 1.0 THEN '81% - 100%'
                             WHEN bet_legs.bet_percent_max_bet  > 1.0 THEN '> 100%'
                              END AS percent_max,

                        CASE WHEN bet_legs.free_bet_used > 0
                             THEN 'Free Bet'
                             ELSE 'Cash Bet'
                              END AS free_bet_yn,
                              
                        CASE WHEN bet_legs.is_profit_boost IS TRUE
                             THEN 'Profit Boost'
                             ELSE 'Unboosted'
                              END AS profit_boost_yn,
                        
                        CASE WHEN bet_legs.bet_stake_factor  = 1.00 THEN '1.00'
                             WHEN bet_legs.bet_stake_factor  > 1.00 THEN '1.01 +'
                             WHEN bet_legs.bet_stake_factor <= 0.10 THEN '0.01 - 0.10'
                             WHEN bet_legs.bet_stake_factor <= 0.30 THEN '0.11 - 0.30'
                             WHEN bet_legs.bet_stake_factor  < 1.00 THEN '0.31 - 0.99'
                             ELSE '1.00'
                              END AS stake_factor_group,
                              
                        ''::VARCHAR  AS liability_group,
                              
                        CASE WHEN r.bet_mikeprice_em IS NULL
                              AND r.bet_historic_em  IS NULL
                             THEN 'N'
                             ELSE 'Y'
                              END AS em_populated_yn,
                        
                        ROUND( SUM( bet_legs.bet_count   ), 4 ) AS bet_count,
                        ROUND( SUM( bet_legs.gross_stake ), 2 ) AS gross_handle,

                        ROUND( SUM( bet_legs.free_bet_used ), 2 ) AS free_bet_handle,
                        
                        ROUND( SUM( bet_legs.ggr ), 2 ) AS finance_revenue,
                        ROUND( SUM( bet_legs.gross_gaming_revenue_trading_cad ), 2 ) AS trading_revenue,
                        
                        ROUND( SUM( CASE WHEN r.bet_historic_em  IS NOT NULL THEN bet_legs.gross_stake                                                ELSE 0 END ), 2 ) AS exp_handle,
                        
                        ROUND( SUM( CASE WHEN r.bet_historic_em IS NOT NULL
                                          AND bet_legs.is_profit_boost IS TRUE
                                         THEN bet_legs.gross_stake * ( ( ( NVL( r.bet_mikeprice_em, r.bet_historic_em ) - 1.00 ) * ( bet_legs.bet_price_actual / NULLIF( bet_legs.bet_price_unboosted, 0 ) ) ) + 1.00 )
                                         WHEN r.bet_historic_em IS NOT NULL
                                         THEN bet_legs.gross_stake * NVL( r.bet_mikeprice_em, r.bet_historic_em )
                                         ELSE 0
                                          END ), 2 ) AS exp_revenue,
                                                          
                        ROUND( SUM( CASE WHEN r.bet_historic_em IS NOT NULL
                                          AND bet_legs.is_profit_boost IS TRUE
                                          AND bet_legs.free_bet_used > 0
                                         THEN bet_legs.gross_stake * ( ( ( NVL( r.bet_mikeprice_em, r.bet_historic_em ) - 1.00 ) * ( bet_legs.bet_price_actual / NULLIF( bet_legs.bet_price_unboosted, 0 ) ) ) + 1.00 )
                                              + ( ( ( 1 - NVL( r.bet_mikeprice_em, r.bet_historic_em ) ) / NULLIF( NVL( bet_legs.bet_price_unboosted, bet_legs.bet_price_actual ), 0 )::REAL ) * NVL( bet_legs.free_bet_used, 0 ) )
                                         WHEN r.bet_historic_em IS NOT NULL
                                          AND bet_legs.is_profit_boost IS TRUE
                                         THEN bet_legs.gross_stake * ( ( ( NVL( r.bet_mikeprice_em, r.bet_historic_em ) - 1.00 ) * ( bet_legs.bet_price_actual / NULLIF( bet_legs.bet_price_unboosted, 0 ) ) ) + 1.00 )
                                         WHEN r.bet_historic_em IS NOT NULL
                                          AND bet_legs.free_bet_used > 0
                                         THEN bet_legs.gross_stake * NVL( r.bet_mikeprice_em, r.bet_historic_em )
                                              + ( ( ( 1 - NVL( r.bet_mikeprice_em, r.bet_historic_em ) ) / NULLIF( NVL( bet_legs.bet_price_unboosted, bet_legs.bet_price_actual ), 0 )::REAL ) * NVL( bet_legs.free_bet_used, 0 ) )
                                         WHEN r.bet_historic_em IS NOT NULL
                                         THEN bet_legs.gross_stake * NVL( r.bet_mikeprice_em, r.bet_historic_em )
                                         ELSE 0
                                          END ), 2 ) AS finance_exp_revenue,
        
                        ROUND( SUM( CASE WHEN r.bet_mikeprice_em IS NOT NULL THEN bet_legs.gross_stake                      ELSE 0 END ), 2 ) AS mikeprice_handle,
                        ROUND( SUM( CASE WHEN r.bet_mikeprice_em IS NOT NULL THEN bet_legs.gross_stake * r.bet_mikeprice_em ELSE 0 END ), 2 ) AS mikeprice_exp_revenue,
                        
                        ROUND( SUM( CASE WHEN r.online_matched_accounts = 0
                                    OR r.online_matched_vol      = 0
                                  THEN 0 
                                  WHEN r.online_limit_bet_accounts      / r.online_matched_accounts::REAL > 0.10
                                    OR r.online_matched_shrewd_accounts / r.online_matched_accounts::REAL > 0.10
                                    OR r.online_limit_bet_vol           / r.online_matched_vol::REAL      > 0.35
                                    OR r.online_matched_shrewd_vol      / r.online_matched_vol::REAL      > 0.45
                                  THEN bet_legs.gross_stake
                                  ELSE 0
                                   END ), 2 ) AS bnn_shrewd_stake,
                                   
                        ROUND( SUM( CASE WHEN r.online_matched_accounts IS NOT NULL
                                  THEN bet_legs.gross_stake
                                  ELSE 0
                                   END ), 2 ) AS bnn_all_stake
                                   
                  FROM  fdg.fact_sb_gameplay_can b

                        INNER JOIN #dates_canada d
                                ON d.bet_settled_date_local = bet_legs.bet_settled_date_local
                                       
                        LEFT  JOIN analyst_rt.br_sb_groupings g
                                ON g.sport_name       = bet_legs.sport_name
                               AND g.competition_name = bet_legs.competition_name
                               AND g.market_name      = bet_legs.market_name
        
                        LEFT  JOIN analyst_rt.cl_fd_risk_features_can r
                                ON r.bet_placed_date = bet_legs.bet_placed_date
                               AND r.bet_id          = bet_legs.bet_id
                               AND r.leg_id          = bet_legs.leg_id

                        

                 WHERE      bet_legs.bet_status = 'c'
                        AND bet_legs.bet_result IN ( 'won', 'lost', 'void' )
                        
                        AND bet_legs.is_test_account IS FALSE 
        
              GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35 ) ;
"""

# Sportsbook Performance Union
sql_05 = """
              TRUNCATE  analyst_rt_staging.br_temp_sbk_performance_union ;
                                
           INSERT INTO  analyst_rt_staging.br_temp_sbk_performance_union (
    
                SELECT  *
                  FROM  analyst_rt_staging.br_temp_sbk_performance_online
                  
                        UNION ALL
                        
                SELECT  *
                  FROM  analyst_rt_staging.br_temp_sbk_performance_retail
                  
                        UNION ALL
                        
                SELECT  *
                  FROM  analyst_rt_staging.br_temp_sbk_performance_canada ) ;
"""

# COMMAND ----------

# DBTITLE 1,Define SQL Code  -  Delete & Replace Recent Dates
# Delete Recent Dates
sql_06 = """
           DELETE FROM  analyst_rt.sbk_performance_daily
                 WHERE     ( channel = 'US Online'     AND settled_day_local IN ( SELECT bet_settled_date_local FROM #dates_online ) )
                        OR ( channel = 'US Retail'     AND settled_day_local IN ( SELECT bet_settled_date_local FROM #dates_retail ) )
                        OR ( channel = 'Canada Online' AND settled_day_local IN ( SELECT bet_settled_date_local FROM #dates_canada ) ) ;
"""

sql_07 = """
           DELETE FROM  analyst_rt.sbk_performance_last_15m
                 WHERE     ( channel = 'US Online'     AND settled_day_local IN ( SELECT bet_settled_date_local FROM #dates_online ) )
                        OR ( channel = 'US Retail'     AND settled_day_local IN ( SELECT bet_settled_date_local FROM #dates_retail ) )
                        OR ( channel = 'Canada Online' AND settled_day_local IN ( SELECT bet_settled_date_local FROM #dates_canada ) ) ;
"""

# Insert Recent Dates
sql_08 = """
           INSERT INTO  analyst_rt.sbk_performance_daily (
           
                SELECT  *
                  FROM  analyst_rt_staging.br_temp_sbk_performance_union ) ;



           INSERT INTO  analyst_rt.sbk_performance_last_15m (
           
                SELECT  *
                  FROM  analyst_rt_staging.br_temp_sbk_performance_union ) ;
"""

# COMMAND ----------

# DBTITLE 1,Define SQL Code - Aggregate Data
# Generate Aggregation Dates
sql_09 = """
  DROP TABLE IF EXISTS  #aggregation_dates ;
                                
          CREATE TABLE  #aggregation_dates AS
        
                SELECT  settled_day_local
                
                  FROM  analyst_rt.sbk_performance_daily
                  
                 WHERE      settled_day_local < DATEADD( week, -2, TRUNC( SYSDATE ) )
                 
                        AND (    handicap_value IS NOT NULL
                              OR selection_name IS NOT NULL
                              OR event_id       IS NOT NULL
                              OR market_id      IS NOT NULL
                              OR selection_id   IS NOT NULL )
                 
              GROUP BY  1 ;
"""

# Aggregated Performance
sql_10 = """
              TRUNCATE  analyst_rt_staging.br_temp_sbk_performance_aggregated ;
                                
           INSERT INTO  analyst_rt_staging.br_temp_sbk_performance_aggregated (
        
                SELECT  p.source_id,
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
                        
                        ROUND( SUM( p.bet_count    ), 4 ) AS bet_count,
                        ROUND( SUM( p.gross_handle ), 2 ) AS gross_handle,

                        ROUND( SUM( p.free_bet_handle ), 2 ) AS free_bet_handle,
                        
                        ROUND( SUM( p.finance_revenue ), 2 ) AS finance_revenue,
                        ROUND( SUM( p.trading_revenue ), 2 ) AS trading_revenue,
                        
                        ROUND( SUM( p.exp_handle  ), 2 ) AS exp_handle,
                        ROUND( SUM( p.exp_revenue ), 2 ) AS exp_revenue,
                        
                        ROUND( SUM( p.finance_exp_revenue ), 2 ) AS finance_exp_revenue,
        
                        ROUND( SUM( p.mikeprice_handle      ), 2 ) AS mikeprice_handle,
                        ROUND( SUM( p.mikeprice_exp_revenue ), 2 ) AS mikeprice_exp_revenue,
                        
                        ROUND( SUM( p.bnn_shrewd_stake ), 2 ) AS bnn_shrewd_stake,
                        ROUND( SUM( p.bnn_all_stake    ), 2 ) AS bnn_all_stake
                                   
                  FROM  analyst_rt.sbk_performance_daily p

                        INNER JOIN #aggregation_dates d
                                ON d.settled_day_local = p.settled_day_local
        
              GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35 ) ;
"""

# Delete Granular & Old Data
sql_11 = """
           DELETE FROM  analyst_rt.sbk_performance_last_15m
                 WHERE  settled_day < DATEADD( month, -15, DATE_TRUNC( 'month', SYSDATE ) ) ;



           DELETE FROM  analyst_rt.sbk_performance_daily
                 WHERE  settled_day_local IN ( SELECT settled_day_local FROM #aggregation_dates ) ;

           DELETE FROM  analyst_rt.sbk_performance_last_15m
                 WHERE  settled_day_local IN ( SELECT settled_day_local FROM #aggregation_dates ) ;
"""

# Insert Aggregated Data
sql_12 = """
           INSERT INTO  analyst_rt.sbk_performance_last_15m (
           
                SELECT  *
                  FROM  analyst_rt_staging.br_temp_sbk_performance_aggregated
                 WHERE  settled_day >= DATEADD( month, -15, DATE_TRUNC( 'month', SYSDATE ) ) ) ;
                 


           INSERT INTO  analyst_rt.sbk_performance_daily (
           
                SELECT  *
                  FROM  analyst_rt_staging.br_temp_sbk_performance_aggregated ) ;
"""

# COMMAND ----------

# DBTITLE 1,Define SQL Code - High Level
# High Level Performance
sql_13 = """
              TRUNCATE  analyst_rt_staging.br_temp_sbk_performance_high_level ;
                                
           INSERT INTO  analyst_rt_staging.br_temp_sbk_performance_high_level (
        
                SELECT  p.source_id,
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
                        
                        ROUND( SUM( p.bet_count    ), 4 ) AS bet_count,
                        ROUND( SUM( p.gross_handle ), 2 ) AS gross_handle,
                        
                        ROUND( SUM( p.free_bet_handle ), 2 ) AS free_bet_handle,
                        
                        ROUND( SUM( p.finance_revenue ), 2 ) AS finance_revenue,
                        ROUND( SUM( p.trading_revenue ), 2 ) AS trading_revenue,
                        
                        ROUND( SUM( p.exp_handle  ), 2 ) AS exp_handle,
                        ROUND( SUM( p.exp_revenue ), 2 ) AS exp_revenue,
                        
                        ROUND( SUM( p.finance_exp_revenue ), 2 ) AS finance_exp_revenue,
        
                        ROUND( SUM( p.mikeprice_handle      ), 2 ) AS mikeprice_handle,
                        ROUND( SUM( p.mikeprice_exp_revenue ), 2 ) AS mikeprice_exp_revenue,
                        
                        ROUND( SUM( p.bnn_shrewd_stake ), 2 ) AS bnn_shrewd_stake,
                        ROUND( SUM( p.bnn_all_stake    ), 2 ) AS bnn_all_stake
                        
                  FROM  analyst_rt.sbk_performance_daily p
                  
                        INNER JOIN #months_union m
                                ON m.settled_month = p.settled_month
                                
              GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22 ) ;
"""

# Delete Recent Months
sql_14 = """
           DELETE FROM  analyst_rt.sbk_performance_high_level
                 WHERE  settled_month IN ( SELECT settled_month FROM #months_union )
"""

# Insert Recent Months
sql_15 = """
           INSERT INTO  analyst_rt.sbk_performance_high_level (
           
                SELECT  *
                  FROM  analyst_rt_staging.br_temp_sbk_performance_high_level ) ;
"""

# Drop Temporary Tables
sql_16 = """
  DROP TABLE IF EXISTS  #dates_online ;
  DROP TABLE IF EXISTS  #dates_retail ;
  DROP TABLE IF EXISTS  #dates_canada ;
  
  DROP TABLE IF EXISTS  #months_online ;
  DROP TABLE IF EXISTS  #months_retail ;
  DROP TABLE IF EXISTS  #months_canada ;
  
  DROP TABLE IF EXISTS  #months_union ;
  
  DROP TABLE IF EXISTS  #aggregation_dates ;
"""

# COMMAND ----------

# DBTITLE 1,Execute SQL Code
# Connect to Redshift
rs = redshift.RedshiftUtility(
    database="fanduel",
    user="svc_risktrading",
    password=dbutils.secrets.get(scope="risk_and_trading", key="svc_risktrading"),
    host="serverless.usdfs.fdbox.net",
    port=5439,
)


# Run Code Blocks
rs.sql(sql_00)
rs.sql(sql_01)
print("[{}]   1. Date List Generated".format(datetime.datetime.now(pytz.timezone('America/New_York')).replace(microsecond=0).strftime("%H:%M:%S")))
rs.sql(sql_02)
print("[{}]   2. Online Data Calculated".format(datetime.datetime.now(pytz.timezone('America/New_York')).replace(microsecond=0).strftime("%H:%M:%S")))
rs.sql(sql_03)
print("[{}]   3. Retail Data Calculated".format(datetime.datetime.now(pytz.timezone('America/New_York')).replace(microsecond=0).strftime("%H:%M:%S")))
rs.sql(sql_04)
print("[{}]   4. Canada Data Calculated".format(datetime.datetime.now(pytz.timezone('America/New_York')).replace(microsecond=0).strftime("%H:%M:%S")))
rs.sql(sql_05)
print("[{}]   5. Data Unioned".format(datetime.datetime.now(pytz.timezone('America/New_York')).replace(microsecond=0).strftime("%H:%M:%S")))
rs.sql(sql_06)
print("[{}]   6. Old Entries Deleted (pt.1)".format(datetime.datetime.now(pytz.timezone('America/New_York')).replace(microsecond=0).strftime("%H:%M:%S")))
rs.sql(sql_07)
print("[{}]   7. Old Entries Deleted (pt.2)".format(datetime.datetime.now(pytz.timezone('America/New_York')).replace(microsecond=0).strftime("%H:%M:%S")))
rs.sql(sql_08)
print("[{}]   8. New Entries Inserted".format(datetime.datetime.now(pytz.timezone('America/New_York')).replace(microsecond=0).strftime("%H:%M:%S")))
rs.sql(sql_09)
print("[{}]   9. Aggregation Dates Generated".format(datetime.datetime.now(pytz.timezone('America/New_York')).replace(microsecond=0).strftime("%H:%M:%S")))
rs.sql(sql_10)
print("[{}]  10. Aggregation Data Calculated".format(datetime.datetime.now(pytz.timezone('America/New_York')).replace(microsecond=0).strftime("%H:%M:%S")))
rs.sql(sql_11)
print("[{}]  11. Granular Data Deleted".format(datetime.datetime.now(pytz.timezone('America/New_York')).replace(microsecond=0).strftime("%H:%M:%S")))
rs.sql(sql_12)
print("[{}]  12. Aggregated Data Inserted".format(datetime.datetime.now(pytz.timezone('America/New_York')).replace(microsecond=0).strftime("%H:%M:%S")))


if run_type == "Gameplay":
  rs.sql("UPDATE analyst_rt.br_table_update_times SET intra_update_dt = '{}'::TIMESTAMP WHERE schema_name = 'analyst_rt' AND table_name IN ( 'sbk_performance_daily', 'sbk_performance_last_15m' )".format(datetime.datetime.now(pytz.timezone("America/New_York")).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")))
if run_type == "Risk Features":
  rs.sql("UPDATE analyst_rt.br_table_update_times SET intra_update_dt = '{}'::TIMESTAMP WHERE schema_name = 'analyst_rt' AND table_name IN ( 'sbk_performance_daily', 'sbk_performance_last_15m' )".format(datetime.datetime.now(pytz.timezone("America/New_York")).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")))
  rs.sql("UPDATE analyst_rt.br_table_update_times SET update_dt = '{}'::TIMESTAMP WHERE schema_name = 'analyst_rt' AND table_name IN ( 'sbk_performance_daily', 'sbk_performance_last_15m' )".format(datetime.datetime.now(pytz.timezone("America/New_York")).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")))



rs.sql(sql_13)
print("[{}]  13. High Level Data Calculated".format(datetime.datetime.now(pytz.timezone('America/New_York')).replace(microsecond=0).strftime("%H:%M:%S")))
rs.sql(sql_14)
print("[{}]  14. Old Months Deleted".format(datetime.datetime.now(pytz.timezone('America/New_York')).replace(microsecond=0).strftime("%H:%M:%S")))
rs.sql(sql_15)
print("[{}]  15. New Months Inserted".format(datetime.datetime.now(pytz.timezone('America/New_York')).replace(microsecond=0).strftime("%H:%M:%S")))

if run_type == "Gameplay":
  rs.sql("UPDATE analyst_rt.br_table_update_times SET intra_update_dt = '{}'::TIMESTAMP WHERE schema_name = 'analyst_rt' AND table_name = 'sbk_performance_high_level'".format(datetime.datetime.now(pytz.timezone("America/New_York")).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")))
if run_type == "Risk Features":
  rs.sql("UPDATE analyst_rt.br_table_update_times SET intra_update_dt = '{}'::TIMESTAMP WHERE schema_name = 'analyst_rt' AND table_name = 'sbk_performance_high_level'".format(datetime.datetime.now(pytz.timezone("America/New_York")).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")))
  rs.sql("UPDATE analyst_rt.br_table_update_times SET update_dt = '{}'::TIMESTAMP WHERE schema_name = 'analyst_rt' AND table_name = 'sbk_performance_high_level'".format(datetime.datetime.now(pytz.timezone("America/New_York")).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")))

rs.sql(sql_16)
print("[{}]  16. Staging Tables Dropped".format(datetime.datetime.now(pytz.timezone('America/New_York')).replace(microsecond=0).strftime("%H:%M:%S")))

# COMMAND ----------

# DBTITLE 1,Table Optimization
# Analyze Tables
rs.sql("ANALYZE analyst_rt.sbk_performance_last_15m")
rs.sql("ANALYZE analyst_rt.sbk_performance_daily")
rs.sql("ANALYZE analyst_rt.sbk_performance_high_level")


# Disconnect from Redshift
del rs

# COMMAND ----------

# DBTITLE 1,Report to Slack
# Slack Setup
slack_token = "xapp-1-A04TVPV45UY-4921610789685-2170c734ecdcaa80f88099b9903c0fb352e07f0c6d1f30a7b578e80f38ab8005"

cl_hook     = "https://hooks.slack.com/services/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"
team_hook   = "https://hooks.slack.com/services/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"

hook_list = [cl_hook, team_hook]


# Write Message
if run_type == "Gameplay":
  slack_msg = {"text": "*Sbk Performance Reporting* \n\nTables Successfully Updated (Gameplay Only)"}
else:
  slack_msg = {"text": "*Sbk Performance Reporting* \n\nTables Successfully Updated"}


# Send Messages
for hook in hook_list:
  slack = Slacker( slack_token, hook )
  slack.incomingwebhook.post( slack_msg )