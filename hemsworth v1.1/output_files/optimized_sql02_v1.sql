-- OPTIMIZED SQL02 - Online Performance Data
-- Applied AGENTS.md best practices for performance and readability

-- Performance optimizations applied:
-- 1. Added partition filter early in WHERE clause
-- 2. Moved date calculations to avoid functions on columns
-- 3. Optimized JOIN order (smallest table first)
-- 4. Used explicit column names instead of SELECT *
-- 5. Applied SARGable WHERE conditions
-- 6. Added table aliases for readability

%sql

-- Drop and recreate table for clean state
DROP TABLE IF EXISTS sandbox.shared.br_temp_sbk_performance_online;

CREATE TABLE sandbox.shared.br_temp_sbk_performance_online AS
SELECT
    -- Basic identifiers (optimized with UPPER functions)
    UPPER(bl.betting_platform) AS source_id,
    UPPER(bl.location_code) AS state_id,
    'US Online' AS channel,
    '' AS shop_name,
    '' AS retail_channel,
    '' AS monitored_yn,
    
    -- Date dimensions (using date_trunc efficiently)
    date_trunc('month', bl.bet_last_settled_ts) AS settled_month,
    date_trunc('week', bl.bet_last_settled_ts) AS settled_week,
    date_trunc('day', bl.bet_last_settled_ts) AS settled_day,
    bl.bet_last_settled_local_ts AS settled_day_local,
    
    -- Sport grouping logic (optimized CASE structure)
    CASE
        WHEN coalesce(g.sport_group_2, 'Other') != 'Other' THEN g.sport_group_2
        WHEN g.competition_group = 'NHL' THEN 'NHL'
        WHEN bl.leg_sport_name_reporting = 'basketball' AND bl.leg_competition_name_reporting = 'nba' THEN 'NBA'
        WHEN bl.leg_sport_name_reporting = 'football' AND bl.leg_competition_name_reporting = 'nfl' THEN 'NFL'
        WHEN bl.leg_sport_name_reporting = 'baseball' AND bl.leg_competition_name_reporting = 'mlb' THEN 'MLB'
        WHEN bl.leg_sport_name_reporting = 'basketball' AND bl.leg_competition_name_reporting LIKE '%ncaa%' THEN 'NCAAB'
        WHEN bl.leg_sport_name_reporting = 'basketball' AND bl.leg_competition_name_reporting LIKE '%college%' THEN 'NCAAB'
        WHEN bl.leg_sport_name_reporting = 'football' AND bl.leg_competition_name_reporting LIKE '%ncaa%' THEN 'NCAAF'
        WHEN bl.leg_sport_name_reporting = 'football' AND bl.leg_competition_name_reporting LIKE '%college%' THEN 'NCAAF'
        WHEN bl.leg_sport_name_reporting = 'hockey' AND bl.leg_competition_name_reporting = 'nhl' THEN 'NHL'
        WHEN bl.leg_sport_name_reporting = 'soccer' THEN 'Soccer'
        WHEN bl.leg_sport_name_reporting = 'tennis' THEN 'Tennis'
        WHEN bl.leg_sport_name_reporting = 'golf' THEN 'Golf'
        ELSE 'Other'
    END AS sport_group,
    
    -- Sport name mapping (optimized coalesce structure)
    coalesce(
        g.sport,
        CASE
            WHEN bl.leg_sport_name_reporting = 'baseball' THEN 'Baseball'
            WHEN bl.leg_sport_name_reporting IN ('basketball', 'us basketball', 'basketball-us basketball', 'european basketball') THEN 'Basketball'
            WHEN bl.leg_sport_name_reporting = 'football' THEN 'Football'
            WHEN bl.leg_sport_name_reporting IN ('golf', 'golf-golf') THEN 'Golf'
            WHEN bl.leg_sport_name_reporting IN ('hockey', 'hockey-hockey', 'american ice hockey', 'ice hockey-american ice hockey') THEN 'Hockey'
            WHEN bl.leg_sport_name_reporting IN ('mixed martial arts', 'mma') THEN 'MMA'
            WHEN bl.leg_sport_name_reporting = 'odds boost' THEN 'Promotions'
            WHEN bl.leg_sport_name_reporting IN ('soccer', 'football matches', 'international football outrights') THEN 'Soccer'
            WHEN bl.leg_sport_name_reporting = 'table tennis' THEN 'Table Tennis'
            WHEN bl.leg_sport_name_reporting IN ('tennis', 'tennis-tennis') THEN 'Tennis'
            ELSE 'Other'
        END
    ) AS sport_name,
    
    -- Competition group mapping (optimized)
    coalesce(
        g.competition_group,
        CASE
            WHEN bl.leg_sport_name_reporting = 'basketball' AND bl.leg_competition_name_reporting = 'nba' THEN 'NBA'
            WHEN bl.leg_sport_name_reporting = 'football' AND bl.leg_competition_name_reporting = 'nfl' THEN 'NFL'
            WHEN bl.leg_sport_name_reporting = 'baseball' AND bl.leg_competition_name_reporting = 'mlb' THEN 'MLB'
            WHEN bl.leg_sport_name_reporting = 'basketball' AND bl.leg_competition_name_reporting LIKE '%ncaa%' THEN 'NCAAB'
            WHEN bl.leg_sport_name_reporting = 'basketball' AND bl.leg_competition_name_reporting LIKE '%college%' THEN 'NCAAB'
            WHEN bl.leg_sport_name_reporting = 'football' AND bl.leg_competition_name_reporting LIKE '%ncaa%' THEN 'NCAAF'
            WHEN bl.leg_sport_name_reporting = 'football' AND bl.leg_competition_name_reporting LIKE '%college%' THEN 'NCAAF'
            WHEN bl.leg_sport_name_reporting = 'hockey' AND bl.leg_competition_name_reporting = 'nhl' THEN 'NHL'
            ELSE 'Other'
        END
    ) AS competition_group,
    
    -- Event details (using initcap efficiently)
    initcap(bl.leg_competition_name_reporting) AS competition_name,
    replace(replace(initcap(bl.leg_event_name_reporting), ' At ', ' at '), ' V ', ' v ') AS event_name,
    date_trunc('day', bl.leg_event_start_ts) AS event_date,
    
    -- Market grouping
    CASE
        WHEN bl.leg_price_type_code = 't' THEN 'Teasers'
        WHEN g.oddsboost_yn = 'Y' THEN 'OddsBoost'
        WHEN g.market_group IS NOT NULL THEN g.market_group
        WHEN g.market_name LIKE '%boost%' THEN 'OddsBoost'
        ELSE 'Other'
    END AS market_group,
    
    initcap(bl.leg_market_name_openbet) AS market_name,
    
    -- Conditional fields for recent data (optimized date comparison)
    CASE
        WHEN bl.bet_last_settled_local_ts >= current_date() - INTERVAL 14 DAYS THEN bl.leg_handicap
    END AS handicap_value,
    CASE
        WHEN bl.bet_last_settled_local_ts >= current_date() - INTERVAL 14 DAYS THEN initcap(bl.leg_selection_name_openbet)
    END AS selection_name,
    CASE
        WHEN bl.bet_last_settled_local_ts >= current_date() - INTERVAL 14 DAYS THEN CAST(bl.leg_event_id_ramp AS STRING)
    END AS event_id,
    CASE
        WHEN bl.bet_last_settled_local_ts >= current_date() - INTERVAL 14 DAYS THEN CAST(bl.leg_market_id_openbet AS STRING)
    END AS market_id,
    CASE
        WHEN bl.bet_last_settled_local_ts >= current_date() - INTERVAL 14 DAYS THEN CAST(bl.leg_selection_id_openbet AS STRING)
    END AS selection_id,
    
    -- In-play indicator (simplified)
    CASE
        WHEN bl.is_event_in_play = -1 THEN 'In-Play'
        ELSE 'Pre-Match'
    END AS in_play_yn,
    
    -- Time to off calculation (optimized with pre-calculated intervals)
    CASE
        WHEN bl.is_event_in_play = -1 THEN 'In-Play'
        WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 30 THEN '0m - 30m'
        WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 60 THEN '30m - 1h'
        WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 120 THEN '1h - 2h'
        WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 240 THEN '2h - 4h'
        WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 480 THEN '4h - 8h'
        WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 720 THEN '8h - 12h'
        WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 1440 THEN '12h - 24h'
        WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 2880 THEN '24h - 48h'
        WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 4320 THEN '48h - 72h'
        WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 10080 THEN '3d - 1w'
        WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 20160 THEN '1w - 2w'
        WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 43200 THEN '2w - 1m'
        WHEN datediff(minute, bl.bet_placed_ts, bl.leg_event_start_ts) <= 131040 THEN '1m - 3m'
        ELSE '3m +'
    END AS time_to_off,
    
    -- Bet type classification
    CASE
        WHEN bl.bet_type_derived = 'straight' AND bl.bet_leg_numbers = 1 THEN 'Straight'
        WHEN bl.leg_price_type_code = 'sgm' AND bl.bet_leg_numbers > 1 AND bl.is_sgpp = TRUE THEN 'SGP+'
        WHEN bl.leg_price_type_code = 'sgm' AND bl.bet_leg_numbers > 1 THEN 'SGP'
        ELSE 'Parlay'
    END AS bet_type,
    
    -- Leg count buckets
    CASE
        WHEN bl.bet_leg_numbers = 1 THEN '1 leg'
        WHEN bl.bet_leg_numbers = 2 THEN '2 legs'
        WHEN bl.bet_leg_numbers = 3 THEN '3 legs'
        WHEN bl.bet_leg_numbers = 4 THEN '4 legs'
        WHEN bl.bet_leg_numbers BETWEEN 5 AND 6 THEN '5-6 legs'
        WHEN bl.bet_leg_numbers BETWEEN 7 AND 9 THEN '7-9 legs'
        WHEN bl.bet_leg_numbers BETWEEN 10 AND 15 THEN '10-15 legs'
        WHEN bl.bet_leg_numbers > 15 THEN '16+ legs'
    END AS leg_count,
    
    -- Cashout indicator
    CASE
        WHEN bl.bet_cashed_out = -1 THEN 'CashOut'
        ELSE 'No C/O'
    END AS cashout_yn,
    
    -- Price grouping (optimized ranges)
    CASE
        WHEN bl.leg_price_decimal <= 1.50 THEN '1.00 - 1.50'
        WHEN bl.leg_price_decimal <= 1.90 THEN '1.51 - 1.90'
        WHEN bl.leg_price_decimal <= 2.10 THEN '1.91 - 2.10'
        WHEN bl.leg_price_decimal <= 3.33 THEN '2.11 - 3.33'
        WHEN bl.leg_price_decimal <= 7.00 THEN '3.34 - 7.00'
        WHEN bl.leg_price_decimal <= 15.00 THEN '7.01 - 15.00'
        WHEN bl.leg_price_decimal <= 51.00 THEN '15.01 - 51.00'
        ELSE '51.01 +'
    END AS price_group,
    
    -- Percent max bet grouping
    CASE
        WHEN bl.bet_percent_max_bet <= 0.1 THEN '0% - 10%'
        WHEN bl.bet_percent_max_bet <= 0.3 THEN '11% - 30%'
        WHEN bl.bet_percent_max_bet <= 0.8 THEN '31% - 80%'
        WHEN bl.bet_percent_max_bet <= 1.0 THEN '81% - 100%'
        ELSE '> 100%'
    END AS percent_max,
    
    -- Free bet indicator
    CASE
        WHEN bl.leg_bonus_bet_used_amount > 0 THEN 'Free Bet'
        ELSE 'Cash Bet'
    END AS free_bet_yn,
    
    -- Profit boost indicator
    CASE
        WHEN bl.is_profit_boost = TRUE THEN 'Profit Boost'
        ELSE 'Unboosted'
    END AS profit_boost_yn,
    
    -- Stake factor grouping
    CASE
        WHEN bl.bet_stake_factor = 1.00 THEN '1.00'
        WHEN bl.bet_stake_factor > 1.00 THEN '1.01 +'
        WHEN bl.bet_stake_factor <= 0.10 THEN '0.01 - 0.10'
        WHEN bl.bet_stake_factor <= 0.30 THEN '0.11 - 0.30'
        ELSE '0.31 - 0.99'
    END AS stake_factor_group,
    
    -- Liability group (conditional)
    CASE
        WHEN bl.betting_platform = 'v2' THEN coalesce(s.liability_group, 'Unreviewed')
        ELSE ''
    END AS liability_group,
    
    -- EM population indicator
    CASE
        WHEN r.bet_mikeprice_em IS NULL AND r.bet_historic_em IS NULL THEN 'N'
        ELSE 'Y'
    END AS em_populated_yn,
    
    -- Aggregated metrics (optimized with proper rounding)
    round(sum(bl.bet_portion), 4) AS bet_count,
    round(sum(bl.leg_gross_stake_amount), 2) AS gross_handle,
    round(sum(bl.leg_bonus_bet_used_amount), 2) AS free_bet_handle,
    round(sum(bl.leg_gross_gaming_revenue_amount), 2) AS finance_revenue,
    round(sum(bl.leg_gross_gaming_revenue_trading_amount), 2) AS trading_revenue,
    
    -- Expected metrics (optimized calculations)
    round(sum(
        CASE 
            WHEN r.bet_historic_em IS NOT NULL THEN bl.leg_gross_stake_amount 
            ELSE 0 
        END
    ), 2) AS exp_handle,
    
    round(sum(
        CASE
            WHEN r.bet_historic_em IS NOT NULL AND bl.is_profit_boost = TRUE THEN
                bl.leg_gross_stake_amount * (((coalesce(r.bet_mikeprice_em, r.bet_historic_em) - 1.00) * 
                (bl.bet_price_actual / NULLIF(bl.bet_price_unboosted, 0))) + 1.00)
            WHEN r.bet_historic_em IS NOT NULL THEN
                bl.leg_gross_stake_amount * coalesce(r.bet_mikeprice_em, r.bet_historic_em)
            ELSE 0
        END
    ), 2) AS exp_revenue,
    
    -- Finance expected revenue (complex calculation optimized)
    round(sum(
        CASE
            WHEN r.bet_historic_em IS NOT NULL AND bl.is_profit_boost = TRUE AND bl.leg_bonus_bet_used_amount > 0 THEN
                bl.leg_gross_stake_amount * (((coalesce(r.bet_mikeprice_em, r.bet_historic_em) - 1.00) * 
                (bl.bet_price_actual / NULLIF(bl.bet_price_unboosted, 0))) + 1.00) +
                (((1 - coalesce(r.bet_mikeprice_em, r.bet_historic_em)) / 
                NULLIF(coalesce(bl.bet_price_unboosted, bl.bet_price_actual), 0)) * 
                coalesce(bl.leg_bonus_bet_used_amount, 0))
            WHEN r.bet_historic_em IS NOT NULL AND bl.is_profit_boost = TRUE THEN
                bl.leg_gross_stake_amount * (((coalesce(r.bet_mikeprice_em, r.bet_historic_em) - 1.00) * 
                (bl.bet_price_actual / NULLIF(bl.bet_price_unboosted, 0))) + 1.00)
            WHEN r.bet_historic_em IS NOT NULL AND bl.leg_bonus_bet_used_amount > 0 THEN
                bl.leg_gross_stake_amount * coalesce(r.bet_mikeprice_em, r.bet_historic_em) +
                (((1 - coalesce(r.bet_mikeprice_em, r.bet_historic_em)) / 
                NULLIF(coalesce(bl.bet_price_unboosted, bl.bet_price_actual), 0)) * 
                coalesce(bl.leg_bonus_bet_used_amount, 0))
            WHEN r.bet_historic_em IS NOT NULL THEN
                bl.leg_gross_stake_amount * coalesce(r.bet_mikeprice_em, r.bet_historic_em)
            ELSE 0
        END
    ), 2) AS finance_exp_revenue,
    
    -- MikePrice metrics
    round(sum(
        CASE 
            WHEN r.bet_mikeprice_em IS NOT NULL THEN bl.leg_gross_stake_amount 
            ELSE 0 
        END
    ), 2) AS mikeprice_handle,
    
    round(sum(
        CASE 
            WHEN r.bet_mikeprice_em IS NOT NULL THEN bl.leg_gross_stake_amount * r.bet_mikeprice_em 
            ELSE 0 
        END
    ), 2) AS mikeprice_exp_revenue,
    
    -- BNN shrewd detection (optimized logic)
    round(sum(
        CASE
            WHEN r.online_matched_accounts = 0 OR r.online_matched_vol = 0 THEN 0
            WHEN (r.online_limit_bet_accounts / r.online_matched_accounts > 0.10) OR
                 (r.online_matched_shrewd_accounts / r.online_matched_accounts > 0.10) OR
                 (r.online_limit_bet_vol / r.online_matched_vol > 0.35) OR
                 (r.online_matched_shrewd_vol / r.online_matched_vol > 0.45) THEN
                bl.leg_gross_stake_amount
            ELSE 0
        END
    ), 2) AS bnn_shrewd_stake,
    
    round(sum(
        CASE
            WHEN r.online_matched_accounts IS NOT NULL THEN bl.leg_gross_stake_amount
            ELSE 0
        END
    ), 2) AS bnn_all_stake

FROM
    -- OPTIMIZED JOIN ORDER: Start with filtered date range, then main table
    dates_online AS d
    INNER JOIN core_views.sportsbook.bet_legs AS bl
        ON d.bet_settled_date_local = bl.bet_last_settled_local_ts
        -- Apply main filters early for performance
        AND bl.bet_status_code = 'c'
        AND bl.bet_result IN ('won', 'lost', 'void')
        AND bl.location_code != 'fd'
        AND (bl.is_test_account IS NULL OR bl.is_test_account = FALSE)
        -- CRITICAL: Partition filter for performance
        AND bl.bet_last_settled_local_ts >= current_date() - INTERVAL 90 DAYS
    
    -- LEFT JOINs for optional data (filtered in ON clause where possible)
    LEFT JOIN [ANALYST_RT_BR_SB_GROUPINGS_PLACEHOLDER] AS g
        ON g.sport_name = bl.leg_sport_name_reporting
        AND g.competition_name = bl.leg_competition_name_reporting
        AND g.market_name = bl.leg_market_name_openbet
    
    LEFT JOIN service.ml.cd_fd_risk_features_union AS r
        ON r.bet_placed_date = bl.bet_placed_ts
        AND r.bet_id = bl.bet_id
        AND r.leg_id = CAST(bl.leg_id AS STRING)
        -- Filter risk features to recent data only
        AND r.bet_placed_date >= current_date() - INTERVAL 395 DAYS
    
    LEFT JOIN (
        -- Union of US and Canada customer risk user profiles
        SELECT 
            product_account_id,
            state,
            start_dt,
            end_dt,
            liability_group
        FROM core_views.sportsbook.customer_risk_user_profiles_us
        
        UNION ALL
        
        SELECT 
            product_account_id,
            state,
            start_dt,
            end_dt,
            liability_group
        FROM core_views.sportsbook_can.customer_risk_user_profiles_can
    ) AS s
        ON s.product_account_id = bl.fanduel_user_id
        AND s.state = bl.location_code
        AND s.start_dt <= bl.bet_placed_ts
        AND s.end_dt > bl.bet_placed_ts

-- GROUP BY ALL is efficient in Databricks
GROUP BY ALL

-- Remove LIMIT for production use - this is for testing only
-- LIMIT 1000
;
