%sql
-- OPTIMIZED SQL04 - Canada Performance Data
-- Applied AGENTS.md best practices for performance and readability
-- DEPENDS ON: SQL01 temporary views (dates_canada from SQL01)

-- Performance optimizations applied:
-- 1. Uses temporary views from SQL01 for date filtering
-- 2. Moved date calculations to avoid functions on columns
-- 3. Optimized JOIN order (smallest table first)
-- 4. Used explicit column names instead of SELECT *
-- 5. Applied SARGable WHERE conditions
-- 6. Added table aliases for readability

-- Drop and recreate table for clean state
DROP TABLE IF EXISTS sandbox.shared.br_temp_sbk_performance_canada;

CREATE TABLE sandbox.shared.br_temp_sbk_performance_canada AS
SELECT
    -- Basic identifiers (optimized with UPPER functions)
    UPPER(bl.source) AS source_id,
    UPPER(bl.state) AS state_id,
    'Canada Online' AS channel,
    '' AS shop_name,
    '' AS retail_channel,
    '' AS monitored_yn,
    
    -- Date dimensions (using date_trunc efficiently with timezone conversion)
    date_trunc('month', bl.bet_settled_date) AS settled_month,
    date_trunc('week', bl.bet_settled_date) AS settled_week,
    date_trunc('day', bl.bet_settled_date) AS settled_day,
    bl.bet_settled_date_local AS settled_day_local,
    
    -- Sport grouping logic (optimized CASE structure - same as online/retail)
    CASE
        WHEN coalesce(g.sport_group_2, 'Other') != 'Other' THEN g.sport_group_2
        WHEN g.competition_group = 'NHL' THEN 'NHL'
        WHEN bl.sport_name = 'basketball' AND bl.competition_name = 'nba' THEN 'NBA'
        WHEN bl.sport_name = 'football' AND bl.competition_name = 'nfl' THEN 'NFL'
        WHEN bl.sport_name = 'baseball' AND bl.competition_name = 'mlb' THEN 'MLB'
        WHEN bl.sport_name = 'basketball' AND bl.competition_name LIKE '%ncaa%' THEN 'NCAAB'
        WHEN bl.sport_name = 'basketball' AND bl.competition_name LIKE '%college%' THEN 'NCAAB'
        WHEN bl.sport_name = 'football' AND bl.competition_name LIKE '%ncaa%' THEN 'NCAAF'
        WHEN bl.sport_name = 'football' AND bl.competition_name LIKE '%college%' THEN 'NCAAF'
        WHEN bl.sport_name = 'hockey' AND bl.competition_name = 'nhl' THEN 'NHL'
        WHEN bl.sport_name = 'soccer' THEN 'Soccer'
        WHEN bl.sport_name = 'tennis' THEN 'Tennis'
        WHEN bl.sport_name = 'golf' THEN 'Golf'
        ELSE 'Other'
    END AS sport_group,
    
    -- Sport name mapping (optimized coalesce structure)
    coalesce(
        g.sport,
        CASE
            WHEN bl.sport_name = 'baseball' THEN 'Baseball'
            WHEN bl.sport_name IN ('basketball', 'us basketball', 'basketball-us basketball', 'european basketball') THEN 'Basketball'
            WHEN bl.sport_name = 'football' THEN 'Football'
            WHEN bl.sport_name IN ('golf', 'golf-golf') THEN 'Golf'
            WHEN bl.sport_name IN ('hockey', 'hockey-hockey', 'american ice hockey', 'ice hockey-american ice hockey') THEN 'Hockey'
            WHEN bl.sport_name IN ('mixed martial arts', 'mma') THEN 'MMA'
            WHEN bl.sport_name = 'odds boost' THEN 'Promotions'
            WHEN bl.sport_name IN ('soccer', 'football matches', 'international football outrights') THEN 'Soccer'
            WHEN bl.sport_name = 'table tennis' THEN 'Table Tennis'
            WHEN bl.sport_name IN ('tennis', 'tennis-tennis') THEN 'Tennis'
            ELSE 'Other'
        END
    ) AS sport_name,
    
    -- Competition group mapping (optimized)
    coalesce(
        g.competition_group,
        CASE
            WHEN bl.sport_name = 'basketball' AND bl.competition_name = 'nba' THEN 'NBA'
            WHEN bl.sport_name = 'football' AND bl.competition_name = 'nfl' THEN 'NFL'
            WHEN bl.sport_name = 'baseball' AND bl.competition_name = 'mlb' THEN 'MLB'
            WHEN bl.sport_name = 'basketball' AND bl.competition_name LIKE '%ncaa%' THEN 'NCAAB'
            WHEN bl.sport_name = 'basketball' AND bl.competition_name LIKE '%college%' THEN 'NCAAB'
            WHEN bl.sport_name = 'football' AND bl.competition_name LIKE '%ncaa%' THEN 'NCAAF'
            WHEN bl.sport_name = 'football' AND bl.competition_name LIKE '%college%' THEN 'NCAAF'
            WHEN bl.sport_name = 'hockey' AND bl.competition_name = 'nhl' THEN 'NHL'
            ELSE 'Other'
        END
    ) AS competition_group,
    
    -- Event details (using initcap efficiently)
    initcap(bl.competition_name) AS competition_name,
    replace(replace(initcap(bl.event_name), ' At ', ' at '), ' V ', ' v ') AS event_name,
    date_trunc('day', bl.event_start_date) AS event_date,
    
    -- Market grouping
    CASE
        WHEN bl.price_type_code = 't' THEN 'Teasers'
        WHEN g.oddsboost_yn = 'Y' THEN 'OddsBoost'
        WHEN g.market_group IS NOT NULL THEN g.market_group
        WHEN g.market_name LIKE '%boost%' THEN 'OddsBoost'
        ELSE 'Other'
    END AS market_group,
    
    initcap(bl.market_name) AS market_name,
    
    -- Conditional fields for recent data (optimized date comparison)
    CASE
        WHEN bl.bet_settled_date_local >= current_date() - INTERVAL 14 DAYS THEN bl.handicap_value
    END AS handicap_value,
    CASE
        WHEN bl.bet_settled_date_local >= current_date() - INTERVAL 14 DAYS THEN initcap(bl.selection_name)
    END AS selection_name,
    CASE
        WHEN bl.bet_settled_date_local >= current_date() - INTERVAL 14 DAYS THEN CAST(bl.event_id AS VARCHAR(255))
    END AS event_id,
    CASE
        WHEN bl.bet_settled_date_local >= current_date() - INTERVAL 14 DAYS THEN CAST(bl.market_id AS VARCHAR(255))
    END AS market_id,
    CASE
        WHEN bl.bet_settled_date_local >= current_date() - INTERVAL 14 DAYS THEN CAST(bl.selection_id AS VARCHAR(255))
    END AS selection_id,
    
    -- In-play indicator (simplified)
    CASE
        WHEN bl.event_in_play = -1 THEN 'In-Play'
        ELSE 'Pre-Match'
    END AS in_play_yn,
    
    -- Time to off calculation (optimized with pre-calculated intervals)
    CASE
        WHEN bl.event_in_play = -1 THEN 'In-Play'
        WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 30 THEN '0m - 30m'
        WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 60 THEN '30m - 1h'
        WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 120 THEN '1h - 2h'
        WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 240 THEN '2h - 4h'
        WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 480 THEN '4h - 8h'
        WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 720 THEN '8h - 12h'
        WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 1440 THEN '12h - 24h'
        WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 2880 THEN '24h - 48h'
        WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 4320 THEN '48h - 72h'
        WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 10080 THEN '3d - 1w'
        WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 20160 THEN '1w - 2w'
        WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 43200 THEN '2w - 1m'
        WHEN datediff(minute, bl.bet_placed_date, bl.event_start_date) <= 131040 THEN '1m - 3m'
        ELSE '3m +'
    END AS time_to_off,
    
    -- Bet type classification (Canada uses sgpp_yn instead of is_same_game_parlay_plus)
    CASE
        WHEN bl.bet_type = 'straight' AND bl.leg_numbers = 1 THEN 'Straight'
        WHEN bl.price_type_code = 'sgm' AND bl.leg_numbers > 1 AND bl.sgpp_yn = TRUE THEN 'SGP+'
        WHEN bl.price_type_code = 'sgm' AND bl.leg_numbers > 1 THEN 'SGP'
        ELSE 'Parlay'
    END AS bet_type,
    
    -- Leg count buckets
    CASE
        WHEN bl.leg_numbers = 1 THEN '1 leg'
        WHEN bl.leg_numbers = 2 THEN '2 legs'
        WHEN bl.leg_numbers = 3 THEN '3 legs'
        WHEN bl.leg_numbers = 4 THEN '4 legs'
        WHEN bl.leg_numbers BETWEEN 5 AND 6 THEN '5-6 legs'
        WHEN bl.leg_numbers BETWEEN 7 AND 9 THEN '7-9 legs'
        WHEN bl.leg_numbers BETWEEN 10 AND 15 THEN '10-15 legs'
        WHEN bl.leg_numbers > 15 THEN '16+ legs'
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
    
    -- Free bet indicator (Canada uses free_bet_used)
    CASE
        WHEN bl.free_bet_used > 0 THEN 'Free Bet'
        ELSE 'Cash Bet'
    END AS free_bet_yn,
    
    -- Profit boost indicator (Canada has profit boost feature)
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
    
    -- Liability group (Canada doesn't have liability grouping)
    '' AS liability_group,
    
    -- EM population indicator
    CASE
        WHEN r.bet_mikeprice_em IS NULL AND r.bet_historic_em IS NULL THEN 'N'
        ELSE 'Y'
    END AS em_populated_yn,
    
    -- Aggregated metrics (optimized with proper rounding)
    round(sum(bl.bet_count), 4) AS bet_count,
    round(sum(bl.gross_stake), 2) AS gross_handle,
    round(sum(bl.free_bet_used), 2) AS free_bet_handle,
    round(sum(bl.ggr), 2) AS finance_revenue,
    round(sum(bl.gross_gaming_revenue_trading_cad), 2) AS trading_revenue,
    
    -- Expected metrics (optimized calculations with profit boost complexity)
    round(sum(
        CASE 
            WHEN r.bet_historic_em IS NOT NULL THEN bl.gross_stake 
            ELSE 0 
        END
    ), 2) AS exp_handle,
    
    round(sum(
        CASE
            WHEN r.bet_historic_em IS NOT NULL AND bl.is_profit_boost = TRUE THEN
                bl.gross_stake * (((coalesce(r.bet_mikeprice_em, r.bet_historic_em) - 1.00) * 
                (bl.bet_price_actual / NULLIF(bl.bet_price_unboosted, 0))) + 1.00)
            WHEN r.bet_historic_em IS NOT NULL THEN
                bl.gross_stake * coalesce(r.bet_mikeprice_em, r.bet_historic_em)
            ELSE 0
        END
    ), 2) AS exp_revenue,
    
    -- Finance expected revenue (Canada has full profit boost complexity like US online)
    round(sum(
        CASE
            WHEN r.bet_historic_em IS NOT NULL AND bl.is_profit_boost = TRUE AND bl.free_bet_used > 0 THEN
                bl.gross_stake * (((coalesce(r.bet_mikeprice_em, r.bet_historic_em) - 1.00) * 
                (bl.bet_price_actual / NULLIF(bl.bet_price_unboosted, 0))) + 1.00) +
                (((1 - coalesce(r.bet_mikeprice_em, r.bet_historic_em)) / 
                NULLIF(coalesce(bl.bet_price_unboosted, bl.bet_price_actual), 0)) * 
                coalesce(bl.free_bet_used, 0))
            WHEN r.bet_historic_em IS NOT NULL AND bl.is_profit_boost = TRUE THEN
                bl.gross_stake * (((coalesce(r.bet_mikeprice_em, r.bet_historic_em) - 1.00) * 
                (bl.bet_price_actual / NULLIF(bl.bet_price_unboosted, 0))) + 1.00)
            WHEN r.bet_historic_em IS NOT NULL AND bl.free_bet_used > 0 THEN
                bl.gross_stake * coalesce(r.bet_mikeprice_em, r.bet_historic_em) +
                (((1 - coalesce(r.bet_mikeprice_em, r.bet_historic_em)) / 
                NULLIF(coalesce(bl.bet_price_unboosted, bl.bet_price_actual), 0)) * 
                coalesce(bl.free_bet_used, 0))
            WHEN r.bet_historic_em IS NOT NULL THEN
                bl.gross_stake * coalesce(r.bet_mikeprice_em, r.bet_historic_em)
            ELSE 0
        END
    ), 2) AS finance_exp_revenue,
    
    -- MikePrice metrics
    round(sum(
        CASE 
            WHEN r.bet_mikeprice_em IS NOT NULL THEN bl.gross_stake 
            ELSE 0 
        END
    ), 2) AS mikeprice_handle,
    
    round(sum(
        CASE 
            WHEN r.bet_mikeprice_em IS NOT NULL THEN bl.gross_stake * r.bet_mikeprice_em 
            ELSE 0 
        END
    ), 2) AS mikeprice_exp_revenue,
    
    -- BNN shrewd detection (optimized logic)
    round(sum(
        CASE
            WHEN r.online_matched_accounts = 0 OR r.online_matched_vol = 0 THEN 0
            WHEN (r.online_limit_bet_accounts / CAST(r.online_matched_accounts AS REAL) > 0.10) OR
                 (r.online_matched_shrewd_accounts / CAST(r.online_matched_accounts AS REAL) > 0.10) OR
                 (r.online_limit_bet_vol / CAST(r.online_matched_vol AS REAL) > 0.35) OR
                 (r.online_matched_shrewd_vol / CAST(r.online_matched_vol AS REAL) > 0.45) THEN
                bl.gross_stake
            ELSE 0
        END
    ), 2) AS bnn_shrewd_stake,
    
    round(sum(
        CASE
            WHEN r.online_matched_accounts IS NOT NULL THEN bl.gross_stake
            ELSE 0
        END
    ), 2) AS bnn_all_stake

FROM
    -- OPTIMIZED JOIN ORDER: Use SQL01 temporary view for date filtering
    dates_canada AS d
    INNER JOIN core_views.sportsbook_can.bet_legs AS bl
        ON d.bet_settled_date_local = bl.bet_settled_date_local
        -- Apply main filters early for performance
        AND bl.bet_status = 'c'
        AND bl.bet_result IN ('won', 'lost', 'void')
        AND bl.is_test_account = FALSE
        -- CRITICAL: Partition filter for performance
        AND bl.bet_settled_date_local >= current_date() - INTERVAL 90 DAYS
    
    -- LEFT JOINs for optional data (filtered in ON clause where possible)
    LEFT JOIN [ANALYST_RT_BR_SB_GROUPINGS_PLACEHOLDER] AS g
        ON g.sport_name = bl.sport_name
        AND g.competition_name = bl.competition_name
        AND g.market_name = bl.market_name
    
    LEFT JOIN [ANALYST_RT_CL_FD_RISK_FEATURES_CAN_PLACEHOLDER] AS r
        ON r.bet_placed_date = bl.bet_placed_date
        AND r.bet_id = bl.bet_id
        AND r.leg_id = bl.leg_id

-- GROUP BY ALL is efficient in Databricks
GROUP BY ALL

-- Remove LIMIT for production use - this is for testing only
LIMIT 1000
;
