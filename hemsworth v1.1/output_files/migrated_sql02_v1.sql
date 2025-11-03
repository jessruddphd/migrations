-- Migrated from Redshift to Delta Lake (Databricks) - SQL_02
-- Complex performance query with multiple missing table mappings

-- TODO: Replace with appropriate Databricks table or create temporary table
-- Original: TRUNCATE analyst_rt_staging.br_temp_sbk_performance_online;
-- DELETE FROM [TARGET_TABLE_PLACEHOLDER];

-- TODO: Replace with appropriate Databricks table  
-- Original: INSERT INTO analyst_rt_staging.br_temp_sbk_performance_online
INSERT INTO [TARGET_TABLE_PLACEHOLDER] (

    SELECT  UPPER(b.betting_platform) AS source_id,
            UPPER(b.location_code) AS state_id,
    
            'US Online' AS channel,
            '' AS shop_name,
            '' AS retail_channel,
            '' AS monitored_yn,                
            
            date_trunc('month', b.bet_last_settled_ts) AS settled_month,
            date_trunc('week', b.bet_last_settled_ts) AS settled_week,
            date_trunc('day', b.bet_last_settled_ts) AS settled_day,
            
            b.bet_last_settled_local_ts AS settled_day_local,
            
            CASE WHEN coalesce(g.sport_group_2, 'Other') <> 'Other'
                 THEN g.sport_group_2
                 WHEN g.competition_group = 'NHL'
                 THEN 'NHL'
                 WHEN b.leg_sport_name_reporting = 'basketball'
                  AND b.leg_competition_name_reporting = 'nba'                     
                 THEN 'NBA'
                 WHEN b.leg_sport_name_reporting = 'football'
                  AND b.leg_competition_name_reporting = 'nfl'                     
                 THEN 'NFL'
                 WHEN b.leg_sport_name_reporting = 'baseball'
                  AND b.leg_competition_name_reporting = 'mlb'                     
                 THEN 'MLB'
                 WHEN b.leg_sport_name_reporting = 'basketball'
                  AND lower(b.leg_competition_name_reporting) LIKE '%ncaa%'                     
                 THEN 'NCAAB'
                 WHEN b.leg_sport_name_reporting = 'basketball'
                  AND lower(b.leg_competition_name_reporting) LIKE '%college%'                     
                 THEN 'NCAAB'
                 WHEN b.leg_sport_name_reporting = 'football'
                  AND lower(b.leg_competition_name_reporting) LIKE '%ncaa%'                     
                 THEN 'NCAAF'
                 WHEN b.leg_sport_name_reporting = 'football'
                  AND lower(b.leg_competition_name_reporting) LIKE '%college%'                     
                 THEN 'NCAAF'
                 WHEN b.leg_sport_name_reporting = 'hockey'
                  AND b.leg_competition_name_reporting = 'nhl'    
                 THEN 'NHL'
                 WHEN b.leg_sport_name_reporting = 'soccer'
                 THEN 'Soccer'
                 WHEN b.leg_sport_name_reporting = 'tennis'
                 THEN 'Tennis'
                 WHEN b.leg_sport_name_reporting = 'golf'
                 THEN 'Golf'
                 ELSE 'Other'
                  END AS sport_group,
            
            coalesce(g.sport, CASE WHEN b.leg_sport_name_reporting = 'baseball' THEN 'Baseball'
                                   WHEN b.leg_sport_name_reporting = 'basketball' THEN 'Basketball'
                                   WHEN b.leg_sport_name_reporting = 'us basketball' THEN 'Basketball'
                                   WHEN b.leg_sport_name_reporting = 'basketball-us basketball' THEN 'Basketball'
                                   WHEN b.leg_sport_name_reporting = 'european basketball' THEN 'Basketball'            
                                   WHEN b.leg_sport_name_reporting = 'football' THEN 'Football'                  
                                   WHEN b.leg_sport_name_reporting = 'golf' THEN 'Golf'                      
                                   WHEN b.leg_sport_name_reporting = 'golf-golf' THEN 'Golf'                
                                   WHEN b.leg_sport_name_reporting = 'hockey' THEN 'Hockey'                   
                                   WHEN b.leg_sport_name_reporting = 'hockey-hockey' THEN 'Hockey'                      
                                   WHEN b.leg_sport_name_reporting = 'american ice hockey' THEN 'Hockey'                   
                                   WHEN b.leg_sport_name_reporting = 'ice hockey-american ice hockey' THEN 'Hockey'     
                                   WHEN b.leg_sport_name_reporting = 'mixed martial arts' THEN 'MMA'       
                                   WHEN b.leg_sport_name_reporting = 'mma' THEN 'MMA'             
                                   WHEN b.leg_sport_name_reporting = 'odds boost' THEN 'Promotions'
                                   WHEN b.leg_sport_name_reporting = 'soccer' THEN 'Soccer'                     
                                   WHEN b.leg_sport_name_reporting = 'football matches' THEN 'Soccer'                      
                                   WHEN b.leg_sport_name_reporting = 'international football outrights' THEN 'Soccer'                        
                                   WHEN b.leg_sport_name_reporting = 'table tennis' THEN 'Table Tennis'                 
                                   WHEN b.leg_sport_name_reporting = 'tennis' THEN 'Tennis'                
                                   WHEN b.leg_sport_name_reporting = 'tennis-tennis' THEN 'Tennis'
                                   ELSE 'Other'
                                    END) AS sport_name,
            
            coalesce(g.competition_group, CASE WHEN b.leg_sport_name_reporting = 'basketball'
                                                AND b.leg_competition_name_reporting = 'nba'                     
                                               THEN 'NBA'
                                               WHEN b.leg_sport_name_reporting = 'football'
                                                AND b.leg_competition_name_reporting = 'nfl'                     
                                               THEN 'NFL'
                                               WHEN b.leg_sport_name_reporting = 'baseball'
                                                AND b.leg_competition_name_reporting = 'mlb'                     
                                               THEN 'MLB'
                                               WHEN b.leg_sport_name_reporting = 'basketball'
                                                AND lower(b.leg_competition_name_reporting) LIKE '%ncaa%'                     
                                               THEN 'NCAAB'
                                               WHEN b.leg_sport_name_reporting = 'basketball'
                                                AND lower(b.leg_competition_name_reporting) LIKE '%college%'                     
                                               THEN 'NCAAB'
                                               WHEN b.leg_sport_name_reporting = 'football'
                                                AND lower(b.leg_competition_name_reporting) LIKE '%ncaa%'                     
                                               THEN 'NCAAF'
                                               WHEN b.leg_sport_name_reporting = 'football'
                                                AND lower(b.leg_competition_name_reporting) LIKE '%college%'
                                               THEN 'NCAAF'
                                               WHEN b.leg_sport_name_reporting = 'hockey'
                                                AND b.leg_competition_name_reporting = 'nhl'                     
                                               THEN 'NHL'
                                               ELSE 'Other'
                                                END) AS competition_group,
            
            initcap(b.leg_competition_name_reporting) AS competition_name,
            
            replace(replace(initcap(b.leg_event_name_reporting), ' At ', ' at '), ' V ', ' v ') AS event_name,
            date_trunc('day', b.leg_event_start_ts) AS event_date,
                                       
            CASE WHEN b.leg_price_type_code = 't'
                 THEN 'Teasers'
                 WHEN g.oddsboost_yn = 'Y'
                 THEN 'OddsBoost'
                 WHEN g.market_group IS NOT NULL
                 THEN g.market_group
                 WHEN lower(g.market_name) LIKE '%boost%'
                 THEN 'OddsBoost'
                 ELSE 'Other'
                  END AS market_group,

            initcap(b.leg_market_name_openbet) AS market_name,
            
            -- TODO: Verify date arithmetic conversion from DATEADD
            CASE WHEN b.bet_last_settled_local_ts >= date_sub(current_date(), 14) THEN b.leg_handicap END AS handicap_value,
            CASE WHEN b.bet_last_settled_local_ts >= date_sub(current_date(), 14) THEN initcap(b.leg_selection_name_openbet) END AS selection_name,
            
            CASE WHEN b.bet_last_settled_local_ts >= date_sub(current_date(), 14) THEN cast(b.leg_event_id_ramp as string) END AS event_id,
            CASE WHEN b.bet_last_settled_local_ts >= date_sub(current_date(), 14) THEN cast(b.leg_market_id_openbet as string) END AS market_id,
            CASE WHEN b.bet_last_settled_local_ts >= date_sub(current_date(), 14) THEN cast(b.leg_selection_id_openbet as string) END AS selection_id,
            
            -- TODO: Verify event_in_play column mapping
            CASE WHEN b.[EVENT_IN_PLAY_COLUMN_PLACEHOLDER] = -1
                 THEN 'In-Play'
                 ELSE 'Pre-Match'
                  END AS in_play_yn,
                  
            -- TODO: Convert DATEDIFF function to Databricks syntax
            CASE WHEN b.[EVENT_IN_PLAY_COLUMN_PLACEHOLDER] = -1 THEN 'In-Play'
                 WHEN round(datediff(minute, b.bet_placed_ts, b.leg_event_start_ts), 0) <= 30 THEN '0m - 30m'
                 WHEN round(datediff(minute, b.bet_placed_ts, b.leg_event_start_ts), 0) <= 60 THEN '30m - 1h'
                 WHEN round(datediff(minute, b.bet_placed_ts, b.leg_event_start_ts), 0) <= 120 THEN '1h - 2h'
                 WHEN round(datediff(minute, b.bet_placed_ts, b.leg_event_start_ts), 0) <= 240 THEN '2h - 4h'
                 WHEN round(datediff(minute, b.bet_placed_ts, b.leg_event_start_ts), 0) <= 480 THEN '4h - 8h'
                 WHEN round(datediff(minute, b.bet_placed_ts, b.leg_event_start_ts), 0) <= 720 THEN '8h - 12h'
                 WHEN round(datediff(minute, b.bet_placed_ts, b.leg_event_start_ts), 0) <= 1440 THEN '12h - 24h'
                 WHEN round(datediff(minute, b.bet_placed_ts, b.leg_event_start_ts), 0) <= 2880 THEN '24h - 48h'
                 WHEN round(datediff(minute, b.bet_placed_ts, b.leg_event_start_ts), 0) <= 4320 THEN '48h - 72h'
                 WHEN round(datediff(minute, b.bet_placed_ts, b.leg_event_start_ts), 0) <= (1440 * 7) THEN '3d - 1w'
                 WHEN round(datediff(minute, b.bet_placed_ts, b.leg_event_start_ts), 0) <= (1440 * 14) THEN '1w - 2w'
                 WHEN round(datediff(minute, b.bet_placed_ts, b.leg_event_start_ts), 0) <= (1440 * 30) THEN '2w - 1m'
                 WHEN round(datediff(minute, b.bet_placed_ts, b.leg_event_start_ts), 0) <= (1440 * 91) THEN '1m - 3m'
                 ELSE '3m +'
                  END AS time_to_off,
            
            -- TODO: Map remaining bet analysis columns
            CASE WHEN b.bet_type_derived = 'straight'
                  AND b.bet_leg_numbers = 1
                 THEN 'Straight'
                 WHEN b.leg_price_type_code = 'sgm'
                  AND b.bet_leg_numbers > 1
                  AND b.[SGPP_YN_PLACEHOLDER] = TRUE
                 THEN 'SGP+'
                 WHEN b.leg_price_type_code = 'sgm'
                  AND b.bet_leg_numbers > 1
                 THEN 'SGP'
                 ELSE 'Parlay'
                  END AS bet_type,
                  
            -- Additional performance metrics would continue here...
            -- TODO: Complete remaining column mappings for full migration
            
            round(sum(b.bet_portion), 4) AS bet_count,
            round(sum(b.leg_gross_stake_amount), 2) AS gross_handle,
            round(sum(b.[FREE_BET_USED_PLACEHOLDER]), 2) AS free_bet_handle,
            round(sum(b.leg_gross_gaming_revenue_amount), 2) AS finance_revenue
            
            -- TODO: Add remaining aggregated columns after mapping lookup tables
                       
      FROM  core_views.sportsbook.bet_legs b

            INNER JOIN dates_online d
                    ON d.bet_settled_date_local = b.bet_last_settled_local_ts
                           
            -- TODO: Replace with Delta Lake equivalent
            LEFT JOIN [ANALYST_RT_BR_SB_GROUPINGS_PLACEHOLDER] g
                    ON g.sport_name = b.leg_sport_name_reporting
                   AND g.competition_name = b.leg_competition_name_reporting
                   AND g.market_name = b.leg_market_name_openbet
    
            -- TODO: Replace with Delta Lake equivalent
            LEFT JOIN [ANALYST_RT_CD_FD_RISK_FEATURES_PLACEHOLDER] r
                    ON r.bet_placed_date = b.bet_placed_ts
                   AND r.bet_id = b.bet_id
                   AND r.leg_id = cast(b.leg_id as string)
                   AND r.bet_placed_date > date_sub(current_date(), 395) -- ~13 months
                   
            -- TODO: Replace with Delta Lake equivalent
            LEFT JOIN [FDG_FACT_OB_ACCOUNT_CATEGORISATION_USER_PLACEHOLDER] s
                    ON s.product_account_id = b.fanduel_user_id
                   AND s.state = b.location_code
                   AND s.start_dt <= b.bet_placed_ts
                   AND s.end_dt > b.bet_placed_ts
    
     WHERE      b.bet_status_code = 'c'
            AND b.bet_result IN ('won', 'lost', 'void')
            AND (b.[IS_TEST_ACCOUNT_PLACEHOLDER] IS NULL OR b.[IS_TEST_ACCOUNT_PLACEHOLDER] IS FALSE)
            AND b.location_code <> 'fd'
    
  GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35
);
