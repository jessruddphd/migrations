-- Migrated from Redshift to Delta Lake (Databricks)
-- Original query group setting not applicable in Databricks
-- SET query_group to 'Sbk Risk Tasks - Sbk Performance';

-- Generate List of Dates
-- Note: Temporary tables converted to CREATE OR REPLACE TEMPORARY VIEW

-- Dates from online gameplay
CREATE OR REPLACE TEMPORARY VIEW dates_online AS
SELECT bet_last_settled_local_ts AS bet_settled_date_local
FROM core_views.sportsbook.bet_legs
WHERE bet_last_settled_local_ts IS NOT NULL
  AND date(bet_last_settled_local_ts) >= date_sub(current_date(), 3) -- TODO: Verify dw_date equivalent mapping
GROUP BY bet_last_settled_local_ts;

-- Dates from retail gameplay  
CREATE OR REPLACE TEMPORARY VIEW dates_retail AS
SELECT bet_settled_date_local
FROM core_views.sportsbook_retail.gameplay_legs
WHERE bet_settled_date_local IS NOT NULL
  AND date(bet_settled_date_local) >= date_sub(current_date(), 3) -- TODO: Verify dw_date equivalent mapping
GROUP BY bet_settled_date_local;

-- Dates from Canada gameplay
CREATE OR REPLACE TEMPORARY VIEW dates_canada AS
SELECT bet_last_settled_local_ts AS bet_settled_date_local
FROM core_views.sportsbook_can.bet_legs
WHERE bet_last_settled_local_ts IS NOT NULL
  AND date(bet_last_settled_local_ts) >= date_sub(current_date(), 3) -- TODO: Verify dw_date equivalent mapping
GROUP BY bet_last_settled_local_ts;

-- Months from online gameplay
CREATE OR REPLACE TEMPORARY VIEW months_online AS
SELECT date_trunc('month', bet_last_settled_ts) AS settled_month
FROM core_views.sportsbook.bet_legs
WHERE bet_last_settled_local_ts IS NOT NULL
  AND date(bet_last_settled_local_ts) >= date_sub(current_date(), 3) -- TODO: Verify dw_date equivalent mapping
GROUP BY date_trunc('month', bet_last_settled_ts);

-- Months from retail gameplay
CREATE OR REPLACE TEMPORARY VIEW months_retail AS
SELECT date_trunc('month', bet_settled_ts) AS settled_month
FROM core_views.sportsbook_retail.gameplay_legs
WHERE bet_settled_date_local IS NOT NULL
  AND date(bet_settled_date_local) >= date_sub(current_date(), 3) -- TODO: Verify dw_date equivalent mapping
GROUP BY date_trunc('month', bet_settled_ts);

-- Months from Canada gameplay
CREATE OR REPLACE TEMPORARY VIEW months_canada AS
SELECT date_trunc('month', bet_last_settled_ts) AS settled_month
FROM core_views.sportsbook_can.bet_legs
WHERE bet_last_settled_local_ts IS NOT NULL
  AND date(bet_last_settled_local_ts) >= date_sub(current_date(), 3) -- TODO: Verify dw_date equivalent mapping
GROUP BY date_trunc('month', bet_last_settled_ts);

-- Union of all months
CREATE OR REPLACE TEMPORARY VIEW months_union AS
SELECT settled_month
FROM (
    SELECT settled_month FROM months_online
    UNION ALL
    SELECT settled_month FROM months_retail
    UNION ALL
    SELECT settled_month FROM months_canada
)
GROUP BY settled_month;
