USE DATABASE SQUADRON_AI_DB;
USE SCHEMA OPERATIONS;

-- Which squadron had the highest mission success rate last month?
SELECT
    squadron_name,
    COUNT(*) AS mission_count,
    SUM(IFF(success_flag, 1, 0)) AS success_count,
    ROUND(SUM(IFF(success_flag, 1, 0)) / NULLIF(COUNT(*), 0), 3) AS success_rate
FROM MISSIONS
WHERE mission_date >= DATE_TRUNC('month', DATEADD(month, -1, CURRENT_DATE))
  AND mission_date < DATE_TRUNC('month', CURRENT_DATE)
GROUP BY squadron_name
ORDER BY success_rate DESC
LIMIT 1;

-- Generate a risk-ranked weekly operations table.
SELECT *
FROM VW_SQUADRON_RISK
ORDER BY risk_score DESC;

-- Find anomalies in aircraft readiness.
SELECT
    aircraft_id,
    squadron_name,
    aircraft_type,
    readiness_state,
    readiness_score,
    open_maintenance_items
FROM AIRCRAFT_READINESS
WHERE readiness_state <> 'READY'
   OR readiness_score < 70
   OR open_maintenance_items >= 3
ORDER BY readiness_score ASC;

-- Which parts or supplies are creating operational risk?
SELECT part_name, aircraft_id, quantity_on_hand, reorder_point, expected_restock_date, priority
FROM PARTS_INVENTORY
WHERE quantity_on_hand <= reorder_point
ORDER BY quantity_on_hand ASC, priority ASC;

-- Summarize delayed missions and likely causes.
SELECT AI_COMPLETE(
    'claude-4-sonnet',
    'Summarize delayed missions and likely causes: '
    || TO_JSON(ARRAY_AGG(OBJECT_CONSTRUCT(*)))
) AS summary
FROM (
    SELECT mission_id, squadron_name, mission_type, delay_minutes, delay_reason, mission_notes
    FROM MISSIONS
    WHERE delay_minutes > 0
);
