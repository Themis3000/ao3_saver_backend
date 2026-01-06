SELECT submitted_by_id, COUNT(*) as counted
FROM queue
-- WHERE submitted_time > '12-1-25'
GROUP BY submitted_by_id
ORDER BY counted DESC, submitted_by_id
LIMIT 500;