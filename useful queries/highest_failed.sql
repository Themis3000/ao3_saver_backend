SELECT work_id, count(work_id) as count
FROM queue
WHERE success = false AND submitted_time > '12-28-2025'
GROUP BY work_id
ORDER BY count desc
LIMIT 500;