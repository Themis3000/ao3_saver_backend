SELECT work_id, count(work_id) as count
FROM queue
WHERE success = false
GROUP BY work_id
ORDER BY count desc
LIMIT 500;