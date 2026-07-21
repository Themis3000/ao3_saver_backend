SELECT requesting_ip, count(requesting_ip) as count
FROM dispatches
WHERE fail_reported = true
GROUP BY requesting_ip
ORDER BY count desc
LIMIT 500;