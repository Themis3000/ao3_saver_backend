SELECT requesting_ip,
       count(dispatch_id) filter (WHERE fail_reported = true AND dispatched_time BETWEEN NOW() - INTERVAL '1 day' AND NOW()) as fails,
       count(dispatch_id) filter (WHERE fail_reported = false AND complete = true AND dispatched_time BETWEEN NOW() - INTERVAL '1 day' AND NOW()) as successes
FROM dispatches
GROUP BY requesting_ip
ORDER BY fails desc
LIMIT 500;