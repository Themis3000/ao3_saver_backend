SELECT requesting_ip,
       count(dispatch_id) filter (WHERE fail_reported = true) as fails,
       count(dispatch_id) filter (WHERE fail_reported = false AND complete = true) as successes
FROM dispatches
GROUP BY requesting_ip
ORDER BY fails desc
LIMIT 500;