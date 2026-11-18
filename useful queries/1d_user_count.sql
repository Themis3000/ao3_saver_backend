SELECT COUNT(*) FROM (
    SELECT DISTINCT submitted_by_id
    FROM queue
    WHERE submitted_time > (NOW() - INTERVAL '1 day')
                     ) AS user_count;
