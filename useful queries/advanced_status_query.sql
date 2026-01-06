        SELECT COUNT(dispatches.dispatch_id) AS dispatches, queue.complete, queue.success
        FROM queue, dispatches
        WHERE queue.job_id=%(job_id)s AND dispatches.job_id=%(job_id)s
        GROUP BY queue.complete, queue.success