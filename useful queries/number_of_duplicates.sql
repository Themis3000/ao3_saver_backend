SELECT found_as_duplicate, COUNT(found_as_duplicate) as count
FROM dispatches
GROUP BY found_as_duplicate