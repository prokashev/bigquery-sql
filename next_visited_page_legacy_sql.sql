SELECT
     t.page_path,
     t.second_page_path,
     count(sessionId) AS total_sessions
FROM (SELECT
     CONCAT(fullVisitorId,"-",STRING(visitStartTime)) AS sessionId,
     hits.hitNumber,
     visitNumber,
     hits.page.pagePath AS page_path,
     LEAD(hits.page.pagePath) OVER (PARTITION BY fullVisitorId, visitStartTime ORDER BY hits.hitNumber) AS second_page_path
FROM
     TABLE_DATE_RANGE([pro-tracker-id.ga_sessions_], TIMESTAMP('2019-01-01'), TIMESTAMP('2019-06-01'))
WHERE
     hits.type="PAGE" AND
     visitNumber = 1) t
WHERE
     t.hits.hitNumber=1 AND page_path = "/page/"
GROUP BY
     t.page_path,
     t.second_page_path
ORDER BY
     total_sessions DESC
