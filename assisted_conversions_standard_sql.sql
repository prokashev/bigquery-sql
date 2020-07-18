--Standard SQL query to get an assisted conversions report. Replace "pro-tracker-id" with your BigQuery project ID

SELECT
    revenue,
    transaction_ID,
    MIN(lookback_window) as lookback_window
FROM (SELECT
    visitor_ID_pageviews,
    visitor_ID_purchases,
    revenue,
    transaction_ID,
    hit_timestamp_transaction,
    TIMESTAMP_DIFF(PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S",hit_timestamp_transaction), PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S",hit_timestamp), day) as lookback_window
FROM (SELECT
    fullVisitorId as visitor_ID_pageviews, visitID, visitNumber, hits.page.pagePath,
    FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", TIMESTAMP_MILLIS(CAST(visitStartTime*1000+hits.time AS INT64))) as hit_timestamp
FROM
    `pro-tracker-id.ga_sessions_*`, UNNEST(hits) AS hits
WHERE
    _TABLE_SUFFIX BETWEEN "20200501" AND "20200531"
    AND
    REGEXP_CONTAINS (hits.page.pagePath, r'\/page\/') AND hits.hitNumber = 1
GROUP BY
    visitor_ID_pageviews,
    visitNumber, visitID,
    hits.page.pagePath,
    hit_timestamp)
AS table_with_visits
INNER JOIN
(SELECT
    fullVisitorId as visitor_ID_purchases,
    totals.totalTransactionRevenue/1000000 AS revenue,
    hits.transaction.transactionId AS transaction_ID,
    FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", TIMESTAMP_MILLIS(CAST(visitStartTime*1000+hits.time AS INT64))) as hit_timestamp_transaction
FROM
    `pro-tracker-id.ga_sessions_*`, UNNEST(hits) AS hits
WHERE
    _TABLE_SUFFIX BETWEEN "20200601" AND "20200630"
    AND
    totals.transactions > 0
GROUP BY
    visitor_ID_purchases,
    revenue,
    transaction_ID,
    hit_timestamp_transaction)
AS table_with_transactions
ON
    table_with_visits.visitor_ID_pageviews = table_with_transactions.visitor_ID_purchases
WHERE
    transaction_ID != 'null' AND hit_timestamp_transaction > hit_timestamp
ORDER BY
    revenue)
WHERE
    lookback_window < 91
GROUP BY
    visitor_ID_pageviews,
    visitor_ID_purchases,
    revenue,
    transaction_ID
ORDER BY
    transaction_ID
