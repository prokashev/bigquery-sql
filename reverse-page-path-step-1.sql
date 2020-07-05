  SELECT
      fullvisitorID,
      FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%S", TIMESTAMP_SECONDS(SAFE_CAST(visitStartTime+hits.time/1000 AS INT64))) AS hit_timestamp,
      hits.page.pagePath AS pagePath,
    FROM
      `pro-tracker-168008.85132606.ga_sessions_20200427`,
      UNNEST(hits) AS hits
    WHERE
      hits.type = "PAGE"
    ORDER BY
      fullvisitorID,
      hit_timestamp
