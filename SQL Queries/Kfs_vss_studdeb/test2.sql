SELECT
  X050ab_extract_371.ORG_371,
  Total(X050ab_extract_371.AMOUNT_371) AS Total_AMOUNT_371
FROM
  X050ab_extract_371
GROUP BY
  X050ab_extract_371.ORG_371
