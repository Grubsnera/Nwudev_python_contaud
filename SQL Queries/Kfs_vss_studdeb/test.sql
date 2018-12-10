SELECT
  X050aa_extract_370.ORG_370,
  Total(X050aa_extract_370.AMOUNT_370) AS Total_AMOUNT_370
FROM
  X050aa_extract_370
GROUP BY
  X050aa_extract_370.ORG_370
