SELECT
  X001ac_impo_reported.FIELD1,
  Count(X001ac_impo_reported.FIELD2) AS Count_FIELD2
FROM
  X001ac_impo_reported
GROUP BY
  X001ac_impo_reported.FIELD1
