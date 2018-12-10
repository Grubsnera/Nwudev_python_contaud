SELECT
  X002ba_vss_transort.CAMPUS_VSS,
  X002ba_vss_transort.MONTH_VSS,
  X002ba_vss_transort.TEMP_DESC_A,
  Sum(X002ba_vss_transort.AMOUNT_VSS) AS Sum_AMOUNT_VSS
FROM
  X002ba_vss_transort
GROUP BY
  X002ba_vss_transort.CAMPUS_VSS,
  X002ba_vss_transort.MONTH_VSS,
  X002ba_vss_transort.TEMP_DESC_A
