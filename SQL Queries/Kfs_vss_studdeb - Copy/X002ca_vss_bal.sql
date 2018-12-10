SELECT
  X002ab_vss_transort.CAMPUS_VSS,
  Sum(X002ab_vss_transort.AMOUNT_DT) AS AMOUNT_DT,
  Sum(X002ab_vss_transort.AMOUNT_CR) AS AMOUNT_CT,
  Sum(X002ab_vss_transort.AMOUNT_VSS) AS AMOUNT
FROM
  X002ab_vss_transort
WHERE
  X002ab_vss_transort.MONTH_VSS < '06'
GROUP BY
  X002ab_vss_transort.CAMPUS_VSS
