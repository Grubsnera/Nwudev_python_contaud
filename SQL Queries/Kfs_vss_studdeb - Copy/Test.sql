SELECT
  X002ab_vss_transort.TRANUSER,
  X002ab_vss_transort.TRANSDATE_VSS,
  Total(X002ab_vss_transort.AMOUNT_VSS) AS Total_AMOUNT_VSS
FROM
  X002ab_vss_transort
WHERE
  (X002ab_vss_transort.TRANSCODE_VSS = '370' OR
    X002ab_vss_transort.TRANSCODE_VSS = '371')
GROUP BY
  X002ab_vss_transort.TRANUSER,
  X002ab_vss_transort.TRANSDATE_VSS
HAVING
  Total(X002ab_vss_transort.AMOUNT_VSS) <> 0.00
