SELECT
  X002cc_vss_summtype.CAMPUS_VSS,
  X002cc_vss_summtype.MONTH_VSS,
  X002cc_vss_summtype.AMOUNT_VSS AS VSS_042,
  VSS_052.AMOUNT_VSS AS VSS_052
FROM
  X002cc_vss_summtype
  LEFT JOIN X002cc_vss_summtype VSS_052 ON VSS_052.CAMPUS_VSS = X002cc_vss_summtype.CAMPUS_VSS AND VSS_052.MONTH_VSS =
    X002cc_vss_summtype.MONTH_VSS
WHERE
  X002cc_vss_summtype.TRANSCODE_VSS = '042' OR
  VSS_052.TRANSCODE_VSS = '052'
