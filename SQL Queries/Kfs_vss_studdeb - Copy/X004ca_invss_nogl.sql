SELECT
  X003aa_vss_gl_join.CAMPUS_VSS AS CAMPUS,
  X003aa_vss_gl_join.MONTH_VSS AS MONTH,
  X003aa_vss_gl_join.TRANSCODE_VSS AS TRANS_TYPE,
  X003aa_vss_gl_join.TEMP_DESC_A AS TRANS_DESCRIPTION,
  Round(X003aa_vss_gl_join.AMOUNT_VSS,2) AS AMOUNT_VSS
FROM
  X003aa_vss_gl_join
WHERE
  X003aa_vss_gl_join.DESC_VSS IS NULL
