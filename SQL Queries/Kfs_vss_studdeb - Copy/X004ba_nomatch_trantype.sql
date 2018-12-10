SELECT
  X003aa_vss_gl_join.CAMPUS_VSS AS CAMPUS,
  X003aa_vss_gl_join.MONTH_VSS AS MONTH,
  X003aa_vss_gl_join.TRANSCODE_VSS AS TRAN_TYPE,
  X003aa_vss_gl_join.TEMP_DESC_A AS TRAN_DESCRIPTION,
  Round(X003aa_vss_gl_join.AMOUNT_VSS,2) AS AMOUNT_VSS,
  Round(X003aa_vss_gl_join.AMOUNT,2) AS AMOUNT_GL,
  Round(0,2) AS DIFF
FROM
  X003aa_vss_gl_join
WHERE
  X003aa_vss_gl_join.AMOUNT IS NOT NULL AND
  X003aa_vss_gl_join.MATCHED = 'X'
