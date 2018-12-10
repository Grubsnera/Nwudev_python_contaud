SELECT
  X003aa_vss_gl_join.CAMPUS_VSS AS CAMPUS,
  X003aa_vss_gl_join.MONTH_VSS AS MONTH,
  Round(Sum(X003aa_vss_gl_join.AMOUNT_VSS),2) AS AMOUNT_VSS,
  Round(Sum(X003aa_vss_gl_join.AMOUNT),2) AS AMOUNT_GL
FROM
  X003aa_vss_gl_join
WHERE
  X003aa_vss_gl_join.MATCHED = 'C'
GROUP BY
  X003aa_vss_gl_join.CAMPUS_VSS,
  X003aa_vss_gl_join.MONTH_VSS,
  X003aa_vss_gl_join.MATCHED
