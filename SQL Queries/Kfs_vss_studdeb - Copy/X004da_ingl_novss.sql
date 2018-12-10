SELECT
  X003ab_gl_vss_join.CAMPUS_VSS AS CAMPUS,
  X003ab_gl_vss_join.MONTH_VSS AS MONTH,
  X003ab_gl_vss_join.DESC_VSS AS GL_DESCRIPTION,
  Round(X003ab_gl_vss_join.AMOUNT,2) AS AMOUNT_GL
FROM
  X003ab_gl_vss_join
