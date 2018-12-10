SELECT
  X010ba_gl_burs.CAMPUS_GL,
  X010ba_gl_burs.MONTH_GL,
  X010ba_gl_burs.TRANSDESC_GL,
  Sum(X010ba_gl_burs.AMOUNT_GL) AS Sum_AMOUNT_GL
FROM
  X010ba_gl_burs
GROUP BY
  X010ba_gl_burs.CAMPUS_GL,
  X010ba_gl_burs.MONTH_GL,
  X010ba_gl_burs.TRANSDESC_GL
