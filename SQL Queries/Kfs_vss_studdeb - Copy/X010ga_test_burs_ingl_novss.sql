SELECT
  X010cb_join_gl_vss_burs.CAMPUS_GL,
  X010cb_join_gl_vss_burs.MONTH_GL,
  X010cb_join_gl_vss_burs.STUDENT_GL,
  X010cb_join_gl_vss_burs.TRANSDATE_GL,
  X010cb_join_gl_vss_burs.TRANSDESC_GL,
  X010cb_join_gl_vss_burs.AMOUNT_GL,
  X010cb_join_gl_vss_burs.BURSCODE_GL,
  X010cb_join_gl_vss_burs.TRANSEDOC_GL,
  X010cb_join_gl_vss_burs.TRANSENTR_GL
FROM
  X010cb_join_gl_vss_burs
WHERE
  X010cb_join_gl_vss_burs.STUDENT_VSS IS NULL
