SELECT
  X010ca_join_vss_gl_burs.ROWID_GL,
  X010ca_join_vss_gl_burs.STUDENT_GL,
  X010ca_join_vss_gl_burs.TRANSDATE_GL,
  X010ca_join_vss_gl_burs.MONTH_GL,
  X010ca_join_vss_gl_burs.CAMPUS_GL,
  X010ca_join_vss_gl_burs.BURSCODE_GL,
  X010ca_join_vss_gl_burs.AMOUNT_GL,
  X010ca_join_vss_gl_burs.TRANSDESC_GL,
  X010ca_join_vss_gl_burs.TRANSEDOC_GL,
  X010ca_join_vss_gl_burs.TRANSENTR_GL,
  X010ca_join_vss_gl_burs."ROWID"
FROM
  X010ca_join_vss_gl_burs
WHERE
  X010ca_join_vss_gl_burs."ROWID" = X010ca_join_vss_gl_burs."ROWID" ISNULL
