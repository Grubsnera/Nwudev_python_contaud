SELECT
  X010ca_join_vss_gl_burs.CAMPUS_VSS,
  X010ca_join_vss_gl_burs.MONTH_VSS,
  X010ca_join_vss_gl_burs.STUDENT_VSS,
  X010ca_join_vss_gl_burs.TRANSDATE_VSS,
  X010ca_join_vss_gl_burs.TRANSCODE_VSS,
  X010ca_join_vss_gl_burs.TRANSDESC_VSS,
  X010ca_join_vss_gl_burs.AMOUNT_VSS,
  X010ca_join_vss_gl_burs.BURSCODE_VSS,
  X010ca_join_vss_gl_burs.BURSNAAM_VSS
FROM
  X010ca_join_vss_gl_burs
WHERE
  X010ca_join_vss_gl_burs.STUDENT_GL IS NULL
