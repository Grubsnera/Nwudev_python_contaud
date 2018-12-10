SELECT
  X010ba_gl_burs.STUDENT_GL,
  X010ba_gl_burs.TRANSDATE_GL,
  X010ba_gl_burs.MONTH_GL,
  X010ba_gl_burs.CAMPUS_GL,
  X010ba_gl_burs.BURSCODE_GL,
  X010ba_gl_burs.AMOUNT_GL,
  X010ba_gl_burs.TRANSDESC_GL,
  X010ba_gl_burs.TRANSEDOC_GL,
  X010ba_gl_burs.TRANSENTR_GL,
  X010aa_vss_burs.STUDENT_VSS,
  X010aa_vss_burs.TRANSDATE_VSS,
  X010aa_vss_burs.MONTH_VSS,
  X010aa_vss_burs.CAMPUS_VSS,
  X010aa_vss_burs.BURSCODE_VSS,
  X010aa_vss_burs.AMOUNT_VSS,
  X010aa_vss_burs.BURSNAAM_VSS,
  X010aa_vss_burs.TRANSCODE_VSS,
  X010aa_vss_burs.TRANSDESC_VSS,
  X010aa_vss_burs.TRANSUSER_VSS
FROM
  X010ba_gl_burs
  LEFT JOIN X010aa_vss_burs ON X010aa_vss_burs.STUDENT_VSS = X010ba_gl_burs.STUDENT_GL AND X010aa_vss_burs.TRANSDATE_VSS
    = X010ba_gl_burs.TRANSDATE_GL AND X010aa_vss_burs.AMOUNT_VSS = X010ba_gl_burs.AMOUNT_GL
