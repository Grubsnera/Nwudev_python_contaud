SELECT
  X010fb_join_prev_reported.CAMPUS_VSS AS VSS_CAMPUS,
  X010fb_join_prev_reported.CAMPUS_GL AS GL_CAMPUS,
  X010fb_join_prev_reported.STUDENT_VSS AS STUDENT,
  X010fb_join_prev_reported.TRANSDATE_VSS AS DATE,
  X010fb_join_prev_reported.AMOUNT_VSS AS AMOUNT,
  X010fb_join_prev_reported.TRANSCODE_VSS AS TRANSCODE,
  X010fb_join_prev_reported.TRANSDESC_VSS AS TRANDESC,
  X010fb_join_prev_reported.BURSCODE_VSS AS BURSCODE,
  X010fb_join_prev_reported.BURSNAAM_VSS AS BURSNAME,
  X010fb_join_prev_reported.TRANSEDOC_GL AS GL_EDOC,
  X010fb_join_prev_reported.TRANSENTR_GL AS GL_DESC,
  X010fb_join_prev_reported.TRANSUSER_VSS AS USER
FROM
  X010fb_join_prev_reported
ORDER BY
  DATE,
  VSS_CAMPUS,
  STUDENT,
  BURSCODE
