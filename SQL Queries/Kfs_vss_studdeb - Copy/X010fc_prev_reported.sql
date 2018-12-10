SELECT
  X010fb_join_prev_reported.STUDENT_VSS,
  X010fb_join_prev_reported.TRANSDATE_VSS,
  X010fb_join_prev_reported.BURSCODE_VSS,
  X010fb_join_prev_reported.AMOUNT_VSS
FROM
  X010fb_join_prev_reported
WHERE
  X010fb_join_prev_reported.PROCESS IS NULL
