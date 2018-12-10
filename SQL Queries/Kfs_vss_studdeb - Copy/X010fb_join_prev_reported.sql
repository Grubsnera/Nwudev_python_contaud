SELECT
  X010fa_test_burs_gl_diffcampus.STUDENT_VSS,
  X010fa_test_burs_gl_diffcampus.TRANSDATE_VSS,
  X010fa_test_burs_gl_diffcampus.BURSCODE_VSS,
  X010fa_test_burs_gl_diffcampus.AMOUNT_VSS,
  X000_PREV_REPORTED.PROCESS,
  X000_PREV_REPORTED.DATE_REPORTED,
  X000_PREV_REPORTED.DATE_RETEST
FROM
  X010fa_test_burs_gl_diffcampus
  LEFT JOIN X000_PREV_REPORTED ON X000_PREV_REPORTED.FIELD1 = X010fa_test_burs_gl_diffcampus.STUDENT_VSS AND
    X000_PREV_REPORTED.FIELD2 = X010fa_test_burs_gl_diffcampus.TRANSDATE_VSS AND X000_PREV_REPORTED.FIELD3 =
    X010fa_test_burs_gl_diffcampus.BURSCODE_VSS AND
    X000_PREV_REPORTED.FIELD4 = X010fa_test_burs_gl_diffcampus.AMOUNT_VSS
