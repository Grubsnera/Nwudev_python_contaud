SELECT
  X002dc_vss_prevbal_curopen.CAMPUS,
  X002dc_vss_prevbal_curopen.STUDENT,
  X002dc_vss_prevbal_curopen.BAL_PREV,
  X002dc_vss_prevbal_curopen.BAL_OPEN,
  X002dc_vss_prevbal_curopen.DIFF_BAL
FROM
  X002dc_vss_prevbal_curopen
WHERE
  (X002dc_vss_prevbal_curopen.DIFF_BAL <> 0) OR
  (X002dc_vss_prevbal_curopen.DIFF_BAL IS NULL AND
  X002dc_vss_prevbal_curopen.BAL_PREV <> 0)
