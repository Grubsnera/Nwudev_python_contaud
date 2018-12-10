SELECT
  X002cb_vss_balmonth.CAMPUS_VSS AS CAMPUS,
  X002cb_vss_balmonth.MONTH_VSS AS MONTH,
  Round(X002cb_vss_balmonth.AMOUNT_DT, 2) AS VSS_TRAN_DT,
  Round(X002cb_vss_balmonth.AMOUNT_CT, 2) AS VSS_TRAN_CT,
  Round(X002cb_vss_balmonth.AMOUNT, 2) AS VSS_TRAN,
  Round(0, 2) AS VSS_BAL,
  Round(X001cb_gl_balmonth.BALANCE, 2) AS GL_TRAN,
  Round(0, 2) AS GL_BAL,
  Round(0, 2) AS VSS_GL_DIFF,
  Round(0, 2) AS VSS_GL_RECON
FROM
  X002cb_vss_balmonth
  LEFT JOIN X001cb_gl_balmonth ON X001cb_gl_balmonth.CAMPUS = X002cb_vss_balmonth.CAMPUS_VSS AND
    X001cb_gl_balmonth.MONTH = X002cb_vss_balmonth.MONTH_VSS
