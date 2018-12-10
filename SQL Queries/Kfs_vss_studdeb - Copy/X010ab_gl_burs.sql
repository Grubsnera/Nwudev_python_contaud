SELECT
  X001aa_gl_tranlist.STUDENT,
  X001aa_gl_tranlist.TRANSACTION_DT AS DATE,
  X001aa_gl_tranlist.CAMPUS_VSS AS CAMPUS,
  X001aa_gl_tranlist.BURSARY_CODE AS BURS_CODE,
  X001aa_gl_tranlist.CALC_AMOUNT AS AMOUNT,
  X001aa_gl_tranlist.DESCRIPTION AS TRAN_DESC,
  X001aa_gl_tranlist.FDOC_NBR AS TRAN_FDOC,
  X001aa_gl_tranlist.TRN_LDGR_ENTR_DESC AS TRAN_ENTR
FROM
  X001aa_gl_tranlist
WHERE
  X001aa_gl_tranlist.STUDENT <> ''
ORDER BY
  DATE,
  X001aa_gl_tranlist.STUDENT,
  BURS_CODE
