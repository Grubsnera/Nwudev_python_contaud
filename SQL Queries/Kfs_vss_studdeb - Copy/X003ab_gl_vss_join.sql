SELECT
  X002ca_vss_transumm.CAMPUS_VSS,
  X002ca_vss_transumm.MONTH_VSS,
  X002ca_vss_transumm.TRANSCODE_VSS,
  X002ca_vss_transumm.TEMP_DESC_A,
  X002ca_vss_transumm.AMOUNT_VSS,
  X001cc_gl_summtype.CAMPUS,
  X001cc_gl_summtype.MONTH,
  X001cc_gl_summtype.DESCRIPTION,
  X001cc_gl_summtype.AMOUNT,
  Length(X002ca_vss_transumm.CAMPUS_VSS) AS CAMPUS_LEN
FROM
  X001cc_gl_summtype
  LEFT JOIN X002ca_vss_transumm ON X002ca_vss_transumm.CAMPUS_VSS = X001cc_gl_summtype.CAMPUS AND
    X002ca_vss_transumm.MONTH_VSS = X001cc_gl_summtype.MONTH AND X002ca_vss_transumm.TEMP_DESC_A =
    X001cc_gl_summtype.DESCRIPTION
WHERE
  Length(X002ca_vss_transumm.CAMPUS_VSS) IS NULL
