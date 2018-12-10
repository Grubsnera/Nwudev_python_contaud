SELECT ALL
  X002ca_vss_transumm.CAMPUS_VSS,
  X002ca_vss_transumm.MONTH_VSS,
  X002ca_vss_transumm.TEMP_DESC_A,
  X002ca_vss_transumm.Sum_AMOUNT_VSS,
  X001cc_gl_summtype.CAMPUS,
  X001cc_gl_summtype.MONTH,
  X001cc_gl_summtype.DESCRIPTION,
  X001cc_gl_summtype.AMOUNT
FROM
  X002ca_vss_transumm
  LEFT JOIN X001cc_gl_summtype ON X001cc_gl_summtype.CAMPUS = X002ca_vss_transumm.CAMPUS_VSS AND
    X001cc_gl_summtype.MONTH = X002ca_vss_transumm.MONTH_VSS AND X001cc_gl_summtype.DESCRIPTION =
    X002ca_vss_transumm.TEMP_DESC_A
