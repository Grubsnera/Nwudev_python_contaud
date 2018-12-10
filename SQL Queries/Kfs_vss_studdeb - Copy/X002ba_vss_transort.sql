SELECT
  X002aa_vss_tranlist.FBUSENTID AS STUDENT_VSS,
  X002aa_vss_tranlist.CAMPUS AS CAMPUS_VSS,
  X002aa_vss_tranlist.TRANSCODE AS TRANSCODE_VSS,
  X002aa_vss_tranlist.MONTH AS MONTH_VSS,
  X002aa_vss_tranlist.TRANSDATE AS TRANSDATE_VSS,
  X002aa_vss_tranlist.AMOUNT AS AMOUNT_VSS,
  X002aa_vss_tranlist.DESCRIPTION_E,
  X002aa_vss_tranlist.DESCRIPTION_A,
  X002aa_vss_tranlist.TRANSDATETIME,
  X002aa_vss_tranlist.POSTDATEDTRANSDATE,
  X002aa_vss_tranlist.FSERVICESITE AS SITE_SERV_VSS,
  X002aa_vss_tranlist.FDEBTCOLLECTIONSITE AS SITE_DEBT_VSS,
  X002aa_vss_tranlist.AUDITDATETIME,
  X002aa_vss_tranlist.FORIGINSYSTEMFUNCTIONID,
  X002aa_vss_tranlist.FAUDITSYSTEMFUNCTIONID,
  X002aa_vss_tranlist.FAUDITUSERCODE,
  X002aa_vss_tranlist.AMOUNT_DT,
  X002aa_vss_tranlist.AMOUNT_CR
FROM
  X002aa_vss_tranlist
WHERE
  X002aa_vss_tranlist.TRANSCODE <> ''
ORDER BY
  X002aa_vss_tranlist.TRANSDATETIME
