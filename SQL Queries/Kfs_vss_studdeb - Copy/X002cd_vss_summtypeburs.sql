SELECT
  X010aa_vss_burs.CAMPUS_VSS,
  X010aa_vss_burs.MONTH_VSS,
  X010aa_vss_burs.TRANSCODE_VSS,
  X010aa_vss_burs.TRANSDESC_VSS,
  Sum(X010aa_vss_burs.AMOUNT_VSS) AS Sum_AMOUNT_VSS
FROM
  X010aa_vss_burs
GROUP BY
  X010aa_vss_burs.CAMPUS_VSS,
  X010aa_vss_burs.MONTH_VSS,
  X010aa_vss_burs.TRANSCODE_VSS,
  X010aa_vss_burs.TRANSDESC_VSS
