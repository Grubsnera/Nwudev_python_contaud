SELECT
  X010_Studdeb_tranlist.CAMPUS,
  X010_Studdeb_tranlist.UNIV_FISCAL_PRD_CD AS MONTH,
  X010_Studdeb_tranlist.DESCRIPTION,
  Sum(X010_Studdeb_tranlist.CALC_AMOUNT) AS AMOUNT
FROM
  X010_Studdeb_tranlist
GROUP BY
  X010_Studdeb_tranlist.CAMPUS,
  X010_Studdeb_tranlist.UNIV_FISCAL_PRD_CD,
  X010_Studdeb_tranlist.DESCRIPTION
ORDER BY
  MONTH DESC
