SELECT
  X010_Studdeb_tranlist.CAMPUS AS CAMPUS,
  X010_Studdeb_tranlist.UNIV_FISCAL_PRD_CD AS MONTH,
  Sum(X010_Studdeb_tranlist.CALC_AMOUNT) AS BALANCE
FROM
  X010_Studdeb_tranlist
GROUP BY
  X010_Studdeb_tranlist.CAMPUS,
  X010_Studdeb_tranlist.UNIV_FISCAL_PRD_CD
