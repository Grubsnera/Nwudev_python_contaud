SELECT
  X010_Studdeb_tranlist.CAMPUS AS CAMPUS,
  Sum(X010_Studdeb_tranlist.CALC_AMOUNT) AS BALANCE
FROM
  X010_Studdeb_tranlist
WHERE
  X010_Studdeb_tranlist.UNIV_FISCAL_PRD_CD != 'BB'
GROUP BY
  X010_Studdeb_tranlist.CAMPUS
