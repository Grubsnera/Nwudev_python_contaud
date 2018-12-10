SELECT
  X000_Organization.FIN_COA_CD,
  X000_Organization.ORG_CD,
  X000_Organization.ORG_TYP_CD,
  X000_Organization.ORG_TYP_NM,
  X000_Organization.ORG_NM,
  X000_Organization.ORG_MGR_UNVL_ID,
  X000_Organization.ORG_LVL,
  X000_Organization1.FIN_COA_CD AS FIN_COA_CD1,
  X000_Organization1.ORG_CD AS ORG_CD1,
  X000_Organization1.ORG_TYP_CD AS ORG_TYP_CD1,
  X000_Organization1.ORG_TYP_NM AS ORG_TYP_NM1,
  X000_Organization1.ORG_NM AS ORG_NM1,
  X000_Organization1.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID1,
  X000_Organization2.FIN_COA_CD AS FIN_COA_CD2,
  X000_Organization2.ORG_CD AS ORG_CD2,
  X000_Organization2.ORG_TYP_CD AS ORG_TYP_CD2,
  X000_Organization2.ORG_TYP_NM AS ORG_TYP_NM2,
  X000_Organization2.ORG_NM AS ORG_NM2,
  X000_Organization2.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID2,
  X000_Organization1.ORG_LVL AS ORG_LVL1,
  X000_Organization3.FIN_COA_CD AS FIN_COA_CD3,
  X000_Organization3.ORG_CD AS ORG_CD3,
  X000_Organization3.ORG_TYP_CD AS ORG_TYP_CD3,
  X000_Organization3.ORG_TYP_NM AS ORG_TYP_NM3,
  X000_Organization3.ORG_NM AS ORG_NM3,
  X000_Organization3.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID3,
  X000_Organization4.FIN_COA_CD AS FIN_COA_CD4,
  X000_Organization4.ORG_CD AS ORG_CD4,
  X000_Organization4.ORG_TYP_CD AS ORG_TYP_CD4,
  X000_Organization4.ORG_TYP_NM AS ORG_TYP_NM4,
  X000_Organization4.ORG_NM AS ORG_NM4,
  X000_Organization4.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID4,
  X000_Organization5.FIN_COA_CD AS FIN_COA_CD5,
  X000_Organization5.ORG_CD AS ORG_CD5,
  X000_Organization5.ORG_TYP_CD AS ORG_TYP_CD5,
  X000_Organization5.ORG_TYP_NM AS ORG_TYP_NM5,
  X000_Organization5.ORG_NM AS ORG_NM5,
  X000_Organization5.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID5,
  X000_Organization6.FIN_COA_CD AS FIN_COA_CD6,
  X000_Organization6.ORG_CD AS ORG_CD6,
  X000_Organization6.ORG_TYP_CD AS ORG_TYP_CD6,
  X000_Organization6.ORG_TYP_NM AS ORG_TYP_NM6,
  X000_Organization6.ORG_NM AS ORG_NM6,
  X000_Organization6.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID6,
  X000_Organization7.FIN_COA_CD AS FIN_COA_CD7,
  X000_Organization7.ORG_CD AS ORG_CD7,
  X000_Organization7.ORG_TYP_CD AS ORG_TYP_CD7,
  X000_Organization7.ORG_TYP_NM AS ORG_TYP_NM7,
  X000_Organization7.ORG_NM AS ORG_NM7,
  X000_Organization7.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID7
FROM
  X000_Organization
  LEFT JOIN X000_Organization X000_Organization1 ON X000_Organization1.FIN_COA_CD = X000_Organization.RPTS_TO_FIN_COA_CD
    AND X000_Organization1.ORG_CD = X000_Organization.RPTS_TO_ORG_CD
  LEFT JOIN X000_Organization X000_Organization2 ON
    X000_Organization2.FIN_COA_CD = X000_Organization1.RPTS_TO_FIN_COA_CD AND X000_Organization2.ORG_CD =
    X000_Organization1.RPTS_TO_ORG_CD
  LEFT JOIN X000_Organization X000_Organization3 ON
    X000_Organization3.FIN_COA_CD = X000_Organization2.RPTS_TO_FIN_COA_CD AND X000_Organization3.ORG_CD =
    X000_Organization2.RPTS_TO_ORG_CD
  LEFT JOIN X000_Organization X000_Organization4 ON
    X000_Organization4.FIN_COA_CD = X000_Organization3.RPTS_TO_FIN_COA_CD AND X000_Organization4.ORG_CD =
    X000_Organization3.RPTS_TO_ORG_CD
  LEFT JOIN X000_Organization X000_Organization5 ON
    X000_Organization5.FIN_COA_CD = X000_Organization4.RPTS_TO_FIN_COA_CD AND X000_Organization5.ORG_CD =
    X000_Organization4.RPTS_TO_ORG_CD
  LEFT JOIN X000_Organization X000_Organization6 ON
    X000_Organization6.FIN_COA_CD = X000_Organization5.RPTS_TO_FIN_COA_CD AND X000_Organization6.ORG_CD =
    X000_Organization5.RPTS_TO_ORG_CD
  LEFT JOIN X000_Organization X000_Organization7 ON
    X000_Organization7.FIN_COA_CD = X000_Organization6.RPTS_TO_FIN_COA_CD AND X000_Organization7.ORG_CD =
    X000_Organization6.RPTS_TO_ORG_CD
WHERE
  X000_Organization.ORG_BEGIN_DT >= Date("2018-01-01")
ORDER BY
  X000_Organization.ORG_NM
