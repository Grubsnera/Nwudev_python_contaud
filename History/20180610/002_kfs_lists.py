"""
Script to build standard KFS lists
Created on: 11 Mar 2018
Copyright: Albert J v Rensburg
"""

# Import python modules
import datetime
import sqlite3
import sys

# Add own module path
sys.path.append('S:/_my_modules')
#print(sys.path)

# Import own modules
import funcdate
import funccsv
import funcfile

# Open the script log file ******************************************************

funcfile.writelog()
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: 002_KFS_LISTS")
funcfile.writelog("---------------------")
ilog_severity = 1

# Declare variables
so_path = "W:/" #Source database path
so_file = "Kfs.sqlite" #Source database
s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("%t OPEN DATABASE: KFS")

print("KFS LISTS")
print("---------")

# Add calculated fields to the current gl transaction list *********************

print("Calculate current gl columns...")

# Calc combined cost string
if "CALC_COST_STRING" not in funccsv.get_colnames_sqlite(so_curs,"GL_ENTRY_T_CURR"):
    so_curs.execute("ALTER TABLE GL_ENTRY_T_CURR ADD COLUMN CALC_COST_STRING TEXT;")
    so_curs.execute("UPDATE GL_ENTRY_T_CURR SET CALC_COST_STRING = Trim(FIN_COA_CD) || '.' || Trim(ACCOUNT_NBR) || '.' || Trim(FIN_OBJECT_CD);")
    so_conn.commit()
    funcfile.writelog("%t CALC COLUMNS: Combined cost string")

# Calc amount
if "CALC_AMOUNT" not in funccsv.get_colnames_sqlite(so_curs,"GL_ENTRY_T_CURR"):
    so_curs.execute("ALTER TABLE GL_ENTRY_T_CURR ADD COLUMN CALC_AMOUNT REAL;")
    so_curs.execute("UPDATE GL_ENTRY_T_CURR " + """
                    SET CALC_AMOUNT = 
                    CASE
                       WHEN TRN_DEBIT_CRDT_CD = "C" THEN TRN_LDGR_ENTR_AMT * -1
                       ELSE TRN_LDGR_ENTR_AMT
                    END
                    ;""")
    so_conn.commit()
    funcfile.writelog("%t CALC COLUMNS: Amount")

# Add calculated fields to the previous gl transaction list ********************

print("Calculate previous gl columns...")

# Calc combined cost string
if "CALC_COST_STRING" not in funccsv.get_colnames_sqlite(so_curs,"GL_ENTRY_T_PREV"):
    so_curs.execute("ALTER TABLE GL_ENTRY_T_PREV ADD COLUMN CALC_COST_STRING TEXT;")
    so_curs.execute("UPDATE GL_ENTRY_T_PREV SET CALC_COST_STRING = Trim(FIN_COA_CD) || '.' || Trim(ACCOUNT_NBR) || '.' || Trim(FIN_OBJECT_CD);")
    so_conn.commit()
    funcfile.writelog("%t CALC COLUMNS: Combined prev cost string")

# Calc amount
if "CALC_AMOUNT" not in funccsv.get_colnames_sqlite(so_curs,"GL_ENTRY_T_PREV"):
    so_curs.execute("ALTER TABLE GL_ENTRY_T_PREV ADD COLUMN CALC_AMOUNT REAL;")
    so_curs.execute("UPDATE GL_ENTRY_T_PREV " + """
                    SET CALC_AMOUNT = 
                    CASE
                       WHEN TRN_DEBIT_CRDT_CD = "C" THEN TRN_LDGR_ENTR_AMT * -1
                       ELSE TRN_LDGR_ENTR_AMT
                    END
                    ;""")
    so_conn.commit()
    funcfile.writelog("%t CALC COLUMNS: Prev amount")

# Build organization list ******************************************************

print("Build organization...")

s_sql = "CREATE TABLE X000_Organization AS " + """
SELECT
  CA_ORG_T.FIN_COA_CD,
  CA_ORG_T.ORG_CD,
  CA_ORG_T.ORG_TYP_CD,
  CA_ORG_TYPE_T.ORG_TYP_NM,
  CA_ORG_T.ORG_NM,
  CA_ORG_T.ORG_BEGIN_DT,
  CA_ORG_T.ORG_END_DT,
  CA_ORG_T.OBJ_ID,
  CA_ORG_T.VER_NBR,
  CA_ORG_T.ORG_MGR_UNVL_ID,
  CA_ORG_T.RC_CD,
  CA_ORG_T.ORG_PHYS_CMP_CD,
  CA_ORG_T.ORG_DFLT_ACCT_NBR,
  CA_ORG_T.ORG_LN1_ADDR,
  CA_ORG_T.ORG_LN2_ADDR,
  CA_ORG_T.ORG_CITY_NM,
  CA_ORG_T.ORG_STATE_CD,
  CA_ORG_T.ORG_ZIP_CD,
  CA_ORG_T.ORG_CNTRY_CD,
  CA_ORG_T.RPTS_TO_FIN_COA_CD,
  CA_ORG_T.RPTS_TO_ORG_CD,
  CA_ORG_T.ORG_ACTIVE_CD,
  CA_ORG_T.ORG_IN_FP_CD,
  CA_ORG_T.ORG_PLNT_ACCT_NBR,
  CA_ORG_T.CMP_PLNT_ACCT_NBR,
  CA_ORG_T.ORG_PLNT_COA_CD,
  CA_ORG_T.CMP_PLNT_COA_CD,
  CA_ORG_T.ORG_LVL
FROM
  CA_ORG_T
  LEFT JOIN CA_ORG_TYPE_T ON CA_ORG_TYPE_T.ORG_TYP_CD = CA_ORG_T.ORG_TYP_CD
ORDER BY
  CA_ORG_T.FIN_COA_CD,
  CA_ORG_T.ORG_LVL,
  CA_ORG_TYPE_T.ORG_TYP_NM,
  CA_ORG_T.ORG_NM,
  CA_ORG_T.ORG_BEGIN_DT,
  CA_ORG_T.ORG_END_DT
"""
so_curs.execute("DROP TABLE IF EXISTS X000_Organization")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: Organization list")

# Build organization structure *************************************************

print("Build organization structure...")

s_sql = "CREATE TABLE X000_Organization_struct AS " + """
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
  X000_Organization1.ORG_LVL AS ORG_LVL1,
  X000_Organization2.FIN_COA_CD AS FIN_COA_CD2,
  X000_Organization2.ORG_CD AS ORG_CD2,
  X000_Organization2.ORG_TYP_CD AS ORG_TYP_CD2,
  X000_Organization2.ORG_TYP_NM AS ORG_TYP_NM2,
  X000_Organization2.ORG_NM AS ORG_NM2,
  X000_Organization2.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID2,
  X000_Organization2.ORG_LVL AS ORG_LVL2,
  X000_Organization3.FIN_COA_CD AS FIN_COA_CD3,
  X000_Organization3.ORG_CD AS ORG_CD3,
  X000_Organization3.ORG_TYP_CD AS ORG_TYP_CD3,
  X000_Organization3.ORG_TYP_NM AS ORG_TYP_NM3,
  X000_Organization3.ORG_NM AS ORG_NM3,
  X000_Organization3.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID3,
  X000_Organization3.ORG_LVL AS ORG_LVL3,
  X000_Organization4.FIN_COA_CD AS FIN_COA_CD4,
  X000_Organization4.ORG_CD AS ORG_CD4,
  X000_Organization4.ORG_TYP_CD AS ORG_TYP_CD4,
  X000_Organization4.ORG_TYP_NM AS ORG_TYP_NM4,
  X000_Organization4.ORG_NM AS ORG_NM4,
  X000_Organization4.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID4,
  X000_Organization4.ORG_LVL AS ORG_LVL4,
  X000_Organization5.FIN_COA_CD AS FIN_COA_CD5,
  X000_Organization5.ORG_CD AS ORG_CD5,
  X000_Organization5.ORG_TYP_CD AS ORG_TYP_CD5,
  X000_Organization5.ORG_TYP_NM AS ORG_TYP_NM5,
  X000_Organization5.ORG_NM AS ORG_NM5,
  X000_Organization5.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID5,
  X000_Organization5.ORG_LVL AS ORG_LVL5,
  X000_Organization6.FIN_COA_CD AS FIN_COA_CD6,
  X000_Organization6.ORG_CD AS ORG_CD6,
  X000_Organization6.ORG_TYP_CD AS ORG_TYP_CD6,
  X000_Organization6.ORG_TYP_NM AS ORG_TYP_NM6,
  X000_Organization6.ORG_NM AS ORG_NM6,
  X000_Organization6.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID6,
  X000_Organization6.ORG_LVL AS ORG_LVL6,
  X000_Organization7.FIN_COA_CD AS FIN_COA_CD7,
  X000_Organization7.ORG_CD AS ORG_CD7,
  X000_Organization7.ORG_TYP_CD AS ORG_TYP_CD7,
  X000_Organization7.ORG_TYP_NM AS ORG_TYP_NM7,
  X000_Organization7.ORG_NM AS ORG_NM7,
  X000_Organization7.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID7,
  X000_Organization7.ORG_LVL AS ORG_LVL7
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
  X000_Organization.ORG_LVL,
  X000_Organization.ORG_NM
"""
so_curs.execute("DROP TABLE IF EXISTS X000_Organization_struct")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: Organization structure")

# Build account list ***********************************************************

print("Build account list...")

s_sql = "CREATE TABLE X000_Account AS " + """
SELECT
  CA_ACCOUNT_T.FIN_COA_CD,
  CA_ACCOUNT_T.ACCOUNT_NBR,
  CA_ACCOUNT_TYPE_T.ACCT_TYP_NM,
  X000_Organization.ORG_NM,
  CA_ACCOUNT_T.ACCOUNT_NM,
  CA_ACCOUNT_T.ACCT_FSC_OFC_UID,
  CA_ACCOUNT_T.ACCT_SPVSR_UNVL_ID,
  CA_ACCOUNT_T.ACCT_MGR_UNVL_ID,
  CA_ACCOUNT_T.ORG_CD,
  CA_ACCOUNT_T.ACCT_TYP_CD,
  CA_ACCOUNT_T.ACCT_PHYS_CMP_CD,
  CA_ACCOUNT_T.ACCT_FRNG_BNFT_CD,
  CA_ACCOUNT_T.FIN_HGH_ED_FUNC_CD,
  CA_ACCOUNT_T.SUB_FUND_GRP_CD,
  CA_ACCOUNT_T.ACCT_RSTRC_STAT_CD,
  CA_ACCOUNT_T.ACCT_RSTRC_STAT_DT,
  CA_ACCOUNT_T.ACCT_CITY_NM,
  CA_ACCOUNT_T.ACCT_STATE_CD,
  CA_ACCOUNT_T.ACCT_STREET_ADDR,
  CA_ACCOUNT_T.ACCT_ZIP_CD,
  CA_ACCOUNT_T.RPTS_TO_FIN_COA_CD,
  CA_ACCOUNT_T.RPTS_TO_ACCT_NBR,
  CA_ACCOUNT_T.ACCT_CREATE_DT,
  CA_ACCOUNT_T.ACCT_EFFECT_DT,
  CA_ACCOUNT_T.ACCT_EXPIRATION_DT,
  CA_ACCOUNT_T.CONT_FIN_COA_CD,
  CA_ACCOUNT_T.CONT_ACCOUNT_NBR,
  CA_ACCOUNT_T.ACCT_CLOSED_IND,
  CA_ACCOUNT_T.OBJ_ID,
  CA_ACCOUNT_T.VER_NBR
FROM
  CA_ACCOUNT_T
  LEFT JOIN X000_Organization ON X000_Organization.FIN_COA_CD = CA_ACCOUNT_T.FIN_COA_CD AND X000_Organization.ORG_CD =
    CA_ACCOUNT_T.ORG_CD
  LEFT JOIN CA_ACCOUNT_TYPE_T ON CA_ACCOUNT_TYPE_T.ACCT_TYP_CD = CA_ACCOUNT_T.ACCT_TYP_CD
"""
so_curs.execute("DROP TABLE IF EXISTS X000_Account")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: Account list")

# Build current transaction list ***********************************************

print("Build current yr transaction list...")

s_sql = "CREATE VIEW X000_GL_trans_curr AS " + """
SELECT
  GL_ENTRY_T_CURR.UNIV_FISCAL_YR,
  GL_ENTRY_T_CURR.UNIV_FISCAL_PRD_CD,
  GL_ENTRY_T_CURR.CALC_COST_STRING,
  X000_Account.ORG_NM,
  X000_Account.ACCOUNT_NM,
  CA_OBJECT_CODE_T.FIN_OBJ_CD_NM,
  GL_ENTRY_T_CURR.TRANSACTION_DT,
  GL_ENTRY_T_CURR.FDOC_NBR,
  GL_ENTRY_T_CURR.CALC_AMOUNT,
  GL_ENTRY_T_CURR.TRN_LDGR_ENTR_DESC,
  X000_Account.ACCT_TYP_NM,
  GL_ENTRY_T_CURR.TRN_POST_DT,
  GL_ENTRY_T_CURR."TIMESTAMP",
  GL_ENTRY_T_CURR.FIN_COA_CD,
  GL_ENTRY_T_CURR.ACCOUNT_NBR,
  GL_ENTRY_T_CURR.FIN_OBJECT_CD,
  GL_ENTRY_T_CURR.FIN_BALANCE_TYP_CD,
  GL_ENTRY_T_CURR.FIN_OBJ_TYP_CD,
  GL_ENTRY_T_CURR.FDOC_TYP_CD,
  GL_ENTRY_T_CURR.FS_ORIGIN_CD,
  FS_ORIGIN_CODE_T.FS_DATABASE_DESC,
  GL_ENTRY_T_CURR.TRN_ENTR_SEQ_NBR,
  GL_ENTRY_T_CURR.FDOC_REF_TYP_CD,
  GL_ENTRY_T_CURR.FS_REF_ORIGIN_CD,
  GL_ENTRY_T_CURR.FDOC_REF_NBR,
  GL_ENTRY_T_CURR.FDOC_REVERSAL_DT,
  GL_ENTRY_T_CURR.TRN_ENCUM_UPDT_CD
FROM
  GL_ENTRY_T_CURR
  LEFT JOIN X000_Account ON X000_Account.FIN_COA_CD = GL_ENTRY_T_CURR.FIN_COA_CD AND X000_Account.ACCOUNT_NBR =
    GL_ENTRY_T_CURR.ACCOUNT_NBR
  LEFT JOIN CA_OBJECT_CODE_T ON CA_OBJECT_CODE_T.UNIV_FISCAL_YR = GL_ENTRY_T_CURR.UNIV_FISCAL_YR AND
    CA_OBJECT_CODE_T.FIN_COA_CD = GL_ENTRY_T_CURR.FIN_COA_CD AND CA_OBJECT_CODE_T.FIN_OBJECT_CD =
    GL_ENTRY_T_CURR.FIN_OBJECT_CD
  LEFT JOIN FS_ORIGIN_CODE_T ON FS_ORIGIN_CODE_T.FS_ORIGIN_CD = GL_ENTRY_T_CURR.FS_ORIGIN_CD
ORDER BY
  GL_ENTRY_T_CURR.CALC_COST_STRING,
  GL_ENTRY_T_CURR.UNIV_FISCAL_PRD_CD
"""
so_curs.execute("DROP VIEW IF EXISTS X000_GL_trans_curr")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD VIEW: Current yr transaction list")

# Build current transaction list ***********************************************

print("Build previous yr transaction list...")

s_sql = "CREATE VIEW X000_GL_trans_prev AS " + """
SELECT
  GL_ENTRY_T_PREV.UNIV_FISCAL_YR,
  GL_ENTRY_T_PREV.UNIV_FISCAL_PRD_CD,
  GL_ENTRY_T_PREV.CALC_COST_STRING,
  X000_Account.ORG_NM,
  X000_Account.ACCOUNT_NM,
  CA_OBJECT_CODE_T.FIN_OBJ_CD_NM,
  GL_ENTRY_T_PREV.TRANSACTION_DT,
  GL_ENTRY_T_PREV.FDOC_NBR,
  GL_ENTRY_T_PREV.CALC_AMOUNT,
  GL_ENTRY_T_PREV.TRN_LDGR_ENTR_DESC,
  X000_Account.ACCT_TYP_NM,
  GL_ENTRY_T_PREV.TRN_POST_DT,
  GL_ENTRY_T_PREV."TIMESTAMP",
  GL_ENTRY_T_PREV.FIN_COA_CD,
  GL_ENTRY_T_PREV.ACCOUNT_NBR,
  GL_ENTRY_T_PREV.FIN_OBJECT_CD,
  GL_ENTRY_T_PREV.FIN_BALANCE_TYP_CD,
  GL_ENTRY_T_PREV.FIN_OBJ_TYP_CD,
  GL_ENTRY_T_PREV.FDOC_TYP_CD,
  GL_ENTRY_T_PREV.FS_ORIGIN_CD,
  FS_ORIGIN_CODE_T.FS_DATABASE_DESC,
  GL_ENTRY_T_PREV.TRN_ENTR_SEQ_NBR,
  GL_ENTRY_T_PREV.FDOC_REF_TYP_CD,
  GL_ENTRY_T_PREV.FS_REF_ORIGIN_CD,
  GL_ENTRY_T_PREV.FDOC_REF_NBR,
  GL_ENTRY_T_PREV.FDOC_REVERSAL_DT,
  GL_ENTRY_T_PREV.TRN_ENCUM_UPDT_CD
FROM
  GL_ENTRY_T_PREV
  LEFT JOIN X000_Account ON X000_Account.FIN_COA_CD = GL_ENTRY_T_PREV.FIN_COA_CD AND X000_Account.ACCOUNT_NBR =
    GL_ENTRY_T_PREV.ACCOUNT_NBR
  LEFT JOIN CA_OBJECT_CODE_T ON CA_OBJECT_CODE_T.UNIV_FISCAL_YR = GL_ENTRY_T_PREV.UNIV_FISCAL_YR AND
    CA_OBJECT_CODE_T.FIN_COA_CD = GL_ENTRY_T_PREV.FIN_COA_CD AND CA_OBJECT_CODE_T.FIN_OBJECT_CD =
    GL_ENTRY_T_PREV.FIN_OBJECT_CD
  LEFT JOIN FS_ORIGIN_CODE_T ON FS_ORIGIN_CODE_T.FS_ORIGIN_CD = GL_ENTRY_T_PREV.FS_ORIGIN_CD
ORDER BY
  GL_ENTRY_T_PREV.CALC_COST_STRING,
  GL_ENTRY_T_PREV.UNIV_FISCAL_PRD_CD
"""
so_curs.execute("DROP VIEW IF EXISTS X000_GL_trans_prev")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD VIEW: Previous yr transaction list")

# Close the table connection ***************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("---------")
funcfile.writelog("COMPLETED")
