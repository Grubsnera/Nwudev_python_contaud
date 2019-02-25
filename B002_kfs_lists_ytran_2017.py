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
sys.path.append('S:\\_my_modules')

# Import own modules
import funcdate
import funccsv
import funcfile

# Open the script log file ******************************************************

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B002_KFS_LISTS")
funcfile.writelog("----------------------")
print("--------------")
print("B002_KFS_LISTS")
print("--------------")
ilog_severity = 1

# Declare variables
so_path = "W:/Kfs/" #Source database path
so_file = "Kfs.sqlite" #Source database
s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("%t OPEN DATABASE: KFS")

"""*****************************************************************************
BEGIN
*****************************************************************************"""

# ADD CALCULATED FIELDS TO PERIOD TRANSACTION LIST *****************************
print("Calculate period gl columns...")

# Calc combined cost string
if "CALC_COST_STRING" not in funccsv.get_colnames_sqlite(so_curs,"GL_ENTRY_T_2017"):
    so_curs.execute("ALTER TABLE GL_ENTRY_T_2017 ADD COLUMN CALC_COST_STRING TEXT;")
    so_curs.execute("UPDATE GL_ENTRY_T_2017 SET CALC_COST_STRING = Trim(FIN_COA_CD) || '.' || Trim(ACCOUNT_NBR) || '.' || Trim(FIN_OBJECT_CD);")
    so_conn.commit()
    funcfile.writelog("%t CALC COLUMNS: Combined cost string")

# Calc amount
if "CALC_AMOUNT" not in funccsv.get_colnames_sqlite(so_curs,"GL_ENTRY_T_2017"):
    so_curs.execute("ALTER TABLE GL_ENTRY_T_2017 ADD COLUMN CALC_AMOUNT REAL;")
    so_curs.execute("UPDATE GL_ENTRY_T_2017 " + """
                    SET CALC_AMOUNT = 
                    CASE
                       WHEN TRN_DEBIT_CRDT_CD = "C" THEN TRN_LDGR_ENTR_AMT * -1
                       ELSE TRN_LDGR_ENTR_AMT
                    END
                    ;""")
    so_conn.commit()
    funcfile.writelog("%t CALC COLUMNS: Amount")

# BUILD PERIOD TRANSACTION LIST ************************************************
print("Build period transaction list...")
s_sql = "CREATE VIEW X000_GL_trans_2017 AS " + """
SELECT
  GL_ENTRY_T_2017.UNIV_FISCAL_YR,
  GL_ENTRY_T_2017.UNIV_FISCAL_PRD_CD,
  GL_ENTRY_T_2017.CALC_COST_STRING,
  X000_Account.ORG_NM,
  X000_Account.ACCOUNT_NM,
  CA_OBJECT_CODE_T.FIN_OBJ_CD_NM,
  GL_ENTRY_T_2017.TRANSACTION_DT,
  GL_ENTRY_T_2017.FDOC_NBR,
  GL_ENTRY_T_2017.CALC_AMOUNT,
  GL_ENTRY_T_2017.TRN_LDGR_ENTR_DESC,
  X000_Account.ACCT_TYP_NM,
  GL_ENTRY_T_2017.TRN_POST_DT,
  GL_ENTRY_T_2017."TIMESTAMP",
  GL_ENTRY_T_2017.FIN_COA_CD,
  GL_ENTRY_T_2017.ACCOUNT_NBR,
  GL_ENTRY_T_2017.FIN_OBJECT_CD,
  GL_ENTRY_T_2017.FIN_BALANCE_TYP_CD,
  GL_ENTRY_T_2017.FIN_OBJ_TYP_CD,
  GL_ENTRY_T_2017.FDOC_TYP_CD,
  GL_ENTRY_T_2017.FS_ORIGIN_CD,
  FS_ORIGIN_CODE_T.FS_DATABASE_DESC,
  GL_ENTRY_T_2017.TRN_ENTR_SEQ_NBR,
  GL_ENTRY_T_2017.FDOC_REF_TYP_CD,
  GL_ENTRY_T_2017.FS_REF_ORIGIN_CD,
  GL_ENTRY_T_2017.FDOC_REF_NBR,
  GL_ENTRY_T_2017.FDOC_REVERSAL_DT,
  GL_ENTRY_T_2017.TRN_ENCUM_UPDT_CD
FROM
  GL_ENTRY_T_2017
  LEFT JOIN X000_Account ON X000_Account.FIN_COA_CD = GL_ENTRY_T_2017.FIN_COA_CD AND X000_Account.ACCOUNT_NBR =
    GL_ENTRY_T_2017.ACCOUNT_NBR
  LEFT JOIN CA_OBJECT_CODE_T ON CA_OBJECT_CODE_T.UNIV_FISCAL_YR = GL_ENTRY_T_2017.UNIV_FISCAL_YR AND
    CA_OBJECT_CODE_T.FIN_COA_CD = GL_ENTRY_T_2017.FIN_COA_CD AND CA_OBJECT_CODE_T.FIN_OBJECT_CD =
    GL_ENTRY_T_2017.FIN_OBJECT_CD
  LEFT JOIN FS_ORIGIN_CODE_T ON FS_ORIGIN_CODE_T.FS_ORIGIN_CD = GL_ENTRY_T_2017.FS_ORIGIN_CD
ORDER BY
  GL_ENTRY_T_2017.CALC_COST_STRING,
  GL_ENTRY_T_2017.UNIV_FISCAL_PRD_CD
"""
so_curs.execute("DROP VIEW IF EXISTS X000_GL_trans_2017")
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: Period transaction list")

"""*****************************************************************************
END
*****************************************************************************"""

# Close the table connection ***************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("-------------------------")
funcfile.writelog("COMPLETED: B002_KFS_LISTS")
