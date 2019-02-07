"""
Script to build GL Student debtor control account reports
Created on: 13 Mar 2018
"""

# Import python modules
import csv
import datetime
import sqlite3
import sys

# Add own module path
sys.path.append('S:/_my_modules')

# Import own modules
import funcdate
import funccsv
import funcfile
import funcmail
import funcsys
import funcmysql

# Open the script log file ******************************************************

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C200_REPORT_STUDDEB_RECON_DEV")
funcfile.writelog("-------------------------------------")
print("-------------------------")
print("C200_REPORT_STUDDEB_RECON")
print("-------------------------")
ilog_severity = 1

# Declare variables
so_path = "W:/Kfs_vss_studdeb/" #Source database path
re_path = "R:/Debtorstud/" #Results
ed_path = "S:/_external_data/" #External data
so_file = "Kfs_vss_studdeb.sqlite" #Source database
s_sql = "" #SQL statements
l_mail = True
l_export = True

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN DATABASE: Kfs_vss_studdeb")

#so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
#funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")

# Open the MYSQL DESTINATION table
s_database = "Web_ia_nwu"
ms_cnxn = funcmysql.mysql_open(s_database)
ms_curs = ms_cnxn.cursor()
funcfile.writelog("%t OPEN MYSQL DATABASE: " + s_database)

# Development script ***********************************************************






# Build a transaction code language list ***************************************
print("Build a transaction code language list...")
sr_file = "X002aa_vss_tranlist_langsumm"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X002aa_vss_tranlist.TRANSCODE,
  X002aa_vss_tranlist.TEMP_DESC_A AS DESC_AFR,
  X002aa_vss_tranlist.TEMP_DESC_E AS DESC_ENG,
  Sum(X002aa_vss_tranlist.AMOUNT) AS SUM,
  Count(X002aa_vss_tranlist.CAMPUS) AS COUNT
FROM
  X002aa_vss_tranlist
WHERE
  X002aa_vss_tranlist.TEMP_DESC_A IS NOT NULL
GROUP BY
  X002aa_vss_tranlist.TRANSCODE,
  X002aa_vss_tranlist.TEMP_DESC_A,
  X002aa_vss_tranlist.TEMP_DESC_E
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# Join the transaction languages to the gl transaction file ********************
print("Join the gl transactions with transaction code languages...")
sr_file = "X001aa_gl_tranlist_lang"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X001aa_gl_tranlist.*,
  X002aa_vss_tranlist_langsumm.DESC_ENG,
  '' AS DESC_GL
FROM
  X001aa_gl_tranlist
  LEFT JOIN X002aa_vss_tranlist_langsumm ON X002aa_vss_tranlist_langsumm.DESC_AFR = X001aa_gl_tranlist.DESCRIPTION
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Build final gl description
print("Add column gl business unit id...")
so_curs.execute("UPDATE X001aa_gl_tranlist_lang " + """
                SET DESC_GL = 
                CASE
                   WHEN DESC_ENG <> '' THEN DESC_ENG
                   ELSE DESCRIPTION
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: Description gl (DESC_GL)")

# Build sort rename column gl transaction file *****************************
print("Build and sort gl transaction file...")
sr_file = "X001ab_gl_transort"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  VSS.X000_Orgunitinstance.ORGUNIT_NAME AS CAMPUS,
  VSS.X000_Orgunitinstance.FORGUNITNUMBER AS CAMPUS_VSS,
  X001aa_gl_tranlist_lang.UNIV_FISCAL_YR AS YEAR,
  X001aa_gl_tranlist_lang.MONTH,
  X001aa_gl_tranlist_lang.CALC_COST_STRING AS COST_STRING,
  X001aa_gl_tranlist_lang.TRANSACTION_DT AS DATE_TRAN,
  X001aa_gl_tranlist_lang.FDOC_NBR AS EDOC,
  Round(X001aa_gl_tranlist_lang.CALC_AMOUNT,2) AS AMOUNT,
  X001aa_gl_tranlist_lang.TRN_LDGR_ENTR_DESC AS DESC_FULL,
  X001aa_gl_tranlist_lang.DESC_GL AS DESC_VSS,
  X001aa_gl_tranlist_lang.BURSARY_CODE AS BURSARY,
  X001aa_gl_tranlist_lang.STUDENT,
  X001aa_gl_tranlist_lang.FS_ORIGIN_CD AS ORIGIN_CODE,
  X001aa_gl_tranlist_lang.FS_DATABASE_DESC AS ORIGIN,
  X001aa_gl_tranlist_lang.ORG_NM AS ORG_NAME,
  X001aa_gl_tranlist_lang.ACCOUNT_NM AS ACC_NAME,
  X001aa_gl_tranlist_lang.FIN_OBJ_CD_NM AS OBJ_NAME,
  X001aa_gl_tranlist_lang.ACCT_TYP_NM AS ACC_TYPE,
  X001aa_gl_tranlist_lang.TRN_POST_DT AS DATE_POST,
  X001aa_gl_tranlist_lang."TIMESTAMP" AS TIME_POST,
  X001aa_gl_tranlist_lang.FIN_COA_CD AS ORG,
  X001aa_gl_tranlist_lang.ACCOUNT_NBR AS ACC,
  X001aa_gl_tranlist_lang.FIN_OBJECT_CD AS OBJ,
  X001aa_gl_tranlist_lang.FIN_BALANCE_TYP_CD AS BAL_TYPE,
  X001aa_gl_tranlist_lang.FIN_OBJ_TYP_CD AS OBJ_TYPE,
  X001aa_gl_tranlist_lang.FDOC_TYP_CD AS DOC_TYPE,
  X001aa_gl_tranlist_lang.TRN_ENTR_SEQ_NBR,
  X001aa_gl_tranlist_lang.FDOC_REF_TYP_CD,
  X001aa_gl_tranlist_lang.FS_REF_ORIGIN_CD,
  X001aa_gl_tranlist_lang.FDOC_REF_NBR,
  X001aa_gl_tranlist_lang.FDOC_REVERSAL_DT,
  X001aa_gl_tranlist_lang.TRN_ENCUM_UPDT_CD
FROM
  X001aa_gl_tranlist_lang
  LEFT JOIN VSS.X000_Orgunitinstance ON VSS.X000_Orgunitinstance.KBUSINESSENTITYID = X001aa_gl_tranlist_lang.BUSINESSENTITYID
ORDER BY
  VSS.X000_Orgunitinstance.ORGUNIT_NAME,
  TIME_POST
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)    
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)








# Close the table connection ***************************************************
so_conn.commit()
so_conn.close()
ms_cnxn.commit()
ms_cnxn.close()

# Close the log writer *********************************************************
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C200_REPORT_STUDDEB_RECON_DEV")
