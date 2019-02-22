""" C200_REPORT_STUDDEB_RECON **************************************************
***
*** Script to compare VSS and GL student transactions
***
*** Albert J van Rensburg (21162395)
*** 26 Jun 2018
***
*****************************************************************************"""

""" CONTENTS *******************************************************************
OBTAIN & PREPARE VSS TRANSACTIONS
LIST GL TRANSACTIONS
LIST VSS TRANSACTIONS
JOIN VSS & GL MONTHLY TOTALS
JOIN VSS & GL TRANSACTIONS
TEST MATCHED TRANSACTION TYPES
TEST TRANSACTION TYPES IN VSS BUT NOT IN GL
TEST TRANSACTION TYPES IN GL BUT NOT IN VSS
BURSARY VSS GL RECON
TEST BURSARY INGL NOVSS
TEST BURSARY INVSS NOGL
TEST BURSARY POST TO DIFF CAMPUS IN GL
*****************************************************************************"""

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
funcfile.writelog("SCRIPT: C200_REPORT_STUDDEB_RECON")
funcfile.writelog("---------------------------------")
print("-------------------------")
print("C200_REPORT_STUDDEB_RECON")
print("-------------------------")
ilog_severity = 1

# Declare variables
so_path = "W:/Kfs_vss_studdeb/" #Source database path
re_path = "R:/Debtorstud/" #Results
ed_path = "S:/_external_data/" #External data
so_file = "Kfs_vss_studdeb_prev.sqlite" #Source database
s_sql = "" #SQL statements
l_mail = True
l_export = True

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN DATABASE: Kfs_vss_studdeb_prev")

# Attach data sources
so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")


"""*************************************************************************


OBTAIN & PREPARE VSS TRANSACTIONS


*************************************************************************"""

print("--- PREPARE VSS TRANSACTIONS ---")
funcfile.writelog("PREPARE VSS TRANSACTIONS")

# Extract vss transactions from VSS.SQLITE *********************************
print("Import vss transactions from VSS.SQLITE...")
sr_file = "X002aa_vss_tranlist"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  *,
  '' AS CAMPUS,
  '' AS MONTH,
  0.00 AS AMOUNT_DT,
  0.00 AS AMOUNT_CR,
  '' AS TEMP_DESC_A,
  '' AS TEMP_DESC_E
FROM
  VSS.X010_Studytrans_prev
WHERE
  TRANSCODE <> ''
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Add column vss campus name
print("Add column vss campus name...")
#so_curs.execute("ALTER TABLE X002aa_vss_tranlist ADD COLUMN CAMPUS TEXT;")
so_curs.execute("UPDATE X002aa_vss_tranlist " + """
                SET CAMPUS = 
                CASE
                   WHEN FDEBTCOLLECTIONSITE = '-9' THEN 'Mafikeng'
                   WHEN FDEBTCOLLECTIONSITE = '-2' THEN 'Vaal Triangle'
                   ELSE 'Potchefstroom'
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: Vss campus name")
# Add column vss transaction month
print("Add column vss transaction month...")
#so_curs.execute("ALTER TABLE X002aa_vss_tranlist ADD COLUMN MONTH TEXT;")
s_sql = "UPDATE X002aa_vss_tranlist " + """
         SET MONTH = 
         CASE
           WHEN SUBSTR(TRANSDATE,6,5)='01-01' AND INSTR('001z031z061',TRANSCODE)>0 THEN '00'
           WHEN SUBSTR(TRANSDATE,1,4)>strftime('%Y','%PYEAREND%') THEN SUBSTR(POSTDATEDTRANSDATE,6,2)
         ELSE SUBSTR(TRANSDATE,6,2)
         END
         ;"""
s_sql = s_sql.replace("%PYEAREND%",funcdate.prev_yearend())
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: Vss transaction month")
# Add column vss debit amount
print("Add column vss dt amount...")
#so_curs.execute("ALTER TABLE X002aa_vss_tranlist ADD COLUMN AMOUNT_DT REAL;")
so_curs.execute("UPDATE X002aa_vss_tranlist " + """
                SET AMOUNT_DT = 
                CASE
                   WHEN AMOUNT > 0 THEN AMOUNT
                   ELSE 0
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: Vss debit amount")
# Add column vss credit amount
print("Add column vss ct amount...")
#so_curs.execute("ALTER TABLE X002aa_vss_tranlist ADD COLUMN AMOUNT_CR REAL;")
so_curs.execute("UPDATE X002aa_vss_tranlist " + """
                SET AMOUNT_CR = 
                CASE
                   WHEN AMOUNT < 0 THEN AMOUNT
                   ELSE 0
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: Vss credit amount")
# Temp description - Remove characters from description ********************
print("Add column vss description in afrikaans...")
#so_curs.execute("ALTER TABLE X002aa_vss_tranlist ADD COLUMN TEMP_DESC_A TEXT;")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(UPPER(TRIM(DESCRIPTION_A)),'0','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'1','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'2','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'3','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'4','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'5','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'6','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'7','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'8','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'9','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'(','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,')','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'.','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'-','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,':','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'/','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'&','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'ë','E');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'?','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,' ','');")
funcfile.writelog("%t CALC COLUMN: Temp afr description")
# Temp description - Remove characters from description ********************
print("Add column vss description in english...")
#so_curs.execute("ALTER TABLE X002aa_vss_tranlist ADD COLUMN TEMP_DESC_E TEXT;")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(UPPER(TRIM(DESCRIPTION_E)),'0','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,'1','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,'2','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,'3','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,'4','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,'5','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,'6','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,'7','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,'8','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,'9','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,'(','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,')','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,'.','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,'-','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,':','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,'/','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,'&','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,'ë','E');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,'?','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,' ','');")
so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,'MISCELANEOUSFEES','MISCELLANEOUSFEES');")
funcfile.writelog("%t CALC COLUMN: Temp eng description")

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

"""*************************************************************************
***
*** LIST GL TRANSACTIONS
***
***   Import GL transactions from KFS.SQLITE
***   Add required fields
***   X001da Identify new accounts linked to a campus
***
*************************************************************************"""

print("--- PREPARE GL TRANSACTIONS ---")
funcfile.writelog("PREPARE GL TRANSACTIONS")

# Import gl transactions **************************************************
print("Import gl transactions from KFS.SQLITE...")
sr_file = "X001aa_gl_tranlist"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  *
FROM
  KFS.X000_GL_trans_prev
WHERE
  (KFS.X000_GL_trans_prev.FIN_OBJECT_CD = '7551') OR
  (KFS.X000_GL_trans_prev.FIN_OBJECT_CD = '7552') OR
  (KFS.X000_GL_trans_prev.FIN_OBJECT_CD = '7553')
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# Add columns **************************************************************
# Calc campus name                      
print("Add column gl business unit id...")
so_curs.execute("ALTER TABLE X001aa_gl_tranlist ADD COLUMN BUSINESSENTITYID INT;")
so_curs.execute("UPDATE X001aa_gl_tranlist " + """
                SET BUSINESSENTITYID = 
                CASE
                   WHEN ACCOUNT_NBR = '1G02018' THEN 1
                   WHEN ACCOUNT_NBR = '1G01772' THEN 2
                   WHEN ACCOUNT_NBR = '1G01804' THEN 9
                   WHEN ACCOUNT_NBR = '1G02012' THEN 9
                   ELSE 0
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: Campus code")
#                       WHEN ACCOUNT_NBR = '1G01804' THEN 9    
# Calc month
print("Add column gl transaction month...")    
so_curs.execute("ALTER TABLE X001aa_gl_tranlist ADD COLUMN MONTH TEXT;")
so_curs.execute("UPDATE X001aa_gl_tranlist " + """
                SET MONTH = 
                CASE
                   WHEN UNIV_FISCAL_PRD_CD = 'BB' THEN '00'
                   WHEN UNIV_FISCAL_PRD_CD = '13' THEN '12'
                   WHEN UNIV_FISCAL_PRD_CD = '14' THEN '12'
                   ELSE UNIV_FISCAL_PRD_CD
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: Transaction month")
# Calc bursary code
print("Add column gl bursary code...")
so_curs.execute("ALTER TABLE X001aa_gl_tranlist ADD COLUMN BURSARY_CODE TEXT;")
so_curs.execute("UPDATE X001aa_gl_tranlist " + """
                SET BURSARY_CODE = 
                CASE
                   WHEN FS_ORIGIN_CD = '01' AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),'REVERSE COLL') > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"BEURS:")+6,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STUDENT:")-8)
                   WHEN FS_ORIGIN_CD = '10' AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),'BEURS:') > 0 AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STATUS:U") > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"BEURS:")+6,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STUDENT:")-8)
                   WHEN FS_ORIGIN_CD = '10' AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),'BURSARY:') > 0 AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STATUS:U") > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"BURSARY:")+8,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STUDENT:")-10)
                   WHEN FS_ORIGIN_CD = '10' AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),'BURSARY :') > 0 AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STATUS : K") > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"BURSARY:")+11,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"LEARNER :")-12)                   ELSE ""
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMNS: Bursary code")
# Calc student number
print("Add column gl student number...")
so_curs.execute("ALTER TABLE X001aa_gl_tranlist ADD COLUMN STUDENT TEXT;")
so_curs.execute("UPDATE X001aa_gl_tranlist " + """
                SET STUDENT = 
                CASE
                   WHEN FS_ORIGIN_CD = '01' AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"REVERSE COLL") > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STUDENT:")+8,8)
                   WHEN FS_ORIGIN_CD = '10' AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STATUS:U") > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STUDENT:")+8,8)
                   WHEN FS_ORIGIN_CD = '10' AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STATUS : K") > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"LEARNER :")+10,8)
                   ELSE ""
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMNS: Student number")
# Temp description - Remove characters from description
print("Add column gl temp description column...")
so_curs.execute("ALTER TABLE X001aa_gl_tranlist ADD COLUMN TEMP TEXT;")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TRN_LDGR_ENTR_DESC,'0','');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'1','');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'2','');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'3','');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'4','');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'5','');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'6','');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'7','');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'8','');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'9','');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'(','');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,')','');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'.','');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'-','');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,':','');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'/','');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'&','');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'ë','E');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'?','E');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,' ','');")
so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'MISCELANEOUSFEES','MISCELLANEOUSFEES');")
funcfile.writelog("%t ADD COLUMNS: Temp description")
# Calc transaction description
print("Add column gl description link...")
so_curs.execute("ALTER TABLE X001aa_gl_tranlist ADD COLUMN DESCRIPTION TEXT;")
so_curs.execute("UPDATE X001aa_gl_tranlist " + """
                SET DESCRIPTION = 
                CASE
                   WHEN FS_ORIGIN_CD = '01' AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"REVERSE COLL") > 0 THEN "BEURSKANSELLASIEKLASGELDE"                   
                   WHEN FS_ORIGIN_CD = '10' AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"LEARNER :") > 0 THEN "BEURSKANSELLASIEKLASGELDE"
                   WHEN FS_ORIGIN_CD = '10' THEN "BEURSEENLENINGSKLASGELDE"
                   WHEN FS_ORIGIN_CD = '11' THEN UPPER(TRIM(TEMP))
                   WHEN MONTH = '00' THEN 'SALDOOORGEDRAKLASGELD'
                   ELSE "X "||UPPER(TRN_LDGR_ENTR_DESC)||" ORIGIN:"||UPPER(FS_DATABASE_DESC)||" EDOC:"||UPPER(FDOC_NBR)
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMNS: Description link")

# Join the transaction languages to the gl transaction file ********************
print("Join the gl transactions with transaction code languages...")
sr_file = "X001aa_gl_tranlist_lang"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT DISTINCT
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
# Export the data
print("Export gl student debtor transactions...")
sr_filet = sr_file
sx_path = re_path + funcdate.prev_year() + "/"
sx_file = "Debtor_001_gltran_"
#sx_filet = sx_file + funcdate.prev_monthendfile()
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
#funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

# Calculate gl balances ****************************************************
print("Calculate gl balances per month...")
sr_file = "X001ca_gl_balance"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X001ab_gl_transort.CAMPUS,
  Total(X001ab_gl_transort.AMOUNT) AS BALANCE
FROM
  X001ab_gl_transort
GROUP BY
  X001ab_gl_transort.CAMPUS
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# Calculate gl balances per month ******************************************
print("Calculate gl balances per month...")
sr_file = "X001cb_gl_balmonth"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X001ab_gl_transort.CAMPUS,
  X001ab_gl_transort.MONTH,
  Total(X001ab_gl_transort.AMOUNT) AS BALANCE
FROM
  X001ab_gl_transort
GROUP BY
  X001ab_gl_transort.CAMPUS,
  X001ab_gl_transort.MONTH
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# Build gl summary per transaction type ****************************************
print("Build gl summary per transaction type...")
sr_file = "X001cc_gl_summtype"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X001ab_gl_transort.CAMPUS,
  X001ab_gl_transort.MONTH,
  X001ab_gl_transort.DESC_VSS,
  Total(X001ab_gl_transort.AMOUNT) AS AMOUNT
FROM
  X001ab_gl_transort
GROUP BY
  X001ab_gl_transort.CAMPUS,
  X001ab_gl_transort.MONTH,
  X001ab_gl_transort.DESC_VSS
ORDER BY
  MONTH
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)    
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Export the data
print("Export gl student debtor summary per transaction type...")
sr_filet = sr_file
sx_path = re_path + funcdate.prev_year() + "/"
sx_file = "Debtor_001_glsummtype_"
sx_filet = sx_file + funcdate.prev_monthendfile()
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
#funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

# Determine gl post month **************************************************
print("Determine gl post month...")
gl_month = '00'
sr_file = "X001cd_gl_postmonth"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  X001cc_gl_summtype.MONTH
FROM
  X001cc_gl_summtype
WHERE
  (X001cc_gl_summtype.MONTH = '%PMONTH%' AND
  X001cc_gl_summtype.DESC_VSS = 'KWITANSIEKLASGELDE') OR
  (X001cc_gl_summtype.MONTH = '%PMONTH%' AND
  X001cc_gl_summtype.DESC_VSS = 'RECEIPTTUITIONFEES')
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = s_sql.replace("%PMONTH%",funcdate.prev_month())
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)
# Export the data
if funcsys.tablerowcount(so_curs,sr_file) > 0:
    gl_month = funcdate.prev_month()
    # Export the data
    print("Export gl post month...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.prev_year() + "/"
    sx_file = "Debtor_000_postmonth"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
    #print(gl_month)
else:
    p_month = str(int(funcdate.prev_month())-1)
    if int(p_month) < 10:
        p_month = "0" + p_month
    gl_month = p_month
    #print(gl_month)
    
# Calculate the running gl balance ********************************************
print("Calculate the running gl balance...")
sr_file = "X001ce_gl_balmonth_calc_runbal"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  a.CAMPUS,
  a.MONTH,
  a.BALANCE,
  TOTAL(b.BALANCE) RUNBAL
FROM
  X001cb_gl_balmonth a,
  X001cb_gl_balmonth b
WHERE
  (a.CAMPUS = b.CAMPUS AND
  a.MONTH >= b.MONTH)
GROUP BY
  a.CAMPUS,
  a.MONTH
ORDER BY
  a.CAMPUS,
  a.MONTH
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

#*** LIST GL TRANSACTIONS Identify new accounts not linked to a campus *********
# See Add column gl business unit id where new account must be linked to campus
print("Identify new accounts linked to a campus...")
sr_file = "X001da_test_newaccount"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X001ab_gl_transort.COST_STRING,
  X001ab_gl_transort.TIME_POST,
  X001ab_gl_transort.EDOC,
  X001ab_gl_transort.DESC_FULL,
  X001ab_gl_transort.AMOUNT,
  Trim(X001ab_gl_transort.COST_STRING) || X001ab_gl_transort.TIME_POST || Trim(X001ab_gl_transort.EDOC) AS "ROWID"
FROM
  X001ab_gl_transort
WHERE
  X001ab_gl_transort.CAMPUS IS NULL
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)    

"""*************************************************************************
***
*** LIST VSS TRANSACTIONS
***
***
*************************************************************************"""

print("--- PREPARE VSS TRANSACTIONS ---")
funcfile.writelog("PREPARE VSS TRANSACTIONS")

# Sort vss transactions **************************************************
print("Build and sort vss transactions...")
sr_file = "X002ab_vss_transort"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X002aa_vss_tranlist.FBUSENTID AS STUDENT_VSS,
  X002aa_vss_tranlist.CAMPUS AS CAMPUS_VSS,
  X002aa_vss_tranlist.TRANSCODE AS TRANSCODE_VSS,
  X002aa_vss_tranlist.MONTH AS MONTH_VSS,
  X002aa_vss_tranlist.TRANSDATE AS TRANSDATE_VSS,
  Round(X002aa_vss_tranlist.AMOUNT,2) AS AMOUNT_VSS,
  X002aa_vss_tranlist.DESCRIPTION_E,
  X002aa_vss_tranlist.DESCRIPTION_A,
  X002aa_vss_tranlist.FINAIDCODE AS BURSCODE_VSS,
  X002aa_vss_tranlist.FINAIDNAAM AS BURSNAAM_VSS,
  X002aa_vss_tranlist.TRANSDATETIME,
  X002aa_vss_tranlist.POSTDATEDTRANSDATE,
  X002aa_vss_tranlist.FSERVICESITE AS SITE_SERV_VSS,
  X002aa_vss_tranlist.FDEBTCOLLECTIONSITE AS SITE_DEBT_VSS,
  X002aa_vss_tranlist.AUDITDATETIME,
  X002aa_vss_tranlist.FORIGINSYSTEMFUNCTIONID,
  X002aa_vss_tranlist.FAUDITSYSTEMFUNCTIONID,
  X002aa_vss_tranlist.FAUDITUSERCODE,
  X002aa_vss_tranlist.FUSERBUSINESSENTITYID AS TRANUSER,
  X002aa_vss_tranlist.AMOUNT_DT AS AMOUNT_DT,
  X002aa_vss_tranlist.AMOUNT_CR AS AMOUNT_CR,
  X002aa_vss_tranlist.TEMP_DESC_A,
  X002aa_vss_tranlist.TEMP_DESC_E  
FROM
  X002aa_vss_tranlist
WHERE
  X002aa_vss_tranlist.TRANSCODE <> ''
ORDER BY
  X002aa_vss_tranlist.TRANSDATETIME
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# PREPARE A TRANSACTION FILE FOR EXPORT ****************************************
print("Prepare data for analysis...")
sr_file = "X002ac_vss_tranexpo"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X002ab_vss_transort.MONTH_VSS,
  X002ab_vss_transort.STUDENT_VSS,
  '2018' AS YEAR,
  X002ab_vss_transort.SITE_SERV_VSS,
  X002ab_vss_transort.SITE_DEBT_VSS,
  X002ab_vss_transort.TRANSCODE_VSS,
  X002ab_vss_transort.TRANSDATE_VSS,
  X002ab_vss_transort.AMOUNT_VSS,
  X002ab_vss_transort.POSTDATEDTRANSDATE,
  "Q" AS QUALIFICATION,
  "M" AS MODULE,
  "B" AS BURSARY,
  X002ab_vss_transort.DESCRIPTION_A,
  X002ab_vss_transort.DESCRIPTION_E
FROM
  X002ab_vss_transort
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)    
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Export the data
print("Export vss campus balances per transaction type...")
sr_filet = sr_file
sx_path = re_path + funcdate.prev_year() + "/"
sx_file = "Debtor_001_vsstran_"
sx_filet = sx_file + funcdate.prev_monthendfile()
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)   

# Calculate vss balances per campus ****************************************
print("Calculate vss campus balances...")
sr_file = "X002ca_vss_balance"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X002ab_vss_transort.CAMPUS_VSS,
  Total(X002ab_vss_transort.AMOUNT_DT) AS AMOUNT_DT,
  Total(X002ab_vss_transort.AMOUNT_CR) AS AMOUNT_CT,
  Total(X002ab_vss_transort.AMOUNT_VSS) AS AMOUNT
FROM
  X002ab_vss_transort
GROUP BY
  X002ab_vss_transort.CAMPUS_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)    
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# Calculate vss balances per campus per month ******************************
print("Calculate vss campus balances per month...")
sr_file = "X002cb_vss_balmonth"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X002ab_vss_transort.CAMPUS_VSS,
  X002ab_vss_transort.MONTH_VSS,
  Total(X002ab_vss_transort.AMOUNT_DT) AS AMOUNT_DT,
  Total(X002ab_vss_transort.AMOUNT_CR) AS AMOUNT_CT,
  Total(X002ab_vss_transort.AMOUNT_VSS) AS AMOUNT
FROM
  X002ab_vss_transort
GROUP BY
  X002ab_vss_transort.CAMPUS_VSS,
  X002ab_vss_transort.MONTH_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)    
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# Calculate vss balances per campus per month per transaction type *********
print("Calculate vss balances per transaction type...")
sr_file = "X002cc_vss_summtype"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X002ab_vss_transort.CAMPUS_VSS,
  X002ab_vss_transort.MONTH_VSS,
  X002ab_vss_transort.TRANSCODE_VSS,  
  X002ab_vss_transort.TEMP_DESC_E,
  Total(X002ab_vss_transort.AMOUNT_DT) AS AMOUNT_DT,
  Total(X002ab_vss_transort.AMOUNT_CR) AS AMOUNT_CT,
  Total(X002ab_vss_transort.AMOUNT_VSS) AS AMOUNT_VSS
FROM
  X002ab_vss_transort
GROUP BY
  X002ab_vss_transort.CAMPUS_VSS,
  X002ab_vss_transort.MONTH_VSS,
  X002ab_vss_transort.TEMP_DESC_E
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)    
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Export the data
print("Export vss campus balances per transaction type...")
sr_filet = sr_file
sx_path = re_path + funcdate.prev_year() + "/"
sx_file = "Debtor_002_vsssummtype_"
sx_filet = sx_file + funcdate.prev_monthendfile()
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
#funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)    

# Calculate the running vss balance ********************************************
print("Calculate the running vss balance...")
sr_file = "X002ce_vss_balmonth_calc_runbal"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  a.CAMPUS_VSS,
  a.MONTH_VSS,
  a.AMOUNT_DT,
  a.AMOUNT_CT,
  a.AMOUNT,
  TOTAL(b.AMOUNT) RUNBAL
FROM
  X002cb_vss_balmonth a,
  X002cb_vss_balmonth b
WHERE
  (a.CAMPUS_VSS = b.CAMPUS_VSS AND
  a.MONTH_VSS >= b.MONTH_VSS)
GROUP BY
  a.CAMPUS_VSS,
  a.MONTH_VSS
ORDER BY
  a.CAMPUS_VSS,
  a.MONTH_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# Sum vss student balances per campus ******************************************
print("Sum vss student balances per campus...")
sr_file = "X002da_vss_student_balance"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X002ab_vss_transort.CAMPUS_VSS AS CAMPUS,
  X002ab_vss_transort.STUDENT_VSS AS STUDENT,  
  0.00 AS BAL_DT,
  0.00 AS BAL_CT,
  Total(X002ab_vss_transort.AMOUNT_VSS) AS BALANCE
FROM
  X002ab_vss_transort
GROUP BY
  X002ab_vss_transort.STUDENT_VSS,
  X002ab_vss_transort.CAMPUS_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)    
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Add column vss debit amount
print("Add column vss dt amount...")
so_curs.execute("UPDATE X002da_vss_student_balance " + """
                SET BAL_DT = 
                CASE
                   WHEN BALANCE > 0 THEN BALANCE
                   ELSE 0.00
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: Vss debit amount")
# Add column vss credit amount
print("Add column vss ct amount...")
so_curs.execute("UPDATE X002da_vss_student_balance " + """
                SET BAL_CT = 
                CASE
                   WHEN BALANCE < 0 THEN BALANCE
                   ELSE 0.00
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: Vss credit amount")

# Sum vss balances per campus ******************************************
print("Sum vss balances per campus...")
sr_file = "X002db_vss_campus_balance"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X002da_vss_student_balance.CAMPUS,
  Total(X002da_vss_student_balance.BAL_DT) AS BAL_DT,
  Total(X002da_vss_student_balance.BAL_CT) AS BAL_CT,
  Total(X002da_vss_student_balance.BALANCE) AS BALANCE
FROM
  X002da_vss_student_balance
GROUP BY
  X002da_vss_student_balance.CAMPUS
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)    
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# EXTRACT POST DATED TRANSACTIONS **********************************************
print("Extract post dated transactions...")
sr_file = "X002fa_vss_tran_postdate"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X002ab_vss_transort.STUDENT_VSS,
  X002ab_vss_transort.CAMPUS_VSS,
  X002ab_vss_transort.TRANSCODE_VSS,
  X002ab_vss_transort.MONTH_VSS,
  X002ab_vss_transort.TRANSDATE_VSS,
  X002ab_vss_transort.TRANSDATETIME,
  X002ab_vss_transort.AMOUNT_VSS,
  X002ab_vss_transort.DESCRIPTION_E,
  X002ab_vss_transort.POSTDATEDTRANSDATE
FROM
  X002ab_vss_transort
WHERE
  X002ab_vss_transort.TRANSDATE_VSS > Date('%PYEAREND%')
;"""
s_sql = s_sql.replace("%PYEAREND%",funcdate.prev_yearend())
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# SUMM POST DATED TRANSACTIONS *************************************************
print("Summ post dated transactions...")
sr_file = "X002fb_vss_tran_postdate"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X002fa_vss_tran_postdate.CAMPUS_VSS,
  X002fa_vss_tran_postdate.TRANSCODE_VSS,
  X002fa_vss_tran_postdate.DESCRIPTION_E,
  Total(X002fa_vss_tran_postdate.AMOUNT_VSS) AS Total_AMOUNT_VSS
FROM
  X002fa_vss_tran_postdate
GROUP BY
  X002fa_vss_tran_postdate.CAMPUS_VSS,
  X002fa_vss_tran_postdate.TRANSCODE_VSS,
  X002fa_vss_tran_postdate.DESCRIPTION_E
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

"""*************************************************************************
***
*** JOIN VSS & GL MONTHLY TOTALS
***
***
***
***
***
*************************************************************************"""

# Join vss gl monthly account totals ******************************************
print("Join vss and gl monthly totals...")
sr_file = "X002ea_vss_gl_balance_month"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  UPPER(SUBSTR(X002ce_vss_balmonth_calc_runbal.CAMPUS_VSS,1,3))||TRIM(X002ce_vss_balmonth_calc_runbal.MONTH_VSS) AS ROWID,
  'NWU' AS ORG,
  X002ce_vss_balmonth_calc_runbal.CAMPUS_VSS AS CAMPUS,
  X002ce_vss_balmonth_calc_runbal.MONTH_VSS AS MONTH,
  X002ce_vss_balmonth_calc_runbal.AMOUNT_DT AS VSS_TRAN_DT,
  X002ce_vss_balmonth_calc_runbal.AMOUNT_CT AS VSS_TRAN_CT,
  X002ce_vss_balmonth_calc_runbal.AMOUNT AS VSS_TRAN,
  X002ce_vss_balmonth_calc_runbal.RUNBAL AS VSS_RUNBAL,
  X001ce_gl_balmonth_calc_runbal.BALANCE AS GL_TRAN,
  X001ce_gl_balmonth_calc_runbal.RUNBAL AS GL_RUNBAL
FROM
  X002ce_vss_balmonth_calc_runbal
  LEFT JOIN X001ce_gl_balmonth_calc_runbal ON X001ce_gl_balmonth_calc_runbal.CAMPUS = X002ce_vss_balmonth_calc_runbal.CAMPUS_VSS AND
    X001ce_gl_balmonth_calc_runbal.MONTH = X002ce_vss_balmonth_calc_runbal.MONTH_VSS
WHERE
  X002ce_vss_balmonth_calc_runbal.MONTH_VSS <= '%PMONTH%'
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
gl_month = "13"
s_sql = s_sql.replace("%PMONTH%",gl_month) 
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# Add columns ******************************************************************
# Reconciling amount
print("Add column vss gl difference...")
so_curs.execute("ALTER TABLE X002ea_vss_gl_balance_month ADD COLUMN DIFF REAL;")
so_curs.execute("UPDATE X002ea_vss_gl_balance_month SET DIFF = VSS_RUNBAL - GL_RUNBAL;")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: DIFF")

# Calculate the running recon amount *******************************************
print("Calculate the running recon amount...")
sr_file = "X002ea_vss_gl_balance_month_move"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  a.ROWID,
  a.ORG,
  a.CAMPUS,
  a.MONTH,
  a.VSS_TRAN_DT,
  a.VSS_TRAN_CT,
  a.VSS_TRAN,
  a.VSS_RUNBAL,
  a.GL_TRAN,
  a.GL_RUNBAL,
  a.DIFF,
  a.DIFF - b.DIFF AS MOVE
FROM
  X002ea_vss_gl_balance_month a,
  X002ea_vss_gl_balance_month b
WHERE
  (a.CAMPUS = b.CAMPUS AND
  a.MONTH > b.MONTH) OR
  (a.CAMPUS = b.CAMPUS AND
  b.MONTH = "00")
  
GROUP BY
  a.CAMPUS,
  a.MONTH
ORDER BY
  a.CAMPUS,
  a.MONTH
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Export the data
print("Export vss campus balances per month...")
sr_filet = sr_file
sx_path = re_path + funcdate.prev_year() + "/"
sx_file = "Debtor_000_vss_gl_summmonth_"
sx_filet = sx_file + funcdate.prev_monthendfile()
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
#funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file) 

# Import the reporting officers ************************************************
print("Import reporting officers from VSS.SQLITE...")
sr_file = "X002eb_impo_report_officer"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  VSS.X000_OWN_LOOKUPS.LOOKUP,
  VSS.X000_OWN_LOOKUPS.LOOKUP_CODE AS CAMPUS,
  VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
  PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
  PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
FROM
  VSS.X000_OWN_LOOKUPS
  LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION
WHERE
  VSS.X000_OWN_LOOKUPS.LOOKUP = 'stud_debt_recon_balance_month_officer'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Import the reporting supervisors *********************************************
print("Import reporting supervisors from VSS.SQLITE...")
sr_file = "X002ec_impo_report_supervisor"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  VSS.X000_OWN_LOOKUPS.LOOKUP,
  VSS.X000_OWN_LOOKUPS.LOOKUP_CODE AS CAMPUS,
  VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
  PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
  PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
FROM
  VSS.X000_OWN_LOOKUPS
  LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION
WHERE
  VSS.X000_OWN_LOOKUPS.LOOKUP = 'stud_debt_recon_balance_month_supervisor'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Add the reporting officer and supervisor *************************************
print("Add the reporting officer and supervisor...")
sr_file = "X002ex_vss_gl_balance_month"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  X002ea_vss_gl_balance_month_move."ROWID",
  X002ea_vss_gl_balance_month_move.ORG,
  X002ea_vss_gl_balance_month_move.CAMPUS,
  X002ea_vss_gl_balance_month_move.MONTH,
  X002ea_vss_gl_balance_month_move.VSS_TRAN_DT,
  X002ea_vss_gl_balance_month_move.VSS_TRAN_CT,
  X002ea_vss_gl_balance_month_move.VSS_TRAN,
  X002ea_vss_gl_balance_month_move.VSS_RUNBAL,
  X002ea_vss_gl_balance_month_move.GL_TRAN,
  X002ea_vss_gl_balance_month_move.GL_RUNBAL,
  X002ea_vss_gl_balance_month_move.DIFF,
  X002ea_vss_gl_balance_month_move.MOVE,
  CAMP_OFFICER.EMPLOYEE_NUMBER AS OFFICER_CAMP,
  CAMP_OFFICER.KNOWN_NAME AS OFFICER_NAME_CAMP,
  CAMP_OFFICER.EMAIL_ADDRESS AS OFFICER_MAIL_CAMP,
  ORG_OFFICER.EMPLOYEE_NUMBER AS OFFICER_ORG,
  ORG_OFFICER.KNOWN_NAME AS OFFICER_NAME_ORG,
  ORG_OFFICER.EMAIL_ADDRESS AS OFFICER_MAIL_ORG,
  CAMP_SUPERVISOR.EMPLOYEE_NUMBER AS SUPERVISOR_CAMP,
  CAMP_SUPERVISOR.KNOWN_NAME AS SUPERVISOR_NAME_CAMP,
  CAMP_SUPERVISOR.EMAIL_ADDRESS AS SUPERVISOR_MAIL_CAMP,
  ORG_SUPERVISOR.EMPLOYEE_NUMBER AS SUPERVISOR_ORG,
  ORG_SUPERVISOR.KNOWN_NAME AS SUPERVISOR_NAME_ORG,
  ORG_SUPERVISOR.EMAIL_ADDRESS AS SUPERVISOR_MAIL_ORG
FROM
  X002ea_vss_gl_balance_month_move
  LEFT JOIN X002eb_impo_report_officer CAMP_OFFICER ON CAMP_OFFICER.CAMPUS = X002ea_vss_gl_balance_month_move.CAMPUS
  LEFT JOIN X002eb_impo_report_officer ORG_OFFICER ON ORG_OFFICER.CAMPUS = X002ea_vss_gl_balance_month_move.ORG
  LEFT JOIN X002ec_impo_report_supervisor CAMP_SUPERVISOR ON CAMP_SUPERVISOR.CAMPUS = X002ea_vss_gl_balance_month_move.CAMPUS
  LEFT JOIN X002ec_impo_report_supervisor ORG_SUPERVISOR ON ORG_SUPERVISOR.CAMPUS = X002ea_vss_gl_balance_month_move.ORG
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)


"""*************************************************************************
***
*** JOIN VSS & GL TRANSACTIONS
***
***
*************************************************************************"""

print("--- JOIN VSS & GL TRANSACTIONS ---")
funcfile.writelog("JOIN VSS & GL TRANSACTIONS")

# Join the VSS and GL transaction summaries on afrikaans description *******
print("Join vss gl transaction summaries...")
sr_file = "X003aa_vss_gl_join"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT ALL
  X002cc_vss_summtype.CAMPUS_VSS,
  X002cc_vss_summtype.MONTH_VSS,
  X002cc_vss_summtype.TRANSCODE_VSS,    
  X002cc_vss_summtype.TEMP_DESC_E,
  Round(X002cc_vss_summtype.AMOUNT_VSS,2) AS AMOUNT_VSS,
  X001cc_gl_summtype.DESC_VSS,
  Round(X001cc_gl_summtype.AMOUNT,2) AS AMOUNT,
  Round(X002cc_vss_summtype.AMOUNT_VSS,2) - Round(X001cc_gl_summtype.AMOUNT,2) AS DIFF,
  '' AS MATCHED,
  '%CYEAR%-'||X002cc_vss_summtype.MONTH_VSS AS PERIOD,
  X001cc_gl_summtype.CAMPUS,
  X001cc_gl_summtype.MONTH
FROM
  X002cc_vss_summtype
  LEFT JOIN X001cc_gl_summtype ON X001cc_gl_summtype.CAMPUS = X002cc_vss_summtype.CAMPUS_VSS AND
    X001cc_gl_summtype.MONTH = X002cc_vss_summtype.MONTH_VSS AND X001cc_gl_summtype.DESC_VSS =
    X002cc_vss_summtype.TEMP_DESC_E
ORDER BY
  X002cc_vss_summtype.CAMPUS_VSS,
  X002cc_vss_summtype.MONTH_VSS,
  X002cc_vss_summtype.TRANSCODE_VSS
;"""
#WHERE
#  X002cc_vss_summtype.MONTH_VSS <= '%PMONTH%'
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
s_sql = s_sql.replace("%CYEAR%",funcdate.cur_year())
s_sql = s_sql.replace("%PMONTH%",gl_month)    
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Calc column difference
print("Calc column difference...")
so_curs.execute("UPDATE X003aa_vss_gl_join " + """
                SET DIFF = 
                CASE
                   WHEN AMOUNT IS NULL THEN Round(AMOUNT_VSS,2)
                   ELSE Round(AMOUNT_VSS,2) - Round(AMOUNT,2)
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t CALC COLUMN: Vss gl difference")
# Calc column matched
print("Calc column matched...")
so_curs.execute("UPDATE X003aa_vss_gl_join " + """
                SET MATCHED = 
                CASE
                   WHEN DESC_VSS IS NULL THEN 'X'                
                   WHEN AMOUNT_VSS <> AMOUNT THEN 'X'
                   ELSE 'C'
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t CALC COLUMN: Vss gl matched")

# Join GL and VSS on afrikaans description *********************************
print("Join gl vss transactions...")
sr_file = "X003aa_gl_vss_join"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X001cc_gl_summtype.CAMPUS AS CAMPUS_VSS,
  X001cc_gl_summtype.MONTH AS MONTH_VSS,
  'X' AS TRANSCODE_VSS,
  X002cc_vss_summtype.TEMP_DESC_E,
  X002cc_vss_summtype.AMOUNT_VSS,
  X001cc_gl_summtype.DESC_VSS,
  X001cc_gl_summtype.AMOUNT,
  Round(X001cc_gl_summtype.AMOUNT*-1,2) AS DIFF,
  'X' AS MATCHED,
  '%CYEAR%-'||X001cc_gl_summtype.MONTH AS PERIOD,  
  X001cc_gl_summtype.CAMPUS,
  X001cc_gl_summtype.MONTH
FROM
  X001cc_gl_summtype
  LEFT JOIN X002cc_vss_summtype ON X002cc_vss_summtype.CAMPUS_VSS = X001cc_gl_summtype.CAMPUS AND
    X002cc_vss_summtype.MONTH_VSS = X001cc_gl_summtype.MONTH AND X002cc_vss_summtype.TEMP_DESC_E =
    X001cc_gl_summtype.DESC_VSS
WHERE
  Length(X002cc_vss_summtype.CAMPUS_VSS) IS NULL AND
  X001cc_gl_summtype.AMOUNT <> 0
ORDER BY
  X001cc_gl_summtype.CAMPUS,
  X001cc_gl_summtype.MONTH,
  X001cc_gl_summtype.DESC_VSS
;"""
#WHERE
#  Length(X002cc_vss_summtype.CAMPUS_VSS) IS NULL AND
#  X001cc_gl_summtype.AMOUNT <> 0 AND
#  X001cc_gl_summtype.MONTH <= '%PMONTH%'
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
s_sql = s_sql.replace("%CYEAR%",funcdate.cur_year())
s_sql = s_sql.replace("%PMONTH%",gl_month)    
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# Transfer GL transactions to the VSS file *********************************
# Open the SOURCE file to obtain column headings
print("Transfer gl data to the vss table...")
funcfile.writelog("%t GET COLUMN HEADINGS: X003aa_vss_gl_join")
s_head = funcmysql.get_colnames_sqlite_text(so_curs,"X003aa_vss_gl_join","")
s_head = "(" + s_head.rstrip(", ") + ")"
#print(s_head)
# Open the SOURCE file to obtain the data
print("Insert gl data into vss table...")
#with sqlite3.connect(so_path+so_file) as rs_conn:
#    rs_conn.row_factory = sqlite3.Row
#rs_curs = rs_conn.cursor()
so_curs.execute("SELECT * FROM X003aa_gl_vss_join")
rows = so_curs.fetchall()
i_tota = 0
i_coun = 0
for row in rows:
    s_data = "("
    for member in row:
        #print(type(member))
        if type(member) == str:
            s_data = s_data + "'" + member + "', "
        elif type(member) == int:
            s_data = s_data + str(member) + ", "
        elif type(member) == float:
            s_data = s_data + str(member) + ", "
        else:
            s_data = s_data + "'', "
    s_data = s_data.rstrip(", ") + ")"
    #print(s_data)
    s_sql = "INSERT INTO `X003aa_vss_gl_join` " + s_head + " VALUES " + s_data + ";"
    so_curs.execute(s_sql)
    i_tota = i_tota + 1
    i_coun = i_coun + 1
    if i_coun == 100:
        so_conn.commit()
        i_coun = 0
so_conn.commit()        
print("Inserted " + str(i_tota) + " rows...")
funcfile.writelog("%t POPULATE TABLE: X003aa_vss_gl_join with " + str(i_tota) + " rows")

# Report on VSS and GL comparison per campus per month *********************
print("Report vss gl join transaction type...")
sr_file = "X003ax_vss_gl_join"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X003aa_vss_gl_join.CAMPUS_VSS AS CAMPUS,
  X003aa_vss_gl_join.MONTH_VSS AS MONTH,
  X003aa_vss_gl_join.TRANSCODE_VSS AS TRANCODE,
  X003aa_vss_gl_join.TEMP_DESC_E AS VSS_DESCRIPTION,
  CAST(X003aa_vss_gl_join.AMOUNT_VSS AS REAL) AS VSS_AMOUNT,
  X003aa_vss_gl_join.DESC_VSS AS GL_DESCRIPTION,
  CAST(X003aa_vss_gl_join.AMOUNT AS REAL) AS GL_AMOUNT,
  X003aa_vss_gl_join.DIFF,
  X003aa_vss_gl_join.MATCHED,
  X003aa_vss_gl_join.PERIOD
FROM
  X003aa_vss_gl_join
ORDER BY
  CAMPUS,
  MONTH,
  TRANCODE
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)    
print("Export vss gl recon...")
sr_filet = sr_file
sx_path = re_path + funcdate.prev_year() + "/"
sx_file = "Debtor_003_vss_gl_recon_"
sx_filet = sx_file + funcdate.cur_monthendfile()
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
#funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)




"""*************************************************************************
***
*** TEST MATCHED TRANSACTION TYPES
***
*** Only a summary per campus per month
*** No detail list of transation types that matched
*** 
*************************************************************************"""

# Compile a summary of matched transaction types *******************************
print("Compile a summary of matched transaction types...")
sr_file = "X004aa_matched_summary"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  X003aa_vss_gl_join.CAMPUS_VSS AS CAMPUS,
  X003aa_vss_gl_join.MONTH_VSS AS MONTH,
  Total(X003aa_vss_gl_join.AMOUNT_VSS) AS AMOUNT_VSS,
  Total(X003aa_vss_gl_join.AMOUNT) AS AMOUNT_GL
FROM
  X003aa_vss_gl_join
WHERE
  X003aa_vss_gl_join.MATCHED = 'C'
GROUP BY
  X003aa_vss_gl_join.CAMPUS_VSS,
  X003aa_vss_gl_join.MONTH_VSS,
  X003aa_vss_gl_join.MATCHED
ORDER BY
  X003aa_vss_gl_join.MONTH_VSS,
  X003aa_vss_gl_join.CAMPUS_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)








"""*************************************************************************
***
*** TEST NON MATCHING TRANSACTION TYPES
***
*** Detail list of transaction types matched but with different totals
***   Add rowid campus+month+trantype
***   Add org as nwu
*** Import reporting officer name and email address for campus and organization
*** Import reporting supervisor name and email address for campus and organization
*** 
*************************************************************************"""

# Identify transaction types that did not match ********************************
print("Identify non matching transaction types...")
sr_file = "X004ba_nomatch_trantype"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  UPPER(SUBSTR(X003aa_vss_gl_join.CAMPUS_VSS,1,3))||TRIM(X003aa_vss_gl_join.MONTH_VSS)||TRIM(X003aa_vss_gl_join.TRANSCODE_VSS) AS ROWID,
  'NWU' AS ORG,
  X003aa_vss_gl_join.CAMPUS_VSS AS CAMPUS,
  X003aa_vss_gl_join.MONTH_VSS AS MONTH,
  X003aa_vss_gl_join.TRANSCODE_VSS AS TRAN_TYPE,
  X003aa_vss_gl_join.TEMP_DESC_E AS TRAN_DESCRIPTION,
  X003aa_vss_gl_join.AMOUNT_VSS AS AMOUNT_VSS,
  X003aa_vss_gl_join.AMOUNT AS AMOUNT_GL,
  X003aa_vss_gl_join.AMOUNT_VSS-X003aa_vss_gl_join.AMOUNT AS DIFF
FROM
  X003aa_vss_gl_join
WHERE
  X003aa_vss_gl_join.AMOUNT IS NOT NULL AND
  X003aa_vss_gl_join.MATCHED = 'X'
ORDER BY
  X003aa_vss_gl_join.MONTH_VSS,
  X003aa_vss_gl_join.CAMPUS_VSS,
  X003aa_vss_gl_join.TRANSCODE_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Import the reporting officers ************************************************
print("Import reporting officers from VSS.SQLITE...")
sr_file = "X004bb_impo_report_officer"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  VSS.X000_OWN_LOOKUPS.LOOKUP,
  VSS.X000_OWN_LOOKUPS.LOOKUP_CODE AS CAMPUS,
  VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
  PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
  PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
FROM
  VSS.X000_OWN_LOOKUPS
  LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION
WHERE
  VSS.X000_OWN_LOOKUPS.LOOKUP = 'stud_debt_recon_test_amount_differ_officer'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Import the reporting supervisors *********************************************
print("Import reporting supervisors from VSS.SQLITE...")
sr_file = "X004bc_impo_report_supervisor"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  VSS.X000_OWN_LOOKUPS.LOOKUP,
  VSS.X000_OWN_LOOKUPS.LOOKUP_CODE AS CAMPUS,
  VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
  PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
  PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
FROM
  VSS.X000_OWN_LOOKUPS
  LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION
WHERE
  VSS.X000_OWN_LOOKUPS.LOOKUP = 'stud_debt_recon_test_amount_differ_supervisor'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Add the reporting officer and supervisor *************************************
print("Add the reporting officer and supervisor...")
sr_file = "X004bx_nomatch_trantype"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  X004ba_nomatch_trantype."ROWID",
  X004ba_nomatch_trantype.ORG,
  X004ba_nomatch_trantype.CAMPUS,
  X004ba_nomatch_trantype.MONTH,
  X004ba_nomatch_trantype.TRAN_TYPE,
  X004ba_nomatch_trantype.TRAN_DESCRIPTION,
  X004ba_nomatch_trantype.AMOUNT_VSS,
  X004ba_nomatch_trantype.AMOUNT_GL,
  X004ba_nomatch_trantype.DIFF,
  CAMP_OFFICER.EMPLOYEE_NUMBER AS OFFICER_CAMP,
  CAMP_OFFICER.KNOWN_NAME AS OFFICER_NAME_CAMP,
  CAMP_OFFICER.EMAIL_ADDRESS AS OFFICER_MAIL_CAMP,
  ORG_OFFICER.EMPLOYEE_NUMBER AS OFFICER_ORG,
  ORG_OFFICER.KNOWN_NAME AS OFFICER_NAME_ORG,
  ORG_OFFICER.EMAIL_ADDRESS AS OFFICER_MAIL_ORG,
  CAMP_SUPERVISOR.EMPLOYEE_NUMBER AS SUPERVISOR_CAMP,
  CAMP_SUPERVISOR.KNOWN_NAME AS SUPERVISOR_NAME_CAMP,
  CAMP_SUPERVISOR.EMAIL_ADDRESS AS SUPERVISOR_MAIL_CAMP,
  ORG_SUPERVISOR.EMPLOYEE_NUMBER AS SUPERVISOR_ORG,
  ORG_SUPERVISOR.KNOWN_NAME AS SUPERVISOR_NAME_ORG,
  ORG_SUPERVISOR.EMAIL_ADDRESS AS SUPERVISOR_MAIL_ORG
FROM
  X004ba_nomatch_trantype
  LEFT JOIN X004bb_impo_report_officer CAMP_OFFICER ON CAMP_OFFICER.CAMPUS = X004ba_nomatch_trantype.CAMPUS
  LEFT JOIN X004bb_impo_report_officer ORG_OFFICER ON ORG_OFFICER.CAMPUS = X004ba_nomatch_trantype.ORG
  LEFT JOIN X004bc_impo_report_supervisor CAMP_SUPERVISOR ON CAMP_SUPERVISOR.CAMPUS = X004ba_nomatch_trantype.CAMPUS
  LEFT JOIN X004bc_impo_report_supervisor ORG_SUPERVISOR ON ORG_SUPERVISOR.CAMPUS = X004ba_nomatch_trantype.ORG
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)










"""*************************************************************************
***
*** TEST TRANSACTION TYPES IN VSS BUT NOT IN GL
***
*** Detail list of transaction types in vss but not in the gl
***   Add rowid campus+month+trantype
***   Add org as nwu
*** Import reporting officer name and email address for campus and organization
*** Import reporting supervisor name and email address for campus and organization
*** 
*************************************************************************"""

# Identify transaction types in vss but not in the gl **************************
print("Identify transaction types in vss but not in gl...")
sr_file = "X004ca_invss_nogl"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  UPPER(SUBSTR(X003aa_vss_gl_join.CAMPUS_VSS,1,3))||TRIM(X003aa_vss_gl_join.MONTH_VSS)||TRIM(X003aa_vss_gl_join.TRANSCODE_VSS) AS ROWID,
  'NWU' AS ORG,
  X003aa_vss_gl_join.CAMPUS_VSS AS CAMPUS,
  X003aa_vss_gl_join.MONTH_VSS AS MONTH,
  X003aa_vss_gl_join.TRANSCODE_VSS AS TRANS_TYPE,
  X003aa_vss_gl_join.TEMP_DESC_E AS TRANS_DESCRIPTION,
  Round(X003aa_vss_gl_join.AMOUNT_VSS,2) AS AMOUNT_VSS
FROM
  X003aa_vss_gl_join
WHERE
  X003aa_vss_gl_join.DESC_VSS IS NULL
ORDER BY
  X003aa_vss_gl_join.MONTH_VSS,
  X003aa_vss_gl_join.CAMPUS_VSS,
  X003aa_vss_gl_join.TRANSCODE_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Import the reporting officers ************************************************
print("Import reporting officers from VSS.SQLITE...")
sr_file = "X004cb_impo_report_officer"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  VSS.X000_OWN_LOOKUPS.LOOKUP,
  VSS.X000_OWN_LOOKUPS.LOOKUP_CODE AS CAMPUS,
  VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
  PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
  PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
FROM
  VSS.X000_OWN_LOOKUPS
  LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION
WHERE
  VSS.X000_OWN_LOOKUPS.LOOKUP = 'stud_debt_recon_test_invss_nogl_officer'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Import the reporting supervisors *********************************************
print("Import reporting supervisors from VSS.SQLITE...")
sr_file = "X004cc_impo_report_supervisor"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  VSS.X000_OWN_LOOKUPS.LOOKUP,
  VSS.X000_OWN_LOOKUPS.LOOKUP_CODE AS CAMPUS,
  VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
  PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
  PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
FROM
  VSS.X000_OWN_LOOKUPS
  LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION
WHERE
  VSS.X000_OWN_LOOKUPS.LOOKUP = 'stud_debt_recon_test_invss_nogl_supervisor'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Add the reporting officer and supervisor *************************************
print("Add the reporting officer and supervisor...")
sr_file = "X004cx_invss_nogl"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  X004ca_invss_nogl."ROWID",
  X004ca_invss_nogl.ORG,
  X004ca_invss_nogl.CAMPUS,
  X004ca_invss_nogl.MONTH,
  X004ca_invss_nogl.TRANS_TYPE,
  X004ca_invss_nogl.TRANS_DESCRIPTION,
  X004ca_invss_nogl.AMOUNT_VSS,
  CAMP_OFFICER.EMPLOYEE_NUMBER AS OFFICER_CAMP,
  CAMP_OFFICER.KNOWN_NAME AS OFFICER_NAME_CAMP,
  CAMP_OFFICER.EMAIL_ADDRESS AS OFFICER_MAIL_CAMP,
  ORG_OFFICER.EMPLOYEE_NUMBER AS OFFICER_ORG,
  ORG_OFFICER.KNOWN_NAME AS OFFICER_NAME_ORG,
  ORG_OFFICER.EMAIL_ADDRESS AS OFFICER_MAIL_ORG,
  CAMP_SUPERVISOR.EMPLOYEE_NUMBER AS SUPERVISOR_CAMP,
  CAMP_SUPERVISOR.KNOWN_NAME AS SUPERVISOR_NAME_CAMP,
  CAMP_SUPERVISOR.EMAIL_ADDRESS AS SUPERVISOR_MAIL_CAMP,
  ORG_SUPERVISOR.EMPLOYEE_NUMBER AS SUPERVISOR_ORG,
  ORG_SUPERVISOR.KNOWN_NAME AS SUPERVISOR_NAME_ORG,
  ORG_SUPERVISOR.EMAIL_ADDRESS AS SUPERVISOR_MAIL_ORG
FROM
  X004ca_invss_nogl
  LEFT JOIN X004cb_impo_report_officer CAMP_OFFICER ON CAMP_OFFICER.CAMPUS = X004ca_invss_nogl.CAMPUS
  LEFT JOIN X004cb_impo_report_officer ORG_OFFICER ON ORG_OFFICER.CAMPUS = X004ca_invss_nogl.ORG
  LEFT JOIN X004cc_impo_report_supervisor CAMP_SUPERVISOR ON CAMP_SUPERVISOR.CAMPUS = X004ca_invss_nogl.CAMPUS
  LEFT JOIN X004cc_impo_report_supervisor ORG_SUPERVISOR ON ORG_SUPERVISOR.CAMPUS = X004ca_invss_nogl.ORG
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)







"""*************************************************************************
***
*** TEST TRANSACTION TYPES IN GL BUT NOT IN VSS
***
*** Detail list of transaction types in gl but not in vss
***   Add rowid campus+month+trantype
***   Add org as nwu
*** Import reporting officer name and email address for campus and organization
*** Import reporting supervisor name and email address for campus and organization
*** 
*************************************************************************"""

# Identify transaction types in gl but not in vss ******************************
print("Identify transaction types in gl but not in vss...")
sr_file = "X004da_ingl_novss"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  UPPER(SUBSTR(X003aa_gl_vss_join.CAMPUS_VSS,1,3))||TRIM(X003aa_gl_vss_join.MONTH_VSS)||REPLACE(UPPER(X003aa_gl_vss_join.DESC_VSS),' ','') AS ROWID,
  'NWU' AS ORG,
  X003aa_gl_vss_join.CAMPUS_VSS AS CAMPUS,
  X003aa_gl_vss_join.MONTH_VSS AS MONTH,
  X003aa_gl_vss_join.DESC_VSS AS GL_DESCRIPTION,
  Round(X003aa_gl_vss_join.AMOUNT,2) AS AMOUNT_GL
FROM
  X003aa_gl_vss_join
ORDER BY
  X003aa_gl_vss_join.MONTH_VSS,
  X003aa_gl_vss_join.CAMPUS_VSS,
  X003aa_gl_vss_join.DESC_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Import the reporting officers ************************************************
print("Import reporting officers from VSS.SQLITE...")
sr_file = "X004db_impo_report_officer"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  VSS.X000_OWN_LOOKUPS.LOOKUP,
  VSS.X000_OWN_LOOKUPS.LOOKUP_CODE AS CAMPUS,
  VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
  PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
  PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
FROM
  VSS.X000_OWN_LOOKUPS
  LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION
WHERE
  VSS.X000_OWN_LOOKUPS.LOOKUP = 'stud_debt_recon_test_ingl_novss_officer'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Import the reporting supervisors *********************************************
print("Import reporting supervisors from VSS.SQLITE...")
sr_file = "X004dc_impo_report_supervisor"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  VSS.X000_OWN_LOOKUPS.LOOKUP,
  VSS.X000_OWN_LOOKUPS.LOOKUP_CODE AS CAMPUS,
  VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
  PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
  PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
FROM
  VSS.X000_OWN_LOOKUPS
  LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION
WHERE
  VSS.X000_OWN_LOOKUPS.LOOKUP = 'stud_debt_recon_test_ingl_novss_supervisor'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Add the reporting officer and supervisor *************************************
print("Add the reporting officer and supervisor...")
sr_file = "X004dx_ingl_novss"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  X004da_ingl_novss."ROWID",
  X004da_ingl_novss.ORG,
  X004da_ingl_novss.CAMPUS,
  X004da_ingl_novss.MONTH,
  X004da_ingl_novss.GL_DESCRIPTION,
  X004da_ingl_novss.AMOUNT_GL,
  CAMP_OFFICER.EMPLOYEE_NUMBER AS OFFICER_CAMP,
  CAMP_OFFICER.KNOWN_NAME AS OFFICER_NAME_CAMP,
  CAMP_OFFICER.EMAIL_ADDRESS AS OFFICER_MAIL_CAMP,
  ORG_OFFICER.EMPLOYEE_NUMBER AS OFFICER_ORG,
  ORG_OFFICER.KNOWN_NAME AS OFFICER_NAME_ORG,
  ORG_OFFICER.EMAIL_ADDRESS AS OFFICER_MAIL_ORG,
  CAMP_SUPERVISOR.EMPLOYEE_NUMBER AS SUPERVISOR_CAMP,
  CAMP_SUPERVISOR.KNOWN_NAME AS SUPERVISOR_NAME_CAMP,
  CAMP_SUPERVISOR.EMAIL_ADDRESS AS SUPERVISOR_MAIL_CAMP,
  ORG_SUPERVISOR.EMPLOYEE_NUMBER AS SUPERVISOR_ORG,
  ORG_SUPERVISOR.KNOWN_NAME AS SUPERVISOR_NAME_ORG,
  ORG_SUPERVISOR.EMAIL_ADDRESS AS SUPERVISOR_MAIL_ORG
FROM
  X004da_ingl_novss
  LEFT JOIN X004db_impo_report_officer CAMP_OFFICER ON CAMP_OFFICER.CAMPUS = X004da_ingl_novss.CAMPUS
  LEFT JOIN X004db_impo_report_officer ORG_OFFICER ON ORG_OFFICER.CAMPUS = X004da_ingl_novss.ORG
  LEFT JOIN X004dc_impo_report_supervisor CAMP_SUPERVISOR ON CAMP_SUPERVISOR.CAMPUS = X004da_ingl_novss.CAMPUS
  LEFT JOIN X004dc_impo_report_supervisor ORG_SUPERVISOR ON ORG_SUPERVISOR.CAMPUS = X004da_ingl_novss.ORG
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)










""" ************************************************************************
***
*** BURSARY VSS GL RECON
***
***   X010aa Extract vss bursary transactions (codes 042,052,381,382,500)
***   X010ab Summarize vss bursary transactions per campus month
***   X002cd Summarize vss bursary transactions per transaction code
***   X001cd Extract gl bursary transactions
***   X010bb Summarize gl bursary transactions per campus month
***   X010ca Join vss gl bursary summary totals
***   X010cb Join the matched vss and gl bursary transactions
***   X010cc Group matched vss gl transactions on campus & month
***   X011aa Join vss gl bursary summary totals
***   X011ab Join vss gl bursary summary totals
***   X011ac Import reporting supervisors from VSS.SQLITE
***   X011ax Add the reporting officer and supervisor
***   
*************************************************************************"""

print("--- PREPARE BURSARY TRANSACTIONS ---")
funcfile.writelog("PREPARE BURSARY TRANSACTIONS")

# *** BURSARY VSS GL RECON Extract the vss bursary transactions *****************************************
print("Extract vss bursary transactions...")
sr_file = "X010aa_vss_burs"    
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X002ab_vss_transort.STUDENT_VSS,
  X002ab_vss_transort.TRANSDATE_VSS,
  X002ab_vss_transort.MONTH_VSS,
  X002ab_vss_transort.CAMPUS_VSS,
  X002ab_vss_transort.BURSCODE_VSS,
  X002ab_vss_transort.AMOUNT_VSS,
  X002ab_vss_transort.BURSNAAM_VSS,
  X002ab_vss_transort.TRANSCODE_VSS,
  X002ab_vss_transort.TEMP_DESC_E AS TRANSDESC_VSS,
  X002ab_vss_transort.TRANUSER AS TRANSUSER_VSS
FROM
  X002ab_vss_transort
WHERE
  (X002ab_vss_transort.TRANSCODE_VSS = '042') OR
  (X002ab_vss_transort.TRANSCODE_VSS = '052') OR
  (X002ab_vss_transort.TRANSCODE_VSS = '381') OR
  (X002ab_vss_transort.TRANSCODE_VSS = '382') OR
  (X002ab_vss_transort.TRANSCODE_VSS = '500')
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
print("Add vss bursary rowid column...")
so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN ROWID INT;")
so_curs.execute("UPDATE "+sr_file+" SET ROWID = _rowid_")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: Bursary vss row id")

# *** BURSARY VSS GL RECON Summarize vss bursary transactions per campus month *
print("Summarize vss bursary transactions per campus month...")
sr_file = "X010ab_vss_burssummmonth"    
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X010aa_vss_burs.CAMPUS_VSS,
  X010aa_vss_burs.MONTH_VSS,
  Total(X010aa_vss_burs.AMOUNT_VSS) AS AMOUNT_VSS
FROM
  X010aa_vss_burs
GROUP BY
  X010aa_vss_burs.CAMPUS_VSS,
  X010aa_vss_burs.MONTH_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# *** BURSARY VSS GL RECON Summarize the vss bursary transactions *****************************************
print("Summarize vss bursary transactions per transaction code...")
sr_file = "X002cd_vss_summtypeburs"    
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X010aa_vss_burs.CAMPUS_VSS,
  X010aa_vss_burs.MONTH_VSS,
  X010aa_vss_burs.TRANSCODE_VSS,
  X010aa_vss_burs.TRANSDESC_VSS,
  Total(X010aa_vss_burs.AMOUNT_VSS) AS Sum_AMOUNT_VSS
FROM
  X010aa_vss_burs
GROUP BY
  X010aa_vss_burs.CAMPUS_VSS,
  X010aa_vss_burs.MONTH_VSS,
  X010aa_vss_burs.TRANSCODE_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Export the data
print("Export vss bursary campus balances per transaction type...")
sr_filet = sr_file
sx_path = re_path + funcdate.prev_year() + "/"
sx_file = "Debtor_002_vsssummtypeburs_"
sx_filet = sx_file + funcdate.prev_monthendfile()
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
#funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

# *** BURSARY VSS GL RECON Extract the gl bursary transactions ******************************************
print("Extract gl bursary transactions...")
sr_file = "X010ba_gl_burs"    
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X001ab_gl_transort.STUDENT AS STUDENT_GL,
  X001ab_gl_transort.DATE_TRAN AS TRANSDATE_GL,
  X001ab_gl_transort.MONTH AS MONTH_GL,
  X001ab_gl_transort.CAMPUS AS CAMPUS_GL,
  X001ab_gl_transort.BURSARY AS BURSCODE_GL,
  X001ab_gl_transort.AMOUNT AS AMOUNT_GL,
  X001ab_gl_transort.DESC_VSS AS TRANSDESC_GL,
  X001ab_gl_transort.EDOC AS TRANSEDOC_GL,
  X001ab_gl_transort.DESC_FULL AS TRANSENTR_GL
FROM
  X001ab_gl_transort
WHERE
  X001ab_gl_transort.STUDENT <> ''
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
print("Add gl bursary rowid column...")
so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN ROWID_GL INT;")
so_curs.execute("UPDATE "+sr_file+" SET ROWID_GL = _rowid_")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: Bursary gl row id")

# *** BURSARY VSS GL RECON Summarize gl bursary transactions per campus month **
print("Summarize gl bursary transactions per campus month...")
sr_file = "X010bb_gl_burssummmonth"    
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X010ba_gl_burs.CAMPUS_GL,
  X010ba_gl_burs.MONTH_GL,
  Total(X010ba_gl_burs.AMOUNT_GL) AS AMOUNT_GL
FROM
  X010ba_gl_burs
GROUP BY
  X010ba_gl_burs.CAMPUS_GL,
  X010ba_gl_burs.MONTH_GL
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# *** BURSARY VSS GL RECON Summarize the gl bursary transactions *****************************************
print("Summarize gl bursary transactions...")
sr_file = "X001cd_gl_summtypeburs"    
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X010ba_gl_burs.CAMPUS_GL,
  X010ba_gl_burs.MONTH_GL,
  X010ba_gl_burs.TRANSDESC_GL,
  Total(X010ba_gl_burs.AMOUNT_GL) AS Sum_AMOUNT_GL
FROM
  X010ba_gl_burs
GROUP BY
  X010ba_gl_burs.CAMPUS_GL,
  X010ba_gl_burs.MONTH_GL,
  X010ba_gl_burs.TRANSDESC_GL
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Export the data
print("Export gl bursary campus balances per transaction type...")
sr_filet = sr_file
sx_path = re_path + funcdate.prev_year() + "/"
sx_file = "Debtor_001_glsummtypeburs_"
sx_filet = sx_file + funcdate.prev_monthendfile()
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
#funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

# *** BURSARY VSS GL RECON Join vss bursary summary totals *********************
so_curs.execute("DROP TABLE IF EXISTS X010ca_join_vss_gl_burssumm")    

# *** BURSARY VSS GL RECON Join vss bursary summary totals *********************
print("Join vss gl bursary summary totals...")
sr_file = "X011aa_join_vss_gl_burssumm"    
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  UPPER(SUBSTR(X010ab_vss_burssummmonth.CAMPUS_VSS,1,3))||TRIM(X010ab_vss_burssummmonth.MONTH_VSS) AS ROWID,
  'NWU' AS ORG,
  X010ab_vss_burssummmonth.CAMPUS_VSS AS CAMPUS,
  X010ab_vss_burssummmonth.MONTH_VSS AS MONTH,
  X010ab_vss_burssummmonth.AMOUNT_VSS AS BALANCE_VSS,
  X010bb_gl_burssummmonth.AMOUNT_GL AS BALANCE_GL,
  X010ab_vss_burssummmonth.AMOUNT_VSS - X010bb_gl_burssummmonth.AMOUNT_GL AS BALANCE_DIFF
FROM
  X010ab_vss_burssummmonth
  INNER JOIN X010bb_gl_burssummmonth ON X010bb_gl_burssummmonth.CAMPUS_GL = X010ab_vss_burssummmonth.CAMPUS_VSS AND
    X010bb_gl_burssummmonth.MONTH_GL = X010ab_vss_burssummmonth.MONTH_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# *** BURSARY VSS GL RECON Import the reporting officers ************************************************
print("Import reporting officers from VSS.SQLITE...")
sr_file = "X011ab_impo_report_officer"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  VSS.X000_OWN_LOOKUPS.LOOKUP,
  VSS.X000_OWN_LOOKUPS.LOOKUP_CODE AS CAMPUS,
  VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
  PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
  PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
FROM
  VSS.X000_OWN_LOOKUPS
  LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION
WHERE
  VSS.X000_OWN_LOOKUPS.LOOKUP = 'stud_debt_recon_burs_balance_month_officer'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# *** BURSARY VSS GL RECON Import the reporting supervisors *********************************************
print("Import reporting supervisors from VSS.SQLITE...")
sr_file = "X011ac_impo_report_supervisor"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  VSS.X000_OWN_LOOKUPS.LOOKUP,
  VSS.X000_OWN_LOOKUPS.LOOKUP_CODE AS CAMPUS,
  VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
  PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
  PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
FROM
  VSS.X000_OWN_LOOKUPS
  LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION
WHERE
  VSS.X000_OWN_LOOKUPS.LOOKUP = 'stud_debt_recon_burs_balance_month_supervisor'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# *** BURSARY VSS GL RECON Add the reporting officer and supervisor *************************************
print("Add the reporting officer and supervisor...")
sr_file = "X011ax_vss_gl_burssumm"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  X011aa_join_vss_gl_burssumm."ROWID",
  X011aa_join_vss_gl_burssumm.ORG,
  X011aa_join_vss_gl_burssumm.CAMPUS,
  X011aa_join_vss_gl_burssumm.MONTH,
  X011aa_join_vss_gl_burssumm.BALANCE_VSS,
  X011aa_join_vss_gl_burssumm.BALANCE_GL,
  X011aa_join_vss_gl_burssumm.BALANCE_DIFF,
  CAMP_OFFICER.EMPLOYEE_NUMBER AS OFFICER_CAMP,
  CAMP_OFFICER.KNOWN_NAME AS OFFICER_NAME_CAMP,
  CAMP_OFFICER.EMAIL_ADDRESS AS OFFICER_MAIL_CAMP,
  ORG_OFFICER.EMPLOYEE_NUMBER AS OFFICER_ORG,
  ORG_OFFICER.KNOWN_NAME AS OFFICER_NAME_ORG,
  ORG_OFFICER.EMAIL_ADDRESS AS OFFICER_MAIL_ORG,
  CAMP_SUPERVISOR.EMPLOYEE_NUMBER AS SUPERVISOR_CAMP,
  CAMP_SUPERVISOR.KNOWN_NAME AS SUPERVISOR_NAME_CAMP,
  CAMP_SUPERVISOR.EMAIL_ADDRESS AS SUPERVISOR_MAIL_CAMP,
  ORG_SUPERVISOR.EMPLOYEE_NUMBER AS SUPERVISOR_ORG,
  ORG_SUPERVISOR.KNOWN_NAME AS SUPERVISOR_NAME_ORG,
  ORG_SUPERVISOR.EMAIL_ADDRESS AS SUPERVISOR_MAIL_ORG
FROM
  X011aa_join_vss_gl_burssumm
  LEFT JOIN X011ab_impo_report_officer CAMP_OFFICER ON CAMP_OFFICER.CAMPUS = X011aa_join_vss_gl_burssumm.CAMPUS
  LEFT JOIN X011ab_impo_report_officer ORG_OFFICER ON ORG_OFFICER.CAMPUS = X011aa_join_vss_gl_burssumm.ORG
  LEFT JOIN X011ac_impo_report_supervisor CAMP_SUPERVISOR ON CAMP_SUPERVISOR.CAMPUS = X011aa_join_vss_gl_burssumm.CAMPUS
  LEFT JOIN X011ac_impo_report_supervisor ORG_SUPERVISOR ON ORG_SUPERVISOR.CAMPUS = X011aa_join_vss_gl_burssumm.ORG
ORDER BY
  X011aa_join_vss_gl_burssumm.MONTH,
  X011aa_join_vss_gl_burssumm.CAMPUS
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# *** BURSARY VSS GL RECON Join the vss and gl bursary transactions *************************************
print("Join the vss and gl bursary transactions...")
sr_file = "X010ca_join_vss_gl_burs"    
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT DISTINCT
  X010aa_vss_burs.ROWID,
  X010aa_vss_burs.STUDENT_VSS,
  X010aa_vss_burs.TRANSDATE_VSS,
  X010aa_vss_burs.MONTH_VSS,
  X010aa_vss_burs.CAMPUS_VSS,
  X010aa_vss_burs.BURSCODE_VSS,
  X010aa_vss_burs.AMOUNT_VSS,
  X010aa_vss_burs.BURSNAAM_VSS,
  X010aa_vss_burs.TRANSCODE_VSS,
  X010aa_vss_burs.TRANSDESC_VSS,
  X010aa_vss_burs.TRANSUSER_VSS,
  X010ba_gl_burs.ROWID_GL,
  X010ba_gl_burs.STUDENT_GL,
  X010ba_gl_burs.TRANSDATE_GL,
  X010ba_gl_burs.MONTH_GL,
  X010ba_gl_burs.CAMPUS_GL,
  X010ba_gl_burs.BURSCODE_GL,
  X010ba_gl_burs.AMOUNT_GL,
  X010ba_gl_burs.TRANSDESC_GL,
  X010ba_gl_burs.TRANSEDOC_GL,
  X010ba_gl_burs.TRANSENTR_GL,
  '%CYEAR%-'||X010aa_vss_burs.MONTH_VSS AS PERIOD,
  '' AS MATCHED
FROM
  X010aa_vss_burs
  LEFT JOIN X010ba_gl_burs ON X010ba_gl_burs.STUDENT_GL = X010aa_vss_burs.STUDENT_VSS AND
  X010ba_gl_burs.TRANSDATE_GL = X010aa_vss_burs.TRANSDATE_VSS AND
  X010ba_gl_burs.BURSCODE_GL = X010aa_vss_burs.BURSCODE_VSS AND
  X010ba_gl_burs.AMOUNT_GL = X010aa_vss_burs.AMOUNT_VSS
ORDER BY
  X010aa_vss_burs.TRANSDATE_VSS,
  X010aa_vss_burs.CAMPUS_VSS,
  X010aa_vss_burs.BURSCODE_VSS,
  X010aa_vss_burs.STUDENT_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
s_sql = s_sql.replace("%CYEAR%",funcdate.prev_year())
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Add column matched
print("Add column matched...")
so_curs.execute("UPDATE X010ca_join_vss_gl_burs " + """
                SET MATCHED = 
                CASE
                   WHEN ROWID_GL IS NULL THEN 'X'                
                   WHEN CAMPUS_VSS <> CAMPUS_GL THEN 'A'
                   ELSE 'C'
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: Vss gl matched")    

# *** BURSARY VSS GL RECON Join the vss and gl bursary transactions (distinct) **************************
print("Join the matched vss and gl bursary transactions...")
sr_file = "X010cb_join_vss_gl_matched"    
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  *
FROM
  X010ca_join_vss_gl_burs
WHERE
  MATCHED <> 'X'
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# BURSARY VSS GL RECON X010cc Group matched vss gl transactions on campus & month

print("Group matched vss and gl bursary transactions on campus and month...")
sr_file = "X010cc_group_vss_gl_matched"    
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X010cb_join_vss_gl_matched.CAMPUS_VSS,
  X010cb_join_vss_gl_matched.MONTH_VSS,
  Total(X010cb_join_vss_gl_matched.AMOUNT_VSS) AS Total_AMOUNT_VSS
FROM
  X010cb_join_vss_gl_matched
GROUP BY
  X010cb_join_vss_gl_matched.CAMPUS_VSS,
  X010cb_join_vss_gl_matched.MONTH_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Export the data
print("Export grouped matched bursary transactions...")
sr_filet = sr_file
sx_path = re_path + funcdate.prev_year() + "/"
sx_file = "Debtor_010c_burs_matched_summmonth_"
sx_filet = sx_file + funcdate.prev_monthendfile()
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
#funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)


"""*************************************************************************
***
*** TEST BURSARY INGL NOVSS
***
*** X010da Identify bursary transactions in gl but not in vss
*** 
*** 
*************************************************************************"""

print("--- TEST BURSARY INGL NOVSS ---")
funcfile.writelog("TEST BURSARY INGL NOVSS")

# *** TEST BURSARY INGL NOVSS Identify bursary transactions in gl but not in vss ***********************
print("Test bursary transactions in gl not in vss...")
sr_file = "X010da_test_burs_ingl_novss"    
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  ROWID_GL,
  CAMPUS_GL,
  MONTH_GL,
  STUDENT_GL,
  TRANSDATE_GL,
  TRANSDESC_GL,
  AMOUNT_GL,
  BURSCODE_GL,
  TRANSEDOC_GL,
  TRANSENTR_GL
FROM
  X010ca_join_vss_gl_burs
WHERE
  ROWID IS NULL
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Export the data
print("Export bursary transactions in gl not in vss...")
sr_filet = sr_file
sx_path = re_path + funcdate.prev_year() + "/"
sx_file = "Debtor_010d_test_burs_ingl_novss_"
sx_filet = sx_file + funcdate.prev_monthendfile()
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)






"""*************************************************************************
***
*** TEST BURSARY INVSS NOGL
***
*** X010ea Identify bursary transactions in vss but not in gl
*** 
*** 
*************************************************************************"""

print("--- TEST BURSARY INVSS NOGL ---")
funcfile.writelog("TEST BURSARY INVSS NOGL")

# *** TEST BURSARY INVSS NOGL Identify bursary transactions in vss but not in gl ***************************
print("Test bursary transactions in vss not in gl...")
sr_file = "X010ea_test_burs_invss_nogl"    
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X010ca_join_vss_gl_burs.ROWID,
  X010ca_join_vss_gl_burs.CAMPUS_VSS,
  X010ca_join_vss_gl_burs.MONTH_VSS,
  X010ca_join_vss_gl_burs.STUDENT_VSS,
  X010ca_join_vss_gl_burs.TRANSDATE_VSS,
  X010ca_join_vss_gl_burs.TRANSCODE_VSS,
  X010ca_join_vss_gl_burs.TRANSDESC_VSS,
  X010ca_join_vss_gl_burs.AMOUNT_VSS,
  X010ca_join_vss_gl_burs.BURSCODE_VSS,
  X010ca_join_vss_gl_burs.BURSNAAM_VSS,
  X010ca_join_vss_gl_burs.TRANSUSER_VSS  
FROM
  X010ca_join_vss_gl_burs
WHERE
  X010ca_join_vss_gl_burs.MATCHED = 'X'
ORDER BY
  X010ca_join_vss_gl_burs.CAMPUS_VSS,
  X010ca_join_vss_gl_burs.MONTH_VSS,
  X010ca_join_vss_gl_burs.TRANSCODE_VSS,
  X010ca_join_vss_gl_burs.BURSCODE_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Export the data
print("Export bursary transactions in vss not in gl...")
sr_filet = sr_file
sx_path = re_path + funcdate.prev_year() + "/"
sx_file = "Debtor_010e_test_burs_invss_nogl_"
sx_filet = sx_file + funcdate.prev_monthendfile()
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)








"""*************************************************************************
***
*** TEST BURSARY POST TO DIFF CAMPUS IN GL
***    
***   X010fa Identify bursary transactions posted to a different campus in gl
***   X010fb Import reporting officers from VSS.SQLITE
***   X010fc Import reporting supervisors from VSS.SQLITE
***   X010fx Final test burs to diff gl campus results
*** 
*************************************************************************"""

print("--- TEST BURSARY VSS DIFF CAMPUS GL ---")
funcfile.writelog("TEST BURSARY VSS DIFF CAMPUS GL")

#*** TEST BURSARY POST TO DIFF CAMPUS IN GL Identify bursary transactions posted to different campus in gl ***************
print("Test bursary transactions posted to different campus in gl...")
sr_file = "X010fa_test_burs_gl_diffcampus"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  UPPER(SUBSTR(X010cb_join_vss_gl_matched.CAMPUS_VSS,1,3))||TRIM(X010cb_join_vss_gl_matched.TRANSDATE_VSS)||TRIM(X010cb_join_vss_gl_matched.STUDENT_VSS)||TRIM(X010cb_join_vss_gl_matched.TRANSCODE_VSS)||TRIM(X010cb_join_vss_gl_matched.BURSCODE_VSS)||TRIM(X010cb_join_vss_gl_matched.AMOUNT_VSS) AS ROWID,
  'NWU' AS ORG,
  X010cb_join_vss_gl_matched.CAMPUS_VSS,
  X010cb_join_vss_gl_matched.MONTH_VSS,
  X010cb_join_vss_gl_matched.STUDENT_VSS,
  X010cb_join_vss_gl_matched.TRANSDATE_VSS,
  X010cb_join_vss_gl_matched.TRANSCODE_VSS,
  X010cb_join_vss_gl_matched.TRANSDESC_VSS,
  X010cb_join_vss_gl_matched.AMOUNT_VSS,
  X010cb_join_vss_gl_matched.BURSCODE_VSS,
  X010cb_join_vss_gl_matched.BURSNAAM_VSS,
  X010cb_join_vss_gl_matched.CAMPUS_GL,
  X010cb_join_vss_gl_matched.TRANSEDOC_GL,
  X010cb_join_vss_gl_matched.TRANSENTR_GL,
  X010cb_join_vss_gl_matched.TRANSDESC_GL,
  X010cb_join_vss_gl_matched.PERIOD,
  X010cb_join_vss_gl_matched.MATCHED,
  X010cb_join_vss_gl_matched.TRANSUSER_VSS
FROM
  X010cb_join_vss_gl_matched
WHERE
  X010cb_join_vss_gl_matched.MATCHED = 'A'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# *** TEST BURSARY POST TO DIFF CAMPUS IN GL Import the reporting officers ************************************************
print("Import reporting officers from VSS.SQLITE...")
sr_file = "X010fb_impo_report_officer"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  VSS.X000_OWN_LOOKUPS.LOOKUP,
  VSS.X000_OWN_LOOKUPS.LOOKUP_CODE AS CAMPUS,
  VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
  PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
  PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
FROM
  VSS.X000_OWN_LOOKUPS
  LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION
WHERE
  VSS.X000_OWN_LOOKUPS.LOOKUP = 'stud_debt_recon_burs_test_diff_campus_officer'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# *** TEST BURSARY POST TO DIFF CAMPUS IN GL Import the reporting supervisors *********************************************
print("Import reporting supervisors from VSS.SQLITE...")
sr_file = "X010fc_impo_report_supervisor"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  VSS.X000_OWN_LOOKUPS.LOOKUP,
  VSS.X000_OWN_LOOKUPS.LOOKUP_CODE AS CAMPUS,
  VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
  PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
  PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
FROM
  VSS.X000_OWN_LOOKUPS
  LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION
WHERE
  VSS.X000_OWN_LOOKUPS.LOOKUP = 'stud_debt_recon_burs_test_diff_campus_supervisor'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# *** TEST BURSARY POST TO DIFF CAMPUS IN GL Prepare final burs posted to diff gl campus reporting file *******************
print("Final test burs to diff gl campus results...")
sr_file = "X010fx_test_burs_gl_diffcampus"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  X010fa_test_burs_gl_diffcampus.CAMPUS_VSS AS VSS_CAMPUS,
  X010fa_test_burs_gl_diffcampus.CAMPUS_GL AS GL_CAMPUS,
  X010fa_test_burs_gl_diffcampus.STUDENT_VSS AS STUDENT,
  X010fa_test_burs_gl_diffcampus.TRANSDATE_VSS AS DATE,
  X010fa_test_burs_gl_diffcampus.MONTH_VSS AS MONTH,
  Round(X010fa_test_burs_gl_diffcampus.AMOUNT_VSS,2) AS AMOUNT,
  X010fa_test_burs_gl_diffcampus.TRANSCODE_VSS AS TRANSCODE,
  X010fa_test_burs_gl_diffcampus.BURSCODE_VSS AS BURSCODE,
  X010fa_test_burs_gl_diffcampus.BURSNAAM_VSS AS BURSNAME,
  X010fa_test_burs_gl_diffcampus.TRANSDESC_VSS AS TRANDESC,
  X010fa_test_burs_gl_diffcampus.TRANSEDOC_GL AS GL_EDOC,
  X010fa_test_burs_gl_diffcampus.TRANSENTR_GL AS GL_DESC,
  X010fa_test_burs_gl_diffcampus.ORG,
  X010fa_test_burs_gl_diffcampus.TRANSUSER_VSS AS USER,
  PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME AS USER_NAME,
  PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS AS USER_MAIL,
  CAMP_OFFICER.EMPLOYEE_NUMBER AS OFFICER_CAMP,
  CAMP_OFFICER.KNOWN_NAME AS OFFICER_NAME_CAMP,
  CAMP_OFFICER.EMAIL_ADDRESS AS OFFICER_MAIL_CAMP,
  ORG_OFFICER.EMPLOYEE_NUMBER AS OFFICER_ORG,
  ORG_OFFICER.KNOWN_NAME AS OFFICER_NAME_ORG,
  ORG_OFFICER.EMAIL_ADDRESS AS OFFICER_MAIL_ORG,
  CAMP_SUPERVISOR.EMPLOYEE_NUMBER AS SUPERVISOR_CAMP,
  CAMP_SUPERVISOR.KNOWN_NAME AS SUPERVISOR_NAME_CAMP,
  CAMP_SUPERVISOR.EMAIL_ADDRESS AS SUPERVISOR_MAIL_CAMP,
  ORG_SUPERVISOR.EMPLOYEE_NUMBER AS SUPERVISOR_ORG,
  ORG_SUPERVISOR.KNOWN_NAME AS SUPERVISOR_NAME_ORG,
  ORG_SUPERVISOR.EMAIL_ADDRESS AS SUPERVISOR_MAIL_ORG,
  X010fa_test_burs_gl_diffcampus.ROWID
FROM
  X010fa_test_burs_gl_diffcampus
  LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X010fa_test_burs_gl_diffcampus.TRANSUSER_VSS
  LEFT JOIN X010fb_impo_report_officer CAMP_OFFICER ON CAMP_OFFICER.CAMPUS = X010fa_test_burs_gl_diffcampus.CAMPUS_VSS
  LEFT JOIN X010fb_impo_report_officer ORG_OFFICER ON ORG_OFFICER.CAMPUS = X010fa_test_burs_gl_diffcampus.ORG
  LEFT JOIN X010fc_impo_report_supervisor CAMP_SUPERVISOR ON CAMP_SUPERVISOR.CAMPUS = X010fa_test_burs_gl_diffcampus.CAMPUS_VSS
  LEFT JOIN X010fc_impo_report_supervisor ORG_SUPERVISOR ON ORG_SUPERVISOR.CAMPUS = X010fa_test_burs_gl_diffcampus.ORG
ORDER BY
  DATE,
  VSS_CAMPUS,
  STUDENT,
  BURSCODE
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)
# Export the data
if funcsys.tablerowcount(so_curs,sr_file) > 0:
    print("Export final test burs to diff gl campus results...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.prev_year() + "/"
    sx_file = "Debtor_010fx_test_burs_gl_diffcampus_"
    sx_filet = sx_file + funcdate.today()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
else:
    print("No final test burs to diff gl campus results...")
    funcfile.writelog("%t EXPORT DATA: No new data to export")










# Close the table connection ***************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("------------------------------------")
funcfile.writelog("COMPLETED: C200_REPORT_STUDDEB_RECON")


