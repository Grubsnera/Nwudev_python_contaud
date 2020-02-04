""" C200_REPORT_STUDDEB_RECON **************************************************
***
*** Script to compare VSS and GL student transactions
***
*** Albert J van Rensburg (21162395)
*** 26 Jun 2018
*** 7 Jan 2020 Read vss data from period database
***
*****************************************************************************"""

# Import python modules
import csv
import sqlite3
import sys

# Add own module path
sys.path.append('S:/_my_modules')

# Import own modules
from _my_modules import funcdate
from _my_modules import funccsv
from _my_modules import funcfile
from _my_modules import funcsys
from _my_modules import funcmysql

""" CONTENTS *******************************************************************
ENVIRONMENT
OPEN DATABASES
DETERMINE GL POST MONTH
LIST VSS TRANSACTIONS ROUND 1
LIST GL TRANSACTIONS
LIST VSS TRANSACTIONS ROUND 2
JOIN VSS & GL MONTHLY TOTALS
JOIN VSS & GL TRANSACTIONS
TEST MATCHED TRANSACTION TYPES
TEST VSS GL DIFFERENCE TRANSACTION SUMMARY
TEST IN VSS NO GL TRANSACTIONS
TEST IN GL NO VSS TRANSACTIONS
TEST VSS GL BURSARY DIFFERENCE TRANSACTION SUMMARY
BURSARY VSS GL RECON
TEST BURSARY INGL NOVSS (UNCOMPLETE)
TEST BURSARY INVSS NOGL
TEST BURSARY VSS GL DIFFERENT CAMPUS
BALANCE ON MORE THAN ONE CAMPUS
TEST STUDENT BALANCE ON MORE THAN ONE CAMPUS 
END OF SCRIPT
*****************************************************************************"""


def Report_studdeb_recon(dOpenMaf=0, dOpenPot=0, dOpenVaa=0, s_period="curr", s_yyyy="0"):

    """ PARAMETERS *************************************************************
    dOpenMaf = GL Opening balances for Mafikeng campus
    dOpenPot = GL Opening balances for Potchefstroom campus
    dOpenVaa = GL Opening balances for Vaal Triangle campus
    Notes:
    1. When new financial year start, GL does not contain opening balances.
       Opening balances are the inserted manually here, until the are inserted
       into the GL by journal, usually at the end of March. This was the case
       for the 2019 financial year
    *************************************************************************"""

    """*************************************************************************
    ENVIRONMENT
    *************************************************************************"""

    # Open the script log file ******************************************************
    print("-------------------------")
    print("C200_REPORT_STUDDEB_RECON")
    print("-------------------------")
    print("ENVIRONMENT")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C200_REPORT_STUDDEB_RECON")
    funcfile.writelog("---------------------------------")
    funcfile.writelog("ENVIRONMENT")

    # Declare variables
    s_year: str = s_yyyy
    so_path = "W:/Kfs_vss_studdeb/" #Source database path
    if s_period == "curr":
        s_year = funcdate.cur_year()
        so_file = "Kfs_vss_studdeb.sqlite"  # Source database
        s_kfs = "KFSCURR"
        s_vss = "VSSCURR"
    elif s_period == "prev":
        s_year = funcdate.prev_year()
        so_file = "Kfs_vss_studdeb_prev.sqlite"  # Source database
        s_kfs = "KFSPREV"
        s_vss = "VSSPREV"
    else:
        so_file = "Kfs_vss_studdeb_" + s_year + ".sqlite"  # Source database
        s_kfs = ""
        s_vss = ""
    re_path = "R:/Debtorstud/" #Results
    ed_path = "S:/_external_data/" #External data
    s_sql = "" #SQL statements
    l_mail = True
    l_export = True
    l_record = True
    l_vacuum = False
    s_burs_code = '042z052z381z500' # Current bursary transaction codes

    """*************************************************************************
    OPEN DATABASES
    *************************************************************************"""
    print("OPEN DATABASES")
    funcfile.writelog("OPEN DATABASES")

    # Open the SOURCE file
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # Attach data sources
    so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs_prev.sqlite' AS 'KFSPREV'")
    funcfile.writelog("%t ATTACH DATABASE: KFS_PREV.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
    funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss_curr.sqlite' AS 'VSSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: VSS_CURR.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss_prev.sqlite' AS 'VSSPREV'")
    funcfile.writelog("%t ATTACH DATABASE: VSS_PREV.SQLITE")

    """*************************************************************************
    LIST VSS TRANSACTIONS ROUND 1
        to obtain a list of transaction types used in the GL setup below
    *************************************************************************"""
    print("LIST VSS TRANSACTION ROUND 1")
    funcfile.writelog("LIST VSS TRANSACTION ROUND 1")
    
    # Extract vss transactions from VSS.SQLITE *********************************
    print("Import vss transactions from VSS.SQLITE...")
    sr_file = "X002aa_vss_tranlist"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      *,
      CASE
        WHEN FDEBTCOLLECTIONSITE = '-9' THEN 'Mafikeng'
        WHEN FDEBTCOLLECTIONSITE = '-2' THEN 'Vaal Triangle'
        ELSE 'Potchefstroom'
      END AS CAMPUS,
      CASE
        WHEN SUBSTR(TRANSDATE,6,5)='01-01' AND INSTR('001z031z061',TRANSCODE)>0 THEN '00'
        WHEN strftime('%Y',TRANSDATE)>strftime('%Y',POSTDATEDTRANSDATE) And
         Strftime('%Y',POSTDATEDTRANSDATE) = '%CYEAR%' THEN strftime('%m',POSTDATEDTRANSDATE)
        ELSE strftime('%m',TRANSDATE)
      END AS MONTH,
      CASE
        WHEN AMOUNT > 0 THEN AMOUNT
        ELSE 0.00
      END AS AMOUNT_DT,
      CASE
        WHEN AMOUNT < 0 THEN AMOUNT
        ELSE 0.00
      END AS AMOUNT_CR,
      UPPER(TRIM(DESCRIPTION_A)) AS TEMP_DESC_A,
      UPPER(TRIM(DESCRIPTION_E)) AS TEMP_DESC_E
    FROM
      %VSS%.X010_Studytrans
    WHERE
      TRANSCODE <> ''
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    s_sql = s_sql.replace("%CYEAR%", s_year)
    s_sql = s_sql.replace("%VSS%", s_vss)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # Temp description - Remove characters from description ********************
    print("Add column vss descriptions...")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'0',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,'0','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'1',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,'1','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'2',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,'2','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'3',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,'3','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'4',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,'4','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'5',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,'5','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'6',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,'6','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'7',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,'7','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'8',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,'8','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'9',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,'9','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'(',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,'(','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,')',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,')','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'.',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,'.','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'-',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,'-','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,':',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,':','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'/',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,'/','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'&',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,'&','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'?',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,'?','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,' ',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,' ','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'\t',''), TEMP_DESC_E = REPLACE(TEMP_DESC_E,'\t','');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_A = REPLACE(TEMP_DESC_A,'ë','E'), TEMP_DESC_E = REPLACE(TEMP_DESC_E,'ë','E');")
    so_curs.execute("UPDATE X002aa_vss_tranlist SET TEMP_DESC_E = REPLACE(TEMP_DESC_E,'MISCELANEOUSFEES','MISCELLANEOUSFEES');")
    funcfile.writelog("%t CALC COLUMN: Temp descriptions")

    # Build a transaction code language list ***************************************
    print("Build a transaction code language list...")
    sr_file = "X002aa_vss_tranlist_langsumm"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT DISTINCT
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
    LIST GL TRANSACTIONS
    *************************************************************************"""
    print("LIST GL TRANSACTIONS")
    funcfile.writelog("LIST GL TRANSACTIONS")

    # Import gl transactions **************************************************
    print("Import gl transactions from KFS_CURR.SQLITE...")
    sr_file = "X001aa_gl_tranlist"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      *,
      CASE
        WHEN ACCOUNT_NBR = '1G02018' THEN 1
        WHEN ACCOUNT_NBR = '1G01772' THEN 2
        WHEN ACCOUNT_NBR = '1G01773' THEN 2
        WHEN ACCOUNT_NBR = '1G01804' THEN 9
        WHEN ACCOUNT_NBR = '1G02012' THEN 9
        ELSE 0
      END AS BUSINESSENTITYID,
      CASE
        WHEN UNIV_FISCAL_PRD_CD = 'BB' THEN '00'
        WHEN UNIV_FISCAL_PRD_CD = '13' THEN '12'
        WHEN UNIV_FISCAL_PRD_CD = '14' THEN '12'
        ELSE UNIV_FISCAL_PRD_CD
      END AS MONTH,
      CASE
        WHEN FS_ORIGIN_CD = '01' AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),'REVERSE COLL') > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"BEURS:")+6,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STUDENT:")-8)
        WHEN FS_ORIGIN_CD = '10' AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),'BEURS:') > 0 AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STATUS:U") > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"BEURS:")+6,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STUDENT:")-8)
        WHEN FS_ORIGIN_CD = '10' AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),'BURSARY:') > 0 AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STATUS:U") > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"BURSARY:")+8,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STUDENT:")-10)
        WHEN FS_ORIGIN_CD = '10' AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),'BURSARY :') > 0 AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STATUS : K") > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"BURSARY:")+11,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"LEARNER :")-12)                   ELSE ""
      END AS BURSARY_CODE,
      CASE
        WHEN FS_ORIGIN_CD = '01' AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"REVERSE COLL") > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STUDENT:")+8,8)
        WHEN FS_ORIGIN_CD = '10' AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STATUS:U") > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STUDENT:")+8,8)
        WHEN FS_ORIGIN_CD = '10' AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"STATUS : K") > 0 THEN SUBSTR(TRN_LDGR_ENTR_DESC,INSTR(UPPER(TRN_LDGR_ENTR_DESC),"LEARNER :")+10,8)
        ELSE ""
      END AS STUDENT,
      Upper(Trim(TRN_LDGR_ENTR_DESC)) AS TEMP,
      '' AS DESCRIPTION
    FROM
      %KFS%.X000_GL_trans
    WHERE
      (FIN_OBJECT_CD = '7551') OR
      (FIN_OBJECT_CD = '7552') OR
      (FIN_OBJECT_CD = '7553')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    s_sql = s_sql.replace("%KFS%", s_kfs)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # Add columns **************************************************************

    # Temp description - Remove characters from description
    print("Add column gl temp description column...")
    so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'REVERSESTF','');")
    so_curs.execute("UPDATE X001aa_gl_tranlist SET TEMP = REPLACE(TEMP,'0','');")
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
    funcfile.writelog("%t ADD COLUMN: Temp description")

    # Calc transaction description
    print("Add column gl description link...")
    so_curs.execute("Update X001aa_gl_tranlist " + """
                    Set DESCRIPTION = 
                    Case
                        When TEMP Like('REVERSESTF%') Then Replace(TEMP,'REVERSESTF','')
                        WHEN FS_ORIGIN_CD = '01' AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"REVERSE COLL") > 0 THEN "BEURSKANSELLASIEKLASGELDE"
                        WHEN FS_ORIGIN_CD = '10' AND INSTR(UPPER(TRN_LDGR_ENTR_DESC),"LEARNER :") > 0 THEN "BEURSKANSELLASIEKLASGELDE"
                        WHEN FS_ORIGIN_CD = '10' THEN "BEURSEENLENINGSKLASGELDE"
                        WHEN FS_ORIGIN_CD = '11' THEN UPPER(TRIM(TEMP))
                        WHEN MONTH = '00' THEN 'SALDOOORGEDRAKLASGELD'
                        ELSE "X "||UPPER(TRN_LDGR_ENTR_DESC)||" ORIGIN:"||UPPER(FS_DATABASE_DESC)||" EDOC:"||UPPER(FDOC_NBR)
                    End
                    ;""")
    so_conn.commit()
    funcfile.writelog("%t ADD COLUMN: Description link")

    # Join the transaction languages to the gl transaction file ********************
    print("Join the gl transactions with transaction code languages...")
    sr_file = "X001aa_gl_tranlist_lang"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT DISTINCT
      X001aa_gl_tranlist.*,
      X002aa_vss_tranlist_langsumm.DESC_ENG,
      CASE
         WHEN DESC_ENG <> '' THEN DESC_ENG
         ELSE DESCRIPTION
      END AS DESC_GL
    FROM
      X001aa_gl_tranlist
      LEFT JOIN X002aa_vss_tranlist_langsumm ON X002aa_vss_tranlist_langsumm.DESC_AFR = X001aa_gl_tranlist.DESCRIPTION
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # ALTER SOME COLUMN NAMES TO ALIGNN WITH VSS *******************************
    print("Add column gl temp description column...")
    so_curs.execute("UPDATE X001aa_gl_tranlist_lang SET DESC_GL = REPLACE(DESC_GL,'EDULOANBPEL','FUNDIBPEL');")
    so_curs.execute("UPDATE X001aa_gl_tranlist_lang SET DESC_GL = REPLACE(DESC_GL,'EDULOANHANDTRANSAKSIES','FUNDIHANDTRANSACTIONS');")
    so_curs.execute("UPDATE X001aa_gl_tranlist_lang SET DESC_GL = REPLACE(DESC_GL,'EDULOANHANDTRANSACTIONS','FUNDIHANDTRANSACTIONS');")
    so_curs.execute("UPDATE X001aa_gl_tranlist_lang SET DESC_GL = REPLACE(DESC_GL,'LEVYFORBOOKSACCOUNT','FUNDIBOOKALLOWANCE');")
    so_curs.execute("UPDATE X001aa_gl_tranlist_lang SET DESC_GL = REPLACE(DESC_GL,'HEFFINGVIRBOEKEREKENING','FUNDIBOOKALLOWANCE');")
    so_curs.execute("UPDATE X001aa_gl_tranlist_lang SET DESC_GL = REPLACE(DESC_GL,'NSFASBOEKE','FUNDIBOOKALLOWANCE');")
    so_curs.execute("UPDATE X001aa_gl_tranlist_lang SET DESC_GL = REPLACE(DESC_GL,'NSFASETES','FUNDIMEALALLOWANCE');")
    so_curs.execute("UPDATE X001aa_gl_tranlist_lang SET DESC_GL = REPLACE(DESC_GL,'NSFASMEALS','FUNDIMEALALLOWANCE');")
    so_curs.execute("UPDATE X001aa_gl_tranlist_lang SET DESC_GL = REPLACE(DESC_GL,'EDULOANBEURSREKENING','FUNDIMEALALLOWANCE');")
    so_curs.execute("UPDATE X001aa_gl_tranlist_lang SET DESC_GL = REPLACE(DESC_GL,'EDULOANBURSARYACCOUNT','FUNDIMEALALLOWANCE');")
    so_curs.execute("UPDATE X001aa_gl_tranlist_lang SET DESC_GL = REPLACE(DESC_GL,'MISCELANEOUSFEES','MISCELLANEOUSFEES');")
    so_curs.execute("UPDATE X001aa_gl_tranlist_lang SET DESC_GL = REPLACE(DESC_GL,'NSFASMEALBOOKALLOWANCE','NSFASMEALALLOWANCE');")

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
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_001_gltran_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    #funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    # Calculate gl balances ****************************************************
    print("Calculate gl balances per campus...")
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

    # Add previous year gl opening balances (TEMPORARY)
    if dOpenMaf != 0:
        s_sql = "INSERT INTO `X001cb_gl_balmonth` (`CAMPUS`,`MONTH`,`BALANCE`) VALUES ('Mafikeng','00','"+dOpenMaf+"');"
        so_curs.execute(s_sql)
    if dOpenPot != 0:
        s_sql = "INSERT INTO `X001cb_gl_balmonth` (`CAMPUS`,`MONTH`,`BALANCE`) VALUES ('Potchefstroom','00','"+dOpenPot+"');"
        so_curs.execute(s_sql)
    if dOpenVaa != 0:
        s_sql = "INSERT INTO `X001cb_gl_balmonth` (`CAMPUS`,`MONTH`,`BALANCE`) VALUES ('Vaal Triangle','00','"+dOpenVaa+"');"
        so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t ADD ROW: GL Opening balances (temporary)")

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
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_001_glsummtype_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    #funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

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

    # DETERMINE GL POST MONTH
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
    if funcsys.tablerowcount(so_curs,sr_file) == 3:
        gl_month = funcdate.prev_month()
        """
        # Export the data
        print("Export gl post month...")
        sr_filet = sr_file
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "Debtor_000_postmonth"
        sx_filet = sx_file + funcdate.prev_monthendfile()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
        #print(gl_month)
        """
    else:
        p_month = str(int(funcdate.prev_month())-1)
        if int(p_month) < 10:
            p_month = "0" + p_month
        gl_month = p_month
        #print(gl_month)
        
    """*************************************************************************
    LIST VSS TRANSACTIONS ROUND 2
    *************************************************************************"""
    print("LIST VSS TRANSACTION ROUND 2")
    funcfile.writelog("LIST VSS TRANSACTION ROUND 2")

    # Sort vss transactions ****************************************************
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

    # Calculate vss balances per campus per month per transaction type *********
    print("Prepare data for analysis...")
    sr_file = "X002ac_vss_tranexpo"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X002ab_vss_transort.MONTH_VSS,
      X002ab_vss_transort.STUDENT_VSS,
      '%CYEAR%' AS YEAR,
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
    s_sql = s_sql.replace("%CYEAR%",s_year)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
    # Export the data
    print("Export vss campus balances per transaction type...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
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
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_002_vsssummtype_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    #funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)    

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
      X002ab_vss_transort.STUDENT_VSS
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
    # Export the data
    print("Export vss student balances...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_002_studbal_"
    #sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)    

    # Sum vss balances per campus ******************************************
    print("Sum vss balances per campus...")
    sr_file = "X002da_vss_campus_balance"
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

    # CALCULATE OPENING BALANCES ***************************************************
    print("Sum vss student opening balances per campus...")
    sr_file = "X002da_vss_student_balance_open"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X002ab_vss_transort.CAMPUS_VSS AS CAMPUS,
      X002ab_vss_transort.STUDENT_VSS AS STUDENT,  
      Round(Total(X002ab_vss_transort.AMOUNT_VSS),2) AS BALANCE
    FROM
      X002ab_vss_transort
    WHERE
      X002ab_vss_transort.MONTH_VSS = '00'
    GROUP BY
      X002ab_vss_transort.STUDENT_VSS,
      X002ab_vss_transort.CAMPUS_VSS
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)    
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
    # Export the data
    print("Export vss student balances...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_002_studbal_open_"
    #sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    # CALCULATE CLOSING BALANCES ***************************************************
    if s_period == "curr":
        print("Sum vss student closing balances per campus...")
        sr_file = "X002da_vss_student_balance_clos"
        s_sql = "Create Table " + sr_file + " As " + """
        Select
            Case
                WHEN FDEBTCOLLECTIONSITE = '-9' THEN 'Mafikeng'
                WHEN FDEBTCOLLECTIONSITE = '-2' THEN 'Vaal Triangle'
                ELSE 'Potchefstroom'    
            End AS CAMPUS,
            TRAN.FBUSENTID AS STUDENT,  
            Round(Total(TRAN.AMOUNT),2) AS BALANCE
        From
            VSSPREV.X010_Studytrans TRAN
        WHERE
            TRAN.TRANSCODE != ""
        GROUP BY
            TRAN.FBUSENTID,
            TRAN.FDEBTCOLLECTIONSITE
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
    else:
        # USE THE SAME BALANCE AS OPENING
        print("Sum vss student opening balances per campus...")
        sr_file = "X002da_vss_student_balance_clos"
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          X002ab_vss_transort.CAMPUS_VSS AS CAMPUS,
          X002ab_vss_transort.STUDENT_VSS AS STUDENT,  
          Round(Total(X002ab_vss_transort.AMOUNT_VSS),2) AS BALANCE
        FROM
          X002ab_vss_transort
        WHERE
          X002ab_vss_transort.MONTH_VSS = '00'
        GROUP BY
          X002ab_vss_transort.STUDENT_VSS,
          X002ab_vss_transort.CAMPUS_VSS
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # JOIN PREVIOUS BALANCE AND CURRENT OPENING BALANCE ****************************
    print("Join previous balance and current opening balance...")
    sr_file = "X002dc_vss_prevbal_curopen"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X002da_vss_student_balance_clos.CAMPUS,
        X002da_vss_student_balance_clos.STUDENT,
        X002da_vss_student_balance_clos.BALANCE As BAL_CLOS,
        X002da_vss_student_balance_open.BALANCE As BAL_OPEN,
        0.00 AS DIFF_BAL
    From
        X002da_vss_student_balance_clos Left Join
        X002da_vss_student_balance_open On X002da_vss_student_balance_open.STUDENT = X002da_vss_student_balance_clos.STUDENT
            And X002da_vss_student_balance_open.CAMPUS = X002da_vss_student_balance_clos.CAMPUS
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)    
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # Add column vss debit amount
    print("Correct the open balance column id null...")
    so_curs.execute("UPDATE X002dc_vss_prevbal_curopen " + """
                    SET BAL_OPEN =
                    CASE
                      WHEN BAL_OPEN IS NULL THEN 0.00
                      ELSE BAL_OPEN
                    END
                    ;""")
    so_conn.commit()
    # Add column vss debit amount
    print("Add column vss dt amount...")
    so_curs.execute("UPDATE X002dc_vss_prevbal_curopen " + """
                    SET DIFF_BAL = round(BAL_OPEN - BAL_CLOS,2)
                    ;""")
    so_conn.commit()

    # SELECT STUDENTS WHERE OPENING BALANCE DIFFER FROM CLOSING BALANCE ************
    print("Select students where closing and opening balances differ...")
    sr_file = "X002dd_vss_closing_open_differ"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X002dc_vss_prevbal_curopen.STUDENT,
      X002dc_vss_prevbal_curopen.CAMPUS,
      X002dc_vss_prevbal_curopen.BAL_CLOS,
      X002dc_vss_prevbal_curopen.BAL_OPEN,
      X002dc_vss_prevbal_curopen.DIFF_BAL
    FROM
      X002dc_vss_prevbal_curopen
    WHERE
      X002dc_vss_prevbal_curopen.DIFF_BAL <> 0
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)    
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # DETERMINE BALANCE CHANGE TYPE
    print("Determine blanace change type...")
    sr_file = "X002de_vss_differ_type"
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        TYPE.STUDENT,
        Count(TYPE.BAL_CLOS) As COUNT,
        Total(TYPE.BAL_OPEN) As TOTAL_BAL_OPEN,
        Total(TYPE.DIFF_BAL) As TOTAL_DIFF_BAL
    From
        X002dd_vss_closing_open_differ TYPE
    Group By
        TYPE.STUDENT
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # JOIN DIFFERENCES AND TYPES
    print("Join differences and types...")
    sr_file = "X002df_vss_differ_join"
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        DIFF.STUDENT,
        DIFF.CAMPUS,
        DIFF.BAL_CLOS,
        DIFF.BAL_OPEN,
        DIFF.DIFF_BAL,
        TYPE.COUNT,
        TYPE.TOTAL_BAL_OPEN,
        TYPE.TOTAL_DIFF_BAL
    From
        X002dd_vss_closing_open_differ DIFF Left Join
        X002de_vss_differ_type TYPE On TYPE.STUDENT = DIFF.STUDENT
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ISOLATE THE ACCOUNTS WHERE CLOSE / OPEN BALANCES DIFFER
    print("Isolate close open balances...")
    sr_file = "X002dg_vss_differ_close_open_differ"
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        *
    From
        X002df_vss_differ_join
    Where
        X002df_vss_differ_join.COUNT = 1
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ISOLATE THE ACCOUNTS CAMPUS DIFFER WITH ZERO BALANCE
    print("Isolate campus differ zero balance...")
    sr_file = "X002dh_vss_differ_campus_differ_zerobal"
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        *
    From
        X002df_vss_differ_join
    Where
        X002df_vss_differ_join.COUNT != 1 And
        X002df_vss_differ_join.TOTAL_BAL_OPEN = 0
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ISOLATE THE ACCOUNTS CAMPUS DIFFER WITH BALANCE
    print("Isolate campus differ balance...")
    sr_file = "X002di_vss_differ_campus_differ_bal"
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        *
    From
        X002df_vss_differ_join
    Where
        X002df_vss_differ_join.COUNT != 1 And
        X002df_vss_differ_join.TOTAL_BAL_OPEN != 0
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # EXTRACT PRE DATED TRANSACTIONS ***********************************************
    print("Extract pre dated transactions...")
    sr_file = "X002fa_vss_tran_predate"
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
      Strftime('%Y',TRANSDATE_VSS) - Strftime('%Y',X002ab_vss_transort.POSTDATEDTRANSDATE) = 1 AND
      Strftime('%Y',TRANSDATE_VSS) = '%CYEAR%'    
    ;"""
    s_sql = s_sql.replace("%CYEAR%", s_year)
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # SUMM PRE DATED TRANSACTIONS *************************************************
    print("Summ pre dated transactions...")
    sr_file = "X002fb_vss_tran_predate_summ"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X002fa_vss_tran_predate.CAMPUS_VSS,
      X002fa_vss_tran_predate.TRANSCODE_VSS,
      X002fa_vss_tran_predate.DESCRIPTION_E,
      Total(X002fa_vss_tran_predate.AMOUNT_VSS) AS Total_AMOUNT_VSS
    FROM
      X002fa_vss_tran_predate
    GROUP BY
      X002fa_vss_tran_predate.CAMPUS_VSS,
      X002fa_vss_tran_predate.TRANSCODE_VSS,
      X002fa_vss_tran_predate.DESCRIPTION_E
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # EXTRACT POST DATED TRANSACTIONS **********************************************
    print("Extract post dated transactions...")
    sr_file = "X002ga_vss_tran_postdate"
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
      Strftime('%Y',TRANSDATE_VSS) - Strftime('%Y',X002ab_vss_transort.POSTDATEDTRANSDATE) = 1 AND
      Strftime('%Y',POSTDATEDTRANSDATE) = '%CYEAR%'    
    ;"""
    s_sql = s_sql.replace("%CYEAR%", s_year)
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # SUMM POST DATED TRANSACTIONS *************************************************
    print("Summ post dated transactions...")
    sr_file = "X002gb_vss_tran_postdate_summ"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X002ga_vss_tran_postdate.CAMPUS_VSS,
      X002ga_vss_tran_postdate.TRANSCODE_VSS,
      X002ga_vss_tran_postdate.DESCRIPTION_E,
      Total(X002ga_vss_tran_postdate.AMOUNT_VSS) AS Total_AMOUNT_VSS
    FROM
      X002ga_vss_tran_postdate
    GROUP BY
      X002ga_vss_tran_postdate.CAMPUS_VSS,
      X002ga_vss_tran_postdate.TRANSCODE_VSS,
      X002ga_vss_tran_postdate.DESCRIPTION_E
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    """*************************************************************************
    JOIN VSS & GL MONTHLY TOTALS
    *************************************************************************"""
    print("JOIN VSS & GL MONTHLY TOTALS")
    funcfile.writelog("JOIN VSS & GL MONTHLY TOTALS")
    
    # Join vss gl monthly account totals ******************************************
    print("Join vss and gl monthly totals...")
    sr_file = "X002ea_vss_gl_balance_month"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      UPPER(SUBSTR(VSSBAL.CAMPUS_VSS,1,3))||TRIM(VSSBAL.MONTH_VSS) AS ROWID,
      CASE
          WHEN VSSBAL.MONTH_VSS = '%CMONTH%' THEN 'Y'
          ELSE 'N'
      END As CURRENT,
      'NWU' AS ORG,
      VSSBAL.CAMPUS_VSS AS CAMPUS,
      VSSBAL.MONTH_VSS AS MONTH,
      VSSBAL.AMOUNT_DT AS VSS_TRAN_DT,
      VSSBAL.AMOUNT_CT AS VSS_TRAN_CT,
      VSSBAL.AMOUNT AS VSS_TRAN,
      VSSBAL.RUNBAL AS VSS_RUNBAL,
      GLBAL.BALANCE AS GL_TRAN,
      GLBAL.RUNBAL AS GL_RUNBAL
    FROM
      X002ce_vss_balmonth_calc_runbal VSSBAL
      LEFT JOIN X001ce_gl_balmonth_calc_runbal GLBAL ON GLBAL.CAMPUS = VSSBAL.CAMPUS_VSS AND
        GLBAL.MONTH = VSSBAL.MONTH_VSS
    ;"""
    #WHERE
    #  X002ce_vss_balmonth_calc_runbal.MONTH_VSS <= '%PMONTH%'
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    s_sql = s_sql.replace("%CMONTH%",funcdate.cur_month()) 
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
      a.CURRENT,
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
    sx_path = re_path + funcdate.cur_year() + "/"
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
      BAL.CAMPUS,
      BAL.MONTH,
      BAL.CURRENT,
      BAL.VSS_TRAN_DT,
      BAL.VSS_TRAN_CT,
      BAL.VSS_TRAN,
      BAL.VSS_RUNBAL,
      BAL.GL_TRAN,
      BAL.GL_RUNBAL,
      BAL.DIFF,
      BAL.MOVE,
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
      X002ea_vss_gl_balance_month_move BAL
      LEFT JOIN X002eb_impo_report_officer CAMP_OFFICER ON CAMP_OFFICER.CAMPUS = BAL.CAMPUS
      LEFT JOIN X002eb_impo_report_officer ORG_OFFICER ON ORG_OFFICER.CAMPUS = BAL.ORG
      LEFT JOIN X002ec_impo_report_supervisor CAMP_SUPERVISOR ON CAMP_SUPERVISOR.CAMPUS = BAL.CAMPUS
      LEFT JOIN X002ec_impo_report_supervisor ORG_SUPERVISOR ON ORG_SUPERVISOR.CAMPUS = BAL.ORG
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*************************************************************************
    JOIN VSS & GL TRANSACTIONS
    *************************************************************************"""
    print("JOIN VSS & GL TRANSACTIONS")
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
    s_sql = s_sql.replace("%CYEAR%", s_year)
    # s_sql = s_sql.replace("%PMONTH%",gl_month)
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
    s_sql = s_sql.replace("%CYEAR%", s_year)
    # s_sql = s_sql.replace("%PMONTH%",gl_month)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # Transfer GL transactions to the VSS file *********************************
    # Open the SOURCE file to obtain column headings
    print("Transfer gl data to the vss table...")
    funcfile.writelog("%t GET COLUMN NAME: X003aa_vss_gl_join")
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
    funcfile.writelog("%t BUILD TABLE: X003aa_vss_gl_join with " + str(i_tota) + " rows")

    # Report on VSS and GL comparison per campus per month *********************
    print("Report vss gl join transaction type...")
    sr_file = "X003ax_vss_gl_join"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      TRAN.CAMPUS_VSS AS CAMPUS,
      TRAN.MONTH_VSS AS MONTH,
      TRAN.TRANSCODE_VSS AS TRANCODE,
      TRAN.TEMP_DESC_E AS VSS_DESCRIPTION,
      CAST(TRAN.AMOUNT_VSS AS REAL) AS VSS_AMOUNT,
      TRAN.DESC_VSS AS GL_DESCRIPTION,
      CAST(TRAN.AMOUNT AS REAL) AS GL_AMOUNT,
      TRAN.DIFF,
      TRAN.MATCHED,
      TRAN.PERIOD,
      CASE
          WHEN TRAN.MONTH_VSS = '%CMONTH%' THEN 'Y'
          ELSE 'N'
      END As CURRENT
    FROM
      X003aa_vss_gl_join TRAN
    ORDER BY
      CAMPUS,
      MONTH,
      TRANCODE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    s_sql = s_sql.replace("%CMONTH%",funcdate.cur_month())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
    if l_export:
        print("Export vss gl recon...")
        sr_filet = sr_file
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "Debtor_003_vss_gl_recon_"
        sx_filet = sx_file + funcdate.cur_monthendfile()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
        funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    """*************************************************************************
    TEST MATCHED TRANSACTION TYPES
    *************************************************************************"""
    print("TEST MATCHED TRANSACTION TYPES")
    funcfile.writelog("TEST MATCHED TRANSACTION TYPES")

    # Compile a summary of matched transaction types *******************************
    print("Compile a summary of matched transaction types...")
    sr_file = "X004aa_matched_summary"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
        TRAN.CAMPUS,
        TRAN.MONTH,
        Round(Total(TRAN.VSS_AMOUNT),2) AS VSS_AMOUNT,
        Round(Total(TRAN.GL_AMOUNT),2) AS GL_AMOUNT
    FROM
        X003ax_vss_gl_join TRAN
    WHERE
        TRAN.MATCHED = 'C'
    GROUP BY
        TRAN.CAMPUS,
        TRAN.MONTH,
        TRAN.MATCHED
    ORDER BY
        TRAN.MONTH,
        TRAN.CAMPUS
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*************************************************************************
    TEST VSS GL DIFFERENCE TRANSACTION SUMMARY
    *************************************************************************"""
    print("VSS GL DIFFERENCE TRANSACTION SUMMARY")
    funcfile.writelog("VSS GL DIFFERENCE TRANSACTION SUMMARY")

    # DECLARE VARIABLES
    i_finding_after: int = 0

    # OBTAIN TEST DATA
    print("Identify vss gl differences...")
    sr_file = "X004ba_vss_gl_difference"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
        'NWU' AS ORG,
        Upper(TRAN.CAMPUS) As CAMPUS,
        TRAN.MONTH,
        TRAN.TRANCODE AS TRAN_TYPE,
        TRAN.VSS_DESCRIPTION AS TRAN_DESCRIPTION,
        TRAN.VSS_AMOUNT AS AMOUNT_VSS,
        TRAN.GL_AMOUNT AS AMOUNT_GL,
        Round(TRAN.DIFF,2) As DIFF
    FROM
        X003ax_vss_gl_join TRAN
    WHERE
        TRAN.MATCHED = 'X' AND
        TRAN.TRANCODE <> 'X' AND
        INSTR('%BURSARY%', TRAN.TRANCODE) = 0 AND
        TRAN.MONTH <= '%PMONTH%' AND
        TRAN.GL_AMOUNT Is Not Null
    ORDER BY
        TRAN.MONTH,
        TRAN.CAMPUS,
        TRAN.TRANCODE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%BURSARY%", s_burs_code)
    s_sql = s_sql.replace("%PMONTH%", gl_month)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X004bb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        *
    From
        X004ba_vss_gl_difference CURR
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " VSS GL DIFFERENCE finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X004bc_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 INT,
            FIELD2 TEXT,
            FIELD3 TEXT,
            FIELD4 TEXT,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            DATE_MAILED TEXT)
            """)
        s_cols = ""
        co = open(ed_path + "200_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "vss gl difference":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[
                    2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[
                             7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the imported data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "200_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    sr_file = "X004bd_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'vss gl difference' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.DATE_MAILED
        From
            X004bb_findings FIND Left Join
            X004bc_get_previous PREV ON PREV.FIELD1 = FIND.CAMPUS AND
                PREV.FIELD2 = FIND.MONTH And
                PREV.FIELD3 = FIND.TRAN_TYPE And
                PREV.FIELD4 = FIND.AMOUNT_VSS And
                PREV.FIELD5 = FIND.AMOUNT_GL And
                PREV.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_yearend())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X004be_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.CAMPUS AS FIELD1,
            PREV.MONTH AS FIELD2,
            PREV.TRAN_TYPE AS FIELD3,
            PREV.AMOUNT_VSS AS FIELD4,
            PREV.AMOUNT_GL AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.DATE_MAILED
        From
            X004bd_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "200_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X004bf_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if i_finding_after > 0:
            print("Import reporting officers for mail purposes...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                OFFICER.LOOKUP,
                Upper(OFFICER.LOOKUP_CODE) AS CAMPUS,
                OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
                PEOP.NAME_ADDR As NAME,
                PEOP.EMAIL_ADDRESS
            From
                VSS.X000_OWN_LOOKUPS OFFICER Left Join
                PEOPLE.X002_PEOPLE_CURR PEOP ON
                    PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_debt_recon_test_amount_differ_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X004bg_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            SUPERVISOR.LOOKUP,
            Upper(SUPERVISOR.LOOKUP_CODE) AS CAMPUS,
            SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            VSS.X000_OWN_LOOKUPS SUPERVISOR Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON 
                PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_debt_recon_test_amount_differ_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X004bh_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.CAMPUS,
            PREV.MONTH,
            PREV.TRAN_TYPE,
            PREV.TRAN_DESCRIPTION,
            PREV.AMOUNT_VSS,
            PREV.AMOUNT_GL,
            PREV.DIFF,    
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END As ORG_SUP_MAIL
        From
            X004bd_add_previous PREV
            Left Join X004bf_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.CAMPUS
            Left Join X004bf_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
            Left Join X004bg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.CAMPUS
            Left Join X004bg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X004bx_vss_gl_difference"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'VSS GL DIFFERENCE' As Audit_finding,
            FIND.ORG As Organization,
            FIND.CAMPUS As Campus,
            FIND.MONTH As Month,
            FIND.TRAN_TYPE As Tran_type,
            FIND.TRAN_DESCRIPTION As Tran_description,
            FIND.AMOUNT_VSS As Amount_vss,
            FIND.AMOUNT_GL As Amount_gl,
            FIND.DIFF As Amount_difference,    
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail            
        From
            X004bh_detail FIND
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "Vssgl_test_004bx_vss_gl_difference_"
            sx_file_dated = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*************************************************************************
    TEST IN VSS NO GL TRANSACTIONS
    *************************************************************************"""
    print("IN VSS NO GL TRANSACTIONS")
    funcfile.writelog("IN VSS NO GL TRANSACTIONS")

    # DECLARE VARIABLES
    i_finding_after: int = 0

    # OBTAIN TEST DATA
    print("Identify vss gl differences...")
    sr_file = "X004ca_invss_nogl"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
        'NWU' AS ORG,
        Upper(TRAN.CAMPUS) As CAMPUS,
        TRAN.MONTH,
        TRAN.TRANCODE AS TRAN_TYPE,
        TRAN.VSS_DESCRIPTION AS TRAN_DESCRIPTION,
        Round(TRAN.VSS_AMOUNT,2) AS AMOUNT_VSS
    FROM
        X003ax_vss_gl_join TRAN
    WHERE
        TRAN.MATCHED = 'X' AND
        TRAN.TRANCODE <> 'X' AND
        INSTR('%BURSARY%', TRAN.TRANCODE) = 0 AND
        TRAN.MONTH <= '%PMONTH%' AND
        TRAN.GL_AMOUNT Is Null
    ORDER BY
        TRAN.MONTH,
        TRAN.CAMPUS,
        TRAN.TRANCODE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%BURSARY%", s_burs_code)
    s_sql = s_sql.replace("%PMONTH%", gl_month)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X004cb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        *
    From
        X004ca_invss_nogl CURR
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " IN VSS NO GL finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X004cc_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 TEXT,
            FIELD2 TEXT,
            FIELD3 TEXT,
            FIELD4 REAL,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            DATE_MAILED TEXT)
            """)
        s_cols = ""
        co = open(ed_path + "200_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "in vss no gl":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[
                    2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[
                             7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the imported data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "200_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    sr_file = "X004cd_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'in vss no gl' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.DATE_MAILED
        From
            X004cb_findings FIND Left Join
            X004cc_get_previous PREV ON PREV.FIELD1 = FIND.CAMPUS AND
                PREV.FIELD2 = FIND.MONTH And
                PREV.FIELD3 = FIND.TRAN_TYPE And
                PREV.FIELD4 = FIND.AMOUNT_VSS And
                PREV.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_yearend())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X004ce_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.CAMPUS AS FIELD1,
            PREV.MONTH AS FIELD2,
            PREV.TRAN_TYPE AS FIELD3,
            PREV.AMOUNT_VSS AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.DATE_MAILED
        From
            X004cd_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "200_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X004cf_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if i_finding_after > 0:
            print("Import reporting officers for mail purposes...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                OFFICER.LOOKUP,
                Upper(OFFICER.LOOKUP_CODE) AS CAMPUS,
                OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
                PEOP.NAME_ADDR As NAME,
                PEOP.EMAIL_ADDRESS
            From
                VSS.X000_OWN_LOOKUPS OFFICER Left Join
                PEOPLE.X002_PEOPLE_CURR PEOP ON
                    PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_debt_recon_test_invss_nogl_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X004cg_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            SUPERVISOR.LOOKUP,
            Upper(SUPERVISOR.LOOKUP_CODE) AS CAMPUS,
            SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            VSS.X000_OWN_LOOKUPS SUPERVISOR Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON 
                PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_debt_recon_test_invss_nogl_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X004ch_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.CAMPUS,
            PREV.MONTH,
            PREV.TRAN_TYPE,
            PREV.TRAN_DESCRIPTION,
            PREV.AMOUNT_VSS,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END As ORG_SUP_MAIL
        From
            X004cd_add_previous PREV
            Left Join X004cf_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.CAMPUS
            Left Join X004cf_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
            Left Join X004cg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.CAMPUS
            Left Join X004cg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X004cx_invss_nogl"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'IN VSS NO GL' As Audit_finding,
            FIND.ORG As Organization,
            FIND.CAMPUS As Campus,
            FIND.MONTH As Month,
            FIND.TRAN_TYPE As Tran_type,
            FIND.TRAN_DESCRIPTION As Tran_description,
            FIND.AMOUNT_VSS As Amount_vss,
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail            
        From
            X004ch_detail FIND
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "Vssgl_test_004cx_invss_nogl_"
            sx_file_dated = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*************************************************************************
    TEST IN GL NO VSS TRANSACTIONS
    *************************************************************************"""
    print("IN GL NO VSS TRANSACTIONS")
    funcfile.writelog("IN GL NO VSS TRANSACTIONS")

    # DECLARE VARIABLES
    i_finding_after: int = 0

    # OBTAIN TEST DATA
    print("Identify transactions in the gl but not in vss...")
    sr_file = "X004da_ingl_novss"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
        'NWU' AS ORG,
        Upper(TRAN.CAMPUS) As CAMPUS,
        TRAN.MONTH,
        TRAN.GL_DESCRIPTION,
        Round(TRAN.GL_AMOUNT,2) As GL_AMOUNT
    FROM
        X003ax_vss_gl_join TRAN
    WHERE
        TRAN.MATCHED = 'X' AND
        TRAN.TRANCODE = 'X'
    ORDER BY
        TRAN.MONTH,
        TRAN.CAMPUS,
        TRAN.TRANCODE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%BURSARY%", s_burs_code)
    s_sql = s_sql.replace("%PMONTH%", gl_month)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X004db_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        *
    From
        X004da_ingl_novss CURR
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " IN GL NO VSS finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X004dc_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 TEXT,
            FIELD2 TEXT,
            FIELD3 TEXT,
            FIELD4 REAL,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            DATE_MAILED TEXT)
            """)
        s_cols = ""
        co = open(ed_path + "200_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "in gl no vss":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[
                    2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[
                             7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the imported data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "200_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    sr_file = "X004dd_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'in gl no vss' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.DATE_MAILED
        From
            X004db_findings FIND Left Join
            X004dc_get_previous PREV ON PREV.FIELD1 = FIND.CAMPUS AND
                PREV.FIELD2 = FIND.MONTH And
                PREV.FIELD3 = FIND.GL_DESCRIPTION And
                PREV.FIELD4 = FIND.GL_AMOUNT And
                PREV.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_yearend())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X004de_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.CAMPUS AS FIELD1,
            PREV.MONTH AS FIELD2,
            PREV.GL_DESCRIPTION AS FIELD3,
            PREV.GL_AMOUNT AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.DATE_MAILED
        From
            X004dd_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "200_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X004df_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if i_finding_after > 0:
            print("Import reporting officers for mail purposes...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                OFFICER.LOOKUP,
                Upper(OFFICER.LOOKUP_CODE) AS CAMPUS,
                OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
                PEOP.NAME_ADDR As NAME,
                PEOP.EMAIL_ADDRESS
            From
                VSS.X000_OWN_LOOKUPS OFFICER Left Join
                PEOPLE.X002_PEOPLE_CURR PEOP ON
                    PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_debt_recon_test_ingl_novss_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X004dg_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            SUPERVISOR.LOOKUP,
            Upper(SUPERVISOR.LOOKUP_CODE) AS CAMPUS,
            SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            VSS.X000_OWN_LOOKUPS SUPERVISOR Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON 
                PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_debt_recon_test_ingl_novss_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X004dh_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.CAMPUS,
            PREV.MONTH,
            PREV.GL_DESCRIPTION,
            PREV.GL_AMOUNT,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END As ORG_SUP_MAIL
        From
            X004dd_add_previous PREV
            Left Join X004df_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.CAMPUS
            Left Join X004df_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
            Left Join X004dg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.CAMPUS
            Left Join X004dg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
            PREV.PREV_PROCESS IS NULL
        Order By
            PREV.CAMPUS,
            PREV.MONTH,
            PREV.GL_DESCRIPTION
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X004dx_ingl_novss"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'IN GL NO VSS' As Audit_finding,
            FIND.ORG As Organization,
            FIND.CAMPUS As Campus,
            FIND.MONTH As Month,
            FIND.GL_DESCRIPTION As Gl_description,
            FIND.GL_AMOUNT As Amount_gl,
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail            
        From
            X004dh_detail FIND
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "Vssgl_test_004dx_ingl_novss_"
            sx_file_dated = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*************************************************************************
    TEST VSS GL BURSARY DIFFERENCE TRANSACTION SUMMARY
    *************************************************************************"""
    print("VSS GL BURSARY DIFFERENCE TRANSACTION SUMMARY")
    funcfile.writelog("VSS GL BURSARY DIFFERENCE TRANSACTION SUMMARY")

    # DECLARE VARIABLES
    i_finding_after: int = 0

    # OBTAIN TEST DATA
    print("Identify vss gl bursary differences...")
    sr_file = "X004ea_vss_gl_burs_difference"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
        'NWU' AS ORG,
        Upper(TRAN.CAMPUS) As CAMPUS,
        TRAN.MONTH,
        TRAN.TRANCODE AS TRAN_TYPE,
        TRAN.VSS_DESCRIPTION AS TRAN_DESCRIPTION,
        TRAN.VSS_AMOUNT AS AMOUNT_VSS,
        TRAN.GL_AMOUNT AS AMOUNT_GL,
        Round(TRAN.DIFF,2) As DIFF
    FROM
        X003ax_vss_gl_join TRAN
    WHERE
        TRAN.MATCHED = 'X' AND
        TRAN.TRANCODE <> 'X' AND
        INSTR('%BURSARY%', TRAN.TRANCODE) > 0 AND
        TRAN.MONTH <= '%PMONTH%'
    ORDER BY
        TRAN.MONTH,
        TRAN.CAMPUS,
        TRAN.TRANCODE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%BURSARY%", s_burs_code)
    s_sql = s_sql.replace("%PMONTH%", gl_month)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X004eb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        *
    From
        X004ea_vss_gl_burs_difference CURR
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " VSS GL DIFFERENCE BURSARY finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X004ec_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 TEXT,
            FIELD2 TEXT,
            FIELD3 TEXT,
            FIELD4 REAL,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            DATE_MAILED TEXT)
            """)
        s_cols = ""
        co = open(ed_path + "200_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "vss gl bursary difference":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[
                    2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[
                             7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the imported data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "200_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    sr_file = "X004ed_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'vss gl bursary difference' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.DATE_MAILED
        From
            X004eb_findings FIND Left Join
            X004ec_get_previous PREV ON PREV.FIELD1 = FIND.CAMPUS AND
                PREV.FIELD2 = FIND.MONTH And
                PREV.FIELD3 = FIND.TRAN_TYPE And
                PREV.FIELD4 = FIND.DIFF And
                PREV.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_yearend())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X004ee_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.CAMPUS AS FIELD1,
            PREV.MONTH AS FIELD2,
            PREV.TRAN_TYPE AS FIELD3,
            Cast(PREV.DIFF As REAL) AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.DATE_MAILED
        From
            X004ed_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "200_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X004ef_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if i_finding_after > 0:
            print("Import reporting officers for mail purposes...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                OFFICER.LOOKUP,
                Upper(OFFICER.LOOKUP_CODE) AS CAMPUS,
                OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
                PEOP.NAME_ADDR As NAME,
                PEOP.EMAIL_ADDRESS
            From
                VSS.X000_OWN_LOOKUPS OFFICER Left Join
                PEOPLE.X002_PEOPLE_CURR PEOP ON
                    PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_debt_recon_test_bursary_amount_differ_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X004eg_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            SUPERVISOR.LOOKUP,
            Upper(SUPERVISOR.LOOKUP_CODE) AS CAMPUS,
            SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            VSS.X000_OWN_LOOKUPS SUPERVISOR Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON 
                PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_debt_recon_test_bursary_amount_differ_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X004eh_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.CAMPUS,
            PREV.MONTH,
            PREV.TRAN_TYPE,
            PREV.TRAN_DESCRIPTION,
            PREV.AMOUNT_VSS,
            PREV.AMOUNT_GL,
            PREV.DIFF,    
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END As ORG_SUP_MAIL
        From
            X004ed_add_previous PREV
            Left Join X004ef_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.CAMPUS
            Left Join X004ef_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
            Left Join X004eg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.CAMPUS
            Left Join X004eg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X004ex_vss_gl_burs_difference"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'VSS GL DIFFERENCE BURSARY' As Audit_finding,
            FIND.ORG As Organization,
            FIND.CAMPUS As Campus,
            FIND.MONTH As Month,
            FIND.TRAN_TYPE As Tran_type,
            FIND.TRAN_DESCRIPTION As Tran_description,
            FIND.AMOUNT_VSS As Amount_vss,
            FIND.AMOUNT_GL As Amount_gl,
            FIND.DIFF As Amount_difference,    
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail            
        From
            X004eh_detail FIND
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "Vssgl_test_004ex_vss_gl_burs_difference_"
            sx_file_dated = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ************************************************************************
    BURSARY VSS GL RECON
        X010aa Extract vss bursary transactions (codes 042,052,381,382,500)
        X010ab Summarize vss bursary transactions per campus month
        X002cd Summarize vss bursary transactions per transaction code
        X001cd Extract gl bursary transactions
        X010bb Summarize gl bursary transactions per campus month
        X010ca Join vss gl bursary summary totals
        X010cb Join the matched vss and gl bursary transactions
        X010cc Group matched vss gl transactions on campus & month
        X011aa Join vss gl bursary summary totals
        X011ab Join vss gl bursary summary totals
        X011ac Import reporting supervisors from VSS.SQLITE
        X011ax Add the reporting officer and supervisor
    *************************************************************************"""
    print("BURSARY VSS GL RECON")
    funcfile.writelog("BURSARY VSS GL RECON")

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
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_002_vsssummtypeburs_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
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
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_001_glsummtypeburs_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
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
    s_sql = s_sql.replace("%CYEAR%", s_year)
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

    # Group matched vss gl transactions on campus & month
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
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_010c_burs_matched_summmonth_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    #funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    """*************************************************************************
    TEST BURSARY INGL NOVSS
        X010da Identify bursary transactions in gl but not in vss
    *************************************************************************"""
    print("TEST BURSARY INGL NOVSS")
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
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_010d_test_burs_ingl_novss_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    """*************************************************************************
    TEST BURSARY INVSS NOGL
    *************************************************************************"""
    print("TEST BURSARY INVSS NOGL")
    funcfile.writelog("TEST BURSARY INVSS NOGL")

    # DECLARE VARIABLES
    i_finding_after: int = 0

    # IDENTIFY TRANSACTIONS
    print("Test bursary transactions in vss not in gl...")
    sr_file = "X010ea_test_burs_invss_nogl"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
        'NWU' As ORG,
        Upper(TRAN.CAMPUS_VSS) As CAMPUS_VSS,
        TRAN.MONTH_VSS,
        TRAN.STUDENT_VSS,
        TRAN.TRANSDATE_VSS,
        TRAN.TRANSCODE_VSS,
        TRAN.TRANSDESC_VSS,
        Round(TRAN.AMOUNT_VSS,2) As AMOUNT_VSS,
        Cast(Case
            When Cast(TRAN.BURSCODE_VSS As INT) > 0 Then TRAN.BURSCODE_VSS
            Else 0
        End As INT) As BURSCODE_VSS,
        TRAN.BURSNAAM_VSS,
        TRAN.TRANSUSER_VSS  
    FROM
        X010ca_join_vss_gl_burs TRAN
    WHERE
        TRAN.MATCHED = 'X' And
        TRAN.MONTH_VSS <= '%PMONTH%'
    ORDER BY
        TRAN.CAMPUS_VSS,
        TRAN.MONTH_VSS,
        TRAN.TRANSCODE_VSS,
        TRAN.BURSCODE_VSS
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%PMONTH%", gl_month)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X010eb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        *
    From
        X010ea_test_burs_invss_nogl CURR
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " BURSARY IN VSS NO GL finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X010ec_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 INT,
            FIELD2 TEXT,
            FIELD3 TEXT,
            FIELD4 REAL,
            FIELD5 INT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            DATE_MAILED TEXT)
            """)
        s_cols = ""
        co = open(ed_path + "200_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "bursary in vss no gl":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[
                    2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[
                             7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the imported data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "200_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    sr_file = "X010ed_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'bursary in vss no gl' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.DATE_MAILED
        From
            X010eb_findings FIND Left Join
            X010ec_get_previous PREV ON PREV.FIELD1 = FIND.STUDENT_VSS AND
                PREV.FIELD2 = FIND.CAMPUS_VSS And
                PREV.FIELD3 = FIND.TRANSDATE_VSS And
                PREV.FIELD4 = FIND.AMOUNT_VSS And
                PREV.FIELD5 = FIND.BURSCODE_VSS And
                PREV.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_yearend())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X010ee_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.STUDENT_VSS AS FIELD1,
            PREV.CAMPUS_VSS AS FIELD2,
            PREV.TRANSDATE_VSS AS FIELD3,
            Cast(PREV.AMOUNT_VSS As REAL) AS FIELD4,
            PREV.BURSCODE_VSS AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.DATE_MAILED
        From
            X010ed_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "200_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X010ef_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if i_finding_after > 0:
            print("Import reporting officers for mail purposes...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                OFFICER.LOOKUP,
                Upper(OFFICER.LOOKUP_CODE) AS CAMPUS,
                OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
                PEOP.NAME_ADDR As NAME,
                PEOP.EMAIL_ADDRESS
            From
                VSS.X000_OWN_LOOKUPS OFFICER Left Join
                PEOPLE.X002_PEOPLE_CURR PEOP ON
                    PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_debt_recon_test_bursary_invss_nogl_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X010eg_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            SUPERVISOR.LOOKUP,
            Upper(SUPERVISOR.LOOKUP_CODE) AS CAMPUS,
            SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            VSS.X000_OWN_LOOKUPS SUPERVISOR Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON 
                PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_debt_recon_test_bursary_invss_nogl_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X010eh_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.CAMPUS_VSS,
            PREV.STUDENT_VSS,
            PREV.MONTH_VSS,
            PREV.TRANSDATE_VSS,
            PREV.TRANSCODE_VSS,
            PREV.TRANSDESC_VSS,
            PREV.AMOUNT_VSS,
            PREV.BURSCODE_VSS,
            PREV.BURSNAAM_VSS,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END As ORG_SUP_MAIL
        From
            X010ed_add_previous PREV
            Left Join X010ef_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.CAMPUS_VSS
            Left Join X010ef_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
            Left Join X010eg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.CAMPUS_VSS
            Left Join X010eg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X010ex_bursary_invss_nogl"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'IN VSS NO GL BURSARY' As Audit_finding,
            FIND.ORG As Organization,
            FIND.CAMPUS_VSS As Campus,
            FIND.STUDENT_VSS As Student,
            FIND.MONTH_VSS As Month,
            FIND.TRANSDATE_VSS As Tran_date,
            FIND.TRANSCODE_VSS As Tran_type,
            FIND.TRANSDESC_VSS As Tran_description,
            FIND.AMOUNT_VSS As Amount_vss,
            FIND.BURSCODE_VSS As Burs_code,
            FIND.BURSNAAM_VSS As Burs_name,
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail            
        From
            X010eh_detail FIND
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "Vssgl_test_010ex_bursary_invss_nogl_"
            sx_file_dated = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*************************************************************************
    TEST BURSARY VSS GL DIFFERENT CAMPUS
    *************************************************************************"""
    print("TEST BURSARY VSS GL DIFFERENT CAMPUS")
    funcfile.writelog("TEST BURSARY VSS GL DIFFERENT CAMPUS")

    # DECLARE VARIABLES
    i_finding_after: int = 0

    # IDENTIFY TRANSACTIONS
    print("Test bursary transactions posted to different campus in gl...")
    sr_file = "X010fa_burs_gl_diffcampus"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
        'NWU' AS ORG,
        Upper(TRAN.CAMPUS_VSS) As CAMPUS_VSS,
        TRAN.MONTH_VSS,
        TRAN.STUDENT_VSS,
        TRAN.TRANSDATE_VSS,
        TRAN.TRANSCODE_VSS,
        TRAN.TRANSDESC_VSS,
        TRAN.AMOUNT_VSS,
        TRAN.BURSCODE_VSS,
        TRAN.BURSNAAM_VSS,
        Upper(TRAN.CAMPUS_GL) As CAMPUS_GL,
        TRAN.TRANSEDOC_GL,
        TRAN.TRANSENTR_GL,
        TRAN.TRANSDESC_GL,
        TRAN.PERIOD,
        TRAN.MATCHED,
        TRAN.TRANSUSER_VSS
    FROM
        X010cb_join_vss_gl_matched TRAN
    WHERE
        TRAN.MATCHED = 'A' And
        TRAN.MONTH_VSS <= '%PMONTH%'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%PMONTH%", gl_month)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X010fb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        *
    From
        X010fa_burs_gl_diffcampus CURR
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " VSS GL DIFFERENCE BURSARY CAMPUS finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X010fc_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 INT,
            FIELD2 TEXT,
            FIELD3 TEXT,
            FIELD4 REAL,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            DATE_MAILED TEXT)
            """)
        s_cols = ""
        co = open(ed_path + "200_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "bursary vss gl different campus":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[
                    2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[
                             7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the imported data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "200_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    sr_file = "X010fd_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'bursary vss gl different campus' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.DATE_MAILED
        From
            X010fb_findings FIND Left Join
            X010fc_get_previous PREV ON PREV.FIELD1 = FIND.CAMPUS_VSS AND
                PREV.FIELD2 = FIND.MONTH_VSS And
                PREV.FIELD3 = FIND.STUDENT_VSS And
                PREV.FIELD4 = FIND.AMOUNT_VSS And
                PREV.FIELD5 = FIND.BURSCODE_VSS And
                PREV.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_yearend())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X010fe_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.CAMPUS_VSS AS FIELD1,
            PREV.MONTH_VSS AS FIELD2,
            PREV.STUDENT_VSS AS FIELD3,
            Cast(PREV.AMOUNT_VSS As REAL) AS FIELD4,
            PREV.BURSCODE_VSS AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.DATE_MAILED
        From
            X010fd_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "200_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X010ff_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if i_finding_after > 0:
            print("Import reporting officers for mail purposes...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                OFFICER.LOOKUP,
                Upper(OFFICER.LOOKUP_CODE) AS CAMPUS,
                OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
                PEOP.NAME_ADDR As NAME,
                PEOP.EMAIL_ADDRESS
            From
                VSS.X000_OWN_LOOKUPS OFFICER Left Join
                PEOPLE.X002_PEOPLE_CURR PEOP ON
                    PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_debt_recon_test_bursary_vss_gl_diff_campus_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X010fg_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            SUPERVISOR.LOOKUP,
            Upper(SUPERVISOR.LOOKUP_CODE) AS CAMPUS,
            SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            VSS.X000_OWN_LOOKUPS SUPERVISOR Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON 
                PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_debt_recon_test_bursary_vss_gl_diff_campus_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X010fh_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.CAMPUS_VSS,
            PREV.STUDENT_VSS,
            PREV.MONTH_VSS,
            PREV.TRANSDATE_VSS,
            PREV.TRANSCODE_VSS,
            PREV.TRANSDESC_VSS,
            PREV.AMOUNT_VSS,
            PREV.BURSCODE_VSS,
            PREV.BURSNAAM_VSS,
            PREV.CAMPUS_GL,
            PREV.TRANSEDOC_GL,
            PREV.TRANSUSER_VSS,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END As ORG_SUP_MAIL
        From
            X010fd_add_previous PREV
            Left Join X010ff_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.CAMPUS_VSS
            Left Join X010ff_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
            Left Join X010fg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.CAMPUS_VSS
            Left Join X010fg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X010fx_burs_gl_diffcampus"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'VSS GL DIFFERENCE BURSARY CAMPUS' As Audit_finding,
            FIND.ORG As Organization,
            FIND.CAMPUS_VSS As Campus_vss,
            FIND.CAMPUS_GL As Campus_gl,
            FIND.STUDENT_VSS As Student,
            FIND.MONTH_VSS As Month,
            FIND.TRANSDATE_VSS As Tran_date,
            FIND.TRANSCODE_VSS As Tran_type,
            FIND.AMOUNT_VSS As Amount_vss,
            FIND.BURSCODE_VSS As Burs_code,
            FIND.BURSNAAM_VSS As Burs_name,
            FIND.TRANSEDOC_GL As Gl_edoc,
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail            
        From
            X010fh_detail FIND
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "Vssgl_test_010fx_burs_gl_diffcampus_"
            sx_file_dated = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*************************************************************************
    TEST STUDENT BALANCE ON MORE THAN ONE CAMPUS 
    *************************************************************************"""
    print("TEST STUDENT BALANCE ON MORE THAN ONE CAMPUS")
    funcfile.writelog("TEST STUDENT BALANCE ON MORE THAN ONE CAMPUS")

    # BUILD A LIST OF STUDENT BALANCES PER CAMPUS
    print("Build balance per campus list...")
    sr_file = "X020_Balance_per_campus"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        TRAN.STUDENT_VSS,
        TRAN.CAMPUS_VSS,
        Round(Total(TRAN.AMOUNT_VSS),2) As BALANCE
    From
        X002ab_vss_transort TRAN
    Group By
        TRAN.STUDENT_VSS,
        TRAN.CAMPUS_VSS
    Having
        Round(Total(TRAN.AMOUNT_VSS),2) <> 0.00    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF BALANCES PER CAMPUS
    print("Build count per campus list...")
    sr_file = "X020_Count_per_campus"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        STUD.STUDENT_VSS,
        Cast(Count(STUD.CAMPUS_VSS) As INT) As COUNT,
        Total(STUD.BALANCE) As BALANCE
    From
        X020_Balance_per_campus STUD
    Group By
        STUD.STUDENT_VSS
    Having
        Count(STUD.CAMPUS_VSS) > 1
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # LIST THE STUDENTS
    print("Build students list...")
    sr_file = "X020_Students"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        COUNT.STUDENT_VSS,
        COUNT.COUNT,
        CAMP.CAMPUS_VSS,
        CAMP.BALANCE,
        COUNT.BALANCE As BALANCE_TOTAL
    From
        X020_Count_per_campus COUNT Inner Join
        X020_Balance_per_campus CAMP On CAMP.STUDENT_VSS = COUNT.STUDENT_VSS
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*************************************************************************
    END OF SCRIPT
    *************************************************************************"""

    # Close the table connection ***************************************************
    if l_vacuum == True:
        print("Vacuum the database...")
        so_conn.commit()
        so_conn.execute('VACUUM')
        funcfile.writelog("%t VACUUM DATABASE: " + so_file)
    so_conn.commit()
    so_conn.close()

    # Close the log writer *********************************************************
    funcfile.writelog("------------------------------------")
    funcfile.writelog("COMPLETED: C200_REPORT_STUDDEB_RECON")

    return
