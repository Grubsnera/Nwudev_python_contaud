""" C200_REPORT_STUDDEB_RECON **************************************************
***
*** Script to compare VSS and GL student transactions
***
*** Albert J van Rensburg (21162395)
*** 26 Jun 2018
***
*****************************************************************************"""

def Report_studdeb_recon():

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

    # Open the script log file ******************************************************

    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C200_REPORT_STUDDEB_RECON")
    funcfile.writelog("---------------------------------")
    print("-------------------------")
    print("C200_REPORT_STUDDEB_RECON")
    print("-------------------------")
    ilog_severity = 1

    # Declare variables
    so_path = "W:/" #Source database path
    re_path = "R:/Debtorstud/" #Results
    ed_path = "S:/_external_data/" #External data
    so_file = "Kfs_vss_studdeb.sqlite" #Source database
    s_sql = "" #SQL statements
    l_mail = True
    l_export = True

    # Open the SOURCE file
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("%t OPEN DATABASE: Kfs")

    # Attach data sources
    so_curs.execute("ATTACH DATABASE 'W:/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Kfs.sqlite' AS 'KFS'")
    funcfile.writelog("%t ATTACH DATABASE: KFS_SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Vss.sqlite' AS 'VSS'")
    funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")

    """*************************************************************************
    ***
    *** PREPARE GL TRANSACTIONS
    ***
    *** Import GL transactions from KFS.SQLITE
    ***   Add required fields
    ***
    *************************************************************************"""

    print("--- PREPARE GL TRANSACTIONS ---")
    funcfile.writelog("%t ---------- PREPARE GL TRANSACTIONS ----------")

    # Import gl transactions **************************************************
    print("Import gl transactions from KFS.SQLITE...")
    sr_file = "X001aa_gl_tranlist"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      *
    FROM
      KFS.X000_GL_trans_curr
    WHERE
      (KFS.X000_GL_trans_curr.FIN_OBJECT_CD = '7551') OR
      (KFS.X000_GL_trans_curr.FIN_OBJECT_CD = '7552') OR
      (KFS.X000_GL_trans_curr.FIN_OBJECT_CD = '7553')
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
                       ELSE 'NEW_ACCOUNT_ALLOCATE'
                    END
                    ;""")
    so_conn.commit()
    funcfile.writelog("%t ADD COLUMN: Campus code")
    # Calc month
    print("Add column gl transaction month...")    
    so_curs.execute("ALTER TABLE X001aa_gl_tranlist ADD COLUMN MONTH TEXT;")
    so_curs.execute("UPDATE X001aa_gl_tranlist " + """
                    SET MONTH = 
                    CASE
                       WHEN UNIV_FISCAL_PRD_CD = 'BB' THEN '00'
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

    # Build sort rename column gl transaction file *****************************
    print("Build and sort gl transaction file...")
    sr_file = "X001ab_gl_transort"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      VSS.X000_Orgunitinstance.ORGUNIT_NAME AS CAMPUS,
      VSS.X000_Orgunitinstance.FORGUNITNUMBER AS CAMPUS_VSS,
      X001aa_gl_tranlist.UNIV_FISCAL_YR AS YEAR,
      X001aa_gl_tranlist.MONTH,
      X001aa_gl_tranlist.CALC_COST_STRING AS COST_STRING,
      X001aa_gl_tranlist.TRANSACTION_DT AS DATE_TRAN,
      X001aa_gl_tranlist.FDOC_NBR AS EDOC,
      X001aa_gl_tranlist.CALC_AMOUNT AS AMOUNT,
      X001aa_gl_tranlist.TRN_LDGR_ENTR_DESC AS DESC_FULL,
      X001aa_gl_tranlist.DESCRIPTION AS DESC_VSS,
      X001aa_gl_tranlist.BURSARY_CODE AS BURSARY,
      X001aa_gl_tranlist.STUDENT,
      X001aa_gl_tranlist.FS_ORIGIN_CD AS ORIGIN_CODE,
      X001aa_gl_tranlist.FS_DATABASE_DESC AS ORIGIN,
      X001aa_gl_tranlist.ORG_NM AS ORG_NAME,
      X001aa_gl_tranlist.ACCOUNT_NM AS ACC_NAME,
      X001aa_gl_tranlist.FIN_OBJ_CD_NM AS OBJ_NAME,
      X001aa_gl_tranlist.ACCT_TYP_NM AS ACC_TYPE,
      X001aa_gl_tranlist.TRN_POST_DT AS DATE_POST,
      X001aa_gl_tranlist."TIMESTAMP" AS TIME_POST,
      X001aa_gl_tranlist.FIN_COA_CD AS ORG,
      X001aa_gl_tranlist.ACCOUNT_NBR AS ACC,
      X001aa_gl_tranlist.FIN_OBJECT_CD AS OBJ,
      X001aa_gl_tranlist.FIN_BALANCE_TYP_CD AS BAL_TYPE,
      X001aa_gl_tranlist.FIN_OBJ_TYP_CD AS OBJ_TYPE,
      X001aa_gl_tranlist.FDOC_TYP_CD AS DOC_TYPE,
      X001aa_gl_tranlist.TRN_ENTR_SEQ_NBR,
      X001aa_gl_tranlist.FDOC_REF_TYP_CD,
      X001aa_gl_tranlist.FS_REF_ORIGIN_CD,
      X001aa_gl_tranlist.FDOC_REF_NBR,
      X001aa_gl_tranlist.FDOC_REVERSAL_DT,
      X001aa_gl_tranlist.TRN_ENCUM_UPDT_CD
    FROM
      X001aa_gl_tranlist
      LEFT JOIN VSS.X000_Orgunitinstance ON VSS.X000_Orgunitinstance.KBUSINESSENTITYID = X001aa_gl_tranlist.BUSINESSENTITYID
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
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    # Build old acl vss gl transaction links ***********************************
    print("Build gl vss for acl transaction file...")
    sr_file = "X001ba_gl_tranvss"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      X001ab_gl_transort.CAMPUS AS gle02_CoaName,
      X001ab_gl_transort.CAMPUS_VSS AS gle02_XCamp,
      X001ab_gl_transort.MONTH AS gle09_YearId,
      X001ab_gl_transort.AMOUNT AS gle15_XAmou,
      X001ab_gl_transort.DATE_TRAN AS gle17_Date,
      X001ab_gl_transort.STUDENT AS gle50_Stud,
      X001ab_gl_transort.BURSARY AS gle51_Burs,
      X001ab_gl_transort.DESC_FULL AS gle52_Desc,
      X001ab_gl_transort.DESC_VSS AS gle53_Desc
    FROM
      X001ab_gl_transort
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)
    # Export the data
    print("Export student debtor gl vss transactions...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_001_gltran_vss_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    # Calculate gl ytd balances ************************************************
    print("Calculate gl ytd balances file...")
    sr_file = "X001ca_gl_bal"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      X001ab_gl_transort.CAMPUS,
      Sum(X001ab_gl_transort.AMOUNT) AS BALANCE
    FROM
      X001ab_gl_transort
    WHERE
      X001ab_gl_transort.MONTH <= %PMONTH%
    GROUP BY
      X001ab_gl_transort.CAMPUS
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)    
    s_sql = s_sql.replace("%PMONTH%",funcdate.prev_month())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)
    # Export the data
    print("Export gl student debtor balances...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_001_glbal_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    # Calculate gl balances per month ******************************************
    print("Calculate gl balances per month...")
    sr_file = "X001cb_gl_balmonth"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      X001ab_gl_transort.CAMPUS,
      X001ab_gl_transort.MONTH,
      Sum(X001ab_gl_transort.AMOUNT) AS BALANCE
    FROM
      X001ab_gl_transort
    GROUP BY
      X001ab_gl_transort.CAMPUS,
      X001ab_gl_transort.MONTH
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)
    # Export the data
    print("Export gl student debtor per month balances...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_001_glbalmonth_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    # Build gl summary per transaction type ****************************************
    print("Build gl summary per transaction type...")
    sr_file = "X001cc_gl_summtype"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      X001ab_gl_transort.CAMPUS,
      X001ab_gl_transort.MONTH,
      X001ab_gl_transort.DESC_VSS,
      Sum(X001ab_gl_transort.AMOUNT) AS AMOUNT
    FROM
      X001ab_gl_transort
    GROUP BY
      X001ab_gl_transort.CAMPUS,
      X001ab_gl_transort.MONTH,
      X001ab_gl_transort.DESC_VSS
    ORDER BY
      MONTH
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)    
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)
    # Export the data
    print("Export gl student debtor summary per transaction type...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_001_glsummtype_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)









    """*************************************************************************
    ***
    *** PREPARE VSS TRANSACTIONS
    ***
    ***
    *************************************************************************"""

    print("--- PREPARE VSS TRANSACTIONS ---")
    funcfile.writelog("%t ---------- PREPARE VSS TRANSACTIONS ----------")
    
    # Extract vss transactions from VSS.SQLITE *********************************
    print("Import vss transactions from VSS.SQLITE...")
    sr_file = "X002aa_vss_tranlist"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      *
    FROM
      VSS.X010_Studytrans
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
    # Add column vss campus name
    print("Add column vss campus name...")
    so_curs.execute("ALTER TABLE X002aa_vss_tranlist ADD COLUMN CAMPUS TEXT;")
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
    so_curs.execute("ALTER TABLE X002aa_vss_tranlist ADD COLUMN MONTH TEXT;")
    so_curs.execute("UPDATE X002aa_vss_tranlist " + """
                    SET MONTH = 
                    CASE
                       WHEN SUBSTR(TRANSDATE,6,5)='01-01' AND INSTR('001z031z061',TRANSCODE)>0 THEN '00'
                       WHEN SUBSTR(POSTDATEDTRANSDATE,1,4)>=strftime('%Y','now') THEN SUBSTR(POSTDATEDTRANSDATE,6,2)
                       ELSE SUBSTR(TRANSDATE,6,2)
                    END
                    ;""")
    so_conn.commit()
    funcfile.writelog("%t ADD COLUMN: Vss transaction month")
    # Add column vss debit amount
    print("Add column vss dt amount...")
    so_curs.execute("ALTER TABLE X002aa_vss_tranlist ADD COLUMN AMOUNT_DT TEXT;")
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
    so_curs.execute("ALTER TABLE X002aa_vss_tranlist ADD COLUMN AMOUNT_CR TEXT;")
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
    so_curs.execute("ALTER TABLE X002aa_vss_tranlist ADD COLUMN TEMP_DESC_A TEXT;")
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
    so_curs.execute("ALTER TABLE X002aa_vss_tranlist ADD COLUMN TEMP_DESC_E TEXT;")
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
    funcfile.writelog("%t CALC COLUMN: Temp eng description")

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
      X002aa_vss_tranlist.AMOUNT AS AMOUNT_VSS,
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
      X002aa_vss_tranlist.AMOUNT_DT,
      X002aa_vss_tranlist.AMOUNT_CR,
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

    # Calculate vss balances per campus ****************************************
    print("Calculate vss campus balances...")
    sr_file = "X002ca_vss_bal"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      X002ab_vss_transort.CAMPUS_VSS,
      Sum(X002ab_vss_transort.AMOUNT_DT) AS AMOUNT_DT,
      Sum(X002ab_vss_transort.AMOUNT_CR) AS AMOUNT_CT,
      Sum(X002ab_vss_transort.AMOUNT_VSS) AS AMOUNT
    FROM
      X002ab_vss_transort
    WHERE
      X002ab_vss_transort.MONTH_VSS <= %PMONTH%
    GROUP BY
      X002ab_vss_transort.CAMPUS_VSS
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    s_sql = s_sql.replace("%PMONTH%",funcdate.prev_month())    
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)
    # Export the data
    print("Export vss campus balances...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_002_vssbal_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
    
    # Calculate vss balances per campus per month ******************************
    print("Calculate vss campus balances per month...")
    sr_file = "X002cb_vss_balmonth"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      X002ab_vss_transort.CAMPUS_VSS,
      X002ab_vss_transort.MONTH_VSS,
      Sum(X002ab_vss_transort.AMOUNT_DT) AS AMOUNT_DT,
      Sum(X002ab_vss_transort.AMOUNT_CR) AS AMOUNT_CT,
      Sum(X002ab_vss_transort.AMOUNT_VSS) AS AMOUNT
    FROM
      X002ab_vss_transort
    GROUP BY
      X002ab_vss_transort.CAMPUS_VSS,
      X002ab_vss_transort.MONTH_VSS
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)    
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)
    # Export the data
    print("Export vss campus balances per month...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_002_vssbalmonth_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
    
    # Calculate vss balances per campus per month per transaction type *********
    print("Calculate vss balances per transaction type...")
    sr_file = "X002cc_vss_summtype"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      X002ab_vss_transort.CAMPUS_VSS,
      X002ab_vss_transort.MONTH_VSS,
      X002ab_vss_transort.TRANSCODE_VSS,  
      X002ab_vss_transort.TEMP_DESC_A,
      X002ab_vss_transort.TEMP_DESC_E,
      Sum(X002ab_vss_transort.AMOUNT_VSS) AS AMOUNT_VSS
    FROM
      X002ab_vss_transort
    GROUP BY
      X002ab_vss_transort.CAMPUS_VSS,
      X002ab_vss_transort.MONTH_VSS,
      X002ab_vss_transort.TEMP_DESC_A
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)    
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)
    # Export the data
    print("Export vss campus balances per transaction type...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_002_vsssummtype_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)    

    # Sum vss student balances per campus ******************************************
    print("Sum vss student balances per campus...")
    sr_file = "X002ba_vss_student_balance"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X002ab_vss_transort.CAMPUS_VSS AS CAMPUS,
      X002ab_vss_transort.STUDENT_VSS AS STUDENT,  
      Round(0,2) AS BAL_DT,
      Round(0,2) AS BAL_CT,
      Round(Sum(X002ab_vss_transort.AMOUNT_VSS),2) AS BALANCE
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
    so_curs.execute("UPDATE X002ba_vss_student_balance " + """
                    SET BAL_DT = 
                    CASE
                       WHEN BALANCE > 0 THEN BALANCE
                       ELSE 0
                    END
                    ;""")
    so_conn.commit()
    funcfile.writelog("%t ADD COLUMN: Vss debit amount")
    # Add column vss credit amount
    print("Add column vss ct amount...")
    so_curs.execute("UPDATE X002ba_vss_student_balance " + """
                    SET BAL_CT = 
                    CASE
                       WHEN BALANCE < 0 THEN BALANCE
                       ELSE 0
                    END
                    ;""")
    so_conn.commit()
    funcfile.writelog("%t ADD COLUMN: Vss credit amount")

    # Sum vss balances per campus ******************************************
    print("Sum vss balances per campus...")
    sr_file = "X002bb_vss_campus_balance"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X002ba_vss_student_balance.CAMPUS,
      Round(Sum(X002ba_vss_student_balance.BAL_DT),2) AS BAL_DT,
      Round(Sum(X002ba_vss_student_balance.BAL_CT),2) AS BAL_CT,
      Round(Sum(X002ba_vss_student_balance.BALANCE),2) AS BALANCE
    FROM
      X002ba_vss_student_balance
    GROUP BY
      X002ba_vss_student_balance.CAMPUS
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)    
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # Join vss gl monthly account totals ******************************************
    print("Join vss and gl monthly totals...")
    sr_file = "X003ba_vss_gl_balance_month"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X002cb_vss_balmonth.CAMPUS_VSS AS CAMPUS,
      X002cb_vss_balmonth.MONTH_VSS AS MONTH,
      Round(X002cb_vss_balmonth.AMOUNT_DT, 2) AS VSS_TRAN_DT,
      Round(X002cb_vss_balmonth.AMOUNT_CT, 2) AS VSS_TRAN_CT,
      Round(X002cb_vss_balmonth.AMOUNT, 2) AS VSS_TRAN,
      Round(X001cb_gl_balmonth.BALANCE, 2) AS GL_TRAN
    FROM
      X002cb_vss_balmonth
      LEFT JOIN X001cb_gl_balmonth ON X001cb_gl_balmonth.CAMPUS = X002cb_vss_balmonth.CAMPUS_VSS AND
        X001cb_gl_balmonth.MONTH = X002cb_vss_balmonth.MONTH_VSS
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)    
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)


























    """*************************************************************************
    ***
    *** JOIN VSS & GL TRANSACTIONS
    ***
    ***
    *************************************************************************"""

    print("--- JOIN VSS & GL TRANSACTIONS ---")
    funcfile.writelog("%t ---------- JOIN VSS & GL TRANSACTIONS ----------")

    # Development script ***********************************************************
    print("Join vss gl transactions...")
    sr_file = "X003aa_vss_gl_join"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT ALL
      X002cc_vss_summtype.CAMPUS_VSS,
      X002cc_vss_summtype.MONTH_VSS,
      X002cc_vss_summtype.TRANSCODE_VSS,    
      X002cc_vss_summtype.TEMP_DESC_A,
      X002cc_vss_summtype.AMOUNT_VSS,
      X001cc_gl_summtype.DESC_VSS,
      X001cc_gl_summtype.AMOUNT,
      X001cc_gl_summtype.CAMPUS,
      X001cc_gl_summtype.MONTH
    FROM
      X002cc_vss_summtype
      LEFT JOIN X001cc_gl_summtype ON X001cc_gl_summtype.CAMPUS = X002cc_vss_summtype.CAMPUS_VSS AND
        X001cc_gl_summtype.MONTH = X002cc_vss_summtype.MONTH_VSS AND X001cc_gl_summtype.DESC_VSS =
        X002cc_vss_summtype.TEMP_DESC_A
    WHERE
      X002cc_vss_summtype.MONTH_VSS <= '%PMONTH%'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    s_sql = s_sql.replace("%PMONTH%",funcdate.prev_month())    
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
    print("Export vss gl recon...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_003_vss_gl_recon_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    # Join vss and gl on english vss descriptions ******************************
    print("Join vss gl transactions eng...")
    sr_file = "X003aa_vss_gl_join_eng"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT ALL
      X002cc_vss_summtype.CAMPUS_VSS,
      X002cc_vss_summtype.MONTH_VSS,
      X002cc_vss_summtype.TRANSCODE_VSS,    
      X002cc_vss_summtype.TEMP_DESC_E,
      X002cc_vss_summtype.AMOUNT_VSS,
      X001cc_gl_summtype.DESC_VSS,
      X001cc_gl_summtype.AMOUNT,
      X001cc_gl_summtype.CAMPUS,
      X001cc_gl_summtype.MONTH
    FROM
      X002cc_vss_summtype
      LEFT JOIN X001cc_gl_summtype ON X001cc_gl_summtype.CAMPUS = X002cc_vss_summtype.CAMPUS_VSS AND
        X001cc_gl_summtype.MONTH = X002cc_vss_summtype.MONTH_VSS AND X001cc_gl_summtype.DESC_VSS =
        X002cc_vss_summtype.TEMP_DESC_E
    WHERE
      X002cc_vss_summtype.MONTH_VSS <= '%PMONTH%'
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    s_sql = s_sql.replace("%PMONTH%",funcdate.prev_month())    
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)
    print("Export vss gl transactions eng...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_003_vss_gl_recon_eng_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)    

    # Join gl and vss on afrikaans description *********************************
    print("Join gl vss transactions...")
    sr_file = "X003ab_gl_vss_join"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X002cc_vss_summtype.CAMPUS_VSS,
      X002cc_vss_summtype.MONTH_VSS,
      X002cc_vss_summtype.TRANSCODE_VSS,
      X002cc_vss_summtype.TEMP_DESC_A,
      X002cc_vss_summtype.AMOUNT_VSS,
      X001cc_gl_summtype.DESC_VSS,
      X001cc_gl_summtype.AMOUNT,
      X001cc_gl_summtype.CAMPUS,
      X001cc_gl_summtype.MONTH,
      Length(X002cc_vss_summtype.CAMPUS_VSS) AS CAMPUS_LEN
    FROM
      X001cc_gl_summtype
      LEFT JOIN X002cc_vss_summtype ON X002cc_vss_summtype.CAMPUS_VSS = X001cc_gl_summtype.CAMPUS AND
        X002cc_vss_summtype.MONTH_VSS = X001cc_gl_summtype.MONTH AND X002cc_vss_summtype.TEMP_DESC_A =
        X001cc_gl_summtype.DESC_VSS
    WHERE
      Length(X002cc_vss_summtype.CAMPUS_VSS) IS NULL AND
      X001cc_gl_summtype.AMOUNT <> 0 AND
      X001cc_gl_summtype.MONTH <= '%PMONTH%'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    s_sql = s_sql.replace("%PMONTH%",funcdate.prev_month())    
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
    print("Export gl vss recon...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_003_vss_gl_recon_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a")
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head,"a")
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
    
    # Join gl and vss on english description *********************************
    print("Join gl vss transactions eng...")
    sr_file = "X003ab_gl_vss_join_eng"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      X002cc_vss_summtype.CAMPUS_VSS,
      X002cc_vss_summtype.MONTH_VSS,
      X002cc_vss_summtype.TRANSCODE_VSS,
      X002cc_vss_summtype.TEMP_DESC_E,
      X002cc_vss_summtype.AMOUNT_VSS,
      X001cc_gl_summtype.DESC_VSS,
      X001cc_gl_summtype.AMOUNT,
      X001cc_gl_summtype.CAMPUS,
      X001cc_gl_summtype.MONTH,
      Length(X002cc_vss_summtype.CAMPUS_VSS) AS CAMPUS_LEN
    FROM
      X001cc_gl_summtype
      LEFT JOIN X002cc_vss_summtype ON X002cc_vss_summtype.CAMPUS_VSS = X001cc_gl_summtype.CAMPUS AND
        X002cc_vss_summtype.MONTH_VSS = X001cc_gl_summtype.MONTH AND X002cc_vss_summtype.TEMP_DESC_E =
        X001cc_gl_summtype.DESC_VSS
    WHERE
      Length(X002cc_vss_summtype.CAMPUS_VSS) IS NULL AND
      X001cc_gl_summtype.AMOUNT <> 0 AND
      X001cc_gl_summtype.MONTH <= '%PMONTH%'
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    s_sql = s_sql.replace("%PMONTH%",funcdate.prev_month())    
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)
    print("Export gl vss recon eng...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_003_vss_gl_recon_eng_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a")
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head,"a")
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)










    """ ************************************************************************
    ***
    *** BURSARY VSS GL RECON
    ***
    *** Scripts to extract vss and gl bursary transactions
    ***
    *** summarize the transactions
    *** join the vss and gl data
    *************************************************************************"""

    print("--- PREPARE BURSARY TRANSACTIONS ---")
    funcfile.writelog("%t ---------- PREPARE BURSARY TRANSACTIONS ----------")

    # Extract the vss bursary transactions *****************************************
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

    # Summarize the vss bursary transactions *****************************************
    print("Summarize vss bursary transactions...")
    sr_file = "X002cd_vss_summtypeburs"    
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
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
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)
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

    # Extract the gl bursary transactions ******************************************
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

    # Summarize the gl bursary transactions *****************************************
    print("Summarize gl bursary transactions...")
    sr_file = "X001cd_gl_summtypeburs"    
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      X010ba_gl_burs.CAMPUS_GL,
      X010ba_gl_burs.MONTH_GL,
      X010ba_gl_burs.TRANSDESC_GL,
      Sum(X010ba_gl_burs.AMOUNT_GL) AS Sum_AMOUNT_GL
    FROM
      X010ba_gl_burs
    GROUP BY
      X010ba_gl_burs.CAMPUS_GL,
      X010ba_gl_burs.MONTH_GL,
      X010ba_gl_burs.TRANSDESC_GL
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)
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

    # Join the vss and gl bursary transactions *************************************
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
      X010ba_gl_burs.TRANSENTR_GL
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
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # Join the vss and gl bursary transactions (distinct) **************************
    print("Join the matched vss and gl bursary transactions...")
    sr_file = "X010cb_join_vss_gl_matched"    
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      *
    FROM
      X010ca_join_vss_gl_burs
    WHERE
      ROWID_GL Is Not Null
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)







    """*************************************************************************
    ***
    *** BURSARY TEST INGL NOVSS
    ***
    *** Identify bursary transactions in gl but not in vss
    *** 
    *** 
    *************************************************************************"""

    print("--- TEST BURSARY INGL NOVSS ---")
    funcfile.writelog("%t ---------- TEST BURSARY INGL NOVSS ----------")

    # Identify bursary transactions in gl but not in vss ***********************
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
    ***
    *** BURSARY TEST INVSS NOGL
    ***
    *** Identify bursary transactions in vss but not in gl
    *** 
    *** 
    *************************************************************************"""

    print("--- TEST BURSARY INVSS NOGL ---")
    funcfile.writelog("%t ---------- TEST BURSARY INVSS NOGL ----------")

    # Identify bursary transactions in vss but not in gl ***************************
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
      X010ca_join_vss_gl_burs.STUDENT_GL IS NULL
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
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_010e_test_burs_invss_nogl_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)








    """*************************************************************************
    ***
    *** BURSARY TEST POSTED TO DIFFERENT CAMPUS IN GL
    ***    
    *** Identify bursary transactions posted to a different campus in gl
    *** 
    *** 
    *************************************************************************"""

    print("--- TEST BURSARY VSS DIFF CAMPUS GL ---")
    funcfile.writelog("%t ---------- TEST BURSARY VSS DIFF CAMPUS GL ----------")

    # Import previously reported findings ******************************************
    print("Import previously reported bursary vss diff campus gl...")
    sr_file = "X010_prev_reported"
    # Create the table
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 TEXT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT)")
    s_cols = ""
    # Read the table parameters from the 02_Table.csv file """
    co = open(ed_path + "200_reported.txt", "rU")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "PROCESS":
            continue
        elif row[0] != "burs_vss_to_gl_diffcampus":
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the impoted data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "200_reported.txt (" + sr_file + ")" )

    # Identify bursary transactions posted to different campus in gl ***************
    print("Test bursary transactions posted to different campus in gl...")
    sr_file = "X010fa_test_burs_gl_diffcampus"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
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
      X010cb_join_vss_gl_matched.TRANSUSER_VSS
    FROM
      X010cb_join_vss_gl_matched
    WHERE
      X010cb_join_vss_gl_matched.CAMPUS_VSS <> X010cb_join_vss_gl_matched.CAMPUS_GL AND
      X010cb_join_vss_gl_matched.STUDENT_GL IS NOT NULL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Join the test burs tran to diff campus in gl prev reported *******************
    print("Join the previously reported test burs tran to diff campus in gl...")
    sr_file = "X010fb_join_prev_reported"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
      X010fa_test_burs_gl_diffcampus.CAMPUS_VSS,
      X010fa_test_burs_gl_diffcampus.MONTH_VSS,
      X010fa_test_burs_gl_diffcampus.STUDENT_VSS,
      X010fa_test_burs_gl_diffcampus.TRANSDATE_VSS,
      X010fa_test_burs_gl_diffcampus.TRANSCODE_VSS,
      X010fa_test_burs_gl_diffcampus.TRANSDESC_VSS,
      X010fa_test_burs_gl_diffcampus.AMOUNT_VSS,
      X010fa_test_burs_gl_diffcampus.BURSCODE_VSS,
      X010fa_test_burs_gl_diffcampus.BURSNAAM_VSS,
      X010fa_test_burs_gl_diffcampus.CAMPUS_GL,
      X010fa_test_burs_gl_diffcampus.TRANSEDOC_GL,
      X010fa_test_burs_gl_diffcampus.TRANSENTR_GL,
      X010fa_test_burs_gl_diffcampus.TRANSDESC_GL,
      X010fa_test_burs_gl_diffcampus.TRANSUSER_VSS,
      X010_prev_reported.PROCESS AS PREV_PROCESS,
      X010_prev_reported.DATE_REPORTED AS PREV_DATE_REPORTED,
      X010_prev_reported.DATE_RETEST AS PREV_DATE_RETEST
    FROM
      X010fa_test_burs_gl_diffcampus
      LEFT JOIN X010_prev_reported ON X010_prev_reported.FIELD1 = X010fa_test_burs_gl_diffcampus.STUDENT_VSS AND
        X010_prev_reported.FIELD2 = X010fa_test_burs_gl_diffcampus.TRANSDATE_VSS AND X010_prev_reported.FIELD3 =
        X010fa_test_burs_gl_diffcampus.BURSCODE_VSS AND
        X010_prev_reported.FIELD4 = X010fa_test_burs_gl_diffcampus.AMOUNT_VSS AND
        X010_prev_reported.DATE_RETEST > '%TODAY%'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%",funcdate.today())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    print("Add prev reported columns...")
    so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN PROCESS TEXT;")
    so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN DATE_REPORTED TEXT;")
    so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN DATE_RETEST TEXT;")
    so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN FIELD5 TEXT;")
    so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN COUNTER INT;")
    so_curs.execute("UPDATE "+sr_file+" SET PROCESS = 'burs_vss_to_gl_diffcampus'")
    so_curs.execute("UPDATE "+sr_file+" SET FIELD5 = '0'")
    s_sql = "UPDATE "+sr_file+" SET DATE_REPORTED = '%TODAY%'"
    s_sql = s_sql.replace("%TODAY%",funcdate.today())
    so_curs.execute(s_sql)
    so_curs.execute("UPDATE "+sr_file+" SET DATE_RETEST = '2099-12-31'")
    so_curs.execute("UPDATE "+sr_file+" SET COUNTER = 1")
    so_conn.commit()
    # Export the data
    print("Export bursary transactions posted to different campus in gl...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Debtor_010fb_test_burs_gl_diffcampus_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
        
    # Add the test burs tran to diff campus in gl to reported **********************
    print("Add test burs tran to diff campus in gl to prev reported...")
    sr_file = "X010fc_prev_reported"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X010fb_join_prev_reported.PROCESS,
      X010fb_join_prev_reported.STUDENT_VSS AS FIELD1,
      X010fb_join_prev_reported.TRANSDATE_VSS AS FIELD2,
      X010fb_join_prev_reported.BURSCODE_VSS AS FIELD3,
      X010fb_join_prev_reported.AMOUNT_VSS AS FIELD4,
      X010fb_join_prev_reported.FIELD5,
      X010fb_join_prev_reported.DATE_REPORTED,
      X010fb_join_prev_reported.DATE_RETEST
    FROM
      X010fb_join_prev_reported
    WHERE
      X010fb_join_prev_reported.PREV_PROCESS IS NULL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
    # Export the data
    if funcsys.tablerowcount(so_curs,sr_file) > 0:
        print("Export the new data to previously reported file...")
        sr_filet = sr_file
        sx_path = ed_path
        sx_file = "200_reported"
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
        funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
    else:
        print("No new data to previously reported file...")
        funcfile.writelog("%t EXPORT DATA: No new data to export")

    # Import the reporting officers ************************************************
    print("Import reporting officers from VSS.SQLITE...")
    sr_file = "X010fd_impo_report_officer"
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
      VSS.X000_OWN_LOOKUPS.LOOKUP = 'stud_debt_recon_diff_campus_officer'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Import the reporting supervisors *********************************************
    print("Import reporting supervisors from VSS.SQLITE...")
    sr_file = "X010fe_impo_report_supervisor"
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
      VSS.X000_OWN_LOOKUPS.LOOKUP = 'stud_debt_recon_diff_campus_supervisor'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Prepare final burs posted to diff gl campus reporting file *******************
    print("Final test burs to diff gl campus results...")
    sr_file = "X010fx_test_burs_gl_diffcampus"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      X010fb_join_prev_reported.CAMPUS_VSS AS VSS_CAMPUS,
      X010fb_join_prev_reported.CAMPUS_GL AS GL_CAMPUS,
      X010fb_join_prev_reported.STUDENT_VSS AS STUDENT,
      X010fb_join_prev_reported.TRANSDATE_VSS AS DATE,
      X010fb_join_prev_reported.MONTH_VSS AS MONTH,
      X010fb_join_prev_reported.AMOUNT_VSS AS AMOUNT,
      X010fb_join_prev_reported.TRANSCODE_VSS AS TRANSCODE,
      X010fb_join_prev_reported.TRANSDESC_VSS AS TRANDESC,
      X010fb_join_prev_reported.BURSCODE_VSS AS BURSCODE,
      X010fb_join_prev_reported.BURSNAAM_VSS AS BURSNAME,
      X010fb_join_prev_reported.TRANSEDOC_GL AS GL_EDOC,
      X010fb_join_prev_reported.TRANSENTR_GL AS GL_DESC,
      X010fb_join_prev_reported.TRANSUSER_VSS AS USER,
      PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME AS USER_NAME,
      PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS AS USER_MAIL,
      X010fd_impo_report_officer.EMPLOYEE_NUMBER AS OFFICER,
      X010fd_impo_report_officer.KNOWN_NAME AS OFFICER_NAME,
      X010fd_impo_report_officer.EMAIL_ADDRESS AS OFFICER_MAIL,
      X010fe_impo_report_supervisor.EMPLOYEE_NUMBER AS SUPERVISOR,
      X010fe_impo_report_supervisor.KNOWN_NAME AS SUPERVISOR_NAME,
      X010fe_impo_report_supervisor.EMAIL_ADDRESS AS SUPERVISOR_MAIL,
      X010fb_join_prev_reported.COUNTER
    FROM
      X010fb_join_prev_reported
      LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X010fb_join_prev_reported.TRANSUSER_VSS
      LEFT JOIN X010fd_impo_report_officer ON X010fd_impo_report_officer.CAMPUS = X010fb_join_prev_reported.CAMPUS_VSS
      LEFT JOIN X010fe_impo_report_supervisor ON X010fe_impo_report_supervisor.CAMPUS = X010fb_join_prev_reported.CAMPUS_VSS
    WHERE
      X010fb_join_prev_reported.PREV_PROCESS IS NULL
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
        sx_path = re_path + funcdate.cur_year() + "/"
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

    return
