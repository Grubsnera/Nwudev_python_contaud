""" Script to build kfs creditor payment tests *********************************
Created on: 16 Apr 2019
Author: Albert J v Rensburg (NWU21162395)
*****************************************************************************"""

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
TEST CREDITOR DUPLICATE PAYMENT METHOD 1 (VENDOR,INVNBR,INVDT,AMOUNT)
END OF SCRIPT
*****************************************************************************"""

def Creditor_test_payments():

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # IMPORT PYTHON MODULES
    import csv
    import datetime
    import sqlite3
    import sys

    # ADD OWN MODULE PATH
    sys.path.append('S:/_my_modules')

    # IMPORT OWN MODULES
    import funcfile
    import funccsv
    import funcdate
    import funcsys

    # OPEN THE SCRIPT LOG FILE
    print("---------------------------")    
    print("C201_CREDITOR_TEST_PAYMENTS")
    print("---------------------------")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C201_CREDITOR_TEST_PAYMENTS")
    funcfile.writelog("-----------------------------------")
    ilog_severity = 1

    # DECLARE VARIABLES
    so_path = "W:/Kfs/" #Source database path
    re_path = "R:/Kfs/" # Results path
    ed_path = "S:/_external_data/" #external data path
    so_file = "Kfs_test_creditor.sqlite" # Source database
    s_sql = "" # SQL statements
    l_export = True
    l_mail = False
    l_record = True
    l_vacuum = True

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN THE WORKING DATABASE
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("%t OPEN DATABASE: " + so_file)

    # ATTACH DATA SOURCES
    so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
    funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """ ****************************************************************************
    TEST CREDITOR DUPLICATE PAYMENT METHOD 1
        Test yesterdays payments if today is not a monday
        Test three days payments if today is a monday
    *****************************************************************************"""
    print("TEST CREDITOR DUPLICATE PAYMENT")
    funcfile.writelog("TEST CREDITOR DUPLICATE PAYMENT")

    # DECLARE TEST VARIABLES
    #l_record = False # Record the findings in the previous reported findings file
    i_find = 0 # Number of findings before previous reported findings
    i_coun = 0 # Number of new findings to report

    # BUILD CREDITOR PAYMENTS MASTER FILES - LAST DAY
    print("Build creditor payment master tables...")
    sr_file = "X001_payments_totest"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        Substr(PAYMENT.PAYEE_TYP_DESC,1,3) As TYP,
        PAYMENT.VENDOR_ID,
        PAYMENT.CUST_PMT_DOC_NBR,
        PAYMENT.INV_NBR,
        PAYMENT.INV_DT,
        PAYMENT.PMT_DT,
        PAYMENT.NET_PMT_AMT,
        0 As INV_CALC,
        '' As INV_CALC1,
        '' As INV_CALC2,
        '' As INV_CALC3
    From
        KFS.X001aa_Report_payments_curr PAYMENT
    Where
        StrfTime('%Y-%m-%d',PAYMENT.PMT_DT) >= StrfTime('%Y-%m-%d','now','%DAYS%') And
        PAYMENT.PMT_STAT_CD = 'EXTR'
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if funcdate.today_dayname() == "Mon":
        s_sql = s_sql.replace('%DAYS%','-3 day')
    else:
        s_sql = s_sql.replace('%DAYS%','-1 day')
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Calculate fields
    print("Calculate invoice number field...")
    s_sql = "Update " + sr_file + " Set INV_CALC1 = " + """
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    INV_NBR,
    'A',''),
    'B',''),
    'C',''),
    'D',''),
    'E',''),
    'F',''),
    'G',''),
    'H',''),
    'I',''),
    'J','')
    """
    so_curs.execute(s_sql)
    so_conn.commit()
    s_sql = "Update " + sr_file + " Set INV_CALC2 = " + """
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    INV_CALC1,
    'K',''),
    'L',''),
    'M',''),
    'N',''),
    'O',''),
    'P',''),
    'Q',''),
    'R',''),
    'S',''),
    'T','')
    """
    so_curs.execute(s_sql)
    so_conn.commit()
    s_sql = "Update " + sr_file + " Set INV_CALC3 = " + """
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    INV_CALC2,
    'U',''),
    'V',''),
    'W',''),
    'X',''),
    'Y',''),
    'Z',''),
    ' ',''),
    '/',''),
    '*',''),
    '+',''),
    '.',''),
    '-','')
    """
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t ADD COLUMNS: INVOICE NUMBER")
    s_sql = "Update " + sr_file + " Set INV_CALC = " + """
    Cast(INV_CALC3 As INT)
    """
    so_curs.execute(s_sql)
    so_conn.commit()

    # BUILD CREDITOR PAYMENTS MASTER FILES - PREVIOUS YEAR PAYMENTS
    print("Build previous year payments...")
    sr_file = "X001_payments_prev"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PAYMENT.VENDOR_ID,
        PAYMENT.CUST_PMT_DOC_NBR,
        PAYMENT.INV_NBR,
        PAYMENT.INV_DT,
        PAYMENT.PMT_DT,    
        PAYMENT.NET_PMT_AMT,
        0 As INV_CALC,
        '' As INV_CALC1,
        '' As INV_CALC2,
        '' As INV_CALC3
    From
        KFS.X001aa_Report_payments_prev PAYMENT
    Where
        PAYMENT.PMT_STAT_CD = 'EXTR'
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Calculate fields
    print("Calculate invoice number field...")
    s_sql = "Update " + sr_file + " Set INV_CALC1 = " + """
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    INV_NBR,
    'A',''),
    'B',''),
    'C',''),
    'D',''),
    'E',''),
    'F',''),
    'G',''),
    'H',''),
    'I',''),
    'J','')
    """
    so_curs.execute(s_sql)
    so_conn.commit()
    s_sql = "Update " + sr_file + " Set INV_CALC2 = " + """
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    INV_CALC1,
    'K',''),
    'L',''),
    'M',''),
    'N',''),
    'O',''),
    'P',''),
    'Q',''),
    'R',''),
    'S',''),
    'T','')
    """
    so_curs.execute(s_sql)
    so_conn.commit()
    s_sql = "Update " + sr_file + " Set INV_CALC3 = " + """
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    INV_CALC2,
    'U',''),
    'V',''),
    'W',''),
    'X',''),
    'Y',''),
    'Z',''),
    ' ',''),
    '/',''),
    '*',''),
    '+',''),
    '.',''),
    '-','')
    """
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t ADD COLUMNS: INVOICE NUMBER")
    s_sql = "Update " + sr_file + " Set INV_CALC = " + """
    Cast(INV_CALC3 As INT)
    """
    so_curs.execute(s_sql)
    so_conn.commit()

    # BUILD CREDITOR PAYMENTS MASTER FILES - CURRENT YEAR PAYMENTS
    print("Build current year payments...")
    sr_file = "X001_payments_curr"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PAYMENT.VENDOR_ID,
        PAYMENT.CUST_PMT_DOC_NBR,
        PAYMENT.INV_NBR,
        PAYMENT.INV_DT,
        PAYMENT.PMT_DT,    
        PAYMENT.NET_PMT_AMT,
        0 As INV_CALC,
        '' As INV_CALC1,
        '' As INV_CALC2,
        '' As INV_CALC3
    From
        KFS.X001aa_Report_payments_curr PAYMENT
    Where
        PAYMENT.PMT_STAT_CD = 'EXTR'
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Calculate fields
    print("Calculate invoice number field...")
    s_sql = "Update " + sr_file + " Set INV_CALC1 = " + """
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    INV_NBR,
    'A',''),
    'B',''),
    'C',''),
    'D',''),
    'E',''),
    'F',''),
    'G',''),
    'H',''),
    'I',''),
    'J','')
    """
    so_curs.execute(s_sql)
    so_conn.commit()
    s_sql = "Update " + sr_file + " Set INV_CALC2 = " + """
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    INV_CALC1,
    'K',''),
    'L',''),
    'M',''),
    'N',''),
    'O',''),
    'P',''),
    'Q',''),
    'R',''),
    'S',''),
    'T','')
    """
    so_curs.execute(s_sql)
    so_conn.commit()
    s_sql = "Update " + sr_file + " Set INV_CALC3 = " + """
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    Replace(
    INV_CALC2,
    'U',''),
    'V',''),
    'W',''),
    'X',''),
    'Y',''),
    'Z',''),
    ' ',''),
    '/',''),
    '*',''),
    '+',''),
    '.',''),
    '-','')
    """
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t ADD COLUMNS: INVOICE NUMBER")
    s_sql = "Update " + sr_file + " Set INV_CALC = " + """
    Cast(INV_CALC3 As INT)
    """
    so_curs.execute(s_sql)
    so_conn.commit()

    # BUILD PAYMENTS
    print("Build payments...")
    sr_file = "X001_payments"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PAYMENT.VENDOR_ID,
        PAYMENT.CUST_PMT_DOC_NBR,
        PAYMENT.INV_NBR,
        PAYMENT.INV_DT,
        PAYMENT.PMT_DT,
        PAYMENT.NET_PMT_AMT,
        PAYMENT.INV_CALC
    From
        X001_payments_prev PAYMENT
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CURRENT YEAR PAYMENTS
    print("Add current year payments...")
    s_sql = "INSERT INTO X001_payments " + """
    Select
        VENDOR_ID,
        CUST_PMT_DOC_NBR,
        INV_NBR,
        INV_DT,
        PMT_DT,    
        NET_PMT_AMT,
        INV_CALC
    FROM
        X001_payments_curr
    """
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IDENTIFY DUPLICATES
    print("Identify possible duplicates...")
    sr_file = "X001aa_paym_dupl"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        TEST.ORG,
        TEST.TYP,
        TEST.VENDOR_ID As VENDOR,
        TEST.CUST_PMT_DOC_NBR As EDOC,
        TEST.INV_NBR As INVOICE,
        TEST.INV_DT As INVOICE_DATE,
        TEST.PMT_DT As PAYMENT_DATE,
        TEST.NET_PMT_AMT As AMOUNT,
        TEST.INV_CALC As CALC,
        BASE.CUST_PMT_DOC_NBR As DUP_EDOC,
        BASE.INV_NBR As DUP_INVOICE,
        BASE.INV_DT As DUP_INVOICE_DATE,
        BASE.PMT_DT As DUP_PAYMENT_DATE,
        BASE.NET_PMT_AMT As DUP_AMOUNT,
        BASE.INV_CALC As DUP_CALC
    From
        X001_payments_totest TEST Left Join
        X001_payments BASE On BASE.CUST_PMT_DOC_NBR <> TEST.CUST_PMT_DOC_NBR And
            BASE.VENDOR_ID = TEST.VENDOR_ID And
            BASE.INV_DT = TEST.INV_DT And
            BASE.NET_PMT_AMT = TEST.NET_PMT_AMT And
            BASE.INV_CALC = TEST.INV_CALC        
    """
    """
    BASE.CUST_PMT_DOC_NBR <> TEST.CUST_PMT_DOC_NBR And

    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IDENTIFY DUPLICATES
    print("Identify possible duplicates...")
    sr_file = "X001ab_paym_dupl"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        FIND.ORG,
        FIND.TYP,
        FIND.VENDOR,
        FIND.EDOC,
        FIND.INVOICE,
        FIND.INVOICE_DATE,
        FIND.PAYMENT_DATE,
        FIND.AMOUNT,
        FIND.CALC,
        FIND.DUP_EDOC,
        FIND.DUP_INVOICE,
        FIND.DUP_INVOICE_DATE,
        FIND.DUP_PAYMENT_DATE,
        FIND.DUP_AMOUNT,
        FIND.DUP_CALC
    From
        X001aa_paym_dupl FIND
    Where
        FIND.DUP_EDOC Is Not Null
    Group By
        FIND.EDOC
    Order By
        FIND.VENDOR
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find = funcsys.tablerowcount(so_curs,sr_file)
    print("*** Found "+str(i_find)+" exceptions ***")
    funcfile.writelog("%t FINDING: "+str(i_find)+" PAYMENT duplicate finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X001ac_paym_getprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,DATE_MAILED TEXT)")
        s_cols = ""
        co = open(ed_path + "201_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "paym_dupl_1":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the impoted data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "201_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    sr_file = "X001ad_paym_addprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        SELECT
          FIND.*,
          'paym_dupl_1' AS PROCESS,
          '%TODAY%' AS DATE_REPORTED,
          '%TODAYPLUS%' AS DATE_RETEST,
          PREV.PROCESS AS PREV_PROCESS,
          PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
          PREV.DATE_RETEST AS PREV_DATE_RETEST,
          PREV.DATE_MAILED
        FROM
          X001ab_paym_dupl FIND
          LEFT JOIN X001ac_paym_getprev PREV ON PREV.FIELD1 = FIND.EDOC AND
              PREV.FIELD2 = FIND.DUP_EDOC AND
              PREV.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%",funcdate.today_plusdays(10))
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X001ae_paym_newprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE "+sr_file+" AS " + """
        SELECT
          FIND.PROCESS,
          FIND.EDOC AS FIELD1,
          FIND.DUP_EDOC AS FIELD2,
          '' AS FIELD3,
          '' AS FIELD4,
          '' AS FIELD5,
          FIND.DATE_REPORTED,
          FIND.DATE_RETEST,
          FIND.DATE_MAILED
        FROM
          X001ad_paym_addprev FIND
        WHERE
          FIND.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: "+sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(so_curs,sr_file)
        if i_coun > 0:
            print("*** " +str(i_coun)+ " Finding(s) to report ***")    
            sr_filet = sr_file
            sx_path = ed_path
            sx_file = "201_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            # Write the data
            if l_record == True:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
                funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
                funcfile.writelog("%t EXPORT DATA: "+sr_file)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X001af_paym_officer"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          LOOKUP.LOOKUP,
          LOOKUP.LOOKUP_CODE AS TYPE,
          LOOKUP.LOOKUP_DESCRIPTION AS EMP,
          PERSON.NAME_ADDR AS NAME,
          PERSON.EMAIL_ADDRESS AS MAIL
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS LOOKUP
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR PERSON ON PERSON.EMPLOYEE_NUMBER = LOOKUP.LOOKUP_DESCRIPTION
        WHERE
          LOOKUP.LOOKUP = 'TEST_PAYM_DUPL1_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X001ag_paym_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          LOOKUP.LOOKUP,
          LOOKUP.LOOKUP_CODE AS TYPE,
          LOOKUP.LOOKUP_DESCRIPTION AS EMP,
          PERSON.NAME_ADDR AS NAME,
          PERSON.EMAIL_ADDRESS AS MAIL
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS LOOKUP
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR PERSON ON PERSON.EMPLOYEE_NUMBER = LOOKUP.LOOKUP_DESCRIPTION
        WHERE
          LOOKUP.LOOKUP = 'TEST_PAYM_DUPL1_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X001ah_paym_contact"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            FIND.ORG,
            FIND.TYP,
            FIND.VENDOR,
            FIND.EDOC,
            FIND.INVOICE,
            FIND.INVOICE_DATE,
            FIND.PAYMENT_DATE,
            FIND.AMOUNT,
            FIND.DUP_EDOC,
            FIND.DUP_INVOICE,
            FIND.DUP_INVOICE_DATE,
            FIND.DUP_PAYMENT_DATE,
            FIND.DUP_AMOUNT,
            CAMP_OFF.EMP As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CAMP_OFF.MAIL As CAMP_OFF_MAIL,
            CAMP_SUP.EMP As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CAMP_SUP.MAIL As CAMP_SUP_MAIL,
            ORG_OFF.EMP As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            ORG_OFF.MAIL As ORG_OFF_MAIL,
            ORG_SUP.EMP As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            ORG_SUP.MAIL As ORG_SUP_MAIL,
            PAYMENT.VENDOR_NAME,
            PAYMENT.PAYEE_TYP_DESC,
            DOC.LBL As DOC_LABEL,
            LINE.FDOC_LINE_DESC As ACC_LINE,
            LINE.COUNT_LINES As ACCL_COUNT,
            DLINE.FDOC_LINE_DESC As DUP_ACC_LINE,
            DLINE.COUNT_LINES As DUP_ACCL_COUNT        
        From
            X001ad_paym_addprev FIND
            Left Join X001af_paym_officer CAMP_OFF On CAMP_OFF.TYPE = FIND.TYP
            Left Join X001af_paym_officer ORG_OFF On ORG_OFF.TYPE = FIND.ORG
            Left Join X001ag_paym_supervisor CAMP_SUP On CAMP_SUP.TYPE = FIND.TYP
            Left Join X001ag_paym_supervisor ORG_SUP On ORG_SUP.TYPE = FIND.ORG
            Left Join KFS.X001aa_Report_payments_curr PAYMENT on PAYMENT.CUST_PMT_DOC_NBR = FIND.EDOC
            Left Join KFS.X000_Documents DOC on DOC.DOC_HDR_ID = FIND.EDOC
            Left Join KFS.X000_Account_line_unique LINE on LINE.FDOC_NBR = FIND.EDOC
            Left Join KFS.X000_Account_line_unique DLINE on DLINE.FDOC_NBR = FIND.DUP_EDOC
        Where
            FIND.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X001ax_paym_duplicate"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'POSSIBLE DUPLICATE PAYMENT (1)' As AUDIT_FINDING,
            FIND.PAYEE_TYP_DESC As PAYMENT_TYPE,
            FIND.VENDOR AS VENDOR_NUMBER,
            FIND.VENDOR_NAME,
            FIND.DOC_LABEL AS DOCUMENT_TYPE,
            FIND.EDOC,
            FIND.INVOICE AS INVOICE_NUMBER,
            FIND.INVOICE_DATE,
            FIND.PAYMENT_DATE,
            FIND.AMOUNT,
            FIND.ACC_LINE AS ACCOUNTING_LINE,
            FIND.DUP_EDOC As DUPLICATE_EDOC,
            FIND.DUP_INVOICE As DUPLICATE_INVOICE_NUMBER,
            FIND.DUP_INVOICE_DATE As DUPLICATE_INVOICE_DATE,
            FIND.DUP_PAYMENT_DATE As DUPLICATE_PAYMENT_DATE,
            FIND.DUP_AMOUNT As DUPLICATE_AMOUNT,
            FIND.DUP_ACC_LINE AS DUPLICATE_ACCOUNTING_LINE,
            FIND.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
            FIND.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
            FIND.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
            FIND.CAMP_SUP_NAME AS SUPERVISOR,
            FIND.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
            FIND.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
            FIND.ORG_OFF_NAME AS ORG_OFFICER,
            FIND.ORG_OFF_NUMB AS ORG_OFFICER_NUMB,
            FIND.ORG_OFF_MAIL AS ORG_OFFICER_MAIL,
            FIND.ORG_SUP_NAME AS ORG_SUPERVISOR,
            FIND.ORG_SUP_NUMB AS ORG_SUPERVISOR_NUMB,
            FIND.ORG_SUP_MAIL AS ORG_SUPERVISOR_MAIL
        From
            X001ah_paym_contact FIND
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
            print("Export findings...")
            sr_filet = sr_file
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "Creditor_test_001ax_paym_duplicate_"
            sx_filet = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
            funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    if l_vacuum == True:
        print("Vacuum the database...")
        so_conn.commit()
        so_conn.execute('VACUUM')
        funcfile.writelog("%t DATABASE: Vacuum kfs")    
    so_conn.commit()
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("--------------------------------------")
    funcfile.writelog("COMPLETED: C201_CREDITOR_TEST_PAYMENTS")

    return
