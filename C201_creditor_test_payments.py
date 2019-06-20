""" Script to build kfs creditor payment tests *********************************
Created on: 16 Apr 2019
Author: Albert J v Rensburg (NWU21162395)
*****************************************************************************"""

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
TEST CREDITOR DUPLICATE PAYMENT METHOD 1 (VENDOR,INVNBR,INVDT,AMOUNT)
TEST CREDITOR BANK VERIFICATION
TEST EMPLOYEE APPROVE OWN PAYMENT
TEST EMPLOYEE INITIATE OWN PAYMENT
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
    funcfile.writelog("%t FINDING: "+str(i_find)+" VENDOR PAYMENT duplicate finding(s)")

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
    TEST CREDITOR BANK VERIFICATION
    *****************************************************************************"""
    print("TEST CREDITOR BANK VERIFICATION")
    funcfile.writelog("TEST CREDITOR BANK VERIFICATION")

    # DECLARE TEST VARIABLES
    #l_record = True # Record the findings in the previous reported findings file
    i_find = 0 # Number of findings before previous reported findings
    i_coun = 0 # Number of new findings to report

    # OBTAIN CURRENT BANK ACCOUNTS
    print("Build vendor banks...")
    sr_file = "X002_vendor_bank"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        VEND.VENDOR_ID,
        VEND.VEND_BANK,
        VEND.VEND_BRANCH
    From
        KFS.X000_Vendor_master VEND
    Where
        VEND.VEND_BANK <> ''
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if funcdate.today_dayname() == "Mon":
        s_sql = s_sql.replace('%DAYS%','-3 day')
    else:
        s_sql = s_sql.replace('%DAYS%','-1 day')
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # GET PREVIOUS BANK ACCOUNTS
    sr_file = "X002_vendor_bank_prev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    print("Import previous vendor banks...")
    so_curs.execute("CREATE TABLE " + sr_file + "(VENDOR_ID_PREV TEXT,VEND_BANK_PREV TEXT,VEND_BRANCH_PREV TEXT)")
    s_cols = ""
    co = open(ed_path + "201_vendor_bank.csv", "r")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "VENDOR_ID":
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the impoted data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "201_vendor_bank.csv (" + sr_file + ")")

    # EXPORT THE PREVIOUS BANK DETAILS
    print("Export previous bank details...")
    sr_filet = "X002_vendor_bank_prev"
    sx_path = ed_path
    sx_file = "201_vendor_bank_prev"
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    # EXPORT THE CURRENT BANK DETAILS
    print("Export current bank details...")
    sr_filet = "X002_vendor_bank"
    sx_path = ed_path
    sx_file = "201_vendor_bank"
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)    

    # COMBINE CURRENT AND PREVIOUS BANK ACCOUNTS
    print("Combine current and previous bank accounts...")
    sr_file = "X002aa_bank_change"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        CURR.VENDOR_ID,
        CURR.VEND_BANK,
        PREV.VEND_BANK_PREV,
        PREV.VEND_BRANCH_PREV
    From
        X002_vendor_bank CURR Left Join
        X002_vendor_bank_prev PREV On PREV.VENDOR_ID_PREV = CURR.VENDOR_ID     
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # SELECT NEW AND BANK CHANGES
    print("Select new and bank changes...")
    sr_file = "X002ab_bank_change"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        VEND.VENDOR_ID,
        VEND.VEND_BANK,
        VEND.VEND_BANK_PREV,
        VEND.VEND_BRANCH_PREV
    From
        X002aa_bank_change VEND
    Where
        VEND.VEND_BANK_PREV Is Not Null And
        VEND.VEND_BANK <> VEND.VEND_BANK_PREV
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find = funcsys.tablerowcount(so_curs,sr_file)
    print("*** Found "+str(i_find)+" exceptions ***")
    funcfile.writelog("%t FINDING: "+str(i_find)+" VENDOR BANK verify finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X002ac_getprev"
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
            elif row[0] != "vend_bank_change":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the impoted data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "201_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    sr_file = "X002ad_addprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        SELECT
          FIND.*,
          'vend_bank_change' AS PROCESS,
          '%TODAY%' AS DATE_REPORTED,
          '%TODAYPLUS%' AS DATE_RETEST,
          PREV.PROCESS AS PREV_PROCESS,
          PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
          PREV.DATE_RETEST AS PREV_DATE_RETEST,
          PREV.DATE_MAILED
        FROM
          X002ab_bank_change FIND
          LEFT JOIN X002ac_getprev PREV ON PREV.FIELD1 = FIND.VENDOR_ID And
              PREV.FIELD2 = FIND.VEND_BANK
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%",funcdate.today_plusdays(0))
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X002ae_newprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE "+sr_file+" AS " + """
        SELECT
          FIND.PROCESS,
          FIND.VENDOR_ID AS FIELD1,
          FIND.VEND_BANK AS FIELD2,
          '' AS FIELD3,
          '' AS FIELD4,
          '' AS FIELD5,
          FIND.DATE_REPORTED,
          FIND.DATE_RETEST,
          FIND.DATE_MAILED
        FROM
          X002ad_addprev FIND
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
    sr_file = "X002af_officer"
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
          LOOKUP.LOOKUP = 'TEST_VENDOR_BANKACC_VERIFY_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X002ag_supervisor"
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
          LOOKUP.LOOKUP = 'TEST_VENDOR_BANKACC_VERIFY_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X002ah_contact"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            FIND.VENDOR_ID,
            FIND.VEND_BANK,
            VEND.VEND_BRANCH,
            FIND.VEND_BANK_PREV,
            FIND.VEND_BRANCH_PREV,
            VEND.VNDR_NM,
            CASE
                WHEN VEND_MAIL = '' And EMAIL = '' Then ''
                WHEN VEND_MAIL = '' And EMAIL <> '' Then EMAIL
                ELSE VEND_MAIL
            END As EMAIL1,
            VEND.EMAIL,
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
            ORG_SUP.MAIL As ORG_SUP_MAIL
        From
            X002ad_addprev FIND
            Left Join X002af_officer CAMP_OFF On CAMP_OFF.TYPE = 'VEN'
            Left Join X002af_officer ORG_OFF On ORG_OFF.TYPE = 'NWU'
            Left Join X002ag_supervisor CAMP_SUP On CAMP_SUP.TYPE = 'VEN'
            Left Join X002ag_supervisor ORG_SUP On ORG_SUP.TYPE = 'NWU'
            Left Join KFS.X000_Vendor_master VEND On VEND.VENDOR_ID = FIND.VENDOR_ID
        Where
            FIND.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X002ax_vendor_bank_change"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'NWU VENDOR BANK ACCOUNT VERIFY' As AUDIT_FINDING,
            FIND.VENDOR_ID As NWU_VENDOR_ID,
            FIND.VNDR_NM As NAME,
            FIND.EMAIL1 As EMAIL1,
            CASE
                WHEN FIND.EMAIL1 <> '' And FIND.EMAIL <> '' And FIND.EMAIL1 <> FIND.EMAIL THEN EMAIL
                ELSE ''
            END As EMAIL2,
            '' As CONTACT,
            '' As TEL1,
            '' As TEL2,
            FIND.VEND_BRANCH As NEW_BRANCH_CODE,
            FIND.VEND_BANK As NEW_BANK_ACC_NUMBER,
            FIND.VEND_BRANCH_PREV As OLD_BRANCH_CODE,
            FIND.VEND_BANK_PREV As OLD_BANK_ACC_NUMBER,
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
            X002ah_contact FIND
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
            sx_file = "Creditor_test_002ax_vendor_bank_verify_"
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
    TEST EMPLOYEE APPROVE OWN PAYMENT
    *****************************************************************************"""
    print("EMPLOYEE APPROVE OWN PAYMENT")
    funcfile.writelog("EMPLOYEE APPROVE OWN PAYMENT")

    # DECLARE VARIABLES
    i_coun: int = 0

    # OBTAIN TEST DATA
    print("Obtain test data...")
    sr_file: str = "X003aa_empl_approve_own_payment"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PAYMENT.*
    From
        X001ac_Report_payments_approute_curr PAYMENT
    Where
        SubStr(PAYMENT.VENDOR_ID, 1, 8) = PAYMENT.APPROVE_EMP_NO
    Order By
        PAYMENT.APPROVE_DATE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X003ab_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        CASE
            WHEN PAYMENT.DOC_TYPE = 'PDV' THEN PAYMENT.DOC_TYPE
            WHEN PAYMENT.DOC_TYPE = 'DV' THEN PAYMENT.DOC_TYPE
            ELSE 'OTHER'
        END As DOC_TYPE,
        PAYMENT.VENDOR_ID,
        PAYMENT.CUST_PMT_DOC_NBR,
        PAYMENT.APPROVE_EMP_NAME,
        PAYMENT.APPROVE_DATE,
        PAYMENT.NET_PMT_AMT,
        PAYMENT.ACC_DESC
    From
        X003aa_empl_approve_own_payment PAYMENT
    Where
        PAYMENT.APPROVE_STATUS = "APPROVED"    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_find) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_find) + " EMPL APPROVE OWN PAYMENT invalid finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X003ac_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 TEXT,
            FIELD2 INT,
            FIELD3 TEXT,
            FIELD4 TEXT,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            DATE_MAILED TEXT)
            """)
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "employee_approve_own_payment":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + \
                         row[
                             3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[
                             8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the imported data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    sr_file = "X003ad_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'employee_approve_own_payment' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.DATE_MAILED
        From
            X003ab_findings FIND Left Join
            X003ac_get_previous PREV ON PREV.FIELD1 = FIND.VENDOR_ID And
                PREV.FIELD2 = FIND.CUST_PMT_DOC_NBR And
                PREV.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.today_plusdays(366))
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X003ae_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.VENDOR_ID AS FIELD1,
            PREV.CUST_PMT_DOC_NBR AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.DATE_MAILED
        From
            X003ad_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(so_curs, sr_file)
        if i_coun > 0:
            print("*** " + str(i_coun) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_coun) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X003af_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
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
          LOOKUP.LOOKUP = 'TEST_EMPL_APPROVE_OWN_PAYMENT_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X003ag_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
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
          LOOKUP.LOOKUP = 'TEST_EMPL_APPROVE_OWN_PAYMENT_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X003ah_contact"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            FIND.DOC_TYPE,
            FIND.VENDOR_ID,
            FIND.CUST_PMT_DOC_NBR,
            FIND.APPROVE_EMP_NAME,
            FIND.APPROVE_DATE,
            FIND.NET_PMT_AMT,
            FIND.ACC_DESC,
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
            ORG_SUP.MAIL As ORG_SUP_MAIL
        From
            X003ad_add_previous FIND Left Join
            X003af_officer CAMP_OFF On CAMP_OFF.TYPE = FIND.DOC_TYPE Left Join
            X003af_officer ORG_OFF On ORG_OFF.TYPE = 'NWU' Left Join
            X003ag_supervisor CAMP_SUP On CAMP_SUP.TYPE = FIND.DOC_TYPE Left Join
            X003ag_supervisor ORG_SUP On ORG_SUP.TYPE = 'NWU'
        Where
            FIND.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X003ax_empl_approve_own_payment"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0 and i_coun > 0:
        print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'EMPLOYEE APPROVE OWN PAYMENT' As Audit_finding,
            FIND.VENDOR_ID As Vendor_id,
            FIND.APPROVE_EMP_NAME As Employee_name,
            FIND.CUST_PMT_DOC_NBR As Edoc,
            FIND.APPROVE_DATE As Approve_date,
            FIND.NET_PMT_AMT As Amount,
            FIND.ACC_DESC As Note,
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
            X003ah_contact FIND
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "Creditor_test_003ax_empl_approve_own_"
            sx_file_date = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_date, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    TEST EMPLOYEE INITIATE OWN PAYMENT
    *****************************************************************************"""
    print("EMPLOYEE INITIATE OWN PAYMENT")
    funcfile.writelog("EMPLOYEE INITIATE OWN PAYMENT")

    # DECLARE VARIABLES
    i_coun: int = 0

    # OBTAIN TEST DATA
    print("Obtain test data...")
    sr_file: str = "X003ba_empl_initiate_own_payment"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PAYMENT.*
    From
        X001ad_Report_payments_initroute_curr PAYMENT
    Where
        SubStr(PAYMENT.VENDOR_ID, 1, 8) = PAYMENT.INIT_EMP_NO
    Order By
        PAYMENT.INIT_DATE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X003bb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        CASE
            WHEN PAYMENT.DOC_TYPE = 'CDV' THEN PAYMENT.DOC_TYPE
            WHEN PAYMENT.DOC_TYPE = 'CM' THEN PAYMENT.DOC_TYPE
            WHEN PAYMENT.DOC_TYPE = 'NEDV' THEN PAYMENT.DOC_TYPE
            WHEN PAYMENT.DOC_TYPE = 'PREQ' THEN PAYMENT.DOC_TYPE
            WHEN PAYMENT.DOC_TYPE = 'RV' THEN PAYMENT.DOC_TYPE
            WHEN PAYMENT.DOC_TYPE = 'SPDV' THEN PAYMENT.DOC_TYPE
            WHEN PAYMENT.DOC_TYPE = 'PDV' THEN PAYMENT.DOC_TYPE
            WHEN PAYMENT.DOC_TYPE = 'DV' THEN PAYMENT.DOC_TYPE
            ELSE 'OTHER'
        END As DOC_TYPE,
        PAYMENT.VENDOR_ID,
        PAYMENT.CUST_PMT_DOC_NBR,
        PAYMENT.INIT_EMP_NAME,
        PAYMENT.INIT_DATE,
        PAYMENT.NET_PMT_AMT,
        PAYMENT.ACC_DESC
    From
        X003ba_empl_initiate_own_payment PAYMENT
    Where
        PAYMENT.INIT_STATUS = "COMPLETED"    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_find) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_find) + " EMPL INITIATE OWN PAYMENT invalid finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X003bc_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 TEXT,
            FIELD2 INT,
            FIELD3 TEXT,
            FIELD4 TEXT,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            DATE_MAILED TEXT)
            """)
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "employee_initiate_own_payment":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + \
                         row[
                             3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[
                             8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the imported data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    sr_file = "X003bd_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'employee_initiate_own_payment' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.DATE_MAILED
        From
            X003bb_findings FIND Left Join
            X003bc_get_previous PREV ON PREV.FIELD1 = FIND.VENDOR_ID And
                PREV.FIELD2 = FIND.CUST_PMT_DOC_NBR And
                PREV.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.today_plusdays(366))
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X003be_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.VENDOR_ID AS FIELD1,
            PREV.CUST_PMT_DOC_NBR AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.DATE_MAILED
        From
            X003bd_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(so_curs, sr_file)
        if i_coun > 0:
            print("*** " + str(i_coun) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_coun) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X003bf_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
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
          LOOKUP.LOOKUP = 'TEST_EMPL_INITIATE_OWN_PAYMENT_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X003bg_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
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
          LOOKUP.LOOKUP = 'TEST_EMPL_INITIATE_OWN_PAYMENT_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X003bh_contact"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            FIND.DOC_TYPE,
            FIND.VENDOR_ID,
            FIND.CUST_PMT_DOC_NBR,
            FIND.INIT_EMP_NAME,
            FIND.INIT_DATE,
            FIND.NET_PMT_AMT,
            FIND.ACC_DESC,
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
            ORG_SUP.MAIL As ORG_SUP_MAIL
        From
            X003bd_add_previous FIND Left Join
            X003bf_officer CAMP_OFF On CAMP_OFF.TYPE = FIND.DOC_TYPE Left Join
            X003bf_officer ORG_OFF On ORG_OFF.TYPE = 'NWU' Left Join
            X003bg_supervisor CAMP_SUP On CAMP_SUP.TYPE = FIND.DOC_TYPE Left Join
            X003bg_supervisor ORG_SUP On ORG_SUP.TYPE = 'NWU'
        Where
            FIND.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X003bx_empl_initiate_own_payment"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0 and i_coun > 0:
        print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'EMPLOYEE INITIATE OWN PAYMENT' As Audit_finding,
            FIND.VENDOR_ID As Vendor_id,
            FIND.INIT_EMP_NAME As Employee_name,
            FIND.CUST_PMT_DOC_NBR As Edoc,
            FIND.INIT_DATE As Initiation_date,
            FIND.NET_PMT_AMT As Amount,
            FIND.ACC_DESC As Note,
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
            X003bh_contact FIND
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "Creditor_test_003bx_empl_initiate_own_"
            sx_file_date = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_date, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
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
