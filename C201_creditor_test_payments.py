""" Script to build kfs creditor payment tests *********************************
Created on: 16 Apr 2019
Author: Albert J v Rensburg (NWU21162395)
*****************************************************************************"""

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcsys

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
TEST CREDITOR DUPLICATE PAYMENT METHOD 1 (X001ax)(VENDOR,INVNBR,INVDT,AMOUNT)
TEST VENDOR SMALL SPLIT PAYMENTS         (X001bx)(V2.0.3)
TEST PAYMENT INITIATOR FISCAL SAME       (X001cx)(V1.0.9)
TEST DV VENDOR POSSIBLE PO VENDOR        (X001dx)(V2.0.3)
TEST VENDOR QUOTE SPLIT PAYMENTS         (X001ex)(V2.0.3)
TEST CREDITOR BANK VERIFICATION          (X002ax)
TEST EMPLOYEE APPROVE OWN PAYMENT        (X003ax)
TEST EMPLOYEE INITIATE OWN PAYMENT       (X003bx)
END OF SCRIPT
*****************************************************************************"""


def creditor_test_payments():

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # DEBUG SWITCH
    l_debug: bool = False

    # IMPORT PYTHON MODULES
    import csv
    import sqlite3
    import sys

    # ADD OWN MODULE PATH
    sys.path.append('S:/_my_modules')

    # IMPORT OWN MODULES
    from _my_modules import funcfile
    from _my_modules import funccsv
    from _my_modules import funcdate
    from _my_modules import funcstat
    from _my_modules import funcsms
    from _my_modules import functest

    # DECLARE VARIABLES
    so_path = "W:/Kfs/" #Source database path
    re_path = "R:/Kfs/" # Results path
    ed_path = "S:/_external_data/" #external data path
    so_file = "Kfs_test_creditor.sqlite" # Source database
    l_export = True
    l_mess: bool = funcconf.l_mess_project
    # l_mess: bool = True
    l_record = True

    # OPEN THE SCRIPT LOG FILE
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C201_CREDITOR_TEST_PAYMENTS")
    funcfile.writelog("-----------------------------------")
    if l_debug:
        print("---------------------------")
        print("C201_CREDITOR_TEST_PAYMENTS")
        print("---------------------------")

    # MESSAGE
    if funcconf.l_mess_project:
        funcsms.send_telegram("", "administrator", "<b>C201 Vendor payment tests</b>")

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    funcfile.writelog("OPEN THE DATABASES")
    if l_debug:
        print("OPEN THE DATABASES")

    # OPEN THE WORKING DATABASE
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH DATA SOURCES
    so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
    funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs_prev.sqlite' AS 'KFSPREV'")
    funcfile.writelog("%t ATTACH DATABASE: KFS_PREV.SQLITE")
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
    funcfile.writelog("TEST CREDITOR DUPLICATE PAYMENT")
    if l_debug:
        print("TEST CREDITOR DUPLICATE PAYMENT")

    # DECLARE TEST VARIABLES
    # l_record = False # Record the findings in the previous reported findings file
    i_find = 0  # Number of findings before previous reported findings
    i_coun = 0  # Number of new findings to report

    # BUILD CREDITOR PAYMENTS MASTER FILES - LAST DAY
    if l_debug:
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
        KFSCURR.X001aa_Report_payments PAYMENT
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
    if l_debug:
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
    funcfile.writelog("%t ADD COLUMN: INVOICE NUMBER")
    s_sql = "Update " + sr_file + " Set INV_CALC = " + """
    Cast(INV_CALC3 As INT)
    """
    so_curs.execute(s_sql)
    so_conn.commit()

    # BUILD CREDITOR PAYMENTS MASTER FILES - PREVIOUS YEAR PAYMENTS
    if l_debug:
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
        KFSPREV.X001aa_Report_payments PAYMENT
    Where
        PAYMENT.PMT_STAT_CD = 'EXTR'
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Calculate fields
    if l_debug:
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
    funcfile.writelog("%t ADD COLUMN: INVOICE NUMBER")
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
        KFSCURR.X001aa_Report_payments PAYMENT
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
    funcfile.writelog("%t ADD COLUMN: INVOICE NUMBER")
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
            if l_mess:
                s_desc = "Duplicate payment"
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
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
            Left Join KFSCURR.X001aa_Report_payments PAYMENT on PAYMENT.CUST_PMT_DOC_NBR = FIND.EDOC
            Left Join KFS.X000_Document DOC on DOC.DOC_HDR_ID = FIND.EDOC
            Left Join KFSCURR.X000_Account_line_unique LINE on LINE.FDOC_NBR = FIND.EDOC
            Left Join KFSCURR.X000_Account_line_unique DLINE on DLINE.FDOC_NBR = FIND.DUP_EDOC
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

    """*****************************************************************************
    TEST VENDOR SMALL SPLIT PAYMENTS
    *****************************************************************************"""
    funcfile.writelog("TEST VENDOR SMALL SPLIT PAYMENTS")
    if l_debug:
        print("TEST VENDOR SMALL SPLIT PAYMENTS")

    # FILES NEEDED

    # DECLARE TEST VARIABLES
    s_days: str = '7'  # Test days between payments - Note - Not in all tests. Remove in other tests
    s_limit: str = '5000'  # Test ceiling limit - Note - Not in all tests. Remove in other tests
    i_finding_after: int = 0
    s_description = "Vendor small split payment"
    s_file_prefix: str = "X001b"
    s_file_name: str = "vendor_small_split_payment"
    s_finding: str = "VENDOR SMALL SPLIT PAYMENT"
    s_report_file: str = "201_reported.txt"

    # IDENTIFY AND SUMMARIZE SMALL PAYMENTS
    if l_debug:
        print("Identify small payments...")
    sr_file: str = s_file_prefix + "a_a_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        a.ACC_COST_STRING,
        a.ORG_NM,
        a.FIN_OBJ_CD_NM,
        a.INIT_EMP_NO,
        a.INIT_EMP_NAME,
        a.VENDOR_ID,
        a.PAYEE_NAME,
        v.VNDR_TYP_CD,
        a.VENDOR_TYPE,
        a.DOC_TYPE,    
        a.PMT_DT,
        a.EDOC,
        Cast(Total(a.ACC_AMOUNT) As Real) As TOT_AMOUNT,
        oe.LOOKUP_DESCRIPTION As EXCLUDE_OBJECT,
        ve.LOOKUP_DESCRIPTION As EXCLUDE_VENDOR
    From
        KFSCURR.X001ad_Report_payments_accroute a Left Join
        KFS.X000_Vendor v on v.vendor_id = a.vendor_id Left Join
        KFS.X000_Own_kfs_lookups oe On oe.LOOKUP = '%OBJECT%' And
            oe.LOOKUP_CODE = Substr(a.ACC_COST_STRING, -4) Left Join
        KFS.X000_Own_kfs_lookups ve On ve.LOOKUP = '%VENDOR%' And
            ve.LOOKUP_CODE = a.VENDOR_ID
    Where
        a.PAYEE_TYPE = 'V' And
        a.DOC_TYPE in ('DV', 'PDV', 'PREQ') And
        Cast(Substr(a.ACC_COST_STRING, -4) As Int) Between 2051 and 4213 And
        oe.LOOKUP_CODE Is Null And
        ve.LOOKUP_CODE Is Null
    Group By
        a.ORG_NM,
        a.FIN_OBJ_CD_NM,    
        a.VENDOR_ID,
        a.PMT_DT,
        a.EDOC
    Having
        TOT_AMOUNT > 0 And
        TOT_AMOUNT < %LIMIT%    
    Order By
        a.ORG_NM,
        a.FIN_OBJ_CD_NM,    
        a.VENDOR_ID,
        a.PMT_DT,
        a.EDOC
    ;"""
    s_sql = s_sql.replace("%OBJECT%", "EXCLUDE OBJECT " + s_finding)
    s_sql = s_sql.replace("%VENDOR%", "EXCLUDE VENDOR " + s_finding)
    s_sql = s_sql.replace("%LIMIT%", s_limit)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # GROUP PAYMENTS BY DATE
    if l_debug:
        print("Group payments by date...")
    sr_file: str = s_file_prefix + "a_b_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        a.ORG_NM,
        a.FIN_OBJ_CD_NM,
        a.VENDOR_ID,
        a.PMT_DT,
        Min(a.EDOC) As EDOC,
        a.DOC_TYPE,
        Cast(Count(a.EDOC) As Int) As TRAN_COUNT,
        Cast(a.TOT_AMOUNT As Real) As TOT_AMOUNT
    From
        %FILEP%a_a_%FILEN% a
    Group By
        a.ORG_NM,
        a.FIN_OBJ_CD_NM,    
        a.VENDOR_ID,
        a.PMT_DT
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", s_file_name)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # IDENTIFY PAYMENT TRANSACTIONS
    if l_debug:
        print("Identify payment transactions...")
    sr_file: str = s_file_prefix + "a_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        a.ACC_COST_STRING,
        a.ORG_NM,
        a.FIN_OBJ_CD_NM,
        a.VENDOR_ID,
        a.PAYEE_NAME,
        a.VNDR_TYP_CD,        
        a.VENDOR_TYPE,
        a.INIT_EMP_NO,
        a.INIT_EMP_NAME,    
        a.EDOC As EDOC_A,
        a.DOC_TYPE As DOC_TYPE_A,
        a.PMT_DT As PMT_DATE_A,
        a.TOT_AMOUNT As AMOUNT_PD_A,
        b.EDOC As EDOC_B,
        b.DOC_TYPE As DOC_TYPE_B,
        cast(julianday(b.PMT_DT) - julianday(a.PMT_DT) As int) As DAYS_AFTER,
        b.PMT_DT As PMT_DATE_B,
        b.TOT_AMOUNT As AMOUNT_PD_B,
        b.TRAN_COUNT,
        cast(a.TOT_AMOUNT + b.TOT_AMOUNT As real) As TOTAL_AMOUNT_PD
    From
        %FILEP%a_a_%FILEN% a Inner Join
        %FILEP%a_b_%FILEN% b On b.ORG_NM = a.ORG_NM
                And b.FIN_OBJ_CD_NM = a.FIN_OBJ_CD_NM
                And b.VENDOR_ID = a.VENDOR_ID
                And julianday(b.PMT_DT) - julianday(a.PMT_DT) >= 0
                And julianday(b.PMT_DT) - julianday(a.PMT_DT) <= %DAYSBETWEEN%
                And cast(a.TOT_AMOUNT + b.TOT_AMOUNT As real) > %LIMIT%
                And a.EDOC != b.EDOC
    Order By
        a.ORG_NM,
        a.VENDOR_ID,
        a.PMT_DT
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", s_file_name)
    s_sql = s_sql.replace("%DAYSBETWEEN%", s_days)
    s_sql = s_sql.replace("%LIMIT%", s_limit)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # IDENTIFY FINDINGS
    if l_debug:
        print("Identify findings...")
    sr_file = s_file_prefix + "b_finding"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.VNDR_TYP_CD As VENDOR_TYPE,
        FIND.ACC_COST_STRING,
        FIND.EDOC_A,
        FIND.AMOUNT_PD_A,
        FIND.EDOC_B,
        FIND.AMOUNT_PD_B
    From
        %FILEP%%FILEN% FIND
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # COUNT THE NUMBER OF FINDINGS
    if l_debug:
        print("Count the number of findings...")
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")

    # GET PREVIOUS FINDINGS
    if i_finding_before > 0:
        functest.get_previous_finding(so_curs, ed_path, s_report_file, s_finding, "TIRIR")
        if l_debug:
            so_conn.commit()

    # SET PREVIOUS FINDINGS
    if i_finding_before > 0:
        functest.set_previous_finding(so_curs)
        if l_debug:
            so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = s_file_prefix + "d_addprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            Lower('%FINDING%') AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DATETEST%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%b_finding FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = FIND.ACC_COST_STRING And
                PREV.FIELD2 = FIND.EDOC_A And
                PREV.FIELD3 = FIND.AMOUNT_PD_A And
                PREV.FIELD4 = FIND.EDOC_B And
                PREV.FIELD5 = FIND.AMOUNT_PD_B
        ;"""
        s_sql = s_sql.replace("%FINDING%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DATETEST%", funcdate.cur_yearend())
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = s_file_prefix + "e_newprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Build list to update findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.ACC_COST_STRING AS FIELD1,
            PREV.EDOC_A AS FIELD2,
            PREV.AMOUNT_PD_A AS FIELD3,
            PREV.EDOC_B AS FIELD4,
            AMOUNT_PD_B AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%d_addprev PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""        
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = s_report_file[:-4]
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_finding_before) + '/' + str(
                    i_finding_after) + '</b> ' + s_description)
        else:
            funcfile.writelog("%t FINDING: No new findings to export")
            if l_debug:
                print("*** No new findings to report ***")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        functest.get_officer(so_curs, "HR", "TEST " + s_finding + " OFFICER")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        functest.get_supervisor(so_curs, "HR", "TEST " + s_finding + " SUPERVISOR")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = s_file_prefix + "h_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            FIND.VENDOR_ID,
            FIND.PAYEE_NAME,
            PREV.VENDOR_TYPE,
            PREV.ACC_COST_STRING,
            FIND.ORG_NM,
            FIND.FIN_OBJ_CD_NM,
            FIND.INIT_EMP_NO,
            FIND.INIT_EMP_NAME,
            PREV.EDOC_A,
            FIND.DOC_TYPE_A,
            FIND.PMT_DATE_A,
            PREV.AMOUNT_PD_A,
            PREV.EDOC_B,
            FIND.DOC_TYPE_B,
            FIND.PMT_DATE_B,
            PREV.AMOUNT_PD_B,
            FIND.DAYS_AFTER,
            FIND.TRAN_COUNT,
            FIND.TOTAL_AMOUNT_PD,
            CAMP_OFF.EMPLOYEE_NUMBER AS CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR AS CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END AS CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS AS CAMP_OFF_MAIL2,        
            CAMP_SUP.EMPLOYEE_NUMBER AS CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR AS CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER != '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END AS CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS AS CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER AS ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR AS ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER != '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END AS ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS AS ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER AS ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR AS ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER != '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END AS ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS AS ORG_SUP_MAIL2,
            AUD_OFF.EMPLOYEE_NUMBER As AUD_OFF_NUMB,
            AUD_OFF.NAME_ADDR As AUD_OFF_NAME,
            AUD_OFF.EMAIL_ADDRESS As AUD_OFF_MAIL,
            AUD_SUP.EMPLOYEE_NUMBER As AUD_SUP_NUMB,
            AUD_SUP.NAME_ADDR As AUD_SUP_NAME,
            AUD_SUP.EMAIL_ADDRESS As AUD_SUP_MAIL
        From
            %FILEP%d_addprev PREV Left Join
            %FILEP%a_%FILEN% FIND on FIND.ACC_COST_STRING = PREV.ACC_COST_STRING And
                FIND.EDOC_A = PREV.EDOC_A And
                FIND.EDOC_B = PREV.EDOC_B Left Join
            Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.VENDOR_TYPE Left Join
            Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD' Left Join
            Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.VENDOR_TYPE Left Join
            Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD'
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%FILEN%", s_file_name)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = s_file_prefix + "x_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            '%FIND%' As Audit_finding,
            FIND.ACC_COST_STRING As Cost_string,
            FIND.ORG_NM As Organization_name,
            FIND.FIN_OBJ_CD_NM As Object_name,
            FIND.VENDOR_ID As Vendor_id,
            FIND.PAYEE_NAME As Vendor_name,
            FIND.VENDOR_TYPE As Vendor_type,        
            FIND.INIT_EMP_NO As Initiator_number,
            FIND.INIT_EMP_NAME As Initiator_name,
            FIND.EDOC_A As Payment1_edoc,
            FIND.DOC_TYPE_A As Payment1_doctype,
            FIND.PMT_DATE_A As Payment1_date,
            FIND.AMOUNT_PD_A As Payment1_amount,
            FIND.DAYS_AFTER As Payment2_days,
            FIND.EDOC_B As Payment2_edoc,
            FIND.DOC_TYPE_B As Payment2_doctype,
            FIND.PMT_DATE_B As Payment2_date,
            FIND.AMOUNT_PD_B As Payment2_amount,
            FIND.TRAN_COUNT As Tran_count,
            FIND.TOTAL_AMOUNT_PD As Total_paid,
            FIND.ORG As Organization,
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
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.AUD_OFF_NAME AS Audit_Officer,
            FIND.AUD_OFF_NUMB AS Audit_Officer_Numb,
            FIND.AUD_OFF_MAIL AS Audit_Officer_Mail,
            FIND.AUD_SUP_NAME AS Audit_Supervisor,
            FIND.AUD_SUP_NUMB AS Audit_Supervisor_Numb,
            FIND.AUD_SUP_MAIL AS Audit_Supervisor_Mail
        From
            %FILEP%h_detail FIND
        ;"""
        s_sql = s_sql.replace("%FIND%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path
            sx_file = s_file_prefix + "_" + s_finding.lower() + "_"
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

    """ ****************************************************************************
    TEST PAYMENT INITIATOR FISCAL SAME
    *****************************************************************************"""
    print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    # DECLARE TEST VARIABLES
    i_finding_before: int = 0  # Number of findings before previous reported findings
    i_finding_after: int = 0  # Number of new findings to report

    # IDENTIFY TRANSACTIONS WHERE INITIATOR IS ALSO THE FISCAL OFFICER
    print("Identify transactions initiator and fiscal officer same...")
    sr_file: str = "X001ca_Same_initiator_fiscaloff"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        LIST.*
    From
        KFSCURR.X001ad_Report_payments_accroute LIST
    Where
        LIST.INIT_EMP_NO != '' And LIST.INIT_EMP_NO = LIST.ACCT_FSC_OFC_UID
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        print("Export findings...")
        sx_path = re_path + "/"
        sx_file = "Creditor_test_001ca_paym_same_init_fiscal_"
        sx_file_dated = sx_file + funcdate.today_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # IDENTIFY FINDINGS
    # NOTES
    # Only payments up to R5000
    print("Identify findings...")
    sr_file = "X001cb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        FIND.EDOC,
        FIND.VEndOR_ID,
        FIND.PAYEE_NAME,
        FIND.PAYEE_TYP_DESC,
        FIND.PMT_DT,
        FIND.INV_NBR,
        FIND.NET_PMT_AMT,
        Case
            When FIND.DOC_TYPE Like('CDV%') Then FIND.DOC_TYPE
            When FIND.DOC_TYPE Like('PDV%') Then FIND.DOC_TYPE
            When FIND.DOC_TYPE Like('DV%') Then FIND.DOC_TYPE
            Else 'OTHER'
        End As DOC_TYPE,
        FIND.INIT_DATE,
        FIND.INIT_EMP_NO,
        FIND.ACC_COST_STRING,
        FIND.ACC_AMOUNT,
        FIND.ORG_NM,
        FIND.ACCOUNT_NM,
        FIND.FIN_OBJ_CD_NM,
        FIND.ACC_DESC    
    From
        X001ca_Same_initiator_fiscaloff FIND
    Where
        FIND.NET_PMT_AMT <= 5000
    Order By
        FIND.INIT_EMP_NO,
        FIND.VEndOR_ID,
        FIND.PMT_DT        
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " INITIATOR FISCAL OFFICER SAME finding(s)")

    # GET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.get_previous_finding(so_curs, ed_path, "201_reported.txt", "initiator fiscal officer same",
                                          "IITTR")
        so_conn.commit()

    # SET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.set_previous_finding(so_curs)
        so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = "X001cd_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'initiator fiscal officer same' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            X001cb_findings FIND Left Join
            Z001ab_setprev PREV ON
                PREV.FIELD1 = FIND.EDOC And
                PREV.FIELD2 = FIND.INIT_EMP_NO And
                PREV.FIELD3 = FIND.INIT_DATE And
                PREV.FIELD4 = FIND.ACC_COST_STRING And
                PREV.FIELD5 = FIND.ACC_AMOUNT
        ;"""
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_yearend())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X001ce_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.EDOC AS FIELD1,
            Cast(PREV.INIT_EMP_NO As Int) AS FIELD2,
            PREV.INIT_DATE AS FIELD3,
            PREV.ACC_COST_STRING AS FIELD4,
            Cast(PREV.ACC_AMOUNT As REAL) AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            X001cd_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
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
            sx_file = "201_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                s_desc = "Initiator fiscal same"
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_officer(so_curs, "HR", "TEST_INITIATOR_FISCALOFFICER_SAME_OFFICER")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_supervisor(so_curs, "HR", "TEST_INITIATOR_FISCALOFFICER_SAME_SUPERVISOR")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X001ch_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.EDOC,
            PREV.VEndOR_ID,
            PREV.PAYEE_NAME,
            PREV.PAYEE_TYP_DESC,
            PREV.PMT_DT,
            PREV.INV_NBR,
            PREV.NET_PMT_AMT,
            PREV.DOC_TYPE,
            PREV.INIT_DATE,
            PREV.INIT_EMP_NO,
            PREV.ACC_COST_STRING,
            PREV.ACC_AMOUNT,
            PREV.ORG_NM,
            PREV.ACCOUNT_NM,
            PREV.FIN_OBJ_CD_NM,
            PREV.ACC_DESC,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR As CAMP_OFF_NAME,
            Case
                When  CAMP_OFF.EMPLOYEE_NUMBER <> '' Then CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                Else CAMP_OFF.EMAIL_ADDRESS
            End As CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL2,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR As CAMP_SUP_NAME,
            Case
                When CAMP_SUP.EMPLOYEE_NUMBER <> '' Then CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                Else CAMP_SUP.EMAIL_ADDRESS
            End As CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR As ORG_OFF_NAME,
            Case
                When ORG_OFF.EMPLOYEE_NUMBER <> '' Then ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                Else ORG_OFF.EMAIL_ADDRESS
            End As ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR As ORG_SUP_NAME,
            Case
                When ORG_SUP.EMPLOYEE_NUMBER <> '' Then ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                Else ORG_SUP.EMAIL_ADDRESS
            End As ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2,
            PREV.INIT_EMP_NO As USER_NUMB,
            Case
                When PREV.INIT_EMP_NO != '' Then PEOP.NAME_ADDR
                When CAMP_OFF.NAME_ADDR != '' Then CAMP_OFF.NAME_ADDR 
                Else ''
            End As USER_NAME,
            Case
                When PREV.INIT_EMP_NO != '' Then PREV.INIT_EMP_NO||'@nwu.ac.za'
                When CAMP_OFF.EMPLOYEE_NUMBER != '' Then CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                Else ''
            End As USER_MAIL,
            Case
                When PREV.INIT_EMP_NO != '' Then PEOP.EMAIL_ADDRESS
                When CAMP_OFF.EMPLOYEE_NUMBER != '' Then CAMP_OFF.EMAIL_ADDRESS
                Else ''
            End As USER_MAIL2
        From
            X001cd_add_previous PREV Left Join
            Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.DOC_TYPE Left Join
            Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = 'NWU' Left Join
            Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.DOC_TYPE Left Join
            Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = 'NWU' Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = PREV.INIT_EMP_NO
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        """
        When CAMP_OFF.NAME_ADDR != '' Then CAMP_OFF.NAME_ADDR 
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X001cx_Same_initiator_fiscaloff"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'INITIATOR FISCAL OFFICER SAME' As Audit_finding,
            FIND.EDOC As Edoc,
            FIND.VENDOR_ID As Vendor_id,
            FIND.PAYEE_NAME As Payee_name,
            FIND.PAYEE_TYP_DESC As Payee_type,
            FIND.PMT_DT As Payment_date,
            FIND.INV_NBR As Invoice_number,
            FIND.NET_PMT_AMT As Payment_amount,
            FIND.DOC_TYPE As Doc_type,
            FIND.INIT_DATE As Date_initiated,
            FIND.USER_NUMB As User_numb,
            FIND.USER_NAME As User_name,
            FIND.ACC_COST_STRING As Account,
            FIND.ACC_AMOUNT As Amount,
            FIND.ORG_NM As Acc_org_name,
            FIND.ACCOUNT_NM As Acc_name,
            FIND.FIN_OBJ_CD_NM As Acc_object,
            FIND.ACC_DESC As Description,
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
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.USER_MAIL As User_mail
        From
            X001ch_detail FIND
        Order By
            User_numb,
            Vendor_id,
            Date_initiated        
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + "/"
            sx_file = "Student_fee_test_021dx_qual_fee_negative_transaction_"
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

    """*****************************************************************************
    TEST DV VENDOR POSSIBLE PO VENDOR
    *****************************************************************************"""
    funcfile.writelog("TEST DV VENDOR POSSIBLE PO VENDOR")
    if l_debug:
        print("TEST DV VENDOR POSSIBLE PO VENDOR")

    # DECLARE TEST VARIABLES
    i_finding_after: int = 0
    s_description = "DV Vendor possible PO Vendor"
    s_file_prefix: str = "X001d"
    s_file_name: str = "dv_vendor_possible_po_vendor"
    s_finding: str = "DV VENDOR POSSIBLE PO VENDOR"
    s_report_file: str = "201_reported.txt"

    # OBTAIN LIST OF OBJECTS TO EXCLUDE
    t_list = funcstat.stat_tuple(so_curs, "KFS.X000_Own_kfs_lookups", "LOOKUP_CODE",
                                 "LOOKUP='EXCLUDE OBJECT VENDOR POSSIBLE PO'")
    if l_debug:
        print(t_list)

    # IDENTIFY DV VENDORS
    if l_debug:
        print("Identify DV vendors...")
    sr_file: str = s_file_prefix + "a_a_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        PAY.VENDOR_ID,
        PAY.PAYEE_NAME,
        PAY.PAYEE_TYPE,
        PAY.VENDOR_TYPE_CALC As VENDOR_TYPE,
        Count(PAY.ORIG_INV_AMT) As TRAN_COUNT,
        Total(PAY.NET_PMT_AMT) As AMOUNT_TOTAL,
        Total(PAY.NET_PMT_AMT) / Count(PAY.ORIG_INV_AMT) As TRAN_VALUE
    From
        KFSCURR.X001ad_Report_payments_accroute PAY
    Where
        PAY.VENDOR_TYPE_CALC = 'DV' And
        PAY.DOC_TYPE Not In ('CDV', 'SPDV') And
        Cast(Substr(PAY.ACC_COST_STRING, -4) As Int) Between 2051 and 4213 And
        Cast(Substr(PAY.ACC_COST_STRING, -4) As Int) Not In %EXCLUDE_LIST%    
    Group By
        PAY.VENDOR_ID
    ;"""
    s_sql = s_sql.replace("%EXCLUDE_LIST%", str(t_list))
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # CALCULATE STANDARD DEVIATIONS
    r_count = funcstat.stat_pstdev(so_curs, sr_file, 'TRAN_COUNT')
    r_amount = funcstat.stat_pstdev(so_curs, sr_file, 'AMOUNT_TOTAL')
    r_value = funcstat.stat_pstdev(so_curs, sr_file, 'TRAN_VALUE')
    if l_debug:
        print(r_count)
        print(r_amount)
        print(r_value)

    # SELECT DV VENDORS AND ADD OBJECTS
    # NOTE: This table not used further in the test. Just used to show objects with each selected vendor.
    if l_debug:
        print("Identify DV vendors and objects...")
    sr_file: str = s_file_prefix + "a_b_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        DVV.*,
        PAY.ACC_COST_STRING,
        PAY.FIN_OBJ_CD_NM
    From
        %FILEP%a_a_%FILEN% DVV Left Join
        KFSCURR.X001ad_Report_payments_accroute PAY On PAY.VENDOR_ID = DVV.VENDOR_ID
    Where
        (DVV.TRAN_COUNT >= %DEVCOUNT% And
        Cast(Substr(PAY.ACC_COST_STRING, -4) As Int) Between 2051 and 4213 And
        Cast(Substr(PAY.ACC_COST_STRING, -4) As Int) Not In %EXCLUDE_LIST%) Or       
        (DVV.TRAN_VALUE >= %DEVVALUE% And
        Cast(Substr(PAY.ACC_COST_STRING, -4) As Int) Between 2051 and 4213 And
        Cast(Substr(PAY.ACC_COST_STRING, -4) As Int) Not In %EXCLUDE_LIST%)       
    Group By
        DVV.VENDOR_ID,
        PAY.FIN_OBJ_CD_NM
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", s_file_name)
    s_sql = s_sql.replace("%DEVCOUNT%", str(int(r_count)))
    s_sql = s_sql.replace("%DEVVALUE%", str(round(r_value, 2)))
    s_sql = s_sql.replace("%EXCLUDE_LIST%", str(t_list))
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # SELECT VENDORS
    if l_debug:
        print("Identify DV vendors and objects...")
    sr_file: str = s_file_prefix + "a_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        DVV.VENDOR_ID,
        DVV.PAYEE_NAME,
        DVV.PAYEE_TYPE,
        VEN.PAYEE_TYPE_DESC,
        VEN.OWNER_TYPE,
        VEN.OWNER_TYPE_DESC,
        DVV.VENDOR_TYPE,
        VEN.VENDOR_TYPE_DESC,
        Cast(DVV.TRAN_COUNT As Int) As TRAN_COUNT,
        Round(Cast(DVV.AMOUNT_TOTAL As Real),2) As AMOUNT_TOTAL,
        Round(Cast(DVV.TRAN_VALUE As Real),2) As TRAN_VALUE
    From
        %FILEP%a_a_%FILEN% DVV Left Join
        KFSCURR.X002aa_Report_payments_summary VEN on VEN.VENDOR_ID = DVV.VENDOR_ID
    Where
        DVV.TRAN_COUNT >= %DEVCOUNT% Or
        DVV.AMOUNT_TOTAL >= %DEVAMOUNT% Or
        DVV.TRAN_VALUE >= %DEVVALUE%
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", s_file_name)
    s_sql = s_sql.replace("%DEVCOUNT%", str(int(r_count)))
    s_sql = s_sql.replace("%DEVAMOUNT%", str(round(r_amount, 2)))
    s_sql = s_sql.replace("%DEVVALUE%", str(round(r_value, 2)))
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # IDENTIFY FINDINGS
    if l_debug:
        print("Identify findings...")
    sr_file = s_file_prefix + "b_finding"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.VENDOR_ID,
        FIND.PAYEE_NAME,
        FIND.TRAN_COUNT,
        FIND.AMOUNT_TOTAL,
        FIND.TRAN_VALUE
    From
        %FILEP%%FILEN% FIND
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # COUNT THE NUMBER OF FINDINGS
    if l_debug:
        print("Count the number of findings...")
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")

    # GET PREVIOUS FINDINGS
    if i_finding_before > 0:
        functest.get_previous_finding(so_curs, ed_path, s_report_file, s_finding, "TTTTT")
        if l_debug:
            so_conn.commit()

    # SET PREVIOUS FINDINGS
    if i_finding_before > 0:
        functest.set_previous_finding(so_curs)
        if l_debug:
            so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = s_file_prefix + "d_addprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            Lower('%FINDING%') AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DATETEST%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%b_finding FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = FIND.VENDOR_ID
        ;"""
        s_sql = s_sql.replace("%FINDING%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DATETEST%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = s_file_prefix + "e_newprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Build list to update findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.VENDOR_ID AS FIELD1,
            PREV.PAYEE_NAME AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%d_addprev PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""        
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = s_report_file[:-4]
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_finding_before) + '/' + str(
                    i_finding_after) + '</b> ' + s_description)
        else:
            funcfile.writelog("%t FINDING: No new findings to export")
            if l_debug:
                print("*** No new findings to report ***")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        functest.get_officer(so_curs, "KFS", "TEST " + s_finding + " OFFICER")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        functest.get_supervisor(so_curs, "KFS", "TEST " + s_finding + " SUPERVISOR")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = s_file_prefix + "h_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.VENDOR_ID,
            PREV.PAYEE_NAME,
            FIND.PAYEE_TYPE,
            FIND.PAYEE_TYPE_DESC,
            FIND.OWNER_TYPE,
            FIND.OWNER_TYPE_DESC,
            FIND.VENDOR_TYPE,
            FIND.VENDOR_TYPE_DESC,
            PREV.TRAN_COUNT,
            PREV.AMOUNT_TOTAL,
            PREV.TRAN_VALUE,
            CAMP_OFF.EMPLOYEE_NUMBER AS CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR AS CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END AS CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS AS CAMP_OFF_MAIL2,        
            CAMP_SUP.EMPLOYEE_NUMBER AS CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR AS CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER != '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END AS CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS AS CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER AS ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR AS ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER != '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END AS ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS AS ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER AS ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR AS ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER != '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END AS ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS AS ORG_SUP_MAIL2,
            AUD_OFF.EMPLOYEE_NUMBER As AUD_OFF_NUMB,
            AUD_OFF.NAME_ADDR As AUD_OFF_NAME,
            AUD_OFF.EMAIL_ADDRESS As AUD_OFF_MAIL,
            AUD_SUP.EMPLOYEE_NUMBER As AUD_SUP_NUMB,
            AUD_SUP.NAME_ADDR As AUD_SUP_NAME,
            AUD_SUP.EMAIL_ADDRESS As AUD_SUP_MAIL
        From
            %FILEP%d_addprev PREV Left Join
            %FILEP%a_%FILEN% FIND on FIND.VENDOR_ID = PREV.VENDOR_ID Left Join
            Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = 'OTHER' Left Join
            Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD' Left Join
            Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = 'OTHER' Left Join
            Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD'
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%FILEN%", s_file_name)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = s_file_prefix + "x_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            '%FIND%' As Audit_finding,
            FIND.VENDOR_ID As Vendor_id,
            FIND.PAYEE_NAME Payee_name,
            FIND.PAYEE_TYPE Payee_type,
            FIND.PAYEE_TYPE_DESC Payee_type_desc,
            FIND.OWNER_TYPE Owner_type,
            FIND.OWNER_TYPE_DESC Owner_type_desc,
            FIND.VENDOR_TYPE Vendor_type,
            FIND.VENDOR_TYPE_DESC Vendor_type_desc,
            FIND.TRAN_COUNT Tran_count,
            FIND.AMOUNT_TOTAL Tran_total,
            FIND.TRAN_VALUE Tran_value,
            FIND.ORG As Organisation,
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
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.AUD_OFF_NAME AS Audit_Officer,
            FIND.AUD_OFF_NUMB AS Audit_Officer_Numb,
            FIND.AUD_OFF_MAIL AS Audit_Officer_Mail,
            FIND.AUD_SUP_NAME AS Audit_Supervisor,
            FIND.AUD_SUP_NUMB AS Audit_Supervisor_Numb,
            FIND.AUD_SUP_MAIL AS Audit_Supervisor_Mail
        From
            %FILEP%h_detail FIND
        ;"""
        s_sql = s_sql.replace("%FIND%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path
            sx_file = s_file_prefix + "_" + s_finding.lower() + "_"
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

    """*****************************************************************************
    TEST VENDOR QUOTE SPLIT PAYMENTS
    *****************************************************************************"""
    funcfile.writelog("TEST VENDOR QUOTE SPLIT PAYMENTS")
    if l_debug:
        print("TEST VENDOR QUOTE SPLIT PAYMENTS")

    # FILES NEEDED

    # DECLARE TEST VARIABLES
    s_days: str = '14'  # Test days between payments - Note - Not in all tests. Remove in other tests
    s_limit: str = '100000'  # Test ceiling limit - Note - Not in all tests. Remove in other tests
    i_finding_after: int = 0
    s_description = "Vendor quote split payment"
    s_file_prefix: str = "X001e"
    s_file_name: str = "vendor_quote_split_payment"
    s_finding: str = "VENDOR QUOTE SPLIT PAYMENT"
    s_report_file: str = "201_reported.txt"

    # IDENTIFY AND SUMMARIZE QUOTE PAYMENTS
    if l_debug:
        print("Identify quote payments...")
    sr_file: str = s_file_prefix + "a_a_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        a.ACC_COST_STRING,
        a.ORG_NM,
        a.FIN_OBJ_CD_NM,
        a.INIT_EMP_NO,
        a.INIT_EMP_NAME,
        a.VENDOR_ID,
        a.PAYEE_NAME,
        v.VNDR_TYP_CD,
        a.VENDOR_TYPE,
        a.DOC_TYPE,    
        a.PMT_DT,
        a.EDOC,
        Cast(Total(a.ACC_AMOUNT) As Real) As TOT_AMOUNT,
        oe.LOOKUP_DESCRIPTION As EXCLUDE_OBJECT,
        ve.LOOKUP_DESCRIPTION As EXCLUDE_VENDOR    
    From
        KFSCURR.X001ad_Report_payments_accroute a Left Join
        KFS.X000_Vendor v on v.vendor_id = a.vendor_id Left Join
        KFS.X000_Own_kfs_lookups oe On oe.LOOKUP = '%OBJECT%' And
            oe.LOOKUP_CODE = Substr(a.ACC_COST_STRING, -4) Left Join
        KFS.X000_Own_kfs_lookups ve On ve.LOOKUP = '%VENDOR%' And
            ve.LOOKUP_CODE = a.VENDOR_ID
    Where
        a.PAYEE_TYPE = 'V' And
        a.DOC_TYPE in ('DV', 'PDV', 'PREQ') And
        Cast(Substr(a.ACC_COST_STRING, -4) As Int) Between 2051 and 4213 And
        oe.LOOKUP_CODE Is Null And
        ve.LOOKUP_CODE Is Null
    Group By
        a.ORG_NM,
        a.FIN_OBJ_CD_NM,    
        a.VENDOR_ID,
        a.PMT_DT,
        a.EDOC
    Having
        TOT_AMOUNT > 0 And
        TOT_AMOUNT < %LIMIT%
    Order By
        a.ORG_NM,
        a.FIN_OBJ_CD_NM,    
        a.VENDOR_ID,
        a.PMT_DT,
        a.EDOC
    ;"""
    s_sql = s_sql.replace("%OBJECT%", "EXCLUDE OBJECT " + s_finding)
    s_sql = s_sql.replace("%VENDOR%", "EXCLUDE VENDOR " + s_finding)
    s_sql = s_sql.replace("%LIMIT%", s_limit)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # GROUP PAYMENTS BY DATE
    if l_debug:
        print("Group payments by date...")
    sr_file: str = s_file_prefix + "a_b_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        a.ORG_NM,
        a.FIN_OBJ_CD_NM,
        a.VENDOR_ID,
        a.PMT_DT,
        Min(a.EDOC) As EDOC,
        a.DOC_TYPE,
        Cast(Count(a.EDOC) As Int) As TRAN_COUNT,
        Cast(a.TOT_AMOUNT As Real) As TOT_AMOUNT
    From
        %FILEP%a_a_%FILEN% a
    Group By
        a.ORG_NM,
        a.FIN_OBJ_CD_NM,    
        a.VENDOR_ID,
        a.PMT_DT
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", s_file_name)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # IDENTIFY PAYMENT TRANSACTIONS
    if l_debug:
        print("Identify payment transactions...")
    sr_file: str = s_file_prefix + "a_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        a.ACC_COST_STRING,
        a.ORG_NM,
        a.FIN_OBJ_CD_NM,
        a.VENDOR_ID,
        a.PAYEE_NAME,
        a.VNDR_TYP_CD,        
        a.VENDOR_TYPE,
        a.DOC_TYPE,
        a.INIT_EMP_NO,
        a.INIT_EMP_NAME,    
        a.EDOC As EDOC_A,
        a.DOC_TYPE As DOC_TYPE_A,
        a.PMT_DT As PMT_DATE_A,
        a.TOT_AMOUNT As AMOUNT_PD_A,
        b.EDOC As EDOC_B,
        b.DOC_TYPE As DOC_TYPE_B,
        cast(julianday(b.PMT_DT) - julianday(a.PMT_DT) As int) As DAYS_AFTER,
        b.PMT_DT As PMT_DATE_B,
        b.TOT_AMOUNT As AMOUNT_PD_B,
        b.TRAN_COUNT,
        cast(a.TOT_AMOUNT + b.TOT_AMOUNT As real) As TOTAL_AMOUNT_PD
    From
        %FILEP%a_a_%FILEN% a Inner Join
        %FILEP%a_b_%FILEN% b On b.ORG_NM = a.ORG_NM
                And b.FIN_OBJ_CD_NM = a.FIN_OBJ_CD_NM
                And b.VENDOR_ID = a.VENDOR_ID
                And julianday(b.PMT_DT) - julianday(a.PMT_DT) >= 0
                And julianday(b.PMT_DT) - julianday(a.PMT_DT) <= %DAYSBETWEEN%
                And cast(a.TOT_AMOUNT + b.TOT_AMOUNT As real) > %LIMIT%
                And a.EDOC != b.EDOC
    Order By
        a.ORG_NM,
        a.VENDOR_ID,
        a.PMT_DT
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", s_file_name)
    s_sql = s_sql.replace("%DAYSBETWEEN%", s_days)
    s_sql = s_sql.replace("%LIMIT%", s_limit)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # IDENTIFY FINDINGS
    if l_debug:
        print("Identify findings...")
    sr_file = s_file_prefix + "b_finding"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.VNDR_TYP_CD As VENDOR_TYPE,
        FIND.ACC_COST_STRING,
        FIND.EDOC_A,
        FIND.AMOUNT_PD_A,
        FIND.EDOC_B,
        FIND.AMOUNT_PD_B
    From
        %FILEP%%FILEN% FIND
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # COUNT THE NUMBER OF FINDINGS
    if l_debug:
        print("Count the number of findings...")
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")

    # GET PREVIOUS FINDINGS
    if i_finding_before > 0:
        functest.get_previous_finding(so_curs, ed_path, s_report_file, s_finding, "TIRIR")
        if l_debug:
            so_conn.commit()

    # SET PREVIOUS FINDINGS
    if i_finding_before > 0:
        functest.set_previous_finding(so_curs)
        if l_debug:
            so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = s_file_prefix + "d_addprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            Lower('%FINDING%') AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DATETEST%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%b_finding FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = FIND.ACC_COST_STRING And
                PREV.FIELD2 = FIND.EDOC_A And
                PREV.FIELD3 = FIND.AMOUNT_PD_A And
                PREV.FIELD4 = FIND.EDOC_B And
                PREV.FIELD5 = FIND.AMOUNT_PD_B
        ;"""
        s_sql = s_sql.replace("%FINDING%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DATETEST%", funcdate.cur_yearend())
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = s_file_prefix + "e_newprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Build list to update findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.ACC_COST_STRING AS FIELD1,
            PREV.EDOC_A AS FIELD2,
            PREV.AMOUNT_PD_A AS FIELD3,
            PREV.EDOC_B AS FIELD4,
            AMOUNT_PD_B AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%d_addprev PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""        
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = s_report_file[:-4]
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_finding_before) + '/' + str(
                    i_finding_after) + '</b> ' + s_description)
        else:
            funcfile.writelog("%t FINDING: No new findings to export")
            if l_debug:
                print("*** No new findings to report ***")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        functest.get_officer(so_curs, "HR", "TEST " + s_finding + " OFFICER")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        functest.get_supervisor(so_curs, "HR", "TEST " + s_finding + " SUPERVISOR")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = s_file_prefix + "h_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            FIND.VENDOR_ID,
            FIND.PAYEE_NAME,
            PREV.VENDOR_TYPE,
            PREV.ACC_COST_STRING,
            FIND.ORG_NM,
            FIND.FIN_OBJ_CD_NM,
            FIND.INIT_EMP_NO,
            FIND.INIT_EMP_NAME,
            PREV.EDOC_A,
            FIND.DOC_TYPE_A,
            FIND.PMT_DATE_A,
            PREV.AMOUNT_PD_A,
            PREV.EDOC_B,
            FIND.DOC_TYPE_B,
            FIND.PMT_DATE_B,
            PREV.AMOUNT_PD_B,
            FIND.DAYS_AFTER,
            FIND.TRAN_COUNT,
            FIND.TOTAL_AMOUNT_PD,
            CAMP_OFF.EMPLOYEE_NUMBER AS CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR AS CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END AS CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS AS CAMP_OFF_MAIL2,        
            CAMP_SUP.EMPLOYEE_NUMBER AS CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR AS CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER != '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END AS CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS AS CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER AS ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR AS ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER != '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END AS ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS AS ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER AS ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR AS ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER != '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END AS ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS AS ORG_SUP_MAIL2,
            AUD_OFF.EMPLOYEE_NUMBER As AUD_OFF_NUMB,
            AUD_OFF.NAME_ADDR As AUD_OFF_NAME,
            AUD_OFF.EMAIL_ADDRESS As AUD_OFF_MAIL,
            AUD_SUP.EMPLOYEE_NUMBER As AUD_SUP_NUMB,
            AUD_SUP.NAME_ADDR As AUD_SUP_NAME,
            AUD_SUP.EMAIL_ADDRESS As AUD_SUP_MAIL
        From
            %FILEP%d_addprev PREV Left Join
            %FILEP%a_%FILEN% FIND on FIND.ACC_COST_STRING = PREV.ACC_COST_STRING And
                FIND.EDOC_A = PREV.EDOC_A And
                FIND.EDOC_B = PREV.EDOC_B Left Join
            Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.VENDOR_TYPE Left Join
            Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD' Left Join
            Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.VENDOR_TYPE Left Join
            Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD'
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%FILEN%", s_file_name)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = s_file_prefix + "x_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            '%FIND%' As Audit_finding,
            FIND.ACC_COST_STRING As Cost_string,
            FIND.ORG_NM As Organization_name,
            FIND.FIN_OBJ_CD_NM As Object_name,
            FIND.VENDOR_ID As Vendor_id,
            FIND.PAYEE_NAME As Vendor_name,
            FIND.VENDOR_TYPE As Vendor_type,        
            FIND.INIT_EMP_NO As Initiator_number,
            FIND.INIT_EMP_NAME As Initiator_name,
            FIND.EDOC_A As Payment1_edoc,
            FIND.DOC_TYPE_A As Payment1_doctype,
            FIND.PMT_DATE_A As Payment1_date,
            FIND.AMOUNT_PD_A As Payment1_amount,
            FIND.DAYS_AFTER As Payment2_days,
            FIND.EDOC_B As Payment2_edoc,
            FIND.DOC_TYPE_B As Payment2_doctype,        
            FIND.PMT_DATE_B As Payment2_date,
            FIND.AMOUNT_PD_B As Payment2_amount,
            FIND.TRAN_COUNT As Tran_count,
            FIND.TOTAL_AMOUNT_PD As Total_paid,
            FIND.ORG As Organization,
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
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.AUD_OFF_NAME AS Audit_Officer,
            FIND.AUD_OFF_NUMB AS Audit_Officer_Numb,
            FIND.AUD_OFF_MAIL AS Audit_Officer_Mail,
            FIND.AUD_SUP_NAME AS Audit_Supervisor,
            FIND.AUD_SUP_NUMB AS Audit_Supervisor_Numb,
            FIND.AUD_SUP_MAIL AS Audit_Supervisor_Mail
        From
            %FILEP%h_detail FIND
        ;"""
        s_sql = s_sql.replace("%FIND%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path
            sx_file = s_file_prefix + "_" + s_finding.lower() + "_"
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

    """ ****************************************************************************
    TEST CREDITOR BANK VERIFICATION
    *****************************************************************************"""
    print("TEST CREDITOR BANK VERIFICATION")
    funcfile.writelog("TEST CREDITOR BANK VERIFICATION")

    # TODO Fix upload to HIGHBOND when there is no records 23 Apr 2020

    # DECLARE TEST VARIABLES
    # l_record = True # Record the findings in the previous reported findings file
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
        KFS.X000_Vendor VEND
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
            if l_mess:
                s_desc = "Vendor bank acc verification"
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
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
            Left Join KFS.X000_Vendor VEND On VEND.VENDOR_ID = FIND.VENDOR_ID
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
        X001ac_Report_payments_approve PAYMENT
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
        co = open(ed_path + "201_reported.txt", "r")
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
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "201_reported.txt (" + sr_file + ")")

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
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_yearend())
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
            sx_file = "201_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_coun) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                s_desc = "Employee approve own payment"
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
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
        X001ab_Report_payments_initiate PAYMENT
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
        co = open(ed_path + "201_reported.txt", "r")
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
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "201_reported.txt (" + sr_file + ")")

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
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_yearend())
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
            sx_file = "201_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_coun) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                s_desc = "Employee initiate own payment"
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
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

    # MESSAGE
    # if funcconf.l_mess_project:
    #     funcsms.send_telegram("", "administrator", "Finished creditor <b>payment</b> tests.")

    """ ****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    so_conn.commit()
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("--------------------------------------")
    funcfile.writelog("COMPLETED: C201_CREDITOR_TEST_PAYMENTS")

    return


if __name__ == '__main__':
    try:
        creditor_test_payments()
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, "C201_creditor_test_payments", "C201_creditor_test_payments")
