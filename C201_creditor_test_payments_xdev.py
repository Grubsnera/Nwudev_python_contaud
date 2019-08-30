""" Script to build kfs creditor payment tests *********************************
Created on: 16 Apr 2019
Author: Albert J v Rensburg (NWU21162395)
*****************************************************************************"""

# IMPORT PYTHON MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcsys

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# OPEN THE SCRIPT LOG FILE
print("----------------------")
print("C201_CREDITOR_TEST_DEV")
print("----------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C201_CREDITOR_TEST_DEV")
funcfile.writelog("------------------------------")
ilog_severity = 1

# DECLARE VARIABLES
so_path = "W:/Kfs/"  # Source database path
so_file = "Kfs_test_creditor.sqlite"  # Source database
re_path = "R:/Kfs/"  # Results path
ed_path = "S:/_external_data/"  # External data path
l_export = True
l_mail = False
l_record = False

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

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
      LOOKUP.LOOKUP = 'TEST_EMPL_INITIATE_OWN_PAYMENT_OFFICER'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
sr_file = "X003bg_supervisor"
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
      LOOKUP.LOOKUP = 'TEST_EMPL_INITIATE_OWN_PAYMENT_SUPERVISOR'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# ADD CONTACT DETAILS TO FINDINGS
sr_file = "X003bh_contact"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
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
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
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
        sx_file = "People_test_007cx_grade_invalid_"
        sx_file_date = sx_file + funcdate.today_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_date, s_head)
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
TEST EMPLOYEE INITIATE OWN PAYMENT
*****************************************************************************"""
print("EMPLOYEE INITIATE OWN PAYMENT")
funcfile.writelog("EMPLOYEE INITIATE OWN PAYMENT")

# DECLARE VARIABLES
i_coun: int = 0

# OBTAIN TEST DATA
print("Obtain test data...")
sr_file: str = "X003ba_employee_initiate_own_payment"
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


""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""

print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("---------------------------------")
funcfile.writelog("COMPLETED: C201_CREDITOR_TEST_DEV")
