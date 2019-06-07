""" Script to test PEOPLE master file data development area
Created on: 1 Mar 2019
Author: Albert Janse van Rensburg (NWU21162395)
"""

# IMPORT SYSTEM MODULES
import csv
import sqlite3

# OPEN OWN MODULES
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcsys

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
END OF SCRIPT
*****************************************************************************"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# OPEN THE LOG
print("-------------------------------")
print("C001_PEOPLE_TEST_MASTERFILE_DEV")
print("-------------------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C001_PEOPLE_TEST_MASTERFILE_DEV")
funcfile.writelog("---------------------------------------")

# DECLARE VARIABLES
ed_path = "S:/_external_data/"  # External data path
so_path = "W:/People/"  # Source database path
so_file = "People_test_masterfile.sqlite"  # Source database
re_path = "R:/People/"  # Results path
l_export: bool = False
l_mail: bool = False
l_record: bool = False

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN DATABASE: PEOPLE_TEST_MASTERFILE.SQLITE")

# ATTACH DATA SOURCES
so_curs.execute("ATTACH DATABASE '" + so_path + "People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

""" ****************************************************************************
BIO MASTER FILE
*****************************************************************************"""

# BUILD TABLE WITH EMPLOYEE BIO INFORMATION
print("Obtain master list of all employees...")
sr_file = "X008_bio_master"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    PEOP.EMPLOYEE_NUMBER,
    PEOP.LOCATION_DESCRIPTION,
    PEOP.ADDRESS_POST,
    PEOP.ADDRESS_SARS,
    PEOP.ADDRESS_HOME,
    PEOP.ADDRESS_OTHE,
    PEOP.PHONE_WORK
From
    PEOPLE.X002_PEOPLE_CURR PEOP
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

""" ****************************************************************************
TEST PHONE WORK INVALID
*****************************************************************************"""
print("PHONE WORK INVALID")
funcfile.writelog("PHONE WORK INVALID")

# DECLARE TEST VARIABLES
l_record: bool = True  # Record the findings in the previous reported findings file
i_finding_after: int = 0

# OBTAIN TEST DATA
print("Obtain test data and add employee details...")
sr_file: str = "X008aa_phone_work_invalid"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    'NWU' AS ORG,
    CASE MASTER.LOCATION_DESCRIPTION
        WHEN 'MAFIKENG CAMPUS' THEN 'MAF'
        WHEN 'POTCHEFSTROOM CAMPUS' THEN 'POT'
        WHEN 'VAAL TRIANGLE CAMPUS' THEN 'VAA'
        ELSE 'NWU'
    END AS LOC,
    MASTER.EMPLOYEE_NUMBER As EMP,
    CASE
        WHEN Substr(MASTER.ADDRESS_POST,1,1) = 'Y' THEN 1
        ELSE 0
    END As PRIMARY_VALID,
    CASE 
        WHEN Substr(MASTER.ADDRESS_POST,1,1) = 'Y' AND
            Substr(MASTER.ADDRESS_POST,2) <> Substr(MASTER.ADDRESS_SARS,2) AND
            Substr(MASTER.ADDRESS_SARS,1,1) = 'N' THEN 1
        WHEN Substr(MASTER.ADDRESS_POST,1,1) = 'Y' AND
            Substr(MASTER.ADDRESS_POST,2) <> Substr(MASTER.ADDRESS_HOME,2) AND
            Substr(MASTER.ADDRESS_HOME,1,1) = 'N' THEN 2
        WHEN Substr(MASTER.ADDRESS_POST,1,1) = 'Y' AND
            Substr(MASTER.ADDRESS_POST,2) <> Substr(MASTER.ADDRESS_OTHE,2) AND
            Substr(MASTER.ADDRESS_OTHE,1,1) = 'N' THEN 3
        ELSE 0 
    END As SECONDARY_VALID,
    CASE
        WHEN Length(MASTER.PHONE_WORK) = 10 THEN 1
        ELSE 0
    END As PHONE_VALID,
    MASTER.PHONE_WORK
From
    X008_bio_master MASTER
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IDENTIFY FINDINGS
print("Identify findings...")
sr_file = "X008ab_findings"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    CURR.ORG,
    CURR.LOC,
    CURR.EMP,
    CURR.PHONE_WORK
From
    X008aa_phone_work_invalid CURR
Where
    CURR.PHONE_VALID = 0
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# COUNT THE NUMBER OF FINDINGS
i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
print("*** Found " + str(i_finding_before) + " exceptions ***")
funcfile.writelog("%t FINDING: " + str(i_finding_before) + " EMPL PHONE WORK invalid finding(s)")

# GET PREVIOUS FINDINGS
sr_file = "X008ac_get_previous"
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
    co = open(ed_path + "001_reported.txt", "r")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "PROCESS":
            continue
        elif row[0] != "phone_work_invalid":
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
sr_file = "X008ad_add_previous"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    print("Join previously reported to current findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        FIND.*,
        'phone_work_invalid' AS PROCESS,
        '%TODAY%' AS DATE_REPORTED,
        '%DAYS%' AS DATE_RETEST,
        PREV.PROCESS AS PREV_PROCESS,
        PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
        PREV.DATE_RETEST AS PREV_DATE_RETEST,
        PREV.DATE_MAILED
    From
        X008ab_findings FIND Left Join
        X008ac_get_previous PREV ON PREV.FIELD1 = FIND.EMP AND
            PREV.DATE_RETEST >= Date('%TODAY%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%", funcdate.today())
    s_sql = s_sql.replace("%DAYS%", funcdate.today_plusdays(10))
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD LIST TO UPDATE FINDINGS
# NOTE ADD CODE
sr_file = "X008ae_new_previous"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.PROCESS,
        PREV.EMP AS FIELD1,
        '' AS FIELD2,
        '' AS FIELD3,
        '' AS FIELD4,
        '' AS FIELD5,
        PREV.DATE_REPORTED,
        PREV.DATE_RETEST,
        PREV.DATE_MAILED
    From
        X008ad_add_previous PREV
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
        sx_file = "001_reported"
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
sr_file = "X008af_officer"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    if i_finding_after > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            OFFICER.LOOKUP,
            OFFICER.LOOKUP_CODE AS CAMPUS,
            OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            PEOPLE.X000_OWN_HR_LOOKUPS OFFICER Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON
                PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
        Where
            OFFICER.LOOKUP = 'TEST_PHONE_WORK_INVALID_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
sr_file = "X008ag_supervisor"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0 and i_finding_after > 0:
    print("Import reporting supervisors for mail purposes...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        SUPERVISOR.LOOKUP,
        SUPERVISOR.LOOKUP_CODE AS CAMPUS,
        SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
        PEOP.NAME_ADDR As NAME,
        PEOP.EMAIL_ADDRESS
    From
        PEOPLE.X000_OWN_HR_LOOKUPS SUPERVISOR Left Join
        PEOPLE.X002_PEOPLE_CURR PEOP ON 
            PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
    Where
        SUPERVISOR.LOOKUP = 'TEST_PHONE_WORK_INVALID_SUPERVISOR'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# ADD CONTACT DETAILS TO FINDINGS
sr_file = "X008ah_detail"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0 and i_finding_after > 0:
    print("Add contact details to findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.ORG,
        PREV.LOC,
        PREV.EMP,
        PEOP.NAME_LIST,
        PREV.PHONE_WORK,
        CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
        CAMP_OFF.NAME As CAMP_OFF_NAME,
        CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
        CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
        CAMP_SUP.NAME As CAMP_SUP_NAME,
        CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
        ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
        ORG_OFF.NAME As ORG_OFF_NAME,
        ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
        ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
        ORG_SUP.NAME As ORG_SUP_NAME,
        ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
    From
        X008ad_add_previous PREV
        Left Join PEOPLE.X002_PEOPLE_CURR PEOP On PEOP.EMPLOYEE_NUMBER = PREV.EMP
        Left Join X008af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC
        Left Join X008af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
        Left Join X008ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC
        Left Join X008ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
    Where
      PREV.PREV_PROCESS IS NULL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD THE FINAL TABLE FOR EXPORT AND REPORT
sr_file = "X008ax_phone_work_invalid"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
print("Build the final report")
if i_finding_before > 0 and i_finding_after > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'EMPLOYEE WORK PHONE INVALID' As Audit_finding,
        FIND.EMP AS Employee,
        FIND.NAME_LIST As Name,
        FIND.PHONE_WORK As Work_phone,
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
        X008ah_detail FIND
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export findings
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        print("Export findings...")
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "People_test_008ax_phone_work_invalid_"
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
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE WORKING DATABASE
so_conn.close()

# CLOSE THE LOG
funcfile.writelog("------------------------------------------")
funcfile.writelog("COMPLETED: C001_PEOPLE_TEST_MASTERFILE_DEV")
