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
from _my_modules import functest

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
funcfile.writelog("OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
so_curs.execute("ATTACH DATABASE '" + so_path + "People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/People_payroll/People_payroll.sqlite' AS 'PAYROLL'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

""" ****************************************************************************
TEST ZA DATE OF BIRTH INVALID
*****************************************************************************"""
print("TEST ZA DATE OF BIRTH INVALID")
funcfile.writelog("TEST ZA DATE OF BIRTH INVALID")

# VARIABLES
s_fprefix: str = 'X002c'
s_finding: str = 'ID DOB INVALID'

# BUILD TABLE WITH NOT EMPTY ID NUMBERS
print("Build not empty ID number table...")
# TODO Delete after first run on 13 Feb 2020
sr_file = s_fprefix + "a_dob_calc"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
sr_file = s_fprefix + "a_iddob_invalid"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    MAST.ORG,
    MAST.LOC,
    MAST.EMP,
    MAST.NUMB,
    MAST.DOB,
    SUBSTR(NUMB,1,2)||'-'||SUBSTR(NUMB,3,2)||'-'||SUBSTR(NUMB,5,2) AS DOBC,
    '' AS VAL
From
    X002_id_master MAST
Where
    MAST.NUMB <> ''
;"""
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# UPDATE COLUMNS
print("Update column valid...")
s_sql = "UPDATE " + s_fprefix + "a_iddob_invalid " + """
    SET VAL =
    CASE
        WHEN SUBSTR(DOB,3,8) = DOBC THEN 'T'
        ELSE 'F'
    END;"""
so_curs.execute(s_sql)
so_conn.commit()

# SELECT ALL EMPLOYEES WITH AN INVALID ID NUMBER
print("Select all employees with an invalid date of birth...")
# TODO Delete after first run on 13 Feb 2020
sr_file = s_fprefix + "b_dob_inva"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
sr_file = s_fprefix + "b_finding"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    FIND.ORG,
    FIND.LOC,
    FIND.EMP,
    FIND.NUMB,
    FIND.DOB,
    FIND.DOBC
From
    %FILEP%a_iddob_invalid FIND
Where
    FIND.VAL = 'F'
;"""
s_sql = s_sql.replace("%FILEP%", s_fprefix)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# COUNT THE NUMBER OF FINDINGS
i_finding_before = funcsys.tablerowcount(so_curs, sr_file)
print("*** Found " + str(i_finding_before) + " exceptions ***")
funcfile.writelog("%t FINDING: " + str(i_finding_before) + s_finding + " invalid finding(s)")

# GET PREVIOUS FINDINGS
# TODO Delete after first run on 13 Feb 2020
sr_file = s_fprefix + "c_dob_getprev"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    i = functest.get_previous_finding(so_curs, ed_path, "001_reported.txt", s_finding, "ITTTT")
    so_conn.commit()

# SET PREVIOUS FINDINGS
# TODO Delete after first run on 13 Feb 2020
sr_file = "X002cc_dob_setprev"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    i = functest.set_previous_finding(so_curs)
    so_conn.commit()

# ADD PREVIOUS FINDINGS
# TODO Delete after first run on 13 Feb 2020
sr_file = s_fprefix + "d_dob_addprev"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
sr_file = s_fprefix + "d_addprev"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    print("Join previously reported to current findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
        FIND.*,
        Lower('%FINDING%') AS PROCESS,
        '%TODAY%' AS DATE_REPORTED,
        '%TODAYPLUS%' AS DATE_RETEST,
        PREV.PROCESS AS PREV_PROCESS,
        PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
        PREV.DATE_RETEST AS PREV_DATE_RETEST,
        PREV.REMARK
    FROM
        %FILEP%b_finding FIND Left Join
        Z001ab_setprev PREV ON PREV.FIELD1 = FIND.EMP
    ;"""
    s_sql = s_sql.replace("%FINDING%", s_finding)
    s_sql = s_sql.replace("%FILEP%", s_fprefix)
    s_sql = s_sql.replace("%TODAY%", funcdate.today())
    s_sql = s_sql.replace("%TODAYPLUS%", funcdate.cur_monthend())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD LIST TO UPDATE FINDINGS
# TODO Delete after first run on 13 Feb 2020
sr_file = s_fprefix + "e_dob_newprev"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
sr_file = s_fprefix + "e_newprev"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
        PREV.PROCESS,
        PREV.EMP AS FIELD1,
        '' AS FIELD2,
        '' AS FIELD3,
        '' AS FIELD4,
        '' AS FIELD5,
        PREV.DATE_REPORTED,
        PREV.DATE_RETEST,
        PREV.REMARK
    FROM
        %FILEP%d_addprev PREV
    WHERE
        PREV.PREV_PROCESS IS NULL Or
        PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_fprefix)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export findings to previous reported file
    i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
    if i_finding_after > 0:
        print("*** " + str(i_finding_after) + " Finding(s) to report ***")
        sr_filet = sr_file
        sx_path = ed_path
        sx_file = "001_reported"
        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        # Write the data
        if l_record == True:
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head, "a", ".txt")
            funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
            funcfile.writelog("%t EXPORT DATA: " + sr_file)
    else:
        print("*** No new findings to report ***")
        funcfile.writelog("%t FINDING: No new findings to export")

# IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
# TODO Delete after first run on 13 Feb 2020
sr_file = s_fprefix + "f_offi"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0 and i_finding_after > 0:
    i = functest.get_officer(so_curs, "HR", "TEST " + s_finding + " OFFICER")
    so_conn.commit()

# IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
# TODO Delete after first run on 13 Feb 2020
sr_file = "X002cg_supe"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0 and i_finding_after > 0:
    i = functest.get_supervisor(so_curs, "HR", "TEST " + s_finding + " SUPERVISOR")
    so_conn.commit()

# ADD CONTACT DETAILS TO FINDINGS
# TODO Delete after first run on 13 Feb 2020
sr_file = s_fprefix + "h_dob_cont"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
sr_file = s_fprefix + "h_detail"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0 and i_finding_after > 0:
    print("Add contact details to findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.ORG,
        PREV.LOC,
        PREV.EMP,
        PEOPLE.X002_PEOPLE_CURR.NAME_LIST AS NAME,
        PREV.NUMB,
        SUBSTR(PREV.DOB,3,8) AS DOB,
        PREV.DOBC,
        CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
        CAMP_OFF.NAME_ADDR As CAMP_OFF_NAME,
        CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
        CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
        CAMP_SUP.NAME_ADDR As CAMP_SUP_NAME,
        CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
        ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
        ORG_OFF.NAME_ADDR As ORG_OFF_NAME,
        ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
        ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
        ORG_SUP.NAME_ADDR As ORG_SUP_NAME,
        ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
    From
        %FILEP%d_addprev PREV
        Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PREV.EMP
        Left Join Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC
        Left Join Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
        Left Join Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC
        Left Join Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
    WHERE
      PREV.PREV_PROCESS IS NULL
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_fprefix)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD THE FINAL TABLE FOR EXPORT AND REPORT

# TODO Delete after first run on 13 Feb 2020
sr_file = s_fprefix + "x_dob_invalid"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
sr_file = s_fprefix + "x_iddob_invalid"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0 and i_finding_after > 0:
    print("Build the final report")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'ID DOB INVALID' As AUDIT_FINDING,
        FIND.ORG AS ORGANIZATION,
        FIND.LOC AS LOCATION,
        FIND.EMP AS EMPLOYEE_NUMBER,
        FIND.NAME,
        FIND.NUMB AS ID_NUMBER,
        FIND.DOBC AS ID_DATE_OF_BIRTH,
        FIND.DOB AS SYSTEM_DATE_OF_BIRTH,
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
        %FILEP%h_detail FIND
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_fprefix)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export findings
    if l_export == True and funcsys.tablerowcount(so_curs, sr_file) > 0:
        print("Export findings...")
        sr_filet = sr_file
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "Peoplemaster_test_" + s_fprefix + "_" + s_finding.lower() + "_"
        sx_filet = sx_file + funcdate.today_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
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
