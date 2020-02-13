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
TEST WORK PERMIT EXPIRED
*****************************************************************************"""
print("WORK PERMIT EXPIRED")
funcfile.writelog("WORK PERMIT EXPIRED")

# DECLARE TEST VARIABLES
s_fprefix: str = "X003e"
s_finding: str = "EMPLOYEE WORK PERMIT EXPIRED"
i_finding_after: int = 0

# OBTAIN TEST DATA
print("Obtain test data and add employee details...")
sr_file: str = s_fprefix + "a_work_permit_expire"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    MASTER.ORG,
    MASTER.LOC,
    MASTER.EMP,
    MASTER.IDNO,
    MASTER.NUMB,
    MASTER.PERMIT,
    Substr(Replace(MASTER.PERMIT_EXPIRE,'/','-'),1,10) As EXPIRE_DATE,
    MASTER.POSITION,
    MASTER.ADDRESS_SARS,
    '' As VALID
From
    X003_pass_master MASTER
;"""
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# UPDATE SELECT FIELD WITH POSSIBLE FINDINGS
print("Update column valid...")
s_sql = "Update " + sr_file + """
    Set VALID =
    Case
        When PERMIT Like('PRP%') Then '0 PRP Permit'
        When PERMIT <> '' And IDNO <> '' Then '0 RSA ID number'
        When
            PERMIT <> '' And
            POSITION Like('EXTRA%')
        Then '0 Extraordinary position'
        When
            NUMB <> '' And
            EXPIRE_DATE >= Date('1900-01-01') And
            EXPIRE_DATE < Date('%TODAY%')
        Then '1 Select with date'
        When
            PERMIT <> '' And
            EXPIRE_DATE = ''
        Then '1 Select no date'        
    End;"""
s_sql = s_sql.replace("%TODAY%", funcdate.today_plusdays(30))
so_curs.execute(s_sql)
so_conn.commit()

# IDENTIFY FINDINGS
# TOTO Delete after first run
sr_file = s_fprefix + "b_findings"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
print("Identify findings...")
sr_file = s_fprefix + "b_finding"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    CURR.ORG,
    CURR.LOC,
    CURR.EMP,
    CURR.NUMB,
    CURR.PERMIT,
    CURR.EXPIRE_DATE
From
    %FILEP%a_work_permit_expire CURR
Where
    CURR.VALID Like('1%')
;"""
s_sql = s_sql.replace("%FILEP%", s_fprefix)
s_sql = s_sql.replace("%TODAY%", funcdate.today())
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# COUNT THE NUMBER OF FINDINGS
i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
print("*** Found " + str(i_finding_before) + " exceptions ***")
funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")

# GET PREVIOUS FINDINGS
if i_finding_before > 0:
    i = functest.get_previous_finding(so_curs, ed_path, "001_reported.txt", s_finding, "ITTTT")
    so_conn.commit()

# SET PREVIOUS FINDINGS
if i_finding_before > 0:
    i = functest.set_previous_finding(so_curs)
    so_conn.commit()

# ADD PREVIOUS FINDINGS
# TODO Delete after first run
sr_file = s_fprefix + "d_add_previous"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
sr_file = s_fprefix + "d_addprev"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    print("Join previously reported to current findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        FIND.*,
        Lower('%FINDING%') AS PROCESS,
        '%TODAY%' AS DATE_REPORTED,
        '%DAYS%' AS DATE_RETEST,
        PREV.PROCESS AS PREV_PROCESS,
        PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
        PREV.DATE_RETEST AS PREV_DATE_RETEST,
        PREV.REMARK
    From
        %FILEP%b_finding FIND Left Join
        Z001ab_setprev PREV ON PREV.FIELD1 = FIND.EMP
    ;"""
    s_sql = s_sql.replace("%FINDING%", s_finding)
    s_sql = s_sql.replace("%FILEP%", s_fprefix)
    s_sql = s_sql.replace("%TODAY%", funcdate.today())
    s_sql = s_sql.replace("%DAYS%", funcdate.cur_monthend())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD LIST TO UPDATE FINDINGS
# TODO Delete after first run
sr_file = s_fprefix + "e_new_previous"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
sr_file = s_fprefix + "e_newprev"
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
        PREV.REMARK
    From
        %FILEP%d_addprev PREV
    Where
        PREV.PREV_PROCESS Is Null Or
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
if i_finding_before > 0 and i_finding_after > 0:
    i = functest.get_officer(so_curs, "HR", "TEST " + s_finding + " OFFICER")
    so_conn.commit()

# IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
if i_finding_before > 0 and i_finding_after > 0:
    i = functest.get_supervisor(so_curs, "HR", "TEST " + s_finding + " SUPERVISOR")
    so_conn.commit()

# ADD CONTACT DETAILS TO FINDINGS
sr_file = s_fprefix + "h_detail"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0 and i_finding_after > 0:
    print("Add contact details to findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.ORG,
        PREV.LOC,
        PREV.EMP,
        PEOP.NAME_LIST,
        PREV.NUMB,
        PEOP.NATIONALITY_NAME,
        PREV.PERMIT,
        PREV.EXPIRE_DATE,
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
        Left Join PEOPLE.X002_PEOPLE_CURR PEOP On PEOP.EMPLOYEE_NUMBER = PREV.EMP
        Left Join Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC
        Left Join Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
        Left Join Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC
        Left Join Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
    Where
        PREV.PREV_PROCESS Is Null Or
        PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_fprefix)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD THE FINAL TABLE FOR EXPORT AND REPORT
sr_file = s_fprefix + "x_work_permit_expire"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
print("Build the final report")
if i_finding_before > 0 and i_finding_after > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'EMPLOYEE WORK PERMIT EXPIRED' As Audit_finding,
        FIND.EMP AS Employee,
        FIND.NAME_LIST As Name,
        FIND.NUMB As Passport,
        FIND.NATIONALITY_NAME As Nationality,
        FIND.PERMIT As Work_permit,
        FIND.EXPIRE_DATE As Permit_expire_date,
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
        %FILEP%h_detail FIND
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_fprefix)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export findings
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        print("Export findings...")
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "Peoplemaster_test_" + s_fprefix + "_" + s_finding.lower() + "_"
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
