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
so_curs.execute("ATTACH DATABASE 'W:/People_payroll/People_payroll.sqlite' AS 'PAYROLL'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

# LIST OF CURRENT SECONDARY ASSIGNMENTS (TEMPORARY ASSIGNMENT and TEMP FIXED TERM CONTRACT)
print("Obtain a list of secondary assignments...")
sr_file = "X007_leave01_secass_all"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    PEOP.EMPLOYEE_NUMBER,
    PEOP.ASS_ID,
    PEOP.NAME_LIST,
    PEOP.PERSON_TYPE,
    SEC.SEC_HOURS_FORECAST,
    SEC.SEC_DATE_FROM,
    SEC.SEC_DATE_TO,
    SEC.SEC_TYPE,
    SEC.SEC_RATE,
    SEC.SEC_UNIT,
    SEC.SEC_FULLPART_FLAG,
    CASE
        WHEN SEC.SEC_UNIT = 'MS' And SEC.SEC_TYPE = 'SALARY' And SEC.SEC_FULLPART_FLAG = 'F' THEN PEOP.PERSON_TYPE||'(F)'
        WHEN SEC.SEC_UNIT = 'MS' And SEC.SEC_TYPE = 'SALARY' And SEC.SEC_FULLPART_FLAG <> 'F' THEN 'C'
        ELSE PEOP.PERSON_TYPE||'(P)'    
    END As PERSON_TYPE_CALC,
    (JulianDay(SEC.SEC_DATE_TO) - JulianDay(SEC.SEC_DATE_FROM)) / 30.4167 As CALC_DUR_MONTH,
    SEC.SEC_HOURS_FORECAST / ((JulianDay(SEC.SEC_DATE_TO) - JulianDay(SEC.SEC_DATE_FROM)) / 30.4167) As CALC_HOUR_MONTH
From
    PEOPLE.X002_PEOPLE_CURR PEOP Inner Join
    PEOPLE.X001_ASSIGNMENT_SEC_CURR_YEAR SEC On SEC.ASSIGNMENT_ID = PEOP.ASS_ID
Where
    (PEOP.PERSON_TYPE = 'TEMPORARY APPOINTMENT' And
    SEC_DATE_FROM <= Date('%TODAY%') And
    SEC_DATE_TO >= Date('%TODAY%')) Or
    (PEOP.PERSON_TYPE = 'TEMP FIXED TERM CONTRACT' And
    SEC_DATE_FROM <= Date('%TODAY%') And
    SEC_DATE_TO >= Date('%TODAY%'))
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = s_sql.replace("%TODAY%", funcdate.today())
so_curs.execute(s_sql)
so_conn.commit()

# ISOLATE RECORDS WITHOUT FURTHER CALCULATION
print("Isolate records without further calculations...")
sr_file = "X007_leave02_ms_list"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    SEC.EMPLOYEE_NUMBER,
    SEC.PERSON_TYPE_CALC,
    Count(SEC.ASS_ID) As COUNT
From
    X007_leave01_secass_all SEC
Where
    SEC.PERSON_TYPE_CALC <> 'C'
Group By
    SEC.EMPLOYEE_NUMBER,
    SEC.PERSON_TYPE_CALC
Order By
    SEC.PERSON_TYPE_CALC
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()

# BUILD UNIQUE LIST OF RECORDS WITHOUT FURTHER CALCULATION
print("Build unique list of records...")
sr_file = "X007_leave03_ms_empl"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    SEC.EMPLOYEE_NUMBER,
    SEC.PERSON_TYPE_CALC
From
    X007_leave02_select1 SEC
Group By
    SEC.EMPLOYEE_NUMBER
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()

# BUILD HOURS PER MONTH SUMMARY LIST
print("Build hours per month list...")
sr_file = "X007_leave04_hoursum"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    SEC.EMPLOYEE_NUMBER,
    Count(SEC.ASS_ID) As SEC_COUNT,
    Sum(SEC.CALC_HOUR_MONTH) As CALC_HOUR_MONTH_SUM,
    CASE
        WHEN Sum(SEC.CALC_HOUR_MONTH) < 24 THEN SEC.PERSON_TYPE||'(P)'
        ELSE SEC.PERSON_TYPE||'(F)'
    END As PERSON_TYPE_CALC
From
    X007_leave01_secass_all SEC
Group By
    SEC.EMPLOYEE_NUMBER
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()

# BUILD GRADE AND LEAVE MASTER TABLE
print("Obtain master list of all grades and leave codes...")
sr_file = "X007_grade_leave_master"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    'NWU' As ORG,
    CASE LOCATION_DESCRIPTION
        WHEN 'MAFIKENG CAMPUS' THEN 'MAF'
        WHEN 'POTCHEFSTROOM CAMPUS' THEN 'POT'
        WHEN 'VAAL TRIANGLE CAMPUS' THEN 'VAA'
        ELSE 'NWU'
    END AS LOC,
    PEOPLE.EMPLOYEE_NUMBER,
    PEOPLE.EMP_START,
    LONG.DATE_LONG_SERVICE As SERVICE_START,
    PEOPLE.ACAD_SUPP,
    PEOPLE.EMPLOYMENT_CATEGORY,
    PEOPLE.PERSON_TYPE,
    PEOPLE.ASS_WEEK_LEN,
    PEOPLE.LEAVE_CODE,
    PEOPLE.GRADE,
    PEOPLE.GRADE_CALC,
    CASE
        WHEN LIST2.PERSON_TYPE_CALC IS NOT NULL THEN LIST2.PERSON_TYPE_CALC
        WHEN LIST1.PERSON_TYPE_CALC IS NOT NULL THEN LIST1.PERSON_TYPE_CALC 
        ELSE PEOPLE.PERSON_TYPE
    END As PERSON_TYPE_LEAVE,
    CASE
        WHEN LONG.DATE_LONG_SERVICE Is Null And PEOPLE.EMP_START < Date('2017-05-01') THEN 'OLD'
        WHEN LONG.DATE_LONG_SERVICE < Date('2017-05-01') THEN 'OLD'
        ELSE '2017'
    END As PERIOD
From
    PEOPLE.X002_PEOPLE_CURR PEOPLE Left Join
    X007_long_service_date LONG On LONG.EMPLOYEE_NUMBER = PEOPLE.EMPLOYEE_NUMBER Left Join
    X007_leave03_ms_empl LIST1 On LIST1.EMPLOYEE_NUMBER = PEOPLE.EMPLOYEE_NUMBER Left Join
    X007_leave04_hoursum LIST2 On LIST2.EMPLOYEE_NUMBER = PEOPLE.EMPLOYEE_NUMBER     
Where
    Substr(PEOPLE.PERSON_TYPE,1,6) <> 'AD HOC'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

""" ****************************************************************************
TEST LEAVE CODE INVALID
*****************************************************************************"""
print("LEAVE CODE INVALID")
funcfile.writelog("LEAVE CODE INVALID")

# DECLARE TEST VARIABLES
i_find = 0  # Number of findings before previous reported findings
i_coun = 0  # Number of new findings to report

# IMPORT LEAVE BENCHMARK
sr_file = "X007_leave_master"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
print("Import leave benchmark...")
so_curs.execute(
    "CREATE TABLE " + sr_file + "(CATEGORY TEXT,ACADSUPP TEXT,PERIOD TEXT,WEEK TEXT, GRADE TEXT, LEAVE TEXT)")
s_cols = ""
co = open(ed_path + "001_employee_leave.csv", "r")
co_reader = csv.reader(co)
# Read the COLUMN database data
for row in co_reader:
    # Populate the column variables
    if row[0] == "CATEGORY":
        continue
    else:
        s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[
            3] + "','" + row[4] + "','" + row[5] + "')"
        so_curs.execute(s_cols)
so_conn.commit()
# Close the impoted data file
co.close()
funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_employee_leave.csv (" + sr_file + ")")

# IDENTIFY FINDING
print("Identify incorrect data...")
sr_file = "X007da_leave"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    MASTER.ORG,
    MASTER.LOC,
    MASTER.EMPLOYEE_NUMBER,
    MASTER.LEAVE_CODE,
    MASTER.GRADE,
    MASTER.EMPLOYMENT_CATEGORY,
    MASTER.ACAD_SUPP,
    MASTER.PERSON_TYPE_LEAVE,
    MASTER.ASS_WEEK_LEN,
    MASTER.EMP_START,
    MASTER.SERVICE_START,
    MASTER.PERIOD,
    CASE
        WHEN MASTER.EMPLOYMENT_CATEGORY = 'PERMANENT' And Instr(PERM.GRADE,'.'||Trim(MASTER.GRADE)||'~') > 0 And Instr(PERM.LEAVE,'.'||Trim(MASTER.LEAVE_CODE)||'~') > 0 Then 'TRUE'
        WHEN MASTER.EMPLOYMENT_CATEGORY = 'TEMPORARY' And  Instr(TEMP.LEAVE,'.'||Trim(MASTER.LEAVE_CODE)||'~') > 0 Then 'TRUE'
        WHEN MASTER.EMPLOYMENT_CATEGORY = 'PERMANENT' Then 'FALSE'
        WHEN MASTER.EMPLOYMENT_CATEGORY = 'TEMPORARY' Then 'FALSE'
        ELSE 'OTHER'
    END As VALID,
    PERM.LEAVE As LEAVEP,
    PERM.GRADE As GRADEP,
    TEMP.LEAVE As LEAVET,
    TEMP.GRADE As GRADET
From
    X007_grade_leave_master MASTER Left Join
    X007_leave_master PERM On PERM.CATEGORY = MASTER.EMPLOYMENT_CATEGORY And
        PERM.ACADSUPP = MASTER.ACAD_SUPP And
        PERM.PERIOD = MASTER.PERIOD And
        PERM.WEEK = MASTER.ASS_WEEK_LEN And
        Instr(PERM.GRADE,'.'||Trim(MASTER.GRADE)||'~') > 0 And
        PERM.CATEGORY = 'PERMANENT' Left Join
    X007_leave_master TEMP On TEMP.CATEGORY = MASTER.EMPLOYMENT_CATEGORY And
        TEMP.GRADE = MASTER.PERSON_TYPE_LEAVE And
        TEMP.CATEGORY = 'TEMPORARY'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)
# EXPORT TEST DATA
if l_export == True:
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "X007da_leave_"
    sx_filet = sx_file + funcdate.cur_month()
    print("Export people birthday..." + sx_path + sx_filet)
    # Read the header data
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    # Write the data
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# ADD DETAILS
print("Add data details...")
sr_file = "X007db_detail"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    FIND.ORG,
    FIND.LOC,
    FIND.EMPLOYEE_NUMBER,
    CASE
        WHEN FIND.LEAVEP Is Null THEN FIND.LEAVET
        ELSE FIND.LEAVEP
    END As LEAVE_PROP,
    FIND.SERVICE_START,
    FIND.PERSON_TYPE_LEAVE
From
    X007da_leave FIND
Where
    FIND.VALID = 'FALSE' And
    FIND.PERSON_TYPE_LEAVE <> 'EXTRAORDINARY APPOINTMENT'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# COUNT THE NUMBER OF FINDINGS
i_find = funcsys.tablerowcount(so_curs, sr_file)
print("*** Found " + str(i_find) + " exceptions ***")
funcfile.writelog("%t FINDING: " + str(i_find) + " EMPL LEAVE CODE invalid finding(s)")

# GET PREVIOUS FINDINGS
sr_file = "X007dc_getprev"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_find > 0:
    print("Import previously reported findings...")
    so_curs.execute(
        "CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,DATE_MAILED TEXT)")
    s_cols = ""
    co = open(ed_path + "001_reported.txt", "r")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "PROCESS":
            continue
        elif row[0] != "empl_leave_invalid":
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + \
                     row[
                         3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[
                         8] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the impoted data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

# ADD PREVIOUS FINDINGS
sr_file = "X007dd_addprev"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_find > 0:
    print("Join previously reported to current findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
      FIND.*,
      'empl_leave_invalid' AS PROCESS,
      '%TODAY%' AS DATE_REPORTED,
      '%TODAYPLUS%' AS DATE_RETEST,
      PREV.PROCESS AS PREV_PROCESS,
      PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
      PREV.DATE_RETEST AS PREV_DATE_RETEST,
      PREV.DATE_MAILED
    From
      X007db_detail FIND Left Join
      X007dc_getprev PREV ON PREV.FIELD1 = FIND.EMPLOYEE_NUMBER AND
          PREV.DATE_RETEST >= Date('%TODAY%')          
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%", funcdate.today())
    s_sql = s_sql.replace("%TODAYPLUS%", funcdate.today_plusdays(10))
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD LIST TO UPDATE FINDINGS
sr_file = "X007de_newprev"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_find > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
      FIND.PROCESS,
      FIND.EMPLOYEE_NUMBER As FIELD1,
      '' As FIELD2,
      '' AS FIELD3,
      '' AS FIELD4,
      '' AS FIELD5,
      FIND.DATE_REPORTED,
      FIND.DATE_RETEST,
      FIND.DATE_MAILED
    From
      X007dd_addprev FIND
    Where
      FIND.PREV_PROCESS Is Null
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export findings to previous reported file
    i_coun = funcsys.tablerowcount(so_curs, sr_file)
    if i_coun > 0:
        print("*** " + str(i_coun) + " Finding(s) to report ***")
        sr_filet = sr_file
        sx_path = ed_path
        sx_file = "001_reported"
        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        # Write the data
        if l_record == True:
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head, "a", ".txt")
            funcfile.writelog("%t FINDING: " + str(i_coun) + " new finding(s) to export")
            funcfile.writelog("%t EXPORT DATA: " + sr_file)
    else:
        print("*** No new findings to report ***")
        funcfile.writelog("%t FINDING: No new findings to export")

# IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
sr_file = "X007df_officer"
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
      LOOKUP.LOOKUP = 'TEST_EMPL_LEAVECODE_INVALID_OFFICER'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
sr_file = "X007dg_supervisor"
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
      LOOKUP.LOOKUP = 'TEST_EMPL_LEAVECODE_INVALID_SUPERVISOR'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# ADD CONTACT DETAILS TO FINDINGS
sr_file = "X007dh_contact"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_find > 0 and i_coun > 0:
    print("Add contact details to findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        FIND.ORG,
        FIND.LOC,
        FIND.EMPLOYEE_NUMBER,
        PEOP.NAME_LIST,
        PEOP.LEAVE_CODE,
        FIND.LEAVE_PROP,
        PEOP.EMPLOYMENT_CATEGORY,
        FIND.PERSON_TYPE_LEAVE,
        PEOP.ACAD_SUPP,
        PEOP.GRADE As PGRADE,
        PEOP.ASS_WEEK_LEN,
        PEOP.EMP_START,
        FIND.SERVICE_START,
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
        X007dd_addprev FIND Left Join
        X007df_officer CAMP_OFF On CAMP_OFF.TYPE = FIND.LOC Left Join
        X007df_officer ORG_OFF On ORG_OFF.TYPE = FIND.ORG Left Join
        X007dg_supervisor CAMP_SUP On CAMP_SUP.TYPE = FIND.LOC Left Join
        X007dg_supervisor ORG_SUP On ORG_SUP.TYPE = FIND.ORG Left Join
        PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = FIND.EMPLOYEE_NUMBER
    Where
        FIND.PREV_PROCESS IS NULL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD THE FINAL TABLE FOR EXPORT AND REPORT
sr_file = "X007dx_leavecode_invalid"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_find > 0 and i_coun > 0:
    print("Build the final report")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'EMPLOYEE LEAVE CODE INVALID' As Audit_finding,
        FIND.EMPLOYEE_NUMBER As Employee,
        FIND.NAME_LIST As Name,
        FIND.LEAVE_CODE As Invalid_leave_code,
        FIND.LEAVE_PROP As Proposed_leave_code,
        FIND.EMP_START As Start_date,
        FIND.SERVICE_START As Service_start_date,
        FIND.PGRADE As Grade,
        FIND.EMPLOYMENT_CATEGORY As Category,
        FIND.PERSON_TYPE_LEAVE As Person_type,
        FIND.ACAD_SUPP As Acad_supp,
        FIND.ASS_WEEK_LEN As Workdays,
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
        X007dh_contact FIND
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export findings
    if l_export == True and funcsys.tablerowcount(so_curs, sr_file) > 0:
        print("Export findings...")
        sr_filet = sr_file
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "People_test_007dx_leave_invalid_"
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
