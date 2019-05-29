""" Script to test PEOPLE master file data *************************************
Created on: 1 Mar 2019
Author: Albert J v Rensburg (NWU21162395)
*****************************************************************************"""

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
END OF SCRIPT
*****************************************************************************"""

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
import funccsv
import funcdate
import funcfile
import funcmail
import funcmysql
import funcpeople
import funcstr
import funcsys

# OPEN THE SCRIPT LOG FILE
print("-------------------------------")    
print("C001_PEOPLE_TEST_MASTERFILE_DEV")
print("-------------------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C001_PEOPLE_TEST_MASTERFILE_DEV")
funcfile.writelog("---------------------------------------")
ilog_severity = 1

# DECLARE VARIABLES
so_path = "W:/People/" #Source database path
re_path = "R:/People/" # Results path
ed_path = "S:/_external_data/" #external data path
so_file = "People_test_masterfile.sqlite" # Source database
s_sql = "" # SQL statements
l_export = True
l_mail = True
l_record = False

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN DATABASE: PEOPLE_TEST_MASTERFILE.SQLITE")

# ATTACH DATA SOURCES
so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

""" ****************************************************************************
TEST GRADE INVALID
*****************************************************************************"""
print("GRADE INVALID")
funcfile.writelog("GRADE INVALID")

# DECLARE TEST VARIABLES
l_record = True # Record the findings in the previous reported findings file
i_find = 0 # Number of findings before previous reported findings
i_coun = 0 # Number of new findings to report

# IMPORT GRADE BENCHMARK
sr_file = "X007_grade_master"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
print("Import grade benchmark...")
so_curs.execute("CREATE TABLE " + sr_file + "(CATEGORY TEXT,TYPE TEXT,ACADSUPP TEXT,GRADE TEXT)")
s_cols = ""
co = open(ed_path + "001_employee_grade.csv", "r")
co_reader = csv.reader(co)
# Read the COLUMN database data
for row in co_reader:
    # Populate the column variables
    if row[0] == "CATEGORY":
        continue
    else:
        s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "')"
        so_curs.execute(s_cols)
so_conn.commit()
# Close the impoted data file
co.close()
funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_employee_grade.csv (" + sr_file + ")")

# IDENTIFY FINDING 
print("Identify incorrect data...")
sr_file = "X007ca_grade"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    MASTER.ORG,
    MASTER.LOC,
    MASTER.EMPLOYEE_NUMBER,
    MASTER.GRADE,
    BENCH.GRADE As GRADE_BENCH,
    MASTER.EMPLOYMENT_CATEGORY,
    MASTER.ACAD_SUPP,
    MASTER.PERSON_TYPE,
    CASE
        WHEN Instr(BENCH.GRADE,'.'||Trim(MASTER.GRADE)||'~') > 0 Then 'TRUE'
        ELSE 'FALSE'
    END As VALID
From
    X007_grade_leave_master MASTER Left Join
    X007_grade_master BENCH On BENCH.CATEGORY = MASTER.EMPLOYMENT_CATEGORY
            And BENCH.TYPE = MASTER.PERSON_TYPE
            And BENCH.ACADSUPP = MASTER.ACAD_SUPP    
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# ADD DETAILS
print("Add data details...")
sr_file = "X007cb_detail"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    FIND.ORG,
    FIND.LOC,
    FIND.EMPLOYEE_NUMBER
From
    X007ca_grade FIND
Where
    FIND.VALID = 'FALSE'
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# COUNT THE NUMBER OF FINDINGS
i_find = funcsys.tablerowcount(so_curs,sr_file)
print("*** Found "+str(i_find)+" exceptions ***")
funcfile.writelog("%t FINDING: "+str(i_find)+" GRADE invalid finding(s)")

# GET PREVIOUS FINDINGS
sr_file = "X007cc_getprev"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0:
    print("Import previously reported findings...")
    so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,DATE_MAILED TEXT)")
    s_cols = ""
    co = open(ed_path + "001_reported.txt", "r")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "PROCESS":
            continue
        elif row[0] != "empl_grade_invalid":
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the impoted data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

# ADD PREVIOUS FINDINGS
sr_file = "X007cd_addprev"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0:
    print("Join previously reported to current findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
      FIND.*,
      'empl_grade_invalid' AS PROCESS,
      '%TODAY%' AS DATE_REPORTED,
      '%TODAYPLUS%' AS DATE_RETEST,
      PREV.PROCESS AS PREV_PROCESS,
      PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
      PREV.DATE_RETEST AS PREV_DATE_RETEST,
      PREV.DATE_MAILED
    From
      X007cb_detail FIND Left Join
      X007cc_getprev PREV ON PREV.FIELD1 = FIND.EMPLOYEE_NUMBER
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%",funcdate.today())
    s_sql = s_sql.replace("%TODAYPLUS%",funcdate.today_plusdays(10))
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD LIST TO UPDATE FINDINGS
sr_file = "X007ce_newprev"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0:
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
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
      X007cd_addprev FIND
    Where
      FIND.PREV_PROCESS Is Null
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
        sx_file = "001_reported"
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
sr_file = "X007cf_officer"
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
      LOOKUP.LOOKUP = 'TEST_EMPL_GRADE_INVALID_OFFICER'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
sr_file = "X007cg_supervisor"
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
      LOOKUP.LOOKUP = 'TEST_EMPL_GRADE_INVALID_SUPERVISOR'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# ADD CONTACT DETAILS TO FINDINGS
sr_file = "X007ch_contact"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0 and i_coun > 0:
    print("Add contact details to findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        FIND.ORG,
        FIND.LOC,
        FIND.EMPLOYEE_NUMBER,
        PEOP.NAME_LIST,
        PEOP.EMPLOYMENT_CATEGORY,
        PEOP.PERSON_TYPE,
        PEOP.ACAD_SUPP,
        PEOP.GRADE As PGRADE,
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
        X007cd_addprev FIND Left Join
        X007cf_officer CAMP_OFF On CAMP_OFF.TYPE = FIND.LOC Left Join
        X007cf_officer ORG_OFF On ORG_OFF.TYPE = FIND.ORG Left Join
        X007cg_supervisor CAMP_SUP On CAMP_SUP.TYPE = FIND.LOC Left Join
        X007cg_supervisor ORG_SUP On ORG_SUP.TYPE = FIND.ORG Left Join
        PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = FIND.EMPLOYEE_NUMBER
    Where
        FIND.PREV_PROCESS IS NULL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD THE FINAL TABLE FOR EXPORT AND REPORT
sr_file = "X007cx_grade_invalid"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0 and i_coun > 0:
    print("Build the final report")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'EMPLOYEE GRADE INVALID' As Audit_finding,
        FIND.EMPLOYEE_NUMBER As Employee,
        FIND.NAME_LIST As Name,
        FIND.EMPLOYMENT_CATEGORY As Categoty,
        FIND.PERSON_TYPE As Person_type,
        FIND.ACAD_SUPP As Acad_supp,
        FIND.PGRADE As Grade,
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
        X007ch_contact FIND
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
        sx_file = "People_test_007cx_grade_invalid_"
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
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("------------------------------------------")
funcfile.writelog("COMPLETED: C001_PEOPLE_TEST_MASTERFILE_DEV")
