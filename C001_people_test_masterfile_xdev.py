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

# OPEN THE MYSQL DESTINATION TABLE
s_database = "Web_ia_nwu"
ms_cnxn = funcmysql.mysql_open(s_database)
ms_curs = ms_cnxn.cursor()
funcfile.writelog("%t OPEN MYSQL DATABASE: " + s_database)

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

""" ****************************************************************************
TEST PAYE NUMBER DUPLICATE
*****************************************************************************"""
print("TEST PAYE NUMBER DUPLICATE")
funcfile.writelog("TEST PAYE NUMBER DUPLICATE")

# DECLARE TEST VARIABLES
l_record = True # Record the findings in the previous reported findings file
i_find = 0 # Number of findings before previous reported findings
i_coun = 0 # Number of new findings to report

# COUNT ALL EMPLOYEES WITH A BANK ACCOUNT NUMBER
print("Count all employees with paye number...")
sr_file = "X005ca_paye_coun"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    X005_paye_master.NUMB,
    Count(X005_paye_master.EMP) As COUNT
From
    X005_paye_master
Where
    X005_paye_master.NUMB <> ''
Group By
    X005_paye_master.NUMB
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# IDENTIFY DUPLICATE ACCOUNTS
print("Build list of duplicate accounts...")
sr_file = "X005cb_paye_dupl"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    X005_paye_master.*,
    X005ca_paye_coun.COUNT
From
    X005_paye_master Left Join
    X005ca_paye_coun On X005ca_paye_coun.NUMB = X005_paye_master.NUMB
Where
    X005ca_paye_coun.COUNT > 1
Order by
    X005ca_paye_coun.NUMB, EMP
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# COUNT THE NUMBER OF FINDINGS
i_find = funcsys.tablerowcount(so_curs,sr_file)
print("*** Found "+str(i_find)+" exceptions ***")
funcfile.writelog("%t FINDING: "+str(i_find)+" PAYE duplicate finding(s)")

# GET PREVIOUS FINDINGS
# NOTE ADD CODE
sr_file = "X005cc_paye_getprev"
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
        elif row[0] != "paye_duplicate":
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the impoted data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

# ADD PREVIOUS FINDINGS
# NOTE ADD CODE
sr_file = "X005cd_paye_addprev"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0:
    print("Join previously reported to current findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
      X005cb_paye_dupl.*,
      'paye_duplicate' AS PROCESS,
      '%TODAY%' AS DATE_REPORTED,
      '%TODAYPLUS%' AS DATE_RETEST,
      X005cc_paye_getprev.PROCESS AS PREV_PROCESS,
      X005cc_paye_getprev.DATE_REPORTED AS PREV_DATE_REPORTED,
      X005cc_paye_getprev.DATE_RETEST AS PREV_DATE_RETEST,
      X005cc_paye_getprev.DATE_MAILED
    FROM
      X005cb_paye_dupl
      LEFT JOIN X005cc_paye_getprev ON X005cc_paye_getprev.FIELD1 = X005cb_paye_dupl.EMP AND
          X005cc_paye_getprev.DATE_RETEST >= Date('%TODAY%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%",funcdate.today())
    s_sql = s_sql.replace("%TODAYPLUS%",funcdate.today_plusdays(10))
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD LIST TO UPDATE FINDINGS
# NOTE ADD CODE
sr_file = "X005ce_paye_newprev"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0:
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X005cd_paye_addprev.PROCESS,
      X005cd_paye_addprev.EMP AS FIELD1,
      '' AS FIELD2,
      '' AS FIELD3,
      '' AS FIELD4,
      '' AS FIELD5,
      X005cd_paye_addprev.DATE_REPORTED,
      X005cd_paye_addprev.DATE_RETEST,
      X005cd_paye_addprev.DATE_MAILED
    FROM
      X005cd_paye_addprev
    WHERE
      X005cd_paye_addprev.PREV_PROCESS IS NULL
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
sr_file = "X005cf_offi"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0 and i_coun > 0:
    print("Import reporting officers for mail purposes...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
      PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
      PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
    FROM
      PEOPLE.X000_OWN_HR_LOOKUPS
      LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
    WHERE
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_PAYE_DUPLICATE_OFFICER'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
sr_file = "X005cg_supe"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0 and i_coun > 0:
    print("Import reporting supervisors for mail purposes...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
      PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
      PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
    FROM
      PEOPLE.X000_OWN_HR_LOOKUPS
      LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
    WHERE
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_PAYE_DUPLICATE_SUPERVISOR'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# ADD CONTACT DETAILS TO FINDINGS
sr_file = "X005ch_paye_cont"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0 and i_coun > 0:
    print("Add contact details to findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        X005cd_paye_addprev.ORG,
        X005cd_paye_addprev.LOC,
        X005cd_paye_addprev.EMP,
        X005cd_paye_addprev.NUMB,
        X005cd_paye_addprev.COUNT,
        PEOPLE.X002_PEOPLE_CURR.NAME_ADDR AS NAME,
        CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
        CAMP_OFF.KNOWN_NAME As CAMP_OFF_NAME,
        CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
        CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
        CAMP_SUP.KNOWN_NAME As CAMP_SUP_NAME,
        CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
        ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
        ORG_OFF.KNOWN_NAME As ORG_OFF_NAME,
        ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
        ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
        ORG_SUP.KNOWN_NAME As ORG_SUP_NAME,
        ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
    From
        X005cd_paye_addprev
        Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X005cd_paye_addprev.EMP
        Left Join X005cf_offi CAMP_OFF On CAMP_OFF.CAMPUS = X005cd_paye_addprev.LOC
        Left Join X005cf_offi ORG_OFF On ORG_OFF.CAMPUS = X005cd_paye_addprev.ORG
        Left Join X005cg_supe CAMP_SUP On CAMP_SUP.CAMPUS = X005cd_paye_addprev.LOC
        Left Join X005cg_supe ORG_SUP On ORG_SUP.CAMPUS = X005cd_paye_addprev.ORG
    WHERE
      X005cd_paye_addprev.PREV_PROCESS IS NULL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD THE FINAL TABLE FOR EXPORT AND REPORT
sr_file = "X005cx_paye_duplicate"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
print("Build the final report")
if i_find > 0 and i_coun > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        X005ch_paye_cont.ORG AS ORGANIZATION,
        X005ch_paye_cont.LOC AS LOCATION,
        X005ch_paye_cont.EMP AS EMPLOYEE_NUMBER,
        X005ch_paye_cont.NAME,
        X005ch_paye_cont.NUMB AS PAYE_NUMBER,
        X005ch_paye_cont.COUNT AS OCCURANCES,
        X005ch_paye_cont.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
        X005ch_paye_cont.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
        X005ch_paye_cont.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
        X005ch_paye_cont.CAMP_SUP_NAME AS SUPERVISOR,
        X005ch_paye_cont.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
        X005ch_paye_cont.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
        X005ch_paye_cont.ORG_OFF_NAME AS ORG_OFFICER,
        X005ch_paye_cont.ORG_OFF_NUMB AS ORG_OFFICER_NUMB,
        X005ch_paye_cont.ORG_OFF_MAIL AS ORG_OFFICER_MAIL,
        X005ch_paye_cont.ORG_SUP_NAME AS ORG_SUPERVISOR,
        X005ch_paye_cont.ORG_SUP_NUMB AS ORG_SUPERVISOR_NUMB,
        X005ch_paye_cont.ORG_SUP_MAIL AS ORG_SUPERVISOR_MAIL
    From
        X005ch_paye_cont
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
        sx_file = "People_test_005cx_paye_duplicate_"
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
