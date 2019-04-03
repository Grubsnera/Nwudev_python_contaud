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
BANK CHANGE MASTER FILE
*****************************************************************************"""

# BUILD TABLE WITH BANK ACCOUNT CHANGES FOR YESTERDAY (FRIDAY IF MONDAY)
print("Obtain master list of all bank changes...")
sr_file = "X004_bank_change"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
    PEOPLE.X000_PAY_ACCOUNTS.ASSIGNMENT_ID,
    PEOPLE.X001_ASSIGNMENT_CURR.EMPLOYEE_NUMBER,
    PEOPLE.X000_PAY_ACCOUNTS.EFFECTIVE_START_DATE,
    PEOPLE.X000_PAY_ACCOUNTS.EFFECTIVE_END_DATE,
    PEOPLE.X000_PAY_ACCOUNTS.PERSONAL_PAYMENT_METHOD_ID,
    PEOPLE.X000_PAY_ACCOUNTS.BUSINESS_GROUP_ID,
    PEOPLE.X000_PAY_ACCOUNTS.ORG_PAYMENT_METHOD_ID,
    PEOPLE.X000_PAY_ACCOUNTS.PPM_INFORMATION_CATEGORY,
    PEOPLE.X000_PAY_ACCOUNTS.PPM_INFORMATION1,
    PEOPLE.X000_PAY_ACCOUNTS.CREATION_DATE,
    PEOPLE.X000_PAY_ACCOUNTS.CREATED_BY,
    PEOPLE.X000_PAY_ACCOUNTS.LAST_UPDATE_DATE,
    PEOPLE.X000_PAY_ACCOUNTS.LAST_UPDATED_BY,
    PEOPLE.X000_PAY_ACCOUNTS.EXTERNAL_ACCOUNT_ID,
    PEOPLE.X000_PAY_ACCOUNTS.TERRITORY_CODE,
    PEOPLE.X000_PAY_ACCOUNTS.ACC_BRANCH,
    PEOPLE.X000_PAY_ACCOUNTS.ACC_TYPE_CODE,
    PEOPLE.X000_PAY_ACCOUNTS.ACC_TYPE,
    PEOPLE.X000_PAY_ACCOUNTS.ACC_NUMBER,
    PEOPLE.X000_PAY_ACCOUNTS.ACC_HOLDER,
    PEOPLE.X000_PAY_ACCOUNTS.ACC_UNKNOWN,
    PEOPLE.X000_PAY_ACCOUNTS.ACC_RELATION_CODE,
    PEOPLE.X000_PAY_ACCOUNTS.ACC_RELATION    
FROM
    PEOPLE.X000_PAY_ACCOUNTS LEFT JOIN
    PEOPLE.X001_ASSIGNMENT_CURR ON PEOPLE.X001_ASSIGNMENT_CURR.ASS_ID = PEOPLE.X000_PAY_ACCOUNTS.ASSIGNMENT_ID AND
        StrfTime('%Y-%m-%d',PEOPLE.X001_ASSIGNMENT_CURR.ASS_START) <= StrfTime('%Y-%m-%d',PEOPLE.X000_PAY_ACCOUNTS.LAST_UPDATE_DATE) AND
        StrfTime('%Y-%m-%d',PEOPLE.X001_ASSIGNMENT_CURR.ASS_END) >= StrfTime('%Y-%m-%d',PEOPLE.X000_PAY_ACCOUNTS.LAST_UPDATE_DATE)
WHERE
    %WHERE%
ORDER BY
    ASSIGNMENT_ID,
    LAST_UPDATE_DATE
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if funcdate.today_dayname() == "Mon":
    s_sql = s_sql.replace("%WHERE%","StrfTime('%Y-%m-%d',PEOPLE.X000_PAY_ACCOUNTS.LAST_UPDATE_DATE)>=StrfTime('%Y-%m-%d','now','-3 day') AND StrfTime('%Y-%m-%d',PEOPLE.X000_PAY_ACCOUNTS.CREATION_DATE)<StrfTime('%Y-%m-%d','now','-3 day')")
else:
    s_sql = s_sql.replace("%WHERE%","StrfTime('%Y-%m-%d',PEOPLE.X000_PAY_ACCOUNTS.LAST_UPDATE_DATE)>=StrfTime('%Y-%m-%d','now','-1 day') AND StrfTime('%Y-%m-%d',PEOPLE.X000_PAY_ACCOUNTS.CREATION_DATE)<StrfTime('%Y-%m-%d','now','-1 day')")
#print(s_sql) # DEBUG
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

""" ****************************************************************************
BANK CHANGE VERIFICATION
*****************************************************************************"""
print("BANK CHANGE VERIFICATION")
funcfile.writelog("BANK CHANGE VERIFICATION")

# DECLARE TEST VARIABLES
l_record = True # Record the findings in the previous reported findings file
i_find = 0 # Number of findings before previous reported findings
i_coun = 0 # Number of new findings to report

# ADD EMPLOYEE DETAILS
print("Add employee details...")
sr_file = "X004ba_bank_verify"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    'NWU' AS ORG,
    CASE PEOPLE.X002_PEOPLE_CURR.LOCATION_DESCRIPTION
        WHEN 'Mafikeng Campus' THEN 'MAF'
        WHEN 'Potchefstroom Campus' THEN 'POT'
        WHEN 'Vaal Triangle Campus' THEN 'VAA'
        ELSE 'NWU'
    END AS LOC,
    X004_bank_change.EMPLOYEE_NUMBER AS EMP,
    PEOPLE.X002_PEOPLE_CURR.NAME_ADDR AS NAME,
    X004_bank_change.EFFECTIVE_START_DATE AS START_DATE,
    X004_bank_change.EFFECTIVE_END_DATE AS END_DATE,
    X004_bank_change.MEANING AS ACC_TYPE,
    X004_bank_change.SEGMENT1 AS ACC_BRANCH,
    X004_bank_change.SEGMENT3 AS ACC_NUMBER,
    X004_bank_change.LAST_UPDATE_DATE AS UPDATE_DATE,
    X004_bank_change.LAST_UPDATED_BY AS UPDATE_BY
From
    X004_bank_change Left Join
    PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X004_bank_change.EMPLOYEE_NUMBER
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# FILTER RECORDS TO REPORT - ONLY EMPLOYEES ACTIVE TODAY
print("Filter employee records...")
sr_file = "X004bb_bank_verify"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    X004ba_bank_verify.ORG,
    X004ba_bank_verify.LOC,
    X004ba_bank_verify.EMP,
    X004ba_bank_verify.ACC_TYPE,
    X004ba_bank_verify.ACC_BRANCH,
    X004ba_bank_verify.ACC_NUMBER,
    X004ba_bank_verify.UPDATE_DATE,
    X004ba_bank_verify.UPDATE_BY
From
    X004ba_bank_verify
Where
    X004ba_bank_verify.NAME <> '' AND
    StrfTime('%Y-%m-%d',X004ba_bank_verify.END_DATE) >= StrfTime('%Y-%m-%d','now')
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# COUNT THE NUMBER OF FINDINGS
i_find = funcsys.tablerowcount(so_curs,sr_file)
print("*** Found "+str(i_find)+" exceptions ***")
funcfile.writelog("%t FINDING: "+str(i_find)+" BANK change finding(s)")

# GET PREVIOUS FINDINGS
# NOTE ADD CODE
sr_file = "X004bc_bank_getprev"
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
        elif row[0] != "bank_change":
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the impoted data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

# ADD PREVIOUS FINDINGS
sr_file = "X004bd_bank_addprev"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0:
    print("Join previously reported to current findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
      X004bb_bank_verify.*,
      'bank_change' AS PROCESS,
      '%TODAY%' AS DATE_REPORTED,
      '%TODAYPLUS%' AS DATE_RETEST,
      X004bc_bank_getprev.PROCESS AS PREV_PROCESS,
      X004bc_bank_getprev.DATE_REPORTED AS PREV_DATE_REPORTED,
      X004bc_bank_getprev.DATE_RETEST AS PREV_DATE_RETEST,
      X004bc_bank_getprev.DATE_MAILED
    FROM
      X004bb_bank_verify
      LEFT JOIN X004bc_bank_getprev ON X004bc_bank_getprev.FIELD1 = X004bb_bank_verify.EMP AND
          X004bc_bank_getprev.FIELD2 = X004bb_bank_verify.ACC_NUMBER AND
          X004bc_bank_getprev.DATE_RETEST >= Date('%TODAY%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%",funcdate.today())
    s_sql = s_sql.replace("%TODAYPLUS%",funcdate.today_plusdays(20000))
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD LIST TO UPDATE FINDINGS
# NOTE ADD CODE
sr_file = "X004be_bank_newprev"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0:
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X004bd_bank_addprev.PROCESS,
      X004bd_bank_addprev.EMP AS FIELD1,
      X004bd_bank_addprev.ACC_NUMBER AS FIELD2,
      '' AS FIELD3,
      '' AS FIELD4,
      '' AS FIELD5,
      X004bd_bank_addprev.DATE_REPORTED,
      X004bd_bank_addprev.DATE_RETEST,
      X004bd_bank_addprev.DATE_MAILED
    FROM
      X004bd_bank_addprev
    WHERE
      X004bd_bank_addprev.PREV_PROCESS IS NULL
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
sr_file = "X004bf_offi"
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
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_BANKACC_VERIFY_OFFICER'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
sr_file = "X004bg_supe"
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
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_BANKACC_VERIFY_SUPERVISOR'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# ADD CONTACT DETAILS TO FINDINGS
sr_file = "X004bh_bank_cont"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0 and i_coun > 0:
    print("Add contact details to findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        X004bd_bank_addprev.ORG,
        X004bd_bank_addprev.LOC,
        X004bd_bank_addprev.EMP,
        PEOPLE.X002_PEOPLE_CURR.NAME_ADDR AS NAME,
        X004bd_bank_addprev.ACC_TYPE,
        X004bd_bank_addprev.ACC_BRANCH,
        X004bd_bank_addprev.ACC_NUMBER,
        PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS AS MAIL,
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
        ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL,
        X004bd_bank_addprev.UPDATE_DATE,
        X004bd_bank_addprev.UPDATE_BY,
        PEOPLE.X000_USER_CURR.EMPLOYEE_NUMBER AS UPDATE_EMP,
        PEOPLE.X000_USER_CURR.KNOWN_NAME AS UPDATE_NAME,
        PEOPLE.X000_USER_CURR.EMAIL_ADDRESS AS UPDATE_MAIL
    From
        X004bd_bank_addprev
        Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X004bd_bank_addprev.EMP
        Left Join PEOPLE.X000_USER_CURR On PEOPLE.X000_USER_CURR.USER_ID = X004bd_bank_addprev.UPDATE_BY
        Left Join X004bf_offi CAMP_OFF On CAMP_OFF.CAMPUS = X004bd_bank_addprev.LOC
        Left Join X004bf_offi ORG_OFF On ORG_OFF.CAMPUS = X004bd_bank_addprev.ORG
        Left Join X004bg_supe CAMP_SUP On CAMP_SUP.CAMPUS = X004bd_bank_addprev.LOC
        Left Join X004bg_supe ORG_SUP On ORG_SUP.CAMPUS = X004bd_bank_addprev.ORG
    WHERE
      X004bd_bank_addprev.PREV_PROCESS IS NULL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD THE FINAL TABLE FOR EXPORT AND REPORT
sr_file = "X004bx_bank_verify"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
print("Build the final report")
if i_find > 0 and i_coun > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        X004bh_bank_cont.ORG AS ORGANIZATION,
        X004bh_bank_cont.LOC AS LOCATION,
        X004bh_bank_cont.EMP AS EMPLOYEE_NUMBER,
        X004bh_bank_cont.NAME,
        X004bh_bank_cont.ACC_TYPE,
        X004bh_bank_cont.ACC_BRANCH,
        X004bh_bank_cont.ACC_NUMBER,
        X004bh_bank_cont.MAIL,
        X004bh_bank_cont.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
        X004bh_bank_cont.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
        X004bh_bank_cont.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
        X004bh_bank_cont.CAMP_SUP_NAME AS SUPERVISOR,
        X004bh_bank_cont.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
        X004bh_bank_cont.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
        X004bh_bank_cont.ORG_OFF_NAME AS ORGANIZATION_OFFICER,
        X004bh_bank_cont.ORG_OFF_NUMB AS ORGANIZATION_OFFICER_NUMB,
        X004bh_bank_cont.ORG_OFF_MAIL AS ORGANIZATION_OFFICER_MAIL,
        X004bh_bank_cont.ORG_SUP_NAME AS ORGANIZATION_SUPERVISOR,
        X004bh_bank_cont.ORG_SUP_NUMB AS ORGANIZATION_SUPERVISOR_NUMB,
        X004bh_bank_cont.ORG_SUP_MAIL AS ORGANIZATION_SUPERVISOR_MAIL,
        X004bh_bank_cont.UPDATE_DATE,
        X004bh_bank_cont.UPDATE_BY,
        X004bh_bank_cont.UPDATE_EMP,
        X004bh_bank_cont.UPDATE_NAME,
        X004bh_bank_cont.UPDATE_MAIL
    From
        X004bh_bank_cont
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
        sx_file = "People_test_004bx_bank_verify_"
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
