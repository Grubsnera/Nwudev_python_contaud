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
TEST TAX NUMBER INVALID
    NOTE 01: SELECT ALL CURRENT EMPLOYEES WITH PAYE TAX NUMBER
*****************************************************************************"""
print("TEST PAYE NUMBER INVALID")
funcfile.writelog("TEST PAYE NUMBER INVALID")

# DECLARE TEST VARIABLES
#l_record = True # Record the findings in the previous reported findings file
i_find = 0 # Number of findings before previous reported findings
i_coun = 0 # Number of new findings to report

# BUILD TABLE WITH EMPLOYEE PAYE NUMBERS
print("Obtain list of all employees...")
sr_file = "X005_paye_master"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    'NWU' AS ORG,
    CASE LOCATION_DESCRIPTION
        WHEN 'Mafikeng Campus' THEN 'MAF'
        WHEN 'Potchefstroom Campus' THEN 'POT'
        WHEN 'Vaal Triangle Campus' THEN 'VAA'
        ELSE 'NWU'
    END AS LOC,
    PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER AS EMP,
    PEOPLE.X002_PEOPLE_CURR.TAX_NUMBER AS NUMB,
    PEOPLE.X002_PEOPLE_CURR.NATIONALITY AS NAT,
    UPPER(PEOPLE.X002_PEOPLE_CURR.PERSON_TYPE) AS TYPE,
    PEOPLE.X002_PEOPLE_CURR.EMP_START AS DSTA
From
    PEOPLE.X002_PEOPLE_CURR
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# SELECT ALL EMPLOYEES WITH A PAYE TAX NUMBER
print("Select all employees with paye number...")
sr_file = "X005ba_paye_calc"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    X005_paye_master.ORG,
    X005_paye_master.LOC,
    X005_paye_master.EMP,
    X005_paye_master.NUMB,
    CASE
        WHEN SUBSTR(NUMB,1,1)*2 > 0 THEN SUBSTR(SUBSTR(NUMB,1,1)*2,1,1)+SUBSTR(SUBSTR(NUMB,1,1)*2,2,1)
        ELSE SUBSTR(NUMB,1,1)*2
    END AS CD1,
    CASE
        WHEN SUBSTR(NUMB,3,1)*2 > 0 THEN SUBSTR(SUBSTR(NUMB,3,1)*2,1,1)+SUBSTR(SUBSTR(NUMB,3,1)*2,2,1)
        ELSE SUBSTR(NUMB,3,1)*2
    END AS CD3,
    CASE
        WHEN SUBSTR(NUMB,5,1)*2 > 0 THEN SUBSTR(SUBSTR(NUMB,5,1)*2,1,1)+SUBSTR(SUBSTR(NUMB,5,1)*2,2,1)
        ELSE SUBSTR(NUMB,5,1)*2
    END AS CD5,
    CASE
        WHEN SUBSTR(NUMB,7,1)*2 > 0 THEN SUBSTR(SUBSTR(NUMB,7,1)*2,1,1)+SUBSTR(SUBSTR(NUMB,7,1)*2,2,1)
        ELSE SUBSTR(NUMB,7,1)*2
    END AS CD7,
    CASE
        WHEN SUBSTR(NUMB,9,1)*2 > 0 THEN SUBSTR(SUBSTR(NUMB,9,1)*2,1,1)+SUBSTR(SUBSTR(NUMB,9,1)*2,2,1)
        ELSE SUBSTR(NUMB,9,1)*2
    END AS CD9,
    0 AS TOT1,
    SUBSTR(NUMB,2,1)+SUBSTR(NUMB,4,1)+SUBSTR(NUMB,6,1)+SUBSTR(NUMB,8,1) AS TOT2,
    '' AS CONT,
    '' AS VAL
From
    X005_paye_master
Where
    X005_paye_master.NUMB <> ''
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
print("Update calculation columns...")
so_curs.execute("UPDATE X005ba_paye_calc SET TOT1 = CD1+CD3+CD5+CD7+CD9+TOT2;")
so_conn.commit()
so_curs.execute("UPDATE X005ba_paye_calc " + """
                 SET CONT =
                 CASE
                     WHEN ABS(SUBSTR(TOT1,-1,1)) = 0 THEN 0
                     WHEN ABS(SUBSTR(TOT1,-1,1)) > 0 THEN 10 - SUBSTR(TOT1,-1,1)
                     ELSE 0
                 END;""")
so_conn.commit()
so_curs.execute("UPDATE X005ba_paye_calc " + """
                 SET VAL =
                 CASE
                     WHEN LENGTH(TRIM(NUMB)) <> 10 THEN 'F'
                     WHEN ABS(SUBSTR(NUMB,10)) = ABS(CONT) THEN 'T'
                     ELSE 'F'
                 END;""")
so_conn.commit()

# SELECT ALL EMPLOYEES WITH AN INVALID PAYE NUMBER
print("Select all employees with an invalid paye number...")
sr_file = "X005bb_paye_inva"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    X005ba_paye_calc.ORG,
    X005ba_paye_calc.LOC,
    X005ba_paye_calc.EMP,
    X005ba_paye_calc.NUMB
From
    X005ba_paye_calc
Where
    X005ba_paye_calc.VAL = 'F'
;"""
so_curs.execute("DROP TABLE IF EXISTS X005ba_numb_calc")
so_curs.execute("DROP TABLE IF EXISTS X005ba_tax_invalid")
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# COUNT THE NUMBER OF FINDINGS
i_find = funcsys.tablerowcount(so_curs,sr_file)
print("*** Found "+str(i_find)+" exceptions ***")
funcfile.writelog("%t FINDING: "+str(i_find)+" PAYE invalid finding(s)")

# GET PREVIOUS FINDINGS
# NOTE ADD CODE
sr_file = "X005bc_paye_getprev"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute("DROP TABLE IF EXISTS X005bc_paye_addprev")
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
        elif row[0] != "paye_invalid":
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
sr_file = "X005bd_paye_addprev"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0:
    print("Join previously reported to current findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
      X005bb_paye_inva.*,
      'paye_invalid' AS PROCESS,
      '%TODAY%' AS DATE_REPORTED,
      '%TODAYPLUS%' AS DATE_RETEST,
      X005bc_paye_getprev.PROCESS AS PREV_PROCESS,
      X005bc_paye_getprev.DATE_REPORTED AS PREV_DATE_REPORTED,
      X005bc_paye_getprev.DATE_RETEST AS PREV_DATE_RETEST,
      X005bc_paye_getprev.DATE_MAILED
    FROM
      X005bb_paye_inva
      LEFT JOIN X005bc_paye_getprev ON X005bc_paye_getprev.FIELD1 = X005bb_paye_inva.EMP AND
          X005bc_paye_getprev.DATE_RETEST >= Date('%TODAY%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%",funcdate.today())
    s_sql = s_sql.replace("%TODAYPLUS%",funcdate.today_plusdays(30))
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD LIST TO UPDATE FINDINGS
# NOTE ADD CODE
sr_file = "X005be_paye_newprev"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0:
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X005bd_paye_addprev.PROCESS,
      X005bd_paye_addprev.EMP AS FIELD1,
      '' AS FIELD2,
      '' AS FIELD3,
      '' AS FIELD4,
      '' AS FIELD5,
      X005bd_paye_addprev.DATE_REPORTED,
      X005bd_paye_addprev.DATE_RETEST,
      X005bd_paye_addprev.DATE_MAILED
    FROM
      X005bd_paye_addprev
    WHERE
      X005bd_paye_addprev.PREV_PROCESS IS NULL
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
sr_file = "X005bf_offi"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0:
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
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_PAYE_INVALID_OFFICER'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
sr_file = "X005bg_supe"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0:
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
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_PAYE_INVALID_SUPERVISOR'
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
funcfile.writelog("------------------------------------------")
funcfile.writelog("COMPLETED: C001_PEOPLE_TEST_MASTERFILE_DEV")
