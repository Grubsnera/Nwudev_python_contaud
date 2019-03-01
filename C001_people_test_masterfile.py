""" Script to test PEOPLE master file data *************************************
Created on: 1 Mar 2019
Author: Albert J v Rensburg (NWU21162395)
*****************************************************************************"""

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
TEST ZA ID EXIST
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
print("-----------------")    
print("B001_PEOPLE_LISTS")
print("-----------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B001_PEOPLE_LISTS_DEV")
funcfile.writelog("-----------------------------")
ilog_severity = 1

# DECLARE VARIABLES
so_path = "W:/People/" #Source database path
re_path = "R:/People/" # Results path
ed_path = "S:/_external_data/" #external data path
so_file = "People_test_masterfile.sqlite" # Source database
s_sql = "" # SQL statements
l_export = False
l_mail = True

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
TEST ZA ID EXIST
*****************************************************************************"""
print("TEST ZA ID EXIST")
funcfile.writelog("TEST ZA ID EXIST")

# IMPORT BASIC ID NUMBER DATA FROM MASTER FILE
print("Import people id number master file data...")
sr_file = "X001_people_id_master"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    'NWU' AS ORG,
    CASE LOCATION_DESCRIPTION
        WHEN 'Mafikeng Campus' THEN 'MAF'
        WHEN 'Potchefstroom Campus' THEN 'POT'
        WHEN 'Vaal Triangle Campus' THEN 'VAA'
        ELSE 'NWU'
    END AS LOC,
    PEOPLE.X002_PEOPLE_CURR.OE_CODE,
    PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER,
    PEOPLE.X002_PEOPLE_CURR.FULL_NAME,
    PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
    PEOPLE.X002_PEOPLE_CURR.DATE_OF_BIRTH,
    PEOPLE.X002_PEOPLE_CURR.NATIONALITY,
    PEOPLE.X002_PEOPLE_CURR.IDNO,
    PEOPLE.X002_PEOPLE_CURR.SEX,
    PEOPLE.X002_PEOPLE_CURR.MAILTO
From
    PEOPLE.X002_PEOPLE_CURR
Where
    PEOPLE.X002_PEOPLE_CURR.NATIONALITY = 'SAF'
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# IDENTIFY BLANK ID NUMBERS
print("Identify blank ID numbers...")
print("Identifier: id_blank")
sr_file = "X002aa_id_blank"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    X001_people_id_master.*
From
    X001_people_id_master
Where
    X001_people_id_master.IDNO = ''
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# IMPORT PREVIOUS REPORTED FINDINGS
print("Import previously reported findings...")
tb_name = "X002ab_impo_reported"
so_curs.execute("DROP TABLE IF EXISTS " + tb_name)
so_curs.execute("CREATE TABLE " + tb_name + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT)")
s_cols = ""
co = open(ed_path + "001_reported.txt", "r")
co_reader = csv.reader(co)
# Read the COLUMN database data
for row in co_reader:
    # Populate the column variables
    if row[0] == "PROCESS":
        continue
    elif row[0] != "id_blank":
        continue
    else:
        s_cols = "INSERT INTO " + tb_name + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "')"
        so_curs.execute(s_cols)
so_conn.commit()
# Close the impoted data file
co.close()
funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + tb_name + ")")

# JOIN THE NEW FINDING WITH OLD FINDINGS
print("Join previously reported to current findings...")
sr_file = "X002ac_join_prev_reported"
s_sql = "CREATE TABLE " + sr_file + " AS" + """
SELECT
  X002aa_id_blank.*,
  X002ab_impo_reported.PROCESS AS PREV_PROCESS,
  X002ab_impo_reported.DATE_REPORTED AS PREV_DATE_REPORTED,
  X002ab_impo_reported.DATE_RETEST AS PREV_DATE_RETEST,
  'id_blank' AS PROCESS,
  '%TODAY%' AS DATE_REPORTED,
  '%TODAY+14%' AS DATE_RETEST
FROM
  X002aa_id_blank
  LEFT JOIN X002ab_impo_reported ON X002ab_impo_reported.FIELD1 = X002aa_id_blank.EMPLOYEE_NUMBER AND
    X002ab_impo_reported.DATE_RETEST >= Date('%TODAY%')
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = s_sql.replace("%TODAY%",funcdate.today())
s_sql = s_sql.replace("%TODAY+14%",funcdate.today_plusdays(30))
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IMPORT OFFICERS
print("Import reporting officers for mail purposes...")
sr_file = "X002ae_impo_report_officer"
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
  PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_ID_BLANK_OFFICER'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Import the reporting supervisors *********************************************
print("Import reporting supervisors from VSS.SQLITE...")
sr_file = "X002ec_impo_report_supervisor"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  VSS.X000_OWN_LOOKUPS.LOOKUP,
  VSS.X000_OWN_LOOKUPS.LOOKUP_CODE AS CAMPUS,
  VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
  PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
  PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
FROM
  VSS.X000_OWN_LOOKUPS
  LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION
WHERE
  VSS.X000_OWN_LOOKUPS.LOOKUP = 'stud_debt_recon_balance_month_supervisor'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Build list to update previous findings file
print("Add new findings to previous reported...")
sr_file = "X002ad_add_prev_reported"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X002ac_join_prev_reported.PROCESS,
  X002ac_join_prev_reported.EMPLOYEE_NUMBER AS FIELD1,
  '' AS FIELD2,
  '' AS FIELD3,
  '' AS FIELD4,
  '' AS FIELD5,
  X002ac_join_prev_reported.DATE_REPORTED,
  X002ac_join_prev_reported.DATE_RETEST
FROM
  X002ac_join_prev_reported
WHERE
  X002ac_join_prev_reported.PREV_PROCESS IS NULL
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Export findings to previous reported file
if funcsys.tablerowcount(so_curs,sr_file) > 0:
    print("Export findings to previously reported file...")
    sr_filet = sr_file
    sx_path = ed_path
    sx_file = "001_reported"
    # Read the header data
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    # Write the data
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
    funcfile.writelog("%t EXPORT DATA: "+sr_file)
else:
    print("No new findings to report...")
    funcfile.writelog("%t EXPORT DATA: No new findings to export")














    

"""
# Build final list of findings
print("Build final list of findings...")
sr_file = "X002ax_id_blank"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
"""
Select
    X002ac_join_prev_reported.EMPLOYEE_NUMBER,
    X002ac_join_prev_reported.FULL_NAME,
    X002ac_join_prev_reported.DATE_OF_BIRTH,
    X002ac_join_prev_reported.NATIONALITY,
    X002ac_join_prev_reported.DATE_REPORTED,
    X002ac_join_prev_reported.DATE_RETEST
From
    X002ac_join_prev_reported
Where
    X002ac_join_prev_reported.PREV_PROCESS Is Null
;"""
"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Export findings to previous reported file
if funcsys.tablerowcount(so_curs,sr_file) > 0:
    print("Export final list of findings...")
    sr_filet = sr_file
    sx_path = ed_path
    sx_file = "001_reported"
    # Read the header data
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    # Write the data
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
    funcfile.writelog("%t EXPORT DATA: "+sr_file)
else:
    print("No new findings to report...")
    funcfile.writelog("%t EXPORT DATA: No new findings to export")

"""    

""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("----------------------------")
funcfile.writelog("COMPLETED: B001_PEOPLE_LISTS")
