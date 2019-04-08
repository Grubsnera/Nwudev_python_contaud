""" Script to test PEOPLE conflict of interest *********************************
Created on: 8 Apr 2019
Author: Albert J v Rensburg (NWU21162395)
*****************************************************************************"""

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
BUILD CONFLICT MASTER TABLES
BUILD ANNUAL TABLES
END OF SCRIPT
*****************************************************************************"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# IMPORT PYTHON MODULES
#import csv
#import datetime
import sqlite3
import sys

# ADD OWN MODULE PATH
sys.path.append('S:/_my_modules')

# IMPORT OWN MODULES
#import funccsv
import funcdate
import funcfile
#import funcmail
#import funcmysql
#import funcpeople
#import funcstr
#import funcsys

# OPEN THE SCRIPT LOG FILE
print("-----------------------------")    
print("C002_PEOPLE_TEST_CONFLICT_DEV")
print("-----------------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C002_PEOPLE_TEST_CONFLICT_DEV")
funcfile.writelog("-------------------------------------")
ilog_severity = 1

# DECLARE VARIABLES
so_path = "W:/People_conflict/" #Source database path
re_path = "R:/People/" # Results path
ed_path = "S:/_external_data/" #external data path
so_file = "People_conflict.sqlite" # Source database
s_sql = "" # SQL statements
l_export = False
l_mail = False
l_record = False

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN DATABASE: "+so_file)

# ATTACH DATA SOURCES
so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

""" ****************************************************************************
BUILD CONFLICT MASTER TABLES
*****************************************************************************"""
print("CONFLICT MASTER TABLES")
funcfile.writelog("CONFLICT MASTER TABLES")

# BUILD DECLARATIONS MASTER TABLE
print("Build declarations master table...")
sr_file = "X000_declarations_all"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    COI_DECLARATIONS.DECLARATION_ID,
    COI_DECLARATIONS.EMPLOYEE_NUMBER,
    COI_DECLARATIONS.DECLARATION_DATE,
    COI_DECLARATIONS.UNDERSTAND_POLICY_FLAG,
    COI_DECLARATIONS.INTEREST_TO_DECLARE_FLAG,
    COI_DECLARATIONS.FULL_DISCLOSURE_FLAG,
    PEOPLE.HR_LOOKUPS.MEANING AS STATUS,
    COI_DECLARATIONS.LINE_MANAGER,
    COI_DECLARATIONS.REJECTION_REASON,
    COI_DECLARATIONS.CREATION_DATE,
    COI_DECLARATIONS.AUDIT_USER,
    COI_DECLARATIONS.LAST_UPDATE_DATE,
    COI_DECLARATIONS.LAST_UPDATED_BY,
    COI_DECLARATIONS.EXTERNAL_REFERENCE
From
  COI_DECLARATIONS
  Left Join PEOPLE.HR_LOOKUPS ON PEOPLE.HR_LOOKUPS.LOOKUP_CODE = COI_DECLARATIONS.STATUS_ID And
      HR_LOOKUPS.LOOKUP_TYPE = "NWU_COI_STATUS" And
      PEOPLE.HR_LOOKUPS.START_DATE_ACTIVE <= COI_DECLARATIONS.DECLARATION_DATE And
      PEOPLE.HR_LOOKUPS.END_DATE_ACTIVE >= COI_DECLARATIONS.DECLARATION_DATE
Order By
  COI_DECLARATIONS.EMPLOYEE_NUMBER,
  COI_DECLARATIONS.DECLARATION_DATE
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# BUILD INTERESTS MASTER TABLE
print("Build interests master table...")
sr_file = "X000_interests_all"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    COI_INTERESTS.INTEREST_ID,
    COI_INTERESTS.DECLARATION_ID,
    COI_DECLARATIONS.EMPLOYEE_NUMBER,
    COI_DECLARATIONS.DECLARATION_DATE,
    COI_INTERESTS.CONFLICT_TYPE_ID,
    HR_LOOKUPS2.MEANING AS CONFLICT_TYPE,
    COI_INTERESTS.INTEREST_TYPE_ID,
    HR_LOOKUPS1.MEANING AS INTEREST_TYPE,
    COI_INTERESTS.STATUS_ID,
    HR_LOOKUPS3.MEANING AS INTEREST_STATUS,
    COI_INTERESTS.PERC_SHARE_INTEREST,
    COI_INTERESTS.ENTITY_NAME,
    COI_INTERESTS.ENTITY_REGISTRATION_NUMBER,
    COI_INTERESTS.OFFICE_ADDRESS,
    COI_INTERESTS.DESCRIPTION,
    COI_INTERESTS.DIR_APPOINTMENT_DATE,
    COI_INTERESTS.LINE_MANAGER,
    COI_INTERESTS.NEXT_LINE_MANAGER,
    COI_INTERESTS.INDUSTRY_CLASS_ID,
    PEOPLE.HR_LOOKUPS.MEANING AS INDUSTRY_TYPE,
    COI_INTERESTS.TASK_PERF_AGREEMENT,
    COI_INTERESTS.MITIGATION_AGREEMENT,
    COI_INTERESTS.REJECTION_REASON,
    COI_INTERESTS.CREATION_DATE,
    COI_INTERESTS.AUDIT_USER,
    COI_INTERESTS.LAST_UPDATE_DATE,
    COI_INTERESTS.LAST_UPDATED_BY,
    COI_INTERESTS.EXTERNAL_REFERENCE
From
  COI_INTERESTS
  Left Join COI_DECLARATIONS ON COI_DECLARATIONS.DECLARATION_ID = COI_INTERESTS.DECLARATION_ID
  Left Join PEOPLE.HR_LOOKUPS ON PEOPLE.HR_LOOKUPS.LOOKUP_CODE = COI_INTERESTS.INTEREST_TYPE_ID AND
      HR_LOOKUPS.LOOKUP_TYPE = "NWU_COI_INDUSTRY_CLASS"
  Left Join PEOPLE.HR_LOOKUPS HR_LOOKUPS1 ON HR_LOOKUPS1.LOOKUP_CODE = COI_INTERESTS.INTEREST_TYPE_ID AND
      HR_LOOKUPS1.LOOKUP_TYPE = "NWU_COI_INTEREST_TYPES"
  Left Join PEOPLE.HR_LOOKUPS HR_LOOKUPS2 ON HR_LOOKUPS2.LOOKUP_CODE = COI_INTERESTS.CONFLICT_TYPE_ID AND
      HR_LOOKUPS2.LOOKUP_TYPE = "NWU_COI_CONFLICT_TYPE"
  Left Join PEOPLE.HR_LOOKUPS HR_LOOKUPS3 ON HR_LOOKUPS3.LOOKUP_CODE = COI_INTERESTS.STATUS_ID AND
      HR_LOOKUPS3.LOOKUP_TYPE = "NWU_COI_STATUS"
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

""" ****************************************************************************
BUILD ANNUAL TABLES
*****************************************************************************"""
print("CURRENT YEAR DECLARATIONS")
funcfile.writelog("CURRENT YEAR DECLARATIONS")

# BUILD CURRENT YEAR DECLARATIONS
print("Build current declarations...")
sr_file = "X001_declarations_curr"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    X000_declarations_all.DECLARATION_ID,
    X000_declarations_all.EMPLOYEE_NUMBER AS EMPLOYEE,
    EMPLOYEE.LAST_NAME AS EMP_SURNAME,
    EMPLOYEE.NAME_ADDR AS EMP_NAME,
    EMPLOYEE.ADDRESS_SARS AS EMP_ADD_SARS,
    EMPLOYEE.ADDRESS_POST AS EMP_ADD_POST,
    EMPLOYEE.EMP_ACTIVE,    
    EMPLOYEE.EMAIL_ADDRESS AS EMP_MAIL,
    EMPLOYEE.POSITION_FULL AS EMP_POSITION,    
    X000_declarations_all.DECLARATION_DATE,
    X000_declarations_all.UNDERSTAND_POLICY_FLAG,
    X000_declarations_all.INTEREST_TO_DECLARE_FLAG,
    X000_declarations_all.FULL_DISCLOSURE_FLAG,
    X000_declarations_all.STATUS,
    X000_declarations_all.LINE_MANAGER,
    MANAGER.LAST_NAME AS MAN_SURNAME,
    MANAGER.NAME_ADDR AS MAN_NAME,
    MANAGER.ADDRESS_SARS AS MAN_ADD_SARS,
    MANAGER.ADDRESS_POST AS MAN_ADD_POST,
    MANAGER.EMP_ACTIVE AS MAN_ACTIVE,    
    MANAGER.EMAIL_ADDRESS AS MAN_MAIL,
    MANAGER.POSITION_FULL AS MAN_POSITION,    
    X000_declarations_all.REJECTION_REASON,
    X000_declarations_all.CREATION_DATE,
    X000_declarations_all.AUDIT_USER,
    X000_declarations_all.LAST_UPDATE_DATE,
    X000_declarations_all.LAST_UPDATED_BY,
    X000_declarations_all.EXTERNAL_REFERENCE,
    EMPLOYEE.SUPERVISOR,
    SUPERVISOR.LAST_NAME AS SUP_SURNAME,
    SUPERVISOR.NAME_ADDR AS SUP_NAME,
    SUPERVISOR.EMP_ACTIVE AS SUP_ACTIVE,    
    SUPERVISOR.EMAIL_ADDRESS AS SUP_MAIL,
    SUPERVISOR.POSITION_FULL AS SUP_POSITION
From
    X000_declarations_all
    Left Join PEOPLE.X002_PEOPLE_CURR_YEAR EMPLOYEE ON EMPLOYEE.EMPLOYEE_NUMBER = X000_declarations_all.EMPLOYEE_NUMBER
    Left Join PEOPLE.X002_PEOPLE_CURR_YEAR MANAGER ON MANAGER.EMPLOYEE_NUMBER = X000_declarations_all.LINE_MANAGER
    Left Join PEOPLE.X002_PEOPLE_CURR_YEAR SUPERVISOR ON SUPERVISOR.EMPLOYEE_NUMBER = EMPLOYEE.SUPERVISOR
Where
  X000_declarations_all.DECLARATION_DATE >= Date("%CYEARB%") AND
  X000_declarations_all.DECLARATION_DATE <= Date("%CYEARE%")
Order By
    EMPLOYEE.LAST_NAME,
    EMPLOYEE.NAME_ADDR,
    X000_declarations_all.LAST_UPDATE_DATE
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
s_sqlp = s_sql
s_sql = s_sql.replace("%CYEARB%",funcdate.cur_yearbegin())
s_sql = s_sql.replace("%CYEARE%",funcdate.cur_yearend())
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# BUILD PREVIOUS YEAR DECLARATIONS
print("Build previous declarations...")
sr_file = "X001_declarations_prev"
s_sql = s_sqlp
s_sql = s_sql.replace("X001_declarations_curr",sr_file)
s_sql = s_sql.replace("X002_PEOPLE_CURR_YEAR","X002_PEOPLE_PREV_YEAR")
s_sql = s_sql.replace("%CYEARB%",funcdate.prev_yearbegin())
s_sql = s_sql.replace("%CYEARE%",funcdate.prev_yearend())
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# BUILD THE CURRENT YEAR INTERESTS
print("Build current interests...")
sr_file = "X002_interests_curr"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    X000_interests_all.DECLARATION_ID,
    X000_interests_all.INTEREST_ID,
    X000_interests_all.EMPLOYEE_NUMBER,
    EMPLOYEE.LAST_NAME AS EMP_SURNAME,
    EMPLOYEE.NAME_ADDR AS EMP_NAME,
    EMPLOYEE.ADDRESS_SARS AS EMP_ADD_SARS,
    EMPLOYEE.ADDRESS_POST AS EMP_ADD_POST,
    EMPLOYEE.EMP_ACTIVE,    
    EMPLOYEE.EMAIL_ADDRESS AS EMP_MAIL,
    EMPLOYEE.POSITION_FULL AS EMP_POSITION,    
    X000_interests_all.DECLARATION_DATE,
    X000_interests_all.CONFLICT_TYPE,
    X000_interests_all.INTEREST_TYPE,
    X000_interests_all.PERC_SHARE_INTEREST,
    X000_interests_all.INDUSTRY_TYPE,
    X000_interests_all.ENTITY_NAME,
    X000_interests_all.ENTITY_REGISTRATION_NUMBER,
    X000_interests_all.OFFICE_ADDRESS,
    X000_interests_all.DESCRIPTION,
    X000_interests_all.INTEREST_STATUS,
    X000_interests_all.LINE_MANAGER,
    MANAGER.LAST_NAME AS MAN_SURNAME,
    MANAGER.NAME_ADDR AS MAN_NAME,
    MANAGER.ADDRESS_SARS AS MAN_ADD_SARS,
    MANAGER.ADDRESS_POST AS MAN_ADD_POST,
    MANAGER.EMP_ACTIVE AS MAN_ACTIVE,    
    MANAGER.EMAIL_ADDRESS AS MAN_MAIL,
    MANAGER.POSITION_FULL AS MAN_POSITION,    
    X000_interests_all.NEXT_LINE_MANAGER,
    NEXTMANAGER.LAST_NAME AS NMAN_SURNAME,
    NEXTMANAGER.NAME_ADDR AS NMAN_NAME,
    NEXTMANAGER.ADDRESS_SARS AS NMAN_ADD_SARS,
    NEXTMANAGER.ADDRESS_POST AS NMAN_ADD_POST,
    NEXTMANAGER.EMP_ACTIVE AS NMAN_ACTIVE,    
    NEXTMANAGER.EMAIL_ADDRESS AS NMAN_MAIL,
    NEXTMANAGER.POSITION_FULL AS NMAN_POSITION,    
    X000_interests_all.DIR_APPOINTMENT_DATE,
    X000_interests_all.TASK_PERF_AGREEMENT,
    X000_interests_all.MITIGATION_AGREEMENT,
    X000_interests_all.REJECTION_REASON,
    X000_interests_all.CREATION_DATE,
    X000_interests_all.AUDIT_USER,
    X000_interests_all.LAST_UPDATE_DATE,
    X000_interests_all.LAST_UPDATED_BY,
    X000_interests_all.EXTERNAL_REFERENCE,
    EMPLOYEE.SUPERVISOR,
    SUPERVISOR.LAST_NAME AS SUP_SURNAME,
    SUPERVISOR.NAME_ADDR AS SUP_NAME,
    SUPERVISOR.EMP_ACTIVE AS SUP_ACTIVE,    
    SUPERVISOR.EMAIL_ADDRESS AS SUP_MAIL,
    SUPERVISOR.POSITION_FULL AS SUP_POSITION
From
    X001_declarations_curr
    Inner Join X000_interests_all ON X000_interests_all.DECLARATION_ID = X001_declarations_curr.DECLARATION_ID
    Left Join PEOPLE.X002_PEOPLE_CURR_YEAR EMPLOYEE ON EMPLOYEE.EMPLOYEE_NUMBER = X000_interests_all.EMPLOYEE_NUMBER
    Left Join PEOPLE.X002_PEOPLE_CURR_YEAR MANAGER ON MANAGER.EMPLOYEE_NUMBER = X000_interests_all.LINE_MANAGER
    Left Join PEOPLE.X002_PEOPLE_CURR_YEAR NEXTMANAGER ON NEXTMANAGER.EMPLOYEE_NUMBER = X000_interests_all.NEXT_LINE_MANAGER
    Left Join PEOPLE.X002_PEOPLE_CURR_YEAR SUPERVISOR ON SUPERVISOR.EMPLOYEE_NUMBER = EMPLOYEE.SUPERVISOR
Where
    X001_declarations_curr.INTEREST_TO_DECLARE_FLAG == "Y"
Order By
    EMPLOYEE.LAST_NAME,
    EMPLOYEE.NAME_ADDR,
    X000_interests_all.LAST_UPDATE_DATE
"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
s_sqlp = s_sql
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# BUILD PREVIOUS YEAR INTERESTS
print("Build previous interests...")
sr_file = "X002_interests_prev"
s_sql = s_sqlp
s_sql = s_sql.replace("X002_interests_curr",sr_file)
s_sql = s_sql.replace("X001_declarations_curr","X001_declarations_prev")
s_sql = s_sql.replace("X002_PEOPLE_CURR_YEAR","X002_PEOPLE_PREV_YEAR")
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C002_PEOPLE_TEST_CONFLICT_DEV")
