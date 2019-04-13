""" Script to test PEOPLE conflict of interest *********************************
Created on: 8 Apr 2019
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
#import datetime
import sqlite3
import sys

# ADD OWN MODULE PATH
sys.path.append('S:/_my_modules')

# IMPORT OWN MODULES
import funccsv
import funcdate
import funcfile
#import funcmail
#import funcmysql
#import funcpeople
#import funcstr
import funcsys

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
l_export = True
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
BUILD DASHBOARD TABLES
*****************************************************************************"""
print("BUILD DASHBOARD TABLES")
funcfile.writelog("BUILD DASHBOARD TABLES")

# BUILD CURRENT DECLARATION DASHBOARD PEOPLE
print("Build current declaration dashboard people data...")
sr_file = "X003aa_people_curr"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    PERSON.EMPLOYEE_NUMBER AS EMPLOYEE,
    PERSON.NAME_ADDR AS NAME,
    Case
        When PERSON.SEX = 'F' Then 'FEMALE'
        Else 'MALE'
    End As GENDER,
    Upper(PERSON.RACE_DESC) As RACE,
    Upper(PERSON.LANG_DESC) As LANGUAGE,
    Upper(PERSON.LOCATION_DESCRIPTION) As LOCATION,
    Upper(PERSON.ACAD_SUPP) As ACAD_SUPP,
    Upper(PERSON.FACULTY) As FACULTY,
    Upper(PERSON.DIVISION) As DIVISION,
    Case
        When PERSON.EMPLOYMENT_CATEGORY = 'T' Then 'TEMPORARY'
        When PERSON.EMPLOYMENT_CATEGORY = 'P' Then 'PERMANENT'
        Else 'OTHER'
    End As CATEGORY,
    Upper(PERSON.GRADE) As POS_GRADE,
    Upper(PERSON.JOB_NAME) As JOB_NAME,
    Upper(PERSON.PERSON_TYPE) As PERSON_TYPE,
    PERSON.AGE
From
    PEOPLE.X002_PEOPLE_CURR PERSON 
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# BUILD CURRENT DECLARATION DASHBOARD DECLARATION DATA
print("Build current declaration dashboard unique declarions...")
sr_file = "X003ab_declarations_curr"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    DECLARE.DECLARATION_ID,
    DECLARE.EMPLOYEE,
    DECLARE.DECLARATION_DATE,
    Case
        When UNDERSTAND_POLICY_FLAG = 'N' Then 'NO'
        When UNDERSTAND_POLICY_FLAG = 'Y' Then 'YES'
        Else 'NA'
    End As UNDERSTAND_POLICY,
    Case
        When INTEREST_TO_DECLARE_FLAG = 'N' Then 'NO'
        When INTEREST_TO_DECLARE_FLAG = 'Y' Then 'YES'
        Else 'NA'
    End As INTERESTS_TO_DECLARE,
    Case
        When FULL_DISCLOSURE_FLAG = 'N' Then 'NO'
        When FULL_DISCLOSURE_FLAG = 'Y' Then 'YES'
        Else 'NA'
    End As FULL_DISCLOSURE,
    Upper(STATUS) AS DECLARE_STATUS
From
    X001_declarations_curr DECLARE
Group By
    DECLARE.EMPLOYEE
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# BUILD CURRENT DECLARATION DASHBOARD DATA
print("Build current declaration dashboard data...")
sr_file = "X003_dashboard_curr"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    *,
    Case
        When DECLARE.DECLARE_STATUS IS NOT NULL Then DECLARE.DECLARE_STATUS
        Else 'NO DECLARATION'
    End As DECLARED
From
    X003aa_people_curr PEOPLE Left Join
    X003ab_declarations_curr DECLARE On DECLARE.EMPLOYEE = PEOPLE.EMPLOYEE
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# BUILD PREVIOUS DECLARATION DASHBOARD PEOPLE
print("Build previous declaration dashboard people data...")
sr_file = "X003aa_people_prev"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    PERSON.EMPLOYEE_NUMBER AS EMPLOYEE,
    PERSON.NAME_ADDR AS NAME,
    Case
        When PERSON.SEX = 'F' Then 'FEMALE'
        Else 'MALE'
    End As GENDER,
    Upper(PERSON.RACE_DESC) As RACE,
    Upper(PERSON.LANG_DESC) As LANGUAGE,
    Upper(PERSON.LOCATION_DESCRIPTION) As LOCATION,
    Upper(PERSON.ACAD_SUPP) As ACAD_SUPP,
    Upper(PERSON.FACULTY) As FACULTY,
    Upper(PERSON.DIVISION) As DIVISION,
    Case
        When PERSON.EMPLOYMENT_CATEGORY = 'T' Then 'TEMPORARY'
        When PERSON.EMPLOYMENT_CATEGORY = 'P' Then 'PERMANENT'
        Else 'OTHER'
    End As CATEGORY,
    Upper(PERSON.GRADE) As POS_GRADE,
    Upper(PERSON.JOB_NAME) As JOB_NAME,
    Upper(PERSON.PERSON_TYPE) As PERSON_TYPE,
    PERSON.AGE
From
    PEOPLE.X002_PEOPLE_PREV_YEAR PERSON
Where
    PERSON.EMP_ACTIVE = 'Y'
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# BUILD PREVIOUS DECLARATION DASHBOARD DECLARATION DATA
print("Build previous declaration dashboard unique declarions...")
sr_file = "X003ab_declarations_prev"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    DECLARE.DECLARATION_ID,
    DECLARE.EMPLOYEE,
    DECLARE.DECLARATION_DATE,
    Case
        When UNDERSTAND_POLICY_FLAG = 'N' Then 'NO'
        When UNDERSTAND_POLICY_FLAG = 'Y' Then 'YES'
        Else 'NA'
    End As UNDERSTAND_POLICY,
    Case
        When INTEREST_TO_DECLARE_FLAG = 'N' Then 'NO'
        When INTEREST_TO_DECLARE_FLAG = 'Y' Then 'YES'
        Else 'NA'
    End As INTERESTS_TO_DECLARE,
    Case
        When FULL_DISCLOSURE_FLAG = 'N' Then 'NO'
        When FULL_DISCLOSURE_FLAG = 'Y' Then 'YES'
        Else 'NA'
    End As FULL_DISCLOSURE,
    Upper(STATUS) AS DECLARE_STATUS
From
    X001_declarations_prev DECLARE
Group By
    DECLARE.EMPLOYEE
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# BUILD PREVIOUS DECLARATION DASHBOARD DATA
print("Build previous declaration dashboard data...")
sr_file = "X003_dashboard_prev"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    *,
    Case
        When DECLARE.DECLARE_STATUS IS NOT NULL Then DECLARE.DECLARE_STATUS
        Else 'NO DECLARATION'
    End As DECLARED
From
    X003aa_people_prev PEOPLE Left Join
    X003ab_declarations_prev DECLARE On DECLARE.EMPLOYEE = PEOPLE.EMPLOYEE
Where
    PEOPLE.EMPLOYEE IS NOT NULL
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# DELETE CALCULATION FILES
so_curs.execute("DROP TABLE IF EXISTS X003aa_people_curr")
so_curs.execute("DROP TABLE IF EXISTS X003ab_declarations_curr")
so_curs.execute("DROP TABLE IF EXISTS X003aa_people_prev")
so_curs.execute("DROP TABLE IF EXISTS X003ab_declarations_prev")

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
