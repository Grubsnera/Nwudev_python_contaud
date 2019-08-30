""" Script to prepare PEOPLE master file lists development area
Created on: 12 Jun 2019
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
funcfile.writelog("SCRIPT: C003_PEOPLE_LIST_MASTERFILE_DEV")
funcfile.writelog("---------------------------------------")

# DECLARE VARIABLES
ed_path = "S:/_external_data/"  # External data path
so_path = "W:/People/"  # Source database path
so_file = "People_list_masterfile.sqlite"  # Source database
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

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

""" ****************************************************************************
LIST AGE MASTER
*****************************************************************************"""
print("AGE MASTER")
funcfile.writelog("AGE MASTER")

# BUILD PEOPLE START AND END DATE MASTER TABLE
print("Build table for age analysis...")
sr_file: str = "X002_People_age_master"
s_sql = "CREATE TABLE " + sr_file + " As " + """
Select
    PEOP.EMPLOYEE_NUMBER AS EMP,
    PEOP.NAME_LIST AS NAME,
    PEOP.EMPLOYMENT_CATEGORY AS PERM_TEMP,
    PEOP.ACAD_SUPP,
    PEOP.SEX As GENDER,
    PEOP.RACE_DESC AS RACE,
    PEOP.PERSON_TYPE,
    PEOP.GRADE,
    Substr(PEOP.GRADE_CALC,1,3) As GRADE_CALC,
    CASE
        WHEN PEOP.NATIONALITY_NAME = 'SOUTH AFRICA' THEN PEOP.NATIONALITY_NAME
        ELSE 'FOREIGN'
    END AS NATIONALITY,
    PEOP.LOCATION_DESCRIPTION AS CAMPUS,
    CASE
        WHEN PEOP.FACULTY <> '' THEN PEOP.FACULTY
        ELSE 'SUPPORT'
    END AS FACULTY,
    PEOP.DIVISION,
    PEOP.POSITION_NAME,
    PEOP.JOB_NAME,
    PEOP.AGE,
    CASE
        When PEOP.AGE >= 10 And PEOP.AGE <= 20 Then '00-20'
        When PEOP.AGE >= 21 And PEOP.AGE <= 25 Then '21-25'
        When PEOP.AGE >= 26 And PEOP.AGE <= 30 Then '26-30'
        When PEOP.AGE >= 31 And PEOP.AGE <= 35 Then '31-35'
        When PEOP.AGE >= 36 And PEOP.AGE <= 40 Then '36-40'
        When PEOP.AGE >= 41 And PEOP.AGE <= 45 Then '41-45'
        When PEOP.AGE >= 46 And PEOP.AGE <= 50 Then '46-50'
        When PEOP.AGE >= 51 And PEOP.AGE <= 55 Then '51-55'
        When PEOP.AGE >= 56 And PEOP.AGE <= 60 Then '56-60'
        When PEOP.AGE >= 61 And PEOP.AGE <= 65 Then '61-65'
        When PEOP.AGE >= 66 And PEOP.AGE <= 70 Then '66-70'
        Else '71-99'
    END As AGE_GROUP,
    Cast(1 As Int) As COUNT
From
    PEOPLE.X002_PEOPLE_CURR PEOP
Where
    Substr(PEOP.PERSON_TYPE,1,6) <> 'AD HOC'
Order By
    PEOP.AGE Desc
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: X002_People_age_master")

""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE WORKING DATABASE
so_conn.close()

# CLOSE THE LOG
funcfile.writelog("------------------------------------------")
funcfile.writelog("COMPLETED: C003_PEOPLE_LIST_MASTERFILE_DEV")
