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
funcfile.writelog("%t OPEN DATABASE: PEOPLE_LIST_MASTERFILE.SQLITE")

# ATTACH DATA SOURCES
so_curs.execute("ATTACH DATABASE '" + so_path + "People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

# BUILD PEOPLE START AND END DATE MASTER TABLE
print("Build table for start date analysis...")
sr_file: str = "X001_People_start_end_master"
s_sql = "CREATE TABLE " + sr_file + " As " + """
Select
    PEOP.EMPLOYEE_NUMBER,
    PEOP.NAME_LIST,
    PEOP.NATIONALITY_NAME,
    PEOP.SEX,
    PEOP.RACE_DESC,
    'START' As DATE_TYPE,
    PEOP.EMP_START As DATE,
    '' As LEAVING_REASON,
    '' As LEAVE_REASON_DESCRIP,
    PEOP.LOCATION_DESCRIPTION,
    PEOP.ACAD_SUPP,
    PEOP.FACULTY,
    PEOP.EMPLOYMENT_CATEGORY,
    PEOP.DIVISION,
    PEOP.GRADE,
    PEOP.GRADE_CALC,
    PEOP.POSITION_NAME,
    PEOP.JOB_NAME,
    PEOP.PERSON_TYPE,
    Strftime('%m', PEOP.EMP_START) As MONTH
From
    PEOPLE.X002_PEOPLE_CURR_YEAR PEOP
Where
    Substr(PEOP.PERSON_TYPE,1,6) <> 'AD HOC' And
    Strftime('%Y', PEOP.EMP_START) = '%CYEAR%'
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = s_sql.replace("%CYEAR%", funcdate.cur_year())
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: X003_PEOPLE_ORGA_REF")

# BUILD PEOPLE START AND END DATE MASTER TABLE
print("Build table for start date analysis...")
sr_file: str = "X001_People_start_end_master"
s_sql = "INSERT INTO " + sr_file + " " + """
Select
    PEOP.EMPLOYEE_NUMBER,
    PEOP.NAME_LIST,
    PEOP.NATIONALITY_NAME,
    PEOP.SEX,
    PEOP.RACE_DESC,
    'END' As DATE_TYPE,
    PEOP.EMP_END As DATE,
    PEOP.LEAVING_REASON,
    PEOP.LEAVE_REASON_DESCRIP,
    PEOP.LOCATION_DESCRIPTION,
    PEOP.ACAD_SUPP,
    PEOP.FACULTY,
    PEOP.EMPLOYMENT_CATEGORY,
    PEOP.DIVISION,
    PEOP.GRADE,
    PEOP.GRADE_CALC,
    PEOP.POSITION_NAME,
    PEOP.JOB_NAME,
    PEOP.PERSON_TYPE,
    Strftime('%m', PEOP.EMP_END) As MONTH
From
    PEOPLE.X002_PEOPLE_CURR_YEAR PEOP
Where
    Substr(PEOP.PERSON_TYPE,1,6) <> 'AD HOC' And
    Strftime('%Y', PEOP.EMP_END) = '%CYEAR%'
"""
s_sql = s_sql.replace("%CYEAR%", funcdate.cur_year())
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: X003_PEOPLE_ORGA_REF")

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
