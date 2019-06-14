""" Script to prepare PEOPLE master file lists
Created on: 12 Jun 2019
Author: Albert Janse van Rensburg (NWU21162395)
"""

# IMPORT SYSTEM MODULES
import sqlite3

# OPEN OWN MODULES
from _my_modules import funcdate
from _my_modules import funcfile

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
LIST ENTRY EXIT DATES MASTER
END OF SCRIPT
*****************************************************************************"""


def people_list_masterfile():
    """
    Script to build PEOPLE master file lists
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # OPEN THE LOG
    print("---------------------------")
    print("C003_PEOPLE_LIST_MASTERFILE")
    print("---------------------------")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C003_PEOPLE_LIST_MASTERFILE")
    funcfile.writelog("-----------------------------------")

    # DECLARE VARIABLES
    so_path = "W:/People/"  # Source database path
    so_file = "People_list_masterfile.sqlite"  # Source database

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

    """ ****************************************************************************
    LIST ENTRY EXIT DATES MASTER
    *****************************************************************************"""
    print("ENTRY EXIT DATES MASTER")
    funcfile.writelog("ENTRY EXIT DATES MASTER")

    # BUILD PEOPLE START AND END DATE MASTER TABLE
    print("Build table for entry date analysis...")
    sr_file: str = "X001_People_start_end_master"
    s_sql = "CREATE TABLE " + sr_file + " As " + """
    Select
        PEOP.EMPLOYEE_NUMBER AS EMP,
        PEOP.NAME_LIST AS NAME,
        CASE
            WHEN PEOP.NATIONALITY_NAME = 'SOUTH AFRICA' THEN PEOP.NATIONALITY_NAME
            ELSE 'FOREIGN'
        END AS NATIONALITY,
        PEOP.SEX As GENDER,
        PEOP.RACE_DESC AS RACE,
        'START' As DATE_TYPE,
        PEOP.EMP_START As DATE,
        '' As LEAVING_REASON,
        '' As LEAVE_REASON_DESCRIP,
        PEOP.LOCATION_DESCRIPTION AS CAMPUS,
        PEOP.ACAD_SUPP,
        CASE
            WHEN PEOP.FACULTY <> '' THEN PEOP.FACULTY
            ELSE 'SUPPORT'
        END AS FACULTY,
        PEOP.EMPLOYMENT_CATEGORY AS PERM_TEMP,
        PEOP.DIVISION,
        PEOP.GRADE,
        Substr(PEOP.GRADE_CALC,1,3) As GRADE_CALC,
        PEOP.POSITION_NAME,
        PEOP.JOB_NAME,
        PEOP.PERSON_TYPE,
        Strftime('%m', PEOP.EMP_START) As MONTH,
        Cast(1 As Int) As COUNT
    From
        PEOPLE.X002_PEOPLE_CURR_YEAR PEOP
    Where
        Substr(PEOP.LEAVE_REASON_DESCRIP,1,6) <> 'AD HOC' And
        Substr(PEOP.PERSON_TYPE,1,6) <> 'AD HOC' And
        Strftime('%Y', PEOP.EMP_START) = '%CYEAR%'
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%CYEAR%", funcdate.cur_year())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: X001_People_start_end_master")

    # BUILD PEOPLE START AND END DATE MASTER TABLE
    print("Insert into table exit date analysis...")
    sr_file: str = "X001_People_start_end_master"
    s_sql = "INSERT INTO " + sr_file + " " + """
    Select
        PEOP.EMPLOYEE_NUMBER AS EMP,
        PEOP.NAME_LIST AS NAME,
        CASE
            WHEN PEOP.NATIONALITY_NAME = 'SOUTH AFRICA' THEN PEOP.NATIONALITY_NAME
            ELSE 'FOREIGN'
        END AS NATIONALITY,        
        PEOP.SEX AS GENDER,
        PEOP.RACE_DESC AS RACE,
        'END' As DATE_TYPE,
        PEOP.EMP_END As DATE,
        PEOP.LEAVING_REASON,
        PEOP.LEAVE_REASON_DESCRIP,
        PEOP.LOCATION_DESCRIPTION AS CAMPUS,
        PEOP.ACAD_SUPP,
        CASE
            WHEN PEOP.FACULTY <> '' THEN PEOP.FACULTY
            ELSE 'SUPPORT'
        END AS FACULTY,
        PEOP.EMPLOYMENT_CATEGORY AS PERM_TEMP,
        PEOP.DIVISION,
        PEOP.GRADE,
        Substr(PEOP.GRADE_CALC,1,3) As GRADE_CALC,
        PEOP.POSITION_NAME,
        PEOP.JOB_NAME,
        PEOP.PERSON_TYPE,
        Strftime('%m', PEOP.EMP_END) As MONTH,
        Cast(-1 As Int) As COUNT
    From
        PEOPLE.X002_PEOPLE_CURR_YEAR PEOP
    Where
        Substr(PEOP.LEAVE_REASON_DESCRIP,1,6) <> 'AD HOC' And
        Substr(PEOP.PERSON_TYPE,1,6) <> 'AD HOC' And
        Strftime('%Y', PEOP.EMP_END) = '%CYEAR%' And
        PEOP.EMP_END < Date('%TODAY%')
    """
    s_sql = s_sql.replace("%CYEAR%", funcdate.cur_year())
    s_sql = s_sql.replace("%TODAY%", funcdate.cur_monthend())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t INSERT TABLE: X001_People_start_end_master")

    """ ****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE THE WORKING DATABASE
    so_conn.close()

    # CLOSE THE LOG
    funcfile.writelog("--------------------------------------")
    funcfile.writelog("COMPLETED: C003_PEOPLE_LIST_MASTERFILE")

    return
