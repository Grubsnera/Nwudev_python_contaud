"""
Test history functions
Created on: 26 Jan 2020
Author: Albert J van Rensburg (NWU:21162395)
"""

# IMPORT SYSTEM FUNCTIONS
import csv

# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funcsys


def get_previous_finding(o_cursor, s_path, s_source, s_key, s_format="ITTTT"):
    """
    Function to fetch previous findings
    :param o_cursor: Database cursor
    :param s_path: Source file path
    :param s_source: Source file name
    :param s_key: Source file key
    :param s_format: Source file format
    :return: Number of records (findings)
    """

    print("Import previously reported findings...")

    # DECLARE / BUILD VARIABLES
    s_formatt: str = "(PROCESS TEXT, "
    for i in range(5):
        # print(s_format[i:i + 1])
        if s_format[i:i + 1] == "I":
            s_formatt += "FIELD" + str(i + 1) + " INT, "
        elif s_format[i:i + 1] == "R":
            s_formatt += "FIELD" + str(i + 1) + " REAL, "
        else:
            s_formatt += "FIELD" + str(i + 1) + " TEXT, "
    s_formatt += "DATE_REPORTED TEXT, DATE_RETEST TEXT, REMARK TEXT)"
    # print(s_formatt)

    sr_file = "Z001aa_getprev"
    o_cursor.execute("DROP TABLE IF EXISTS  " + sr_file)
    o_cursor.execute("CREATE TABLE " + sr_file + s_formatt)
    co = open(s_path + s_source, "r")
    co_reader = csv.reader(co)
    # READ THE TEXT DATA INTO THE TABLE
    for row in co_reader:
        # Populate the column variables
        if row[0] == "PROCESS":
            continue
        elif row[0] != s_key:
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" \
                     + row[0] + "','" \
                     + row[1] + "','" \
                     + row[2] + "','" \
                     + row[3] + "','" \
                     + row[4] + "','" \
                     + row[5] + "','" \
                     + row[6] + "','" \
                     + row[7] + "','" \
                     + row[8] + "')"
        # print(s_cols)
        o_cursor.execute(s_cols)
    # ClOSE THE SOURCE FILE
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + s_path + s_source + "(" + sr_file + ")")

    return funcsys.tablerowcount(o_cursor, sr_file)


def set_previous_finding(o_cursor):
    """
    Function order and set last finding
    :param o_cursor: Database cursor
    :return: Number of records
    """

    # SET PREVIOUS FINDINGS
    sr_file = "Z001ab_setprev"
    o_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Obtain the latest previous finding...")
    s_sql = "Create Table " + sr_file + " As" + """
    Select
        GET.PROCESS,
        GET.FIELD1,
        GET.FIELD2,
        GET.FIELD3,
        GET.FIELD4,
        GET.FIELD5,
        Max(GET.DATE_REPORTED) As DATE_REPORTED,
        GET.DATE_RETEST,
        GET.REMARK
    From
        Z001aa_getprev GET
    Group By
        GET.FIELD1        
    ;"""
    o_cursor.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    return funcsys.tablerowcount(o_cursor, sr_file)


def get_officer(o_cursor, s_key):
    """
    Function order and set last finding
    :param o_cursor: Database cursor
    :param s_key: Officer search key
    :return: Number of records
    """

    # SET PREVIOUS FINDINGS
    sr_file = "Z001af_officer"
    o_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Import reporting officers for mail purposes...")
    s_sql = "Create Table " + sr_file + " As" + """
        Select
            LOOKUP.LOOKUP,
            LOOKUP.LOOKUP_CODE AS CAMPUS,
            LOOKUP.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOPLE.NAME_ADDR,
            PEOPLE.EMAIL_ADDRESS
        FROM
            PEOPLE.X000_OWN_HR_LOOKUPS LOOKUP
            LEFT JOIN PEOPLE.X002_PEOPLE_CURR PEOPLE ON PEOPLE.EMPLOYEE_NUMBER = LOOKUP.LOOKUP_DESCRIPTION
        WHERE
            LOOKUP.LOOKUP = '%KEY%'
        ;"""
    s_sql = s_sql.replace("%KEY%", s_key)
    o_cursor.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    return funcsys.tablerowcount(o_cursor, sr_file)


def get_supervisor(o_cursor, s_key):
    """
    Function order and set last finding
    :param o_cursor: Database cursor
    :param s_key: Officer search key
    :return: Number of records
    """

    # SET PREVIOUS FINDINGS
    sr_file = "Z001ag_supervisor"
    o_cursor.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Import reporting supervisors for mail purposes...")
    s_sql = "Create Table " + sr_file + " As" + """
        Select
            LOOKUP.LOOKUP,
            LOOKUP.LOOKUP_CODE AS CAMPUS,
            LOOKUP.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOPLE.NAME_ADDR,
            PEOPLE.EMAIL_ADDRESS
        FROM
            PEOPLE.X000_OWN_HR_LOOKUPS LOOKUP
            LEFT JOIN PEOPLE.X002_PEOPLE_CURR PEOPLE ON PEOPLE.EMPLOYEE_NUMBER = LOOKUP.LOOKUP_DESCRIPTION
        WHERE
            LOOKUP.LOOKUP = '%KEY%'
        ;"""
    s_sql = s_sql.replace("%KEY%", s_key)
    o_cursor.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    return funcsys.tablerowcount(o_cursor, sr_file)
