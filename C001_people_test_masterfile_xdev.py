""" Script to test PEOPLE master file data development area
Created on: 1 Mar 2019
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
funcfile.writelog("SCRIPT: C001_PEOPLE_TEST_MASTERFILE_DEV")
funcfile.writelog("---------------------------------------")

# DECLARE VARIABLES
ed_path = "S:/_external_data/"  # External data path
so_path = "W:/People/"  # Source database path
so_file = "People_test_masterfile.sqlite"  # Source database
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
funcfile.writelog("%t OPEN DATABASE: PEOPLE_TEST_MASTERFILE.SQLITE")

# ATTACH DATA SOURCES
so_curs.execute("ATTACH DATABASE '" + so_path + "People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE WORKING DATABASE
so_conn.close()

# CLOSE THE LOG
funcfile.writelog("------------------------------------------")
funcfile.writelog("COMPLETED: C001_PEOPLE_TEST_MASTERFILE_DEV")
