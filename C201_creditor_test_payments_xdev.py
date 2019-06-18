""" Script to build kfs creditor payment tests *********************************
Created on: 16 Apr 2019
Author: Albert J v Rensburg (NWU21162395)
*****************************************************************************"""

# IMPORT PYTHON MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcsys

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# OPEN THE SCRIPT LOG FILE
print("----------------------")
print("C201_CREDITOR_TEST_DEV")
print("----------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C201_CREDITOR_TEST_DEV")
funcfile.writelog("------------------------------")
ilog_severity = 1

# DECLARE VARIABLES
so_path = "W:/Kfs/"  # Source database path
so_file = "Kfs_test_creditor.sqlite"  # Source database
re_path = "R:/Kfs/"  # Results path
ed_path = "S:/_external_data/"  # External data path
l_export = False
l_mail = False
l_record = False

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

""" ****************************************************************************
TEST CREDITOR BANK VERIFICATION
*****************************************************************************"""
print("TEST CREDITOR BANK VERIFICATION")
funcfile.writelog("TEST CREDITOR BANK VERIFICATION")

# DECLARE TEST VARIABLES
i_find: int = 0  # Number of findings before previous reported findings
i_coun: int = 0  # Number of new findings to report

""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("---------------------------------")
funcfile.writelog("COMPLETED: C201_CREDITOR_TEST_DEV")
