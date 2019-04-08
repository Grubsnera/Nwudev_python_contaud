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
#import csv
#import datetime
import sqlite3
import sys

# ADD OWN MODULE PATH
sys.path.append('S:/_my_modules')

# IMPORT OWN MODULES
#import funccsv
#import funcdate
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
so_path = "W:/People/" #Source database path
re_path = "R:/People/" # Results path
ed_path = "S:/_external_data/" #external data path
so_file = "People_test_conflict.sqlite" # Source database
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
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C002_PEOPLE_TEST_CONFLICT_DEV")
