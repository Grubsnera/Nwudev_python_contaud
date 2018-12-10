""" Script to build standard PEOPLE lists
Created on: 12 Apr 2018
Author: Albert J v Rensburg (NWU21162395)
"""

# Import python modules
import datetime
import sqlite3
import csv
import sys

# Add own module path
sys.path.append('S:/_my_modules')

# Import own modules
import funcdate
import funccsv
import funcfile
import funcpeople
import funcmail

# Script log file
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B001_PEOPLE_LISTS")
funcfile.writelog("-------------------------")
print("-----------------")    
print("B001_PEOPLE_LISTS")
print("-----------------")
ilog_severity = 1

# Declare variables
so_path = "W:/" #Source database path
re_path = "R:/People/"
so_file = "People.sqlite" #Source database
s_sql = "" #SQL statements
l_export = True
l_mail = True

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("%t OPEN DATABASE: PEOPLE.SQLITE")

# People script development *****************************************

print("Build grades...")

s_sql = "CREATE TABLE X000_GRADES AS " + """
"""
so_curs.execute("DROP TABLE IF EXISTS X000_GRADES")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: X000_GRADES")

# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("----------------------------")
funcfile.writelog("COMPLETED: B001_PEOPLE_LISTS")
