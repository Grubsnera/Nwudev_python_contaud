"""
Script to build standard KFS lists
Created on: 11 Mar 2018
Copyright: Albert J v Rensburg
"""

# Import python modules
import datetime
import sqlite3
import sys

# Add own module path
sys.path.append('S:\\_my_modules')

# Import own modules
import funcdate
import funccsv
import funcfile

# Open the script log file ******************************************************

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B002_KFS_LISTS")
funcfile.writelog("----------------------")
print("--------------")
print("B002_KFS_LISTS")
print("--------------")
ilog_severity = 1

# Declare variables
so_path = "W:/Kfs/" #Source database path
so_file = "Kfs.sqlite" #Source database
s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("%t OPEN DATABASE: KFS")

"""*****************************************************************************
BEGIN
*****************************************************************************"""

  

"""*****************************************************************************
END
*****************************************************************************"""

# Close the table connection ***************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("-------------------------")
funcfile.writelog("COMPLETED: B002_KFS_LISTS")
