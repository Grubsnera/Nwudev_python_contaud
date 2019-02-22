"""
Script to build standard VSS lists
Created on: 01 Mar 2018
Copyright: Albert J v Rensburg
"""

# Import python modules
import csv
import datetime
import sqlite3
import sys

# Add own module path
sys.path.append('S:/_my_modules')
#print(sys.path)

# Import own modules
import funcdate
import funccsv
import funcfile

# Open the script log file ******************************************************

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B004_PAYROLL_LISTS")
funcfile.writelog("--------------------------")
print("------------------")
print("B004_PAYROLL_LISTS")
print("------------------")
ilog_severity = 1

# Declare variables
so_path = "W:/People_payroll/" #Source database path
so_file = "People_payroll.sqlite" #Source database
re_path = "R:/People/" #Results
ed_path = "S:/_external_data/"
s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: PEOPLE_PAYROLL.SQLITE")

# Attach data sources
so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

"""*****************************************************************************
BEGIN
*****************************************************************************"""



"""*************************************************************************
END
*************************************************************************"""

# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("-------------------------")
funcfile.writelog("COMPLETED: B003_VSS_LISTS")


