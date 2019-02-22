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
funcfile.writelog("SCRIPT: B003_VSS_LISTS_DEV")
funcfile.writelog("--------------------------")
print("--------------")
print("B003_VSS_LISTS")
print("--------------")
ilog_severity = 1

# Declare variables
so_path = "W:/Vss/" #Source database path
re_path = "R:/Vss/" #Results
so_file = "Vss.sqlite" #Source database
ed_path = "S:/_external_data/"
s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("%t OPEN DATABASE: VSS.SQLITE")

"""*****************************************************************************
BEGIN
*****************************************************************************"""



"""*****************************************************************************
BEGIN
*****************************************************************************"""

# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("-----------------------------")
funcfile.writelog("COMPLETED: B003_VSS_LISTS_DEV")
