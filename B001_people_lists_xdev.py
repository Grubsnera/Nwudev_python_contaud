""" Development are for PEOPLE LISTS
Created: 1 Mar 2019
Author: Albert Janse van Rensburg (NWU21162395)
"""

# IMPORT SYSTEM MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funcdate

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
End OF SCRIPT
*****************************************************************************"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# OPEN THE SCRIPT LOG FILE
print("-----------------")
print("B001_PEOPLE_LISTS")
print("-----------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B001_PEOPLE_LISTS_DEV")
funcfile.writelog("-----------------------------")
ilog_severity = 1

# DECLARE VARIABLES
so_path = "W:/People/"
so_file = "People.sqlite"
re_path = "R:/People/"
l_export: bool = False
l_mail: bool = False

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN DATABASE: PEOPLE.SQLITE")

# Import the X000_OWN_HR_LOOKUPS table
print("Import own lookups...")
ed_path = "S:/_external_data/"
tb_name = "X000_OWN_HR_LOOKUPS"
so_curs.execute("DROP TABLE IF EXISTS " + tb_name)
so_curs.execute("CREATE TABLE " + tb_name + "(LOOKUP TEXT,LOOKUP_CODE TEXT,LOOKUP_DESCRIPTION TEXT)")
s_cols = ""
co = open(ed_path + "001_own_hr_lookups.csv", newline=None)
co_reader = csv.reader(co)
for row in co_reader:
    if row[0] == "LOOKUP":
        continue
    else:
        s_cols = "INSERT INTO " + tb_name + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "')"
        so_curs.execute(s_cols)
so_conn.commit()
# Close the imported data file
co.close()
funcfile.writelog("%t IMPORT TABLE: " + tb_name)

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

""" ****************************************************************************
End OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("----------------------------")
funcfile.writelog("COMPLETED: B001_PEOPLE_LISTS")
