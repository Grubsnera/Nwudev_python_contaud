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

# Import OWN LOOKUPS table *************************************************
print("Import own lookups...")
tb_name = "X000_OWN_LOOKUPS"
so_curs.execute("DROP TABLE IF EXISTS " + tb_name)
so_curs.execute("CREATE TABLE " + tb_name + "(LOOKUP TEXT,LOOKUP_CODE TEXT,LOOKUP_DESCRIPTION TEXT)")
s_cols = ""
co = open(ed_path + "001_own_vss_lookups.csv", "rU")
co_reader = csv.reader(co)
for row in co_reader:
    if row[0] == "LOOKUP":
        continue
    else:
        s_cols = "INSERT INTO " + tb_name + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "')"
        so_curs.execute(s_cols)
so_conn.commit()
# Close the impoted data file
co.close()
funcfile.writelog("%t IMPORT TABLE: " + tb_name)

"""*****************************************************************************
BEGIN
*****************************************************************************"""

"""*****************************************************************************
END
*****************************************************************************"""

# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("-----------------------------")
funcfile.writelog("COMPLETED: B003_VSS_LISTS_DEV")
