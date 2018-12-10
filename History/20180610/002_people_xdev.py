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
import funcmail

# Script log file
funcfile.writelog()
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: 002_PEOPLE_LISTS")
funcfile.writelog("------------------------")
ilog_severity = 1

# Declare variables
so_path = "W:/" #Source database path
re_path = "R:/People/" #Results path
so_file = "People.sqlite" #Source database
s_sql = "" #SQL statements
l_export = True
l_mail = True

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("%t OPEN DATABASE: People.sqlite")

print("PEOPLE DEVELOPEMENT")
print("-------------------")

# ******************************************************************************

print("Build people birthday...")

s_sql = "CREATE TABLE X003_PEOPLE_BIRT AS " + """
SELECT
  X002_PEOPLE_CURR.EMPLOYEE_NUMBER,
  X002_PEOPLE_CURR.DATE_OF_BIRTH,
  X002_PEOPLE_CURR.NAME_LIST,
  X002_PEOPLE_CURR.KNOWN_NAME,
  X002_PEOPLE_CURR.POSITION_FULL,
  X002_PEOPLE_CURR.OE_CODE
FROM
  X002_PEOPLE_CURR
WHERE
  StrfTime('%m-%d', X002_PEOPLE_CURR.DATE_OF_BIRTH) >= StrfTime('%m-%d', 'now') AND
  StrfTime('%m-%d', X002_PEOPLE_CURR.DATE_OF_BIRTH) <= StrfTime('%m-%d', 'now','+7 day')
ORDER BY
  StrfTime('%m-%d', X002_PEOPLE_CURR.DATE_OF_BIRTH),
  X002_PEOPLE_CURR.POSITION_FULL
"""
so_curs.execute("DROP TABLE IF EXISTS X003_PEOPLE_BIRT")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: X003_PEOPLE_BIRT")

if l_export == True:
    
    # Data export
    sr_file = "X003_PEOPLE_BIRT"
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "People_003_birthday_"
    sx_filet = sx_file + funcdate.cur_month()

    print("Export people birthday..." + sx_path + sx_filet)

    # Read the header data
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

    # Write the data
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)

    funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

if l_mail == True:
    funcmail.Mail("hr_people_birthday")
    
# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("---------")
funcfile.writelog("COMPLETED")

