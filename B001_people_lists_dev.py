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
so_path = "W:/People/" #Source database path
re_path = "R:/People/"
so_file = "People.sqlite" #Source database
s_sql = "" #SQL statements
l_export = False
l_mail = True

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("%t OPEN DATABASE: PEOPLE.SQLITE")

""" ****************************************************************************
BEGIN
*****************************************************************************"""

# BUILD PERSON TYPES ***********************************************************

print("Build person types...")
sr_file = "X000_PER_PEOPLE_TYPES"
s_sql = "CREATE VIEW "+sr_file+" AS " + """
SELECT
  PER_PERSON_TYPE_USAGES_F.PERSON_TYPE_USAGE_ID,
  PER_PERSON_TYPE_USAGES_F.PERSON_ID,
  PER_PERSON_TYPE_USAGES_F.PERSON_TYPE_ID,
  PER_PERSON_TYPE_USAGES_F.EFFECTIVE_START_DATE,
  PER_PERSON_TYPE_USAGES_F.EFFECTIVE_END_DATE,
  PER_PERSON_TYPES.ACTIVE_FLAG,
  PER_PERSON_TYPES.DEFAULT_FLAG,
  PER_PERSON_TYPES.SYSTEM_PERSON_TYPE,
  PER_PERSON_TYPES.USER_PERSON_TYPE
FROM
  PER_PERSON_TYPE_USAGES_F
  LEFT JOIN PER_PERSON_TYPES ON PER_PERSON_TYPES.PERSON_TYPE_ID = PER_PERSON_TYPE_USAGES_F.PERSON_TYPE_ID
;"""
so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: "+sr_file)

""" ****************************************************************************
END
*****************************************************************************"""

# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("----------------------------")
funcfile.writelog("COMPLETED: B001_PEOPLE_LISTS")
