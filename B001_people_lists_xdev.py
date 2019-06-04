import csv
import sqlite3
import sys

""" Script to test PEOPLE master file data *************************************
Created on: 1 Mar 2019
Author: Albert J v Rensburg (NWU21162395)
*****************************************************************************"""

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
End OF SCRIPT
*****************************************************************************"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# ADD OWN MODULE PATH
sys.path.append('S:/_my_modules')

# IMPORT OWN MODULES
import funcfile

# OPEN THE SCRIPT LOG FILE
print("-----------------")    
print("B001_PEOPLE_LISTS")
print("-----------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B001_PEOPLE_LISTS_DEV")
funcfile.writelog("-----------------------------")
ilog_severity = 1

# DECLARE VARIABLES
so_path = "W:/People/" #Source database path
re_path = "R:/People/"
so_file = "People.sqlite" #Source database
s_sql = "" #SQL statements
l_export = False
l_mail = True

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("%t OPEN DATABASE: PEOPLE.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

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
# Close the impoted data file
co.close()
funcfile.writelog("%t IMPORT TABLE: " + tb_name)

""" ****************************************************************************
BUILD ADDRESSES AND PHONES
*****************************************************************************"""
print("BUILD ADDRESSES")
funcfile.writelog("BUILD ADDRESSES")



# 15 Build ADDRESS HOME ********************************************************

print("Build home adresses...")

s_sql = "CREATE VIEW X000_ADDRESS_HOME AS " + """
SELECT
  ADDR.ADDRESS_ID,
  ADDR.PERSON_ID,
  ADDR.DATE_FROM,
  ADDR.DATE_TO,
  ADDR.ADDRESS_HOME
FROM
  X000_ADDRESSES ADDR
WHERE
  LENGTH(ADDR.ADDRESS_HOME) > 0
Order by
    ADDR.PERSON_ID,
    ADDR.DATE_FROM Desc,
    ADDR.DATE_TO Desc
"""
so_curs.execute("DROP VIEW IF EXISTS X000_ADDRESS_HOME")
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: X000_ADDRESS_HOME")

# 15 Build ADDRESS HOME ********************************************************

print("Build home adresses...")

s_sql = "CREATE TABLE X999_ADDRESS_HOME AS " + """
Select
    ADDR.*,
    ADDR.DATE_TO As DATE_TO_CALC
From
    X000_ADDRESS_HOME ADDR
"""
so_curs.execute("DROP TABLE IF EXISTS X999_ADDRESS_HOME")
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: X999_ADDRESS_HOME")

s_sql = """
Update
    X999_ADDRESS_HOME
Set
    DATE_TO_CALC =
    (
    Select
        DATE_FROM
    From
        X999_ADDRESS_HOME NEWW
    Where
        NEWW.PERSON_ID = X999_ADDRESS_HOME.PERSON_ID And
        NEWW.DATE_FROM < X999_ADDRESS_HOME.DATE_FROM
    Limit 1
    )
;"""
print(s_sql)
so_curs.execute(s_sql)






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
