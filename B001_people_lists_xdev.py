"""
SCRIPT TO BUILD PEOPLE LISTS
AUTHOR: Albert J v Rensburg (NWU:21162395)
CREATED: 12 APR 2018
MODIFIED: 5 APR 2020
"""

# IMPORT SYSTEM MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcmail
from _my_modules import funcpayroll
from _my_modules import funcpeople
from _my_modules import funcsms
from _my_modules import funcsys

""" INDEX
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
End OF SCRIPT
"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# SCRIPT LOG
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B001_PEOPLE_LISTS_DEV")
funcfile.writelog("-----------------------------")
print("---------------------")
print("B001_PEOPLE_LISTS_DEV")
print("---------------------")

# DECLARE VARIABLES
so_path = "W:/People/"  # Source database path
so_file = "People.sqlite"  # Source database
sr_file: str = ""  # Current sqlite table
re_path = "R:/People/"  # Results path
l_debug: bool = True
l_export: bool = True
l_mail: bool = True
l_vacuum: bool = False

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# Open the SQLITE SOURCE file
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
so_curs.execute("ATTACH DATABASE 'W:/People_payroll/People_payroll.sqlite' AS 'PAYROLL'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

"""*****************************************************************************
DO NOT DELETE
IMPORT OWN LOOKUPS
DO NOT DELETE
*****************************************************************************"""
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

"""*****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
if l_debug:
    print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

print("Build position structure...")
sr_file = "X000_POSITIONS_STRUCT"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    peop.max_persons,
    peop.position_id As position1,
    peop.position_name As position_name1,
    peop.employee_number As employee1,
    peop.name_full As name1,
    peop.employee_category As category1,
    peop.user_person_type As type1,
    peop2.max_persons max_persons2,
    post.POS02 As position2,
    peop2.position_name As position_name2,
    peop2.employee_number As employee2,
    peop2.name_full As name2,
    peop.supervisor_name,
    peop.oe_head_name_name
From
    X000_PEOPLE peop Left Join
    X000_POS_STRUCT_10 post On post.POS01 = peop.position_id Left Join
    X000_PEOPLE peop2 On peop2.position_id = post.POS02
Order By
    employee2,
    position2,
    position1
;"""
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

""" ****************************************************************************
End OF SCRIPT
*****************************************************************************"""
if l_debug:
    print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.commit()
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("--------------------------------")
funcfile.writelog("COMPLETED: B001_PEOPLE_LISTS_DEV")
