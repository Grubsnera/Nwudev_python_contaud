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

"""*****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

""" ****************************************************************************
BUILD SECONDARY ASSIGNMENTS
*****************************************************************************"""
print("BUILD SECONDARY ASSIGNMENTS")
funcfile.writelog("BUILD SECONDARY ASSIGNMENTS")

# BUILD SECONDARY ASSIGNMENTS
print("Build secondary assignments...")
sr_file = "X000_PER_ALL_ASSIGNMENTS_SECONDARY"
s_sql = "CREATE VIEW " + sr_file + " AS " + """
SELECT
    SEC.ASSIGNMENT_EXTRA_INFO_ID,
    SEC.ASSIGNMENT_ID,
    SEC.INFORMATION_TYPE,
    SEC.AEI_INFORMATION_CATEGORY,
    SEC.AEI_INFORMATION1 As SEC_POSITION,
    SEC.AEI_INFORMATION2 As SEC_ASS_ID,
    Upper(SEC.AEI_INFORMATION3) As SEC_BUDGET,
    SEC.AEI_INFORMATION4 As SEC_BUDGET_POSITION,
    Cast(SEC.AEI_INFORMATION5 as REAL) As SEC_BUDGET_VALUE,
    SEC.AEI_INFORMATION6 As SEC_YEAR,
    Cast(SEC.AEI_INFORMATION7 As REAL) As SEC_HOURS_FORECAST,
    SEC.AEI_INFORMATION8,
    SEC.AEI_INFORMATION9,
    SEC.AEI_INFORMATION10,
    SEC.AEI_INFORMATION11,
    Replace(SEC.AEI_INFORMATION12,'/','-') As SEC_DATE_FROM,
    Replace(SEC.AEI_INFORMATION13,'/','-') As SEC_DATE_TO,
    Upper(SEC.AEI_INFORMATION14) As SEC_TYPE,
    Cast(SEC.AEI_INFORMATION15 As REAL) As SEC_RATE,
    Upper(SEC.AEI_INFORMATION16) As SEC_UNIT,
    SEC.AEI_INFORMATION17 As SEC_APPROVER,
    Upper(SEC.AEI_INFORMATION18) As SEC_POSITION_NAME,
    SEC.AEI_INFORMATION19,
    Upper(SEC.AEI_INFORMATION20) As SEC_FULLPART_FLAG,
    SEC.AEI_INFORMATION21,
    SEC.AEI_INFORMATION22,
    SEC.AEI_INFORMATION23,
    SEC.AEI_INFORMATION24 As SEC_CHART,
    SEC.AEI_INFORMATION25 As SEC_ACCOUNT,
    SEC.AEI_INFORMATION26 As SEC_OBJECT,
    SEC.AEI_INFORMATION27,
    SEC.AEI_INFORMATION28,
    SEC.AEI_INFORMATION29,
    SEC.AEI_INFORMATION30,
    SEC.LAST_UPDATE_DATE,
    SEC.OBJECT_VERSION_NUMBER,
    SEC.LAST_UPDATED_BY,
    SEC.LAST_UPDATE_LOGIN,
    SEC.CREATED_BY,
    SEC.CREATION_DATE
FROM
    PER_ASSIGNMENT_EXTRA_INFO_SEC SEC
ORDER BY
    ASSIGNMENT_ID,
    AEI_INFORMATION12            
;"""
so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: " + sr_file)

""" ****************************************************************************
BUILD CURRENT YEAR SECONDARY ASSIGNMENTS
*****************************************************************************"""
print("BUILD CURRENT YEAR SECONDARY ASSIGNMENTS")
funcfile.writelog("BUILD CURRENT YEAR SECONDARY ASSIGNMENTS")

# BUILD CURRENT YEAR SECONDARY ASSIGNMENTS
print("Build current year secondary assignments...")
sr_file = "X001_ASSIGNMENT_SEC_CURR_YEAR"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    SEC.ASSIGNMENT_EXTRA_INFO_ID,
    SEC.ASSIGNMENT_ID,
    SEC.SEC_POSITION,
    SEC.SEC_ASS_ID,
    SEC.SEC_BUDGET,
    SEC.SEC_BUDGET_POSITION,
    SEC.SEC_BUDGET_VALUE,
    SEC.SEC_YEAR,
    SEC.SEC_HOURS_FORECAST,
    SEC.SEC_DATE_FROM,
    SEC.SEC_DATE_TO,
    SEC.SEC_TYPE,
    SEC.SEC_RATE,
    SEC.SEC_UNIT,
    SEC.SEC_APPROVER,
    SEC.SEC_POSITION_NAME,
    SEC.SEC_FULLPART_FLAG,
    SEC.SEC_CHART,
    SEC.SEC_ACCOUNT,
    SEC.SEC_OBJECT,
    SEC.LAST_UPDATE_DATE,
    SEC.OBJECT_VERSION_NUMBER,
    SEC.LAST_UPDATED_BY,
    SEC.LAST_UPDATE_LOGIN,
    SEC.CREATED_BY,
    SEC.CREATION_DATE
From
    X000_PER_ALL_ASSIGNMENTS_SECONDARY SEC
Where
    (SEC.SEC_DATE_TO >= Date('%CYEARB%') AND
    SEC.SEC_DATE_TO <= Date('%CYEARE%')) OR
    (SEC.SEC_DATE_FROM >= Date('%CYEARB%') AND
    SEC.SEC_DATE_FROM <= Date('%CYEARE%')) OR
    (SEC.SEC_DATE_TO >= Date('%CYEARE%') AND
    SEC.SEC_DATE_FROM <= Date('%CYEARB%'))
"""
s_sql = s_sql.replace("%CYEARB%", funcdate.cur_yearbegin())
s_sql = s_sql.replace("%CYEARE%", funcdate.cur_yearend())
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: " + sr_file)

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
