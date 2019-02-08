"""
Script to build standard VSS lists
Created on: 01 Mar 2018
Author: Albert J v Rensburg (21162395)
"""

# Import python modules
import datetime
import sqlite3
import sys

# Add own module path
sys.path.append('S:/_my_modules')

# Import own modules
import funcdate
import funccsv
import funcfile

# Open the script log file ******************************************************

funcfile.writelog()
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: Report_people_leave_per_person")
ilog_severity = 1

# Declare variables
so_path = "W:/People_leave/" #Source database path
so_file = "People_leave.sqlite" #Source database
s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("%t OPEN DATABASE: People_leave.sqlite")

so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")    
funcfile.writelog("%t ATTACH DATABASE: People.sqlite")

print("PEOPLE LEAVE PER PERSON")
print("-----------------------")

print("")
s_em = input("Employee? ")
print("")

# Build CURRENT ABSENCE ATTENDANCES ********************************************

print("Build employee absence attendances...")

s_sql = "CREATE TABLE X104_PER_ABSENCE_ATTENDANCES_EMPL AS " + """
SELECT
  X100_Per_absence_attendances.EMPLOYEE_NUMBER,
  X100_Per_absence_attendances.PERSON_ID,
  X100_Per_absence_attendances.ABSENCE_ATTENDANCE_ID,
  X100_Per_absence_attendances.BUSINESS_GROUP_ID,
  X100_Per_absence_attendances.DATE_NOTIFICATION,
  X100_Per_absence_attendances.DATE_START,
  X100_Per_absence_attendances.DATE_END,
  X100_Per_absence_attendances.ABSENCE_DAYS,
  X100_Per_absence_attendances.ABSENCE_ATTENDANCE_TYPE_ID,
  X102_Per_absence_attendance_types.NAME AS LEAVE_TYPE,
  X100_Per_absence_attendances.ABS_ATTENDANCE_REASON_ID,
  X101_Per_abs_attendance_reasons.NAME AS LEAVE_REASON,
  X101_Per_abs_attendance_reasons.MEANING AS REASON_DESCRIP,
  X100_Per_absence_attendances.AUTHORISING_PERSON_ID,
  PEOPLE.PER_ALL_PEOPLE_F.EMPLOYEE_NUMBER AS EMPLOYEE_AUTHORISE,
  X100_Per_absence_attendances.ABSENCE_HOURS,
  X100_Per_absence_attendances.OCCURRENCE,
  X100_Per_absence_attendances.SSP1_ISSUED,
  X100_Per_absence_attendances.PROGRAM_APPLICATION_ID,
  X100_Per_absence_attendances.ATTRIBUTE1,
  X100_Per_absence_attendances.ATTRIBUTE2,
  X100_Per_absence_attendances.ATTRIBUTE3,
  X100_Per_absence_attendances.LAST_UPDATE_DATE,
  X100_Per_absence_attendances.LAST_UPDATED_BY,
  X100_Per_absence_attendances.LAST_UPDATE_LOGIN,
  X100_Per_absence_attendances.CREATED_BY,
  X100_Per_absence_attendances.CREATION_DATE,
  X100_Per_absence_attendances.REASON_FOR_NOTIFICATION_DELAY,
  X100_Per_absence_attendances.ACCEPT_LATE_NOTIFICATION_FLAG,
  X100_Per_absence_attendances.OBJECT_VERSION_NUMBER,
  X102_Per_absence_attendance_types.INPUT_VALUE_ID,
  X102_Per_absence_attendance_types.ABSENCE_CATEGORY,
  X102_Per_absence_attendance_types.MEANING AS TYPE_DESCRIP
FROM
  X100_Per_absence_attendances
  LEFT JOIN X102_Per_absence_attendance_types ON X102_Per_absence_attendance_types.ABSENCE_ATTENDANCE_TYPE_ID =
    X100_Per_absence_attendances.ABSENCE_ATTENDANCE_TYPE_ID
  LEFT JOIN X101_Per_abs_attendance_reasons ON X101_Per_abs_attendance_reasons.ABS_ATTENDANCE_REASON_ID =
    X100_Per_absence_attendances.ABS_ATTENDANCE_REASON_ID
  LEFT JOIN PEOPLE.PER_ALL_PEOPLE_F ON PEOPLE.PER_ALL_PEOPLE_F.PERSON_ID = X100_Per_absence_attendances.AUTHORISING_PERSON_ID AND
    PEOPLE.PER_ALL_PEOPLE_F.EFFECTIVE_START_DATE <= X100_Per_absence_attendances.DATE_START AND
    PEOPLE.PER_ALL_PEOPLE_F.EFFECTIVE_END_DATE >= X100_Per_absence_attendances.DATE_START"""

if s_em != "":
   s_sql += """
WHERE
  (X100_Per_absence_attendances.EMPLOYEE_NUMBER = '""" + s_em + "')"

s_sql += """
ORDER BY
  X100_Per_absence_attendances.EMPLOYEE_NUMBER,
  X100_Per_absence_attendances.DATE_START,
  X100_Per_absence_attendances.DATE_END
"""

# print(s_sql)

"""
WHERE
  (X100_Per_absence_attendances.DATE_START >= Date("%CYEARB%") AND
  X100_Per_absence_attendances.DATE_END <= Date("%CYEARE%")) OR
  (X100_Per_absence_attendances.DATE_START >= Date("%CYEARB%") AND
  X100_Per_absence_attendances.DATE_START <= Date("%CYEARE%")) OR
  (X100_Per_absence_attendances.DATE_END >= Date("%CYEARB%") AND
  X100_Per_absence_attendances.DATE_END <= Date("%CYEARE%"))
"""

so_curs.execute("DROP TABLE IF EXISTS X104_PER_ABSENCE_ATTENDANCES_EMPL")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: X104_Per_absence_attendances_empl")

# Export the declaration data

sr_file = "X104_Per_absence_attendances_empl"
sr_filet = sr_file
sx_path = "R:/People/" + funcdate.cur_year() + "/"
sx_file = "Leave_104_lst_transact_" + s_em + "_"
sx_filet = sx_file + funcdate.today_file() #Today

print("Export data..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: Employee leave transactions")

# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("COMPLETED")
