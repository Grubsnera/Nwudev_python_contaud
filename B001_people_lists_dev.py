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
so_path = "W:/" #Source database path
re_path = "R:/People/"
so_file = "People.sqlite" #Source database
s_sql = "" #SQL statements
l_export = False
l_mail = True

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("%t OPEN DATABASE: PEOPLE.SQLITE")

# People script development *****************************************


# Build PEOPLE ORGANIZATION STRUCTURE REF **********************************
print("Build reference people organogram...")
s_sql = "CREATE TABLE X003_PEOPLE_ORGA_REF AS " + """
SELECT
  X002_PEOPLE_CURR.EMPLOYEE_NUMBER AS employee_one,
  X002_PEOPLE_CURR.NAME_LIST AS name_list_one,
  X002_PEOPLE_CURR.KNOWN_NAME AS known_name_one,
  X002_PEOPLE_CURR.POSITION_FULL AS position_full_one,
  X002_PEOPLE_CURR.LOCATION_DESCRIPTION AS location_description_one,
  X002_PEOPLE_CURR.DIVISION AS division_one,
  X002_PEOPLE_CURR.FACULTY AS faculty_one,
  X002_PEOPLE_CURR.EMAIL_ADDRESS AS email_address_one,
  X002_PEOPLE_CURR.PHONE_WORK AS phone_work_one,
  X002_PEOPLE_CURR.PHONE_MOBI AS phone_mobi_one,
  X002_PEOPLE_CURR.PHONE_HOME AS phone_home_one,
  X002_PEOPLE_CURR.GRADE_CALC AS grade_calc_one,
  X002_PEOPLE_CURR.EMPLOYEE_NUMBER AS employee_two,
  X002_PEOPLE_CURR.NAME_LIST AS name_list_two,
  X002_PEOPLE_CURR.KNOWN_NAME AS known_name_two,
  X002_PEOPLE_CURR.POSITION_FULL AS position_full_two,
  X002_PEOPLE_CURR.LOCATION_DESCRIPTION AS location_description_two,
  X002_PEOPLE_CURR.DIVISION AS division_two,
  X002_PEOPLE_CURR.FACULTY AS faculty_two,
  X002_PEOPLE_CURR.EMAIL_ADDRESS AS email_address_two,
  X002_PEOPLE_CURR.PHONE_WORK AS phone_work_two,
  X002_PEOPLE_CURR.PHONE_MOBI AS phone_mobi_two,
  X002_PEOPLE_CURR.PHONE_HOME AS phone_home_two,
  X002_PEOPLE_CURR.GRADE_CALC AS grade_calc_two,
  X002_PEOPLE_CURR.EMPLOYEE_NUMBER AS employee_three,
  X002_PEOPLE_CURR.NAME_LIST AS name_list_three,
  X002_PEOPLE_CURR.KNOWN_NAME AS known_name_three,
  X002_PEOPLE_CURR.POSITION_FULL AS position_full_three,
  X002_PEOPLE_CURR.LOCATION_DESCRIPTION AS location_description_three,
  X002_PEOPLE_CURR.DIVISION AS division_three,
  X002_PEOPLE_CURR.FACULTY AS faculty_three,
  X002_PEOPLE_CURR.EMAIL_ADDRESS AS email_address_three,
  X002_PEOPLE_CURR.PHONE_WORK AS phone_work_three,
  X002_PEOPLE_CURR.PHONE_MOBI AS phone_mobi_three,
  X002_PEOPLE_CURR.PHONE_HOME AS phone_home_three,
  X002_PEOPLE_CURR.GRADE_CALC AS grade_calc_three
FROM
  X002_PEOPLE_CURR
  LEFT JOIN X002_PEOPLE_CURR X002_PEOPLE_TWO ON X002_PEOPLE_TWO.EMPLOYEE_NUMBER = X002_PEOPLE_CURR.SUPERVISOR
  LEFT JOIN X002_PEOPLE_CURR X002_PEOPLE_THREE ON X002_PEOPLE_THREE.EMPLOYEE_NUMBER = X002_PEOPLE_TWO.SUPERVISOR
"""
so_curs.execute("DROP TABLE IF EXISTS X003_PEOPLE_ORGA_REF")
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: X003_PEOPLE_ORGA_REF")










# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("----------------------------")
funcfile.writelog("COMPLETED: B001_PEOPLE_LISTS")
