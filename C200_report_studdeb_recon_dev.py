"""
Script to build GL Student debtor control account reports
Created on: 13 Mar 2018
"""

# Import python modules
import csv
import datetime
import sqlite3
import sys

# Add own module path
sys.path.append('S:/_my_modules')

# Import own modules
import funcdate
import funccsv
import funcfile
import funcmail
import funcsys

# Open the script log file ******************************************************

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C200_REPORT_STUDDEB_RECON_DEV")
funcfile.writelog("-------------------------------------")
print("-------------------------")
print("C200_REPORT_STUDDEB_RECON")
print("-------------------------")
ilog_severity = 1

# Declare variables
so_path = "W:/" #Source database path
re_path = "R:/Debtorstud/" #Results
ed_path = "S:/_external_data/" #External data
so_file = "Kfs_vss_studdeb.sqlite" #Source database
s_sql = "" #SQL statements
l_mail = True
l_export = True

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN DATABASE: Kfs_vss_studdeb")


#so_curs.execute("ATTACH DATABASE 'W:/People.sqlite' AS 'PEOPLE'")
#funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
#so_curs.execute("ATTACH DATABASE 'W:/Kfs.sqlite' AS 'KFS'")
#funcfile.writelog("%t ATTACH DATABASE: KFS_SQLITE")
#so_curs.execute("ATTACH DATABASE 'W:/Vss.sqlite' AS 'VSS'")
#funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")

# Development script ***********************************************************

gl_month = "07"



"""*************************************************************************
***
*** TEST TRANSACTION TYPES IN GL BUT NOT IN VSS
***
*** Detail list of transaction types in gl but not in vss
***   Add rowid campus+month+trantype
***   Add org as nwu
*** Import reporting officer name and email address for campus and organization
*** Import reporting supervisor name and email address for campus and organization
*** 
*************************************************************************"""

# Identify transaction types in gl but not in vss ******************************
print("Identify transaction types in gl but not in vss...")
sr_file = "X004da_ingl_novss"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  UPPER(SUBSTR(X003ab_gl_vss_join.CAMPUS_VSS,1,3))||TRIM(X003ab_gl_vss_join.MONTH_VSS)||REPLACE(UPPER(X003ab_gl_vss_join.DESC_VSS),' ','') AS ROWID,
  'NWU' AS ORG,
  X003ab_gl_vss_join.CAMPUS_VSS AS CAMPUS,
  X003ab_gl_vss_join.MONTH_VSS AS MONTH,
  X003ab_gl_vss_join.DESC_VSS AS GL_DESCRIPTION,
  Round(X003ab_gl_vss_join.AMOUNT,2) AS AMOUNT_GL
FROM
  X003ab_gl_vss_join
ORDER BY
  X003ab_gl_vss_join.MONTH_VSS,
  X003ab_gl_vss_join.CAMPUS_VSS,
  X003ab_gl_vss_join.DESC_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Import the reporting officers ************************************************
print("Import reporting officers from VSS.SQLITE...")
sr_file = "X004db_impo_report_officer"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  VSS.X000_OWN_LOOKUPS.LOOKUP,
  VSS.X000_OWN_LOOKUPS.LOOKUP_CODE AS CAMPUS,
  VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
  PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
  PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
FROM
  VSS.X000_OWN_LOOKUPS
  LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION
WHERE
  VSS.X000_OWN_LOOKUPS.LOOKUP = 'stud_debt_recon_test_ingl_novss_officer'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Import the reporting supervisors *********************************************
print("Import reporting supervisors from VSS.SQLITE...")
sr_file = "X004dc_impo_report_supervisor"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  VSS.X000_OWN_LOOKUPS.LOOKUP,
  VSS.X000_OWN_LOOKUPS.LOOKUP_CODE AS CAMPUS,
  VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
  PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
  PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
FROM
  VSS.X000_OWN_LOOKUPS
  LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = VSS.X000_OWN_LOOKUPS.LOOKUP_DESCRIPTION
WHERE
  VSS.X000_OWN_LOOKUPS.LOOKUP = 'stud_debt_recon_test_ingl_novss_supervisor'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Add the reporting officer and supervisor *************************************
print("Add the reporting officer and supervisor...")
sr_file = "X004dx_ingl_novss"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  X004da_ingl_novss."ROWID",
  X004da_ingl_novss.ORG,
  X004da_ingl_novss.CAMPUS,
  X004da_ingl_novss.MONTH,
  X004da_ingl_novss.GL_DESCRIPTION,
  X004da_ingl_novss.AMOUNT_GL,
  CAMP_OFFICER.EMPLOYEE_NUMBER AS OFFICER_CAMP,
  CAMP_OFFICER.KNOWN_NAME AS OFFICER_NAME_CAMP,
  CAMP_OFFICER.EMAIL_ADDRESS AS OFFICER_MAIL_CAMP,
  ORG_OFFICER.EMPLOYEE_NUMBER AS OFFICER_ORG,
  ORG_OFFICER.KNOWN_NAME AS OFFICER_NAME_ORG,
  ORG_OFFICER.EMAIL_ADDRESS AS OFFICER_MAIL_ORG,
  CAMP_SUPERVISOR.EMPLOYEE_NUMBER AS SUPERVISOR_CAMP,
  CAMP_SUPERVISOR.KNOWN_NAME AS SUPERVISOR_NAME_CAMP,
  CAMP_SUPERVISOR.EMAIL_ADDRESS AS SUPERVISOR_MAIL_CAMP,
  ORG_SUPERVISOR.EMPLOYEE_NUMBER AS SUPERVISOR_ORG,
  ORG_SUPERVISOR.KNOWN_NAME AS SUPERVISOR_NAME_ORG,
  ORG_SUPERVISOR.EMAIL_ADDRESS AS SUPERVISOR_MAIL_ORG
FROM
  X004da_ingl_novss
  LEFT JOIN X004db_impo_report_officer CAMP_OFFICER ON CAMP_OFFICER.CAMPUS = X004da_ingl_novss.CAMPUS
  LEFT JOIN X004db_impo_report_officer ORG_OFFICER ON ORG_OFFICER.CAMPUS = X004da_ingl_novss.ORG
  LEFT JOIN X004dc_impo_report_supervisor CAMP_SUPERVISOR ON CAMP_SUPERVISOR.CAMPUS = X004da_ingl_novss.CAMPUS
  LEFT JOIN X004dc_impo_report_supervisor ORG_SUPERVISOR ON ORG_SUPERVISOR.CAMPUS = X004da_ingl_novss.ORG
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)


















# Close the table connection ***************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C200_REPORT_STUDDEB_RECON_DEV")
