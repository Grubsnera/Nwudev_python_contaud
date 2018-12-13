""" Script to extract STUDENT DEFERMENTS
Created on: 19 MAR 2018
Author: Albert J v Rensburg
"""

""" SCRIPT DESCRIPTION (and reulting table/view) *******************************
Can be run at any time depeninding on update status of dependancies listed
01 Build the current year deferment list (X000_DEFERMENTS)
02 Build the previous year deferment list (X001_deferments_prev)
03 Build the current year deferment list (X001_deferments_curr)
**************************************************************************** """

""" DEPENDANCIES ***************************************************************
CODEDESCRIPTION table (Vss.sqlite)
ACCDEFERMENT table (Vss_deferment.sqlite)
STUDACC (Vss.sqlite)
SYSTEMUSER (Vss.sqlite)
STUDENTSITE (Vss.sqlite)
**************************************************************************** """


# IMPORT MODULES ***************************************************************

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

# OPEN THE SCRIPT LOG FILE *****************************************************

funcfile.writelog()
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: Report_vss_deferments")

# DECLARE MODULE WIDE VARIABLES ************************************************

so_path = "W:/Vss_deferment/" #Source database path
so_file = "Vss_deferment.sqlite" #Source database
s_sql = "" #SQL statements

# OPEN DATA FILES **************************************************************

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("%t OPEN DATABASE: Vss_deferment")

so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")

# 01 BUILD THE DEFERMENT LIST **************************************************

# Extract the current deferments and add lookups
# All deferments starting on 1 Jan of current year
# Join subaccount description language 3 english
# Join deferment type language 3 english
# Join deferment reason  language 3 english
# Join the system user employee number

print("DEFERMENTS")
print("----------")
print("Build deferments...")

s_sql = "CREATE TABLE X000_DEFERMENTS AS" + """
SELECT
  ACCDEFERMENT.KACCDEFERMENTID,
  ACCDEFERMENT.FACCID,
  VSS.STUDACC.FBUSENTID,
  ACCDEFERMENT.DATEARRANGED,
  VSS.SYSTEMUSER.FUSERBUSINESSENTITYID,
  ACCDEFERMENT.STARTDATE,
  ACCDEFERMENT.ENDDATE,
  ACCDEFERMENT.TOTALAMOUNT,
  VSS.CODEDESCRIPTION.CODESHORTDESCRIPTION AS SUBACCOUNTTYPE,
  VSS.CODEDESCRIPTION1.CODESHORTDESCRIPTION AS DEFERMENTTYPE,
  VSS.CODEDESCRIPTION2.CODESHORTDESCRIPTION AS DEFERMENTREASON,
  ACCDEFERMENT.NOTE,
  ACCDEFERMENT.FAUDITUSERCODE,
  ACCDEFERMENT.AUDITDATETIME,
  ACCDEFERMENT.FAUDITSYSTEMFUNCTIONID
FROM
  ACCDEFERMENT
  LEFT JOIN VSS.CODEDESCRIPTION ON VSS.CODEDESCRIPTION.KCODEDESCID = ACCDEFERMENT.FSUBACCTYPECODEID
  LEFT JOIN VSS.CODEDESCRIPTION CODEDESCRIPTION1 ON VSS.CODEDESCRIPTION1.KCODEDESCID = ACCDEFERMENT.FDEFERMENTTYPECODEID
  LEFT JOIN VSS.CODEDESCRIPTION CODEDESCRIPTION2 ON VSS.CODEDESCRIPTION2.KCODEDESCID = ACCDEFERMENT.FDEFERMENTREASONCODEID
  LEFT JOIN VSS.STUDACC ON VSS.STUDACC.KACCID = ACCDEFERMENT.FACCID
  LEFT JOIN VSS.SYSTEMUSER ON VSS.SYSTEMUSER.KUSERCODE = ACCDEFERMENT.FAUDITUSERCODE
WHERE
  VSS.CODEDESCRIPTION.KSYSTEMLANGUAGECODEID = 3 AND
  VSS.CODEDESCRIPTION1.KSYSTEMLANGUAGECODEID = 3 AND
  VSS.CODEDESCRIPTION2.KSYSTEMLANGUAGECODEID = 3
ORDER BY
  VSS.STUDACC.FBUSENTID,
  ACCDEFERMENT.AUDITDATETIME
"""

so_curs.execute("DROP TABLE IF EXISTS X000_DEFERMENTS")
so_curs.execute(s_sql)

funcfile.writelog("%t BUILD TABLE: X000_Deferments")

# 02 BUILD THE PREVIOUS YEAR DEFERMENT LIST ************************************

print("Build the previous deferments...")

s_sql = "CREATE TABLE X001_DEFERMENTS_PREV AS" + """
SELECT
  X000_DEFERMENTS.KACCDEFERMENTID,
  X000_DEFERMENTS.FBUSENTID AS 'STUDENT',
  VSS.STUDENTSITE.FDEBTCOLLECTIONSITE AS 'CAMPUS',
  X000_DEFERMENTS.DATEARRANGED,
  X000_DEFERMENTS.FUSERBUSINESSENTITYID AS 'EMPLOYEE',
  X000_DEFERMENTS.STARTDATE AS 'DATESTART',
  X000_DEFERMENTS.ENDDATE AS 'DATEEND',
  X000_DEFERMENTS.TOTALAMOUNT,
  X000_DEFERMENTS.SUBACCOUNTTYPE,
  X000_DEFERMENTS.DEFERMENTTYPE,
  X000_DEFERMENTS.DEFERMENTREASON,
  X000_DEFERMENTS.NOTE,
  X000_DEFERMENTS.FAUDITUSERCODE,
  X000_DEFERMENTS.AUDITDATETIME,
  X000_DEFERMENTS.FAUDITSYSTEMFUNCTIONID,
  VSS.STUDENTSITE.FADMISSIONSITE,
  VSS.STUDENTSITE.FMAINQUALSITE
FROM
  X000_DEFERMENTS
  LEFT JOIN VSS.STUDENTSITE ON STUDENTSITE.KSTUDENTBUSENTID = X000_DEFERMENTS.FBUSENTID
WHERE
  VSS.STUDENTSITE.KSTARTDATETIME <= X000_DEFERMENTS.DATEARRANGED AND
  VSS.STUDENTSITE.ENDDATETIME > X000_DEFERMENTS.DATEARRANGED AND
  X000_DEFERMENTS.STARTDATE >= Date('%PYEARB%') AND
  X000_DEFERMENTS.ENDDATE <= Date('%PYEARE%')
"""
s_sql = s_sql.replace("%PYEARB%",funcdate.prev_yearbegin())
s_sql = s_sql.replace("%PYEARE%",funcdate.prev_yearend())
so_curs.execute("DROP TABLE IF EXISTS X001_DEFERMENTS_PREV")
so_curs.execute(s_sql)

funcfile.writelog("%t BUILD TABLE: X001_Deferments_prev")

# Export the declaration data

sr_file = "X001_deferments_prev"
sr_filet = sr_file
sx_path = "R:/Debtorstud/" + funcdate.prev_year() + "/"
sx_file = "Deferment_001_deferment_"
sx_filet = sx_file + funcdate.today_file() #Today

print("Export data..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)

funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# 03 BUILD THE CURRENT DEFERMENT LIST ******************************************

print("Build the current deferments...")

s_sql = "CREATE TABLE X001_DEFERMENTS_CURR AS" + """
SELECT
  X000_DEFERMENTS.KACCDEFERMENTID,
  X000_DEFERMENTS.FBUSENTID AS 'STUDENT',
  STUDENTSITE.FDEBTCOLLECTIONSITE AS 'CAMPUS',
  X000_DEFERMENTS.DATEARRANGED,
  X000_DEFERMENTS.FUSERBUSINESSENTITYID AS 'EMPLOYEE',
  X000_DEFERMENTS.STARTDATE AS 'DATESTART',
  X000_DEFERMENTS.ENDDATE AS 'DATEEND',
  X000_DEFERMENTS.TOTALAMOUNT,
  X000_DEFERMENTS.SUBACCOUNTTYPE,
  X000_DEFERMENTS.DEFERMENTTYPE,
  X000_DEFERMENTS.DEFERMENTREASON,
  X000_DEFERMENTS.NOTE,
  X000_DEFERMENTS.FAUDITUSERCODE,
  X000_DEFERMENTS.AUDITDATETIME,
  X000_DEFERMENTS.FAUDITSYSTEMFUNCTIONID,
  STUDENTSITE.FADMISSIONSITE,
  STUDENTSITE.FMAINQUALSITE
FROM
  X000_DEFERMENTS
  LEFT JOIN STUDENTSITE ON STUDENTSITE.KSTUDENTBUSENTID = X000_DEFERMENTS.FBUSENTID
WHERE
  STUDENTSITE.KSTARTDATETIME <= X000_DEFERMENTS.DATEARRANGED AND
  STUDENTSITE.ENDDATETIME > X000_DEFERMENTS.DATEARRANGED AND
  X000_DEFERMENTS.STARTDATE >= Date('%CYEARB%') AND
  X000_DEFERMENTS.ENDDATE <= Date('%CYEARE%')
"""
s_sql = s_sql.replace("%CYEARB%",funcdate.cur_yearbegin())
s_sql = s_sql.replace("%CYEARE%",funcdate.cur_yearend())
so_curs.execute("DROP TABLE IF EXISTS X001_DEFERMENTS_CURR")
so_curs.execute(s_sql)

funcfile.writelog("%t BUILD TABLE: X001_Deferments_curr")

# Export the declaration data

sr_file = "X001_deferments_curr"
sr_filet = sr_file
sx_path = "R:/Debtorstud/" + funcdate.cur_year() + "/"
sx_file = "Deferment_001_deferment_"
sx_filet = sx_file + funcdate.today_file() #Today

print("Export data..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# CLOSE THE DATABASE CONNECTION ************************************************
so_conn.close()

# CLOSE THE LOG WRITER *********************************************************
funcfile.writelog("COMPLETED")
