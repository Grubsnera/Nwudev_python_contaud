""" Script to extract STUDENT DEFERMENTS
Created on: 19 MAR 2018
Author: Albert J v Rensburg
"""

""" SCRIPT DESCRIPTION (and reulting table/view) *******************************
Can be run at any time depeninding on update status of dependancies listed
01 Build the current year deferment list (X000_DEFERMENTS)
02 Build the previous year deferment list (X001_deferments_prev)
03 Build the current year deferment list (X001_deferments_curr)
04 OBTAIN THE LIST OF CURRENT REGISTERED STUDENTS (X002_Students_curr)
05 OBTAIN A LIST OF CURRENT YEAR OPENING BALANCES (X003_TRAN_BALOPEN_CURR)
06 OBTAIN A LIST OF CURRENT YEAR REGISTRATION FEES (X003_TRAN_FEEREG_CURR)
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
sr_file = "X000_Deferments"
s_sql = "CREATE TABLE " + sr_file + " AS" + """
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
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# 02 BUILD THE PREVIOUS YEAR DEFERMENT LIST ************************************

print("Build the previous deferments...")
sr_file = "X000_Deferments_prev"
s_sql = "CREATE TABLE " + sr_file + " AS" + """
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
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
# Export the declaration data
sr_filet = sr_file
sx_path = "R:/Debtorstud/" + funcdate.prev_year() + "/"
sx_file = "Deferment_000_deferment_"
sx_filet = sx_file + funcdate.today_file() #Today
print("Export data..." + sx_path + sx_filet)
# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# 03 BUILD THE CURRENT DEFERMENT LIST ******************************************

print("Build the current deferments...")
sr_file = "X000_Deferments_curr"
s_sql = "CREATE TABLE " + sr_file + " AS" + """
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
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
# Export the declaration data
sr_filet = sr_file
sx_path = "R:/Debtorstud/" + funcdate.cur_year() + "/"
sx_file = "Deferment_000_deferment_"
sx_filet = sx_file + funcdate.today_file() #Today
print("Export data..." + sx_path + sx_filet)
# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# 04 OBTAIN THE LIST OF CURRENT REGISTERED STUDENTS ****************************

# Exclude all short courses (QUAL_TYPE Not Like '%Short Course%')
# Only include contact students (PRESENT_CAT = 'Contact')
# Only include active students (ACTIVE_IND = 'Active')

print("Obtain the current registered students...")
sr_file = "X000_Students_curr"
s_sql = "CREATE TABLE " + sr_file+ " AS" + """
SELECT
  *
FROM
  VSS.X001cx_Stud_qual_curr
WHERE
  VSS.X001cx_Stud_qual_curr.QUAL_TYPE Not Like '%Short Course%' AND
  VSS.X001cx_Stud_qual_curr.PRESENT_CAT = 'Contact' AND
  VSS.X001cx_Stud_qual_curr.ACTIVE_IND = 'Active'
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# 05 OBTAIN A LIST OF CURRENT YEAR OPENING BALANCES ****************************

# All transaction type 001, 031 and 061 transactions

print("Obtain the current year opening balances...")
sr_file = "X000_Tran_balopen_curr"
s_sql = "CREATE TABLE " + sr_file+ " AS" + """
SELECT
  VSS.X010_Studytrans.FBUSENTID AS STUDENT,
  Cast(Total(VSS.X010_Studytrans.AMOUNT) AS REAL) AS BAL_OPEN
FROM
  VSS.X010_Studytrans
WHERE
  (VSS.X010_Studytrans.TRANSCODE = "001") OR
  (VSS.X010_Studytrans.TRANSCODE = "031") OR
  (VSS.X010_Studytrans.TRANSCODE = "061")
GROUP BY
  VSS.X010_Studytrans.FBUSENTID
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# 06 OBTAIN A LIST OF CURRENT YEAR REGISTRATION FEES ***************************

# All transaction type 002 transactions

print("Obtain the current year registration fee transactions...")
sr_file = "X000_Tran_feereg_curr"
s_sql = "CREATE TABLE " + sr_file+ " AS" + """
SELECT
  VSS.X010_Studytrans.FBUSENTID AS STUDENT,
  Cast(Total(VSS.X010_Studytrans.AMOUNT) AS REAL) AS FEE_REG
FROM
  VSS.X010_Studytrans
WHERE
  VSS.X010_Studytrans.TRANSCODE = "002"
GROUP BY
  VSS.X010_Studytrans.FBUSENTID
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# CLOSE THE DATABASE CONNECTION ************************************************
so_conn.close()

# CLOSE THE LOG WRITER *********************************************************
funcfile.writelog("COMPLETED")
