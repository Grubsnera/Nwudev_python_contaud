""" Script to extract RESIDENCES
Created on: 27 FEB 2018
Copyright: Albert J v Rensburg
"""

""" SCRIPT DESCRIPTION (and reulting table/view) *******************************
Can be run at any time depeninding on update status of dependancies listed
01 Build previous year residences (X001_Previous_residence)
02 Build current year residences (X001_Active_residence)
03 Build previous year rates (X002_Previous_rate)
04 Build current year rates (X002_Current_rate)
05 Build the previous year accomodation log (X003_Previous_accom_log)
06 Build the current year accomodation log (X003_Current_accom_log)
**************************************************************************** """

""" DEPENDANCIES ***************************************************************
CODEDESCRIPTION table (Vss.sqlite)
RESIDENCE table (Vss_residence.sqlite)
RESIDENCENAME table (Vss_residence.sqlite)
TRANSINST (Vss_residence.sqlite)
ACCOMMRESIDENCY (Vss_residence.sqlite)
**************************************************************************** """

# Import python modules
import sqlite3
import sys

# Add own module path
sys.path.append('X:\\Python\\_my_modules')

# Import own modules
import funcdatn
import funccsv
import funcfile

# Log
funcfile.writelog()
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: Report_vss_residences")

# Declare variables
so_path = "W:/" #Source database path
so_file = "Vss_residence.sqlite" #Source database
s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
    
funcfile.writelog("OPEN DATABASE: " + so_file)

so_curs.execute("ATTACH DATABASE 'W:/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")

print("RESIDENCE")
print("---------")

# 01 Extract the previous year residences and add lookups **********************

print("Extract the previous year residences...")

s_sql = "CREATE TABLE X001_Previous_residence AS " + """
SELECT
  RESIDENCE.KRESIDENCEID AS RESIDENCEID,
  RESIDENCENAME.NAME,
  VSS.X000_Codedescription.SHORT AS RESIDENCE_TYPE,
  RESIDENCE.FSITEORGUNITNUMBER AS CAMPUS,
  RESIDENCE.STARTDATE,
  RESIDENCE.ENDDATE,
  RESIDENCE.RESIDENCECAPACITY
FROM
  RESIDENCE
  LEFT JOIN RESIDENCENAME ON RESIDENCENAME.KRESIDENCEID = RESIDENCE.KRESIDENCEID
  LEFT JOIN VSS.X000_Codedescription ON VSS.X000_Codedescription.KCODEDESCID = RESIDENCE.FRESIDENCETYPECODEID
WHERE
  (RESIDENCENAME.KSYSTEMLANGUAGECODEID = 3 AND
  RESIDENCE.STARTDATE <= Date("%PYEARB%") AND
  RESIDENCE.ENDDATE >= Date("%PYEARE%")) OR
  (RESIDENCENAME.KSYSTEMLANGUAGECODEID = 3 AND
  RESIDENCE.ENDDATE >= Date("%PYEARB%") AND
  RESIDENCE.ENDDATE <= Date("%PYEARE%")) OR
  (RESIDENCENAME.KSYSTEMLANGUAGECODEID = 3 AND
  RESIDENCE.STARTDATE >= Date("%PYEARB%") AND
  RESIDENCE.STARTDATE <= Date("%PYEARE%"))
ORDER BY
  RESIDENCEID
"""
s_sql = s_sql.replace("%PYEARB%",funcdatn.get_previous_year_begin())
s_sql = s_sql.replace("%PYEARE%",funcdatn.get_previous_year_end())
so_curs.execute("DROP TABLE IF EXISTS X001_Previous_residence")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: X001_Previous_residence")
                  
# Data export
sr_file = "X001_Previous_residence"
sr_filet = sr_file
sx_path = "O:/Nwudata/Debtorstud/" + funcdatn.get_previous_year() + "/"
sx_file = "Residence_001_residence_"
sx_filet = sx_file + funcdatn.get_previous_year()

print("Export previous residences..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# 02 Extract the current residences and add lookups ****************************

print("Extract the current year residences...")

s_sql = "CREATE TABLE X001_Active_residence AS " + """
SELECT
  RESIDENCE.KRESIDENCEID AS RESIDENCEID,
  RESIDENCENAME.NAME,
  VSS.X000_Codedescription.SHORT AS RESIDENCE_TYPE,
  RESIDENCE.FSITEORGUNITNUMBER AS CAMPUS,
  RESIDENCE.STARTDATE,
  RESIDENCE.ENDDATE,
  RESIDENCE.RESIDENCECAPACITY
FROM
  RESIDENCE
  LEFT JOIN RESIDENCENAME ON RESIDENCENAME.KRESIDENCEID = RESIDENCE.KRESIDENCEID
  LEFT JOIN VSS.X000_Codedescription ON VSS.X000_Codedescription.KCODEDESCID = RESIDENCE.FRESIDENCETYPECODEID
WHERE
  (RESIDENCENAME.KSYSTEMLANGUAGECODEID = 3 AND
  RESIDENCE.STARTDATE <= Date("%CYEARB%") AND
  RESIDENCE.ENDDATE >= Date("%CYEARE%")) OR
  (RESIDENCENAME.KSYSTEMLANGUAGECODEID = 3 AND
  RESIDENCE.ENDDATE >= Date("%CYEARB%") AND
  RESIDENCE.ENDDATE <= Date("%CYEARE%")) OR
  (RESIDENCENAME.KSYSTEMLANGUAGECODEID = 3 AND
  RESIDENCE.STARTDATE >= Date("%CYEARB%") AND
  RESIDENCE.STARTDATE <= Date("%CYEARE%"))
ORDER BY
  RESIDENCEID
"""
s_sql = s_sql.replace("%CYEARB%",funcdatn.get_current_year_begin())
s_sql = s_sql.replace("%CYEARE%",funcdatn.get_current_year_end())
so_curs.execute("DROP TABLE IF EXISTS X001_Active_residence")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: X001_Active_residence")

# Data export
sr_file = "X001_active_residence"
sr_filet = sr_file
sx_path = "O:/Nwudata/Debtorstud/" + funcdatn.get_current_year() + "/"
sx_file = "Residence_001_residence_"
sx_filet = sx_file + funcdatn.get_today_date_file()

print("Export active residences..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# 03 Extract the previous year rates *******************************************

print("Extract the previous year residence rates...")

s_sql = "CREATE TABLE X002_Previous_rate AS " + """
SELECT
  TRANSINST.FRESIDENCEID,
  X001_Previous_residence.NAME,
  TRANSINST.FROOMTYPECODEID,
  VSS.X000_Codedescription.SHORT AS ROOM_TYPE,
  TRANSINST.STARTDATE,
  TRANSINST.ENDDATE,
  TRANSINST.AMOUNT,
  TRANSINST.DAILYRATE,
  TRANSINST.KTRANSINSTID
FROM
  TRANSINST
  LEFT JOIN VSS.X000_Codedescription ON VSS.X000_Codedescription.KCODEDESCID = TRANSINST.FROOMTYPECODEID
  INNER JOIN X001_Previous_residence ON X001_Previous_residence.RESIDENCEID = TRANSINST.FRESIDENCEID
WHERE
  (TRANSINST.STARTDATE >= Date("%PYEARB%") AND
  TRANSINST.STARTDATE <= Date("%PYEARE%")) OR
  (TRANSINST.ENDDATE >= Date("%PYEARB%") AND
  TRANSINST.ENDDATE <= Date("%PYEARE%")) OR
  (TRANSINST.STARTDATE <= Date("%PYEARB%") AND
  TRANSINST.ENDDATE >= Date("%PYEARE%"))
"""
s_sql = s_sql.replace("%PYEARB%",funcdatn.get_previous_year_begin())
s_sql = s_sql.replace("%PYEARE%",funcdatn.get_previous_year_end())
so_curs.execute("DROP TABLE IF EXISTS X002_Previous_rate")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: X002_Previous_rate")

# Data export
sr_file = "X002_Previous_rate"
sr_filet = sr_file
sx_path = "O:/Nwudata/Debtorstud/" + funcdatn.get_previous_year() + "/"
sx_file = "Residence_002_rate_"
sx_filet = sx_file + funcdatn.get_previous_year()

print("Export data..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# 04 Extract the current year rates ********************************************

print("Extract the current year residence rates...")

s_sql = "CREATE TABLE X002_Current_rate AS " + """
SELECT
  TRANSINST.FRESIDENCEID,
  X001_Active_residence.NAME,
  TRANSINST.FROOMTYPECODEID,
  VSS.X000_Codedescription.SHORT AS ROOM_TYPE,
  TRANSINST.STARTDATE,
  TRANSINST.ENDDATE,
  TRANSINST.AMOUNT,
  TRANSINST.DAILYRATE,
  TRANSINST.KTRANSINSTID
FROM
  TRANSINST
  LEFT JOIN VSS.X000_Codedescription ON VSS.X000_Codedescription.KCODEDESCID = TRANSINST.FROOMTYPECODEID
  INNER JOIN X001_Active_residence ON X001_Active_residence.RESIDENCEID = TRANSINST.FRESIDENCEID
WHERE
  (TRANSINST.STARTDATE >= Date("%CYEARB%") AND
  TRANSINST.STARTDATE <= Date("%CYEARE%")) OR
  (TRANSINST.ENDDATE >= Date("%CYEARB%") AND
  TRANSINST.ENDDATE <= Date("%CYEARE%")) OR
  (TRANSINST.STARTDATE <= Date("%CYEARB%") AND
  TRANSINST.ENDDATE >= Date("%CYEARE%"))
"""
s_sql = s_sql.replace("%CYEARB%",funcdatn.get_current_year_begin())
s_sql = s_sql.replace("%CYEARE%",funcdatn.get_current_year_end())
so_curs.execute("DROP TABLE IF EXISTS X002_Current_rate")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: X002_Current_rate")

# Data export
sr_file = "X002_Current_rate"
sr_filet = sr_file
sx_path = "O:/Nwudata/Debtorstud/" + funcdatn.get_current_year() + "/"
sx_file = "Residence_002_rate_"
sx_filet = sx_file + funcdatn.get_today_date_file()

print("Export data..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# 05 Extract the previous year accomodation log*********************************

print("Extract the current accommodation log...")

s_sql = "CREATE TABLE X003_Previous_accom_log AS " + """
SELECT
  ACCOMMRESIDENCY.KACCOMMRESIDENCYID,
  ACCOMMRESIDENCY.FSTUDENTBUSENTID AS STUDENT,
  ACCOMMRESIDENCY.FRESIDENCEID AS RESIDENCEID,
  X001_Previous_residence.NAME,
  ACCOMMRESIDENCY.FROOMTYPECODEID,
  VSS.X000_Codedescription.SHORT AS ROOM_TYPE,
  ACCOMMRESIDENCY.STARTDATE,
  ACCOMMRESIDENCY.ENDDATE,
  ACCOMMRESIDENCY.FACCOMMCANCELCODEID,
  VSS.X000_Codedescription1.LONG,
  ACCOMMRESIDENCY.ACCOMMCANCELREASONOTHER
FROM
  ACCOMMRESIDENCY
  INNER JOIN VSS.X000_Codedescription ON VSS.X000_Codedescription.KCODEDESCID = ACCOMMRESIDENCY.FROOMTYPECODEID
  LEFT JOIN VSS.X000_Codedescription X000_Codedescription1 ON VSS.X000_Codedescription1.KCODEDESCID =
    ACCOMMRESIDENCY.FACCOMMCANCELCODEID
  INNER JOIN X001_Previous_residence ON X001_Previous_residence.RESIDENCEID = ACCOMMRESIDENCY.FRESIDENCEID
WHERE
  ACCOMMRESIDENCY.STARTDATE >= Date("%PYEARB%") AND
  ACCOMMRESIDENCY.ENDDATE <= Date("%PYEARE%")
ORDER BY
  STUDENT,
  ACCOMMRESIDENCY.AUDITDATETIME
"""
s_sql = s_sql.replace("%PYEARB%",funcdatn.get_previous_year_begin())
s_sql = s_sql.replace("%PYEARE%",funcdatn.get_previous_year_end())
so_curs.execute("DROP TABLE IF EXISTS X003_Previous_accom_log")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: X003_Previous_accom_log")

# Data export
sr_file = "X003_Previous_accom_log"
sr_filet = sr_file
sx_path = "O:/Nwudata/Debtorstud/" + funcdatn.get_previous_year() + "/"
sx_file = "Residence_003_log_"
sx_filet = sx_file + funcdatn.get_previous_year()

print("Export data..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# 06 Extract the current year accomodation log**********************************

print("Extract the current accommodation log...")

s_sql = "CREATE TABLE X003_Current_accom_log AS " + """
SELECT
  ACCOMMRESIDENCY.KACCOMMRESIDENCYID,
  ACCOMMRESIDENCY.FSTUDENTBUSENTID AS STUDENT,
  ACCOMMRESIDENCY.FRESIDENCEID AS RESIDENCEID,
  X001_Active_residence.NAME,
  ACCOMMRESIDENCY.FROOMTYPECODEID,
  VSS.X000_Codedescription.SHORT AS ROOM_TYPE,
  ACCOMMRESIDENCY.STARTDATE,
  ACCOMMRESIDENCY.ENDDATE,
  ACCOMMRESIDENCY.FACCOMMCANCELCODEID,
  VSS.X000_Codedescription1.LONG,
  ACCOMMRESIDENCY.ACCOMMCANCELREASONOTHER
FROM
  ACCOMMRESIDENCY
  INNER JOIN VSS.X000_Codedescription ON VSS.X000_Codedescription.KCODEDESCID = ACCOMMRESIDENCY.FROOMTYPECODEID
  LEFT JOIN VSS.X000_Codedescription X000_Codedescription1 ON VSS.X000_Codedescription1.KCODEDESCID =
    ACCOMMRESIDENCY.FACCOMMCANCELCODEID
  INNER JOIN X001_Active_residence ON X001_Active_residence.RESIDENCEID = ACCOMMRESIDENCY.FRESIDENCEID
WHERE
  ACCOMMRESIDENCY.STARTDATE >= Date("%CYEARB%") AND
  ACCOMMRESIDENCY.ENDDATE <= Date("%CYEARE%")
ORDER BY
  STUDENT,
  ACCOMMRESIDENCY.AUDITDATETIME
"""
s_sql = s_sql.replace("%CYEARB%",funcdatn.get_current_year_begin())
s_sql = s_sql.replace("%CYEARE%",funcdatn.get_current_year_end())
so_curs.execute("DROP TABLE IF EXISTS X003_Current_accom_log")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: X003_Current_accom_log")

# Data export
sr_file = "X003_Current_accom_log"
sr_filet = sr_file
sx_path = "O:/Nwudata/Debtorstud/" + funcdatn.get_current_year() + "/"
sx_file = "Residence_003_log_"
sx_filet = sx_file + funcdatn.get_today_date_file()

print("Export data..." + sx_path + sx_filet)

# Read the header data
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

# Write the data
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# Close the table connection ***************************************************
so_conn.close()
funcfile.writelog("COMPLETED")
