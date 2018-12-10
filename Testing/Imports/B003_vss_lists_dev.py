# Import python modules
import datetime
import sqlite3
import sys

# Add own module path
sys.path.append('S:/_my_modules')
#print(sys.path)

# Import own modules
import funcdate
import funccsv
import funcfile

# Open the script log file ******************************************************

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B003_VSS_LISTS")
funcfile.writelog("----------------------")
print("--------------")
print("B003_VSS_LISTS")
print("--------------")
ilog_severity = 1

# Declare variables
so_path = "W:/" #Source database path
so_file = "Vss.sqlite" #Source database
s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("%t OPEN DATABASE: VSS.SQLITE")

# Build test code **************************************************************

print("Build module present organization...")

s_sql = "CREATE VIEW X002ba_Module_present_org AS " + """
SELECT
  MODULEPRESENTINGOU.KPRESENTINGOUID,
  MODULEPRESENTINGOU.STARTDATE,
  MODULEPRESENTINGOU.ENDDATE,
  MODULEPRESENTINGOU.FBUSINESSENTITYID,
  X000_Orgunitinstance.FSITEORGUNITNUMBER,
  X000_Orgunitinstance.ORGUNIT_TYPE,
  X000_Orgunitinstance.ORGUNIT_NAME,
  MODULEPRESENTINGOU.FMODULEAPID,
  X002aa_Module.COURSECODE,
  X002aa_Module.COURSELEVEL,
  X002aa_Module.COURSEMODULE,
  MODULEPRESENTINGOU.FCOURSEGROUPCODEID,
  X000_Codedescription_coursegroup.LONG AS NAME_GROUP,
  X000_Codedescription_coursegroup.LANK AS NAAM_GROEP,
  MODULEPRESENTINGOU.ISEXAMMODULE,
  MODULEPRESENTINGOU.LOCKSTAMP,
  MODULEPRESENTINGOU.AUDITDATETIME,
  MODULEPRESENTINGOU.FAUDITSYSTEMFUNCTIONID,
  MODULEPRESENTINGOU.FAUDITUSERCODE
FROM
  MODULEPRESENTINGOU
  LEFT JOIN X000_Orgunitinstance ON X000_Orgunitinstance.KBUSINESSENTITYID = MODULEPRESENTINGOU.FBUSINESSENTITYID
  LEFT JOIN X002aa_Module ON X002aa_Module.MODULE_ID = MODULEPRESENTINGOU.FMODULEAPID
  LEFT JOIN X000_Codedescription X000_Codedescription_coursegroup ON X000_Codedescription_coursegroup.KCODEDESCID =
    MODULEPRESENTINGOU.FCOURSEGROUPCODEID
"""
so_curs.execute("DROP VIEW IF EXISTS X002ba_Module_present_org")
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD VIEW: X002ba_Module_present_org")

# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("---------")
funcfile.writelog("COMPLETED")
