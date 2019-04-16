""" C300_TEST_STUDENT_GENERAL **************************************************
*** Script to vss student general items
*** Albert J van Rensburg (21162395)
*** 25 Jun 2018
*****************************************************************************"""

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
funcfile.writelog("SCRIPT: C300_TEST_STUDENT_GENERAL_DEV")
funcfile.writelog("----------------------------------=--")
print("-------------------------")
print("C300_TEST_STUDENT_GENERAL")
print("-------------------------")
ilog_severity = 1

# Declare variables
so_path = "W:/Vss_general/" #Source database path
re_path = "R:/Vss/" #Results
ed_path = "S:/_external_data/"
so_file = "Vss_general.sqlite" #Source database
s_sql = "" #SQL statements
l_mail = True
l_export = True

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("%t OPEN DATABASE: VSS_GENERAL")

so_curs.execute("ATTACH DATABASE 'W:/vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")

""" ****************************************************************************
BEGIN
*****************************************************************************"""

# Join the tran and party data *********************************************
print("IDNo list join the vss tran and party data...")
sr_file = "X001bb_join_tran_vss"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  VSS.X000_party.IDNO,
  X001aa_impo_vsstran.STUDENT,
  VSS.X000_Party.FULL_NAME AS NAME,
  X001aa_impo_vsstran.YEAR,
  X001aa_impo_vsstran.CAMPUS,
  Trim(VSS.X000_Party.FIRSTNAMES) AS FIRSTNAME,
  VSS.X000_Party.INITIALS,
  VSS.X000_Party.SURNAME,
  VSS.X000_Party.TITLE,
  VSS.X000_Party.DATEOFBIRTH,
  VSS.X000_Party.GENDER,
  VSS.X000_Party.NATIONALITY,
  VSS.X000_Party.POPULATION,
  VSS.X000_Party.RACE,
  VSS.X000_Party.FAUDITUSERCODE AS PARTY_AUDITDATETIME,
  VSS.X000_Party.AUDITDATETIME AS PARTY_AUDITUSERCODE
FROM
  X001aa_impo_vsstran
  INNER JOIN VSS.X000_Party ON VSS.X000_Party.KBUSINESSENTITYID = X001aa_impo_vsstran.STUDENT
ORDER BY
  X001aa_impo_vsstran.STUDENT
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
 
""" ****************************************************************************
END
*****************************************************************************"""

# Close the table connection ***************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C300_TEST_STUDENT_GENERAL_DEV")
