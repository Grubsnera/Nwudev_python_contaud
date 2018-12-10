""" C201_TETS_STUDDEB_DEV ******************************************************
***
*** Script to test various aspects of student debtors
***
*** Albert v Rensburg (21162395)
*** Created: 21 Jul 2018
***
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
funcfile.writelog("SCRIPT: C201_TEST_STUDDEB_DEV")
funcfile.writelog("-----------------------------")
print("---------------------")
print("C201_TEST_STUDDEB_DEV")
print("---------------------")
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

# Attach other data files
"""
so_curs.execute("ATTACH DATABASE 'W:/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS_SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")
"""

# Development script ***********************************************************










"""*************************************************************************
***
*** TEST IMBALANCE 370 AND 371
***
***   X050aa Extract type 370 transactions   
***   X050ab Extract type 371 transactions
***   X050ac Join 370 and 371 transactions
***
*************************************************************************"""

print("---------- PREPARE TRANSACTIONS ----------")
funcfile.writelog("%t ---------- PREPARE TRANSACTIONS ----------")

# *** TEST IMBALANCE 370 AND 371 Extract 370 type transactions *****************
print("Extract type 370 transactions...")
sr_file = "X050aa_extract_370"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  'NWU' AS ORG_370,
  X002ab_vss_transort.STUDENT_VSS AS STUDENT_370,
  X002ab_vss_transort.CAMPUS_VSS AS CAMPUS_370,
  X002ab_vss_transort.TRANSCODE_VSS AS TRANCODE_370,
  X002ab_vss_transort.MONTH_VSS AS MONTH_370,
  X002ab_vss_transort.TRANSDATE_VSS AS TRANDATE_370,
  X002ab_vss_transort.AMOUNT_VSS AS AMOUNT_370,  
  Abs(X002ab_vss_transort.AMOUNT_VSS) AS ABSAMOUNT_370,  
  Strftime('%H:%M:%S',X002ab_vss_transort.TRANSDATETIME) AS TRANTIME_370,
  X002ab_vss_transort.DESCRIPTION_E AS DESC_370,
  X002ab_vss_transort.TRANUSER AS USER_370
FROM
  X002ab_vss_transort
WHERE
  X002ab_vss_transort.TRANSCODE_VSS = '370'
ORDER BY
  X002ab_vss_transort.MONTH_VSS,
  X002ab_vss_transort.STUDENT_VSS,
  X002ab_vss_transort.AMOUNT_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# *** TEST IMBALANCE 370 AND 371 Extract 371 type transactions *****************
print("Extract type 371 transactions...")
sr_file = "X050ab_extract_371"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  'NWU' AS ORG_371,
  X002ab_vss_transort.STUDENT_VSS AS STUDENT_371,
  X002ab_vss_transort.CAMPUS_VSS AS CAMPUS_371,
  X002ab_vss_transort.TRANSCODE_VSS AS TRANCODE_371,
  X002ab_vss_transort.MONTH_VSS AS MONTH_371,
  X002ab_vss_transort.TRANSDATE_VSS AS TRANDATE_371,
  X002ab_vss_transort.AMOUNT_VSS AS AMOUNT_371,  
  Abs(X002ab_vss_transort.AMOUNT_VSS) AS ABSAMOUNT_371,  
  Strftime('%H:%M:%S',X002ab_vss_transort.TRANSDATETIME) AS TRANTIME_371,
  X002ab_vss_transort.DESCRIPTION_E AS DESC_371,
  X002ab_vss_transort.TRANUSER AS USER_371
FROM
  X002ab_vss_transort
WHERE
  X002ab_vss_transort.TRANSCODE_VSS = '371'
ORDER BY
  X002ab_vss_transort.MONTH_VSS,
  X002ab_vss_transort.STUDENT_VSS,
  X002ab_vss_transort.AMOUNT_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)



so_curs.execute("DROP TABLE IF EXISTS X050ac_summ_370")
so_curs.execute("DROP TABLE IF EXISTS X050ad_summ_371")
so_curs.execute("DROP TABLE IF EXISTS X050ba_test_in370_no371")
so_curs.execute("DROP TABLE IF EXISTS X050ca_test_in371_no370")


# Close the table connection ***************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("--------------------------------")
funcfile.writelog("COMPLETED: C201_TEST_STUDDEB_DEV")
