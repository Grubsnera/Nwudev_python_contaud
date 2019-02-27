""" C200_REPORT_STUDDEB_RECON **************************************************
***
*** Script to compare VSS and GL student transactions
***
*** Albert J van Rensburg (21162395)
*** 26 Jun 2018
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
funcfile.writelog("SCRIPT: C200_REPORT_STUDDEB_RECON_PREV")
funcfile.writelog("--------------------------------------")
print("------------------------------")
print("C200_REPORT_STUDDEB_RECON_PREV")
print("------------------------------")
ilog_severity = 1

# Declare variables
so_path = "W:/Kfs_vss_studdeb/" #Source database path
re_path = "R:/Debtorstud/" #Results
ed_path = "S:/_external_data/" #External data
so_file = "Kfs_vss_studdeb_prev.sqlite" #Source database
s_sql = "" #SQL statements
l_mail = True
l_export = True

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN DATABASE: Kfs_vss_studdeb_prev")

# Attach data sources
so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")

"""*****************************************************************************
*****************************************************************************"""

# EXTRACT PRE DATED TRANSACTIONS ***********************************************
print("Extract pre dated transactions...")
sr_file = "X002fa_vss_tran_predate"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X002ab_vss_transort.STUDENT_VSS,
  X002ab_vss_transort.CAMPUS_VSS,
  X002ab_vss_transort.TRANSCODE_VSS,
  X002ab_vss_transort.MONTH_VSS,
  X002ab_vss_transort.TRANSDATE_VSS,
  X002ab_vss_transort.TRANSDATETIME,
  X002ab_vss_transort.AMOUNT_VSS,
  X002ab_vss_transort.DESCRIPTION_E,
  X002ab_vss_transort.POSTDATEDTRANSDATE
FROM
  X002ab_vss_transort
WHERE
  Strftime('%Y',TRANSDATE_VSS) - Strftime('%Y',X002ab_vss_transort.POSTDATEDTRANSDATE) = 1 AND
  Strftime('%Y',TRANSDATE_VSS) = '%PYEAR%'
;"""
s_sql = s_sql.replace("%PYEAR%",funcdate.prev_year())
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# SUMM PRE DATED TRANSACTIONS *************************************************
print("Summ pre dated transactions...")
sr_file = "X002fb_vss_tran_predate_summ"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X002fa_vss_tran_predate.CAMPUS_VSS,
  X002fa_vss_tran_predate.TRANSCODE_VSS,
  X002fa_vss_tran_predate.DESCRIPTION_E,
  Total(X002fa_vss_tran_predate.AMOUNT_VSS) AS Total_AMOUNT_VSS
FROM
  X002fa_vss_tran_predate
GROUP BY
  X002fa_vss_tran_predate.CAMPUS_VSS,
  X002fa_vss_tran_predate.TRANSCODE_VSS,
  X002fa_vss_tran_predate.DESCRIPTION_E
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# EXTRACT POST DATED TRANSACTIONS **********************************************
print("Extract post dated transactions...")
sr_file = "X002ga_vss_tran_postdate"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X002ab_vss_transort.STUDENT_VSS,
  X002ab_vss_transort.CAMPUS_VSS,
  X002ab_vss_transort.TRANSCODE_VSS,
  X002ab_vss_transort.MONTH_VSS,
  X002ab_vss_transort.TRANSDATE_VSS,
  X002ab_vss_transort.TRANSDATETIME,
  X002ab_vss_transort.AMOUNT_VSS,
  X002ab_vss_transort.DESCRIPTION_E,
  X002ab_vss_transort.POSTDATEDTRANSDATE
FROM
  X002ab_vss_transort
WHERE
  Strftime('%Y',TRANSDATE_VSS) - Strftime('%Y',X002ab_vss_transort.POSTDATEDTRANSDATE) = 1 AND
  Strftime('%Y',POSTDATEDTRANSDATE) = '%PYEAR%'
;"""
s_sql = s_sql.replace("%PYEAR%",funcdate.prev_year())
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# SUMM POST DATED TRANSACTIONS *************************************************
print("Summ post dated transactions...")
sr_file = "X002gb_vss_tran_postdate_summ"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X002ga_vss_tran_postdate.CAMPUS_VSS,
  X002ga_vss_tran_postdate.TRANSCODE_VSS,
  X002ga_vss_tran_postdate.DESCRIPTION_E,
  Total(X002ga_vss_tran_postdate.AMOUNT_VSS) AS Total_AMOUNT_VSS
FROM
  X002ga_vss_tran_postdate
GROUP BY
  X002ga_vss_tran_postdate.CAMPUS_VSS,
  X002ga_vss_tran_postdate.TRANSCODE_VSS,
  X002ga_vss_tran_postdate.DESCRIPTION_E
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

"""*****************************************************************************
END
*****************************************************************************"""

# Close the table connection ***************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("------------------------------------")
funcfile.writelog("COMPLETED: C200_REPORT_STUDDEB_RECON")


