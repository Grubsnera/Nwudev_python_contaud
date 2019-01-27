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
so_path = "W:/Kfs_vss_studdeb/" #Source database path
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


#so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
#funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
#so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
#funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
#so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
#funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")

# Development script ***********************************************************

gl_month = "01"

# Calculate the running vss balance ********************************************
print("Calculate the running vss balance...")
sr_file = "X002ce_vss_balmonth_calc_runbal"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  a.CAMPUS_VSS,
  a.MONTH_VSS,
  a.AMOUNT_DT,
  a.AMOUNT_CT,
  a.AMOUNT,
  TOTAL(b.AMOUNT) RUNBAL
FROM
  X002cb_vss_balmonth a,
  X002cb_vss_balmonth b
WHERE
  (a.CAMPUS_VSS = b.CAMPUS_VSS AND
  a.MONTH_VSS >= b.MONTH_VSS)
GROUP BY
  a.CAMPUS_VSS,
  a.MONTH_VSS
ORDER BY
  a.CAMPUS_VSS,
  a.MONTH_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# Calculate the running gl balance ********************************************
print("Calculate the running gl balance...")
sr_file = "X001ce_gl_balmonth_calc_runbal"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  a.CAMPUS,
  a.MONTH,
  a.BALANCE,
  TOTAL(b.BALANCE) RUNBAL
FROM
  X001cb_gl_balmonth a,
  X001cb_gl_balmonth b
WHERE
  (a.CAMPUS = b.CAMPUS AND
  a.MONTH >= b.MONTH)
GROUP BY
  a.CAMPUS,
  a.MONTH
ORDER BY
  a.CAMPUS,
  a.MONTH
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# Join vss gl monthly account totals ******************************************
print("Join vss and gl monthly totals...")
sr_file = "X002ea_vss_gl_balance_month"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  UPPER(SUBSTR(X002ce_vss_balmonth_calc_runbal.CAMPUS_VSS,1,3))||TRIM(X002ce_vss_balmonth_calc_runbal.MONTH_VSS) AS ROWID,
  'NWU' AS ORG,
  X002ce_vss_balmonth_calc_runbal.CAMPUS_VSS AS CAMPUS,
  X002ce_vss_balmonth_calc_runbal.MONTH_VSS AS MONTH,
  X002ce_vss_balmonth_calc_runbal.AMOUNT_DT AS VSS_TRAN_DT,
  X002ce_vss_balmonth_calc_runbal.AMOUNT_CT AS VSS_TRAN_CT,
  X002ce_vss_balmonth_calc_runbal.AMOUNT AS VSS_TRAN,
  X002ce_vss_balmonth_calc_runbal.RUNBAL AS VSS_RUNBAL,
  X001ce_gl_balmonth_calc_runbal.BALANCE AS GL_TRAN,
  X001ce_gl_balmonth_calc_runbal.RUNBAL AS GL_RUNBAL
FROM
  X002ce_vss_balmonth_calc_runbal
  LEFT JOIN X001ce_gl_balmonth_calc_runbal ON X001ce_gl_balmonth_calc_runbal.CAMPUS = X002ce_vss_balmonth_calc_runbal.CAMPUS_VSS AND
    X001ce_gl_balmonth_calc_runbal.MONTH = X002ce_vss_balmonth_calc_runbal.MONTH_VSS
WHERE
  X002ce_vss_balmonth_calc_runbal.MONTH_VSS <= '%PMONTH%'
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
s_sql = s_sql.replace("%PMONTH%",gl_month) 
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# Add columns ******************************************************************
# Reconciling amount
print("Add column vss gl difference...")
so_curs.execute("ALTER TABLE X002ea_vss_gl_balance_month ADD COLUMN DIFF REAL;")
so_curs.execute("UPDATE X002ea_vss_gl_balance_month SET DIFF = VSS_RUNBAL - GL_RUNBAL;")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: DIFF")

# Calculate the running recon amount *******************************************
print("Calculate the running recon amount...")
sr_file = "X002ea_vss_gl_balance_month_move"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  a.ROWID,
  a.ORG,
  a.CAMPUS,
  a.MONTH,
  a.VSS_TRAN_DT,
  a.VSS_TRAN_CT,
  a.VSS_TRAN,
  a.VSS_RUNBAL,
  a.GL_TRAN,
  a.GL_RUNBAL,
  a.DIFF,
  a.DIFF - b.DIFF AS MOVE
FROM
  X002ea_vss_gl_balance_month a,
  X002ea_vss_gl_balance_month b
WHERE
  (a.CAMPUS = b.CAMPUS AND
  a.MONTH > b.MONTH) OR
  (a.CAMPUS = b.CAMPUS AND
  b.MONTH = "00")
  
GROUP BY
  a.CAMPUS,
  a.MONTH
ORDER BY
  a.CAMPUS,
  a.MONTH
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Export the data
print("Export vss campus balances per month...")
sr_filet = sr_file
sx_path = re_path + funcdate.cur_year() + "/"
sx_file = "Debtor_000_vss_gl_summmonth_"
sx_filet = sx_file + funcdate.prev_monthendfile()
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
#funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)









# Close the table connection ***************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C200_REPORT_STUDDEB_RECON_DEV")
