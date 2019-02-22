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
import funcmysql

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
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs_vss_studdeb/Kfs_vss_studdeb_prev.sqlite' AS 'PREV'")
funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")

# Open the MYSQL DESTINATION table
s_database = "Web_ia_nwu"
ms_cnxn = funcmysql.mysql_open(s_database)
ms_curs = ms_cnxn.cursor()
funcfile.writelog("%t OPEN MYSQL DATABASE: " + s_database)

"""*****************************************************************************
BEGIN
*****************************************************************************"""

# CALCULATE OPENING BALANCES ***************************************************
print("Sum vss student opening balances per campus...")
sr_file = "X002da_vss_student_balance_open"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X002ab_vss_transort.CAMPUS_VSS AS CAMPUS,
  X002ab_vss_transort.STUDENT_VSS AS STUDENT,  
  0.00 AS BAL_DT,
  0.00 AS BAL_CT,
  Round(Total(X002ab_vss_transort.AMOUNT_VSS),2) AS BALANCE
FROM
  X002ab_vss_transort
WHERE
  X002ab_vss_transort.MONTH_VSS = '00'
GROUP BY
  X002ab_vss_transort.STUDENT_VSS,
  X002ab_vss_transort.CAMPUS_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)    
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Add column vss debit amount
print("Add column vss dt amount...")
so_curs.execute("UPDATE X002da_vss_student_balance_open " + """
                SET BAL_DT = 
                CASE
                   WHEN BALANCE > 0 THEN BALANCE
                   ELSE 0.00
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: Vss debit amount")
# Add column vss credit amount
print("Add column vss ct amount...")
so_curs.execute("UPDATE X002da_vss_student_balance_open " + """
                SET BAL_CT = 
                CASE
                   WHEN BALANCE < 0 THEN BALANCE
                   ELSE 0.00
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: Vss credit amount")

# JOIN PREVIOUS BALANCE AND CURRENT OPENING BALANCE ****************************
print("Join previous balance and current opening balance...")
sr_file = "X002dc_vss_prevbal_curopen"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  PREV.X002da_vss_student_balance.CAMPUS,
  PREV.X002da_vss_student_balance.STUDENT,
  round(PREV.X002da_vss_student_balance.BALANCE,2) AS BAL_PREV,
  round(X002da_vss_student_balance_open.BALANCE,2) AS BAL_OPEN,
  0.00 AS DIFF_BAL
FROM
  PREV.X002da_vss_student_balance
  LEFT JOIN X002da_vss_student_balance_open ON X002da_vss_student_balance_open.CAMPUS =
  PREV.X002da_vss_student_balance.CAMPUS AND X002da_vss_student_balance_open.STUDENT = PREV.X002da_vss_student_balance.STUDENT
WHERE
  PREV.X002da_vss_student_balance.BALANCE <> 0.00
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)    
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Add column vss debit amount
print("Correct the open balance column id null...")
so_curs.execute("UPDATE X002dc_vss_prevbal_curopen " + """
                SET BAL_OPEN =
                CASE
                  WHEN BAL_OPEN IS NULL THEN 0.00
                  ELSE BAL_OPEN
                END
                ;""")
so_conn.commit()
# Add column vss debit amount
print("Add column vss dt amount...")
so_curs.execute("UPDATE X002dc_vss_prevbal_curopen " + """
                SET DIFF_BAL = round(BAL_OPEN - BAL_PREV,2)
                ;""")
so_conn.commit()

# SELECT STUDENTS WHERE OPENING BALANCE DIFFER FROM CLOSING BALANCE ************
print("Select students where closing and opening balances differ...")
sr_file = "X002dd_vss_closing_open_differ"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X002dc_vss_prevbal_curopen.CAMPUS,
  X002dc_vss_prevbal_curopen.STUDENT,
  X002dc_vss_prevbal_curopen.BAL_PREV,
  X002dc_vss_prevbal_curopen.BAL_OPEN,
  X002dc_vss_prevbal_curopen.DIFF_BAL
FROM
  X002dc_vss_prevbal_curopen
WHERE
  X002dc_vss_prevbal_curopen.DIFF_BAL <> 0
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)    
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

"""*****************************************************************************
END
*****************************************************************************"""

# Close the table connection ***************************************************
so_conn.commit()
so_conn.close()
ms_cnxn.commit()
ms_cnxn.close()

# Close the log writer *********************************************************
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C200_REPORT_STUDDEB_RECON_DEV")
