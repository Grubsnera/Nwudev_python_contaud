"""
Script to build GL Student debtor control account reports
Created on: 13 Mar 2018
"""

# IMPORT PYTHON MODULES
import sqlite3
import csv

# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funcdate
from _my_modules import funcsys
from _my_modules import funccsv
from _my_modules import funcmysql

# OPEN THE LOG
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C200_REPORT_STUDDEB_RECON_DEV")
funcfile.writelog("-------------------------------------")
print("-------------------------")
print("C200_REPORT_STUDDEB_RECON")
print("-------------------------")

# DECLARE VARIABLES
so_path = "W:/Kfs_vss_studdeb/"  # Source database path
so_file = "Kfs_vss_studdeb.sqlite"  # Source database
re_path = "R:/Debtorstud/"  # Results
ed_path = "S:/_external_data/"  # External data
gl_month = '09'
l_mail = False
l_export = False
l_record = False
s_burs_code = '042z052z381z500'

# OPEN THE SOURCE
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# Attach data sources
so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs_prev.sqlite' AS 'KFSPREV'")
funcfile.writelog("%t ATTACH DATABASE: KFS_PREV.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss_curr.sqlite' AS 'VSSCURR'")
funcfile.writelog("%t ATTACH DATABASE: VSS_CURR.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss_prev.sqlite' AS 'VSSPREV'")
funcfile.writelog("%t ATTACH DATABASE: VSS_PREV.SQLITE")

"""*****************************************************************************
BEGIN
*****************************************************************************"""

# DETERMINE BALANCE CHANGE TYPE
print("Determine blanace change type...")
sr_file = "X002de_vss_differ_type"
s_sql = "Create Table " + sr_file + " As " + """
Select
    TYPE.STUDENT,
    Count(TYPE.BAL_CLOS) As COUNT,
    Total(TYPE.BAL_OPEN) As TOTAL_BAL_OPEN,
    Total(TYPE.DIFF_BAL) As TOTAL_DIFF_BAL
From
    X002dd_vss_closing_open_differ TYPE
Group By
    TYPE.STUDENT
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# JOIN DIFFERENCES AND TYPES
print("Join differences and types...")
sr_file = "X002df_vss_differ_join"
s_sql = "Create Table " + sr_file + " As " + """
Select
    DIFF.STUDENT,
    DIFF.CAMPUS,
    DIFF.BAL_CLOS,
    DIFF.BAL_OPEN,
    DIFF.DIFF_BAL,
    TYPE.COUNT,
    TYPE.TOTAL_BAL_OPEN,
    TYPE.TOTAL_DIFF_BAL
From
    X002dd_vss_closing_open_differ DIFF Left Join
    X002de_vss_differ_type TYPE On TYPE.STUDENT = DIFF.STUDENT
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# ISOLATE THE ACCOUNTS WHERE CLOSE / OPEN BALANCES DIFFER
print("Isolate close open balances...")
sr_file = "X002dg_vss_differ_close_open_differ"
s_sql = "Create Table " + sr_file + " As " + """
Select
    *
From
    X002df_vss_differ_join
Where
    X002df_vss_differ_join.COUNT = 1
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# ISOLATE THE ACCOUNTS CAMPUS DIFFER WITH ZERO BALANCE
print("Isolate campus differ zero balance...")
sr_file = "X002dh_vss_differ_campus_differ_zerobal"
s_sql = "Create Table " + sr_file + " As " + """
Select
    *
From
    X002df_vss_differ_join
Where
    X002df_vss_differ_join.COUNT != 1 And
    X002df_vss_differ_join.TOTAL_BAL_OPEN = 0
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# ISOLATE THE ACCOUNTS CAMPUS DIFFER WITH BALANCE
print("Isolate campus differ balance...")
sr_file = "X002di_vss_differ_campus_differ_bal"
s_sql = "Create Table " + sr_file + " As " + """
Select
    *
From
    X002df_vss_differ_join
Where
    X002df_vss_differ_join.COUNT != 1 And
    X002df_vss_differ_join.TOTAL_BAL_OPEN != 0
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

"""*****************************************************************************
END
*****************************************************************************"""

# CLOSE THE CONNECTION
so_conn.commit()
so_conn.close()

# CLOSE THE LOG
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C200_REPORT_STUDDEB_RECON_DEV")
