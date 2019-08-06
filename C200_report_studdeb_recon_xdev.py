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
gl_month = '06'
l_mail = False
l_export = False
l_record = False
s_burs_code = '042z052z381z500'

# OPEN THE SOURCE
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN DATABASE: Kfs_vss_studdeb")

# ATTACH DATA FILES
so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs_vss_studdeb/Kfs_vss_studdeb_prev.sqlite' AS 'PREV'")
funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")

"""*****************************************************************************
BEGIN
*****************************************************************************"""

# BUILD A LIST OF STUDENT BALANCES PER CAMPUS
print("Build balance per campus list...")
sr_file = "X020_Balance_per_campus"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    TRAN.STUDENT_VSS,
    TRAN.CAMPUS_VSS,
    Round(Total(TRAN.AMOUNT_VSS),2) As BALANCE
From
    X002ab_vss_transort TRAN
Group By
    TRAN.STUDENT_VSS,
    TRAN.CAMPUS_VSS
Having
    Round(Total(TRAN.AMOUNT_VSS),2) <> 0.00    
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# COUNT THE NUMBER OF BALANCES PER CAMPUS
print("Build count per campus list...")
sr_file = "X020_Count_per_campus"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    STUD.STUDENT_VSS,
    Count(STUD.CAMPUS_VSS) As COUNT
From
    X020_Balance_per_campus STUD
Group By
    STUD.STUDENT_VSS
Having
    Count(STUD.CAMPUS_VSS) > 1
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# LIST THE STUDENTS
print("Build students list...")
sr_file = "X020_Students"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    COUNT.STUDENT_VSS,
    COUNT.COUNT,
    CAMP.CAMPUS_VSS,
    CAMP.BALANCE
From
    X020_Count_per_campus COUNT Inner Join
    X020_Balance_per_campus CAMP On CAMP.STUDENT_VSS = COUNT.STUDENT_VSS
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
