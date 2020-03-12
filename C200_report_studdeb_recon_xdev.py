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
from _my_modules import funcsms
from _my_modules import functest

# OPEN THE LOG
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C200_REPORT_STUDDEB_RECON_DEV")
funcfile.writelog("-------------------------------------")
print("-------------------------")
print("C200_REPORT_STUDDEB_RECON")
print("-------------------------")

# DECLARE VARIABLES
gl_month = '03'
s_period="curr"
s_yyyy="2020"
s_year: str = s_yyyy
so_path = "W:/Kfs_vss_studdeb/"  # Source database path
if s_period == "curr":
    s_year = funcdate.cur_year()
    so_file = "Kfs_vss_studdeb.sqlite"  # Source database
    s_kfs = "KFSCURR"
    s_vss = "VSSCURR"
elif s_period == "prev":
    s_year = funcdate.prev_year()
    so_file = "Kfs_vss_studdeb_prev.sqlite"  # Source database
    s_kfs = "KFSPREV"
    s_vss = "VSSPREV"
else:
    so_file = "Kfs_vss_studdeb_" + s_year + ".sqlite"  # Source database
    s_kfs = ""
    s_vss = ""
re_path = "R:/Debtorstud/"  # Results
ed_path = "S:/_external_data/"  # External data
s_sql = ""  # SQL statements
l_mess = False
l_mail = False
l_export = True
l_record = False
l_vacuum = False
s_burs_code = '042z052z381z500'  # Current bursary transaction codes

# OPEN THE SOURCE
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
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

"""*****************************************************************************
END
*****************************************************************************"""

# CLOSE THE CONNECTION
so_conn.commit()
so_conn.close()

# CLOSE THE LOG
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C200_REPORT_STUDDEB_RECON_DEV")
