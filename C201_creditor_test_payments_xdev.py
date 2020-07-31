""" Script to build kfs creditor payment tests *********************************
Created on: 16 Apr 2019
Author: Albert J v Rensburg (NWU21162395)
*****************************************************************************"""

# IMPORT PYTHON MODULES
import csv
import sqlite3
import datetime

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcfile
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcsms
from _my_modules import funcstat
from _my_modules import funcsys
from _my_modules import functest

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# DECLARE VARIABLES
l_debug: bool = True
so_path: str = "W:/Kfs/"  # Source database path
so_file: str = "Kfs_test_creditor.sqlite"  # Source database
re_path: str = "R:/Kfs/"  # Results path
ed_path: str = "S:/_external_data/"  # External data path
l_export: bool = True
# l_mail: bool = funcconf.l_mail_project
l_mail: bool = False
# l_mess: bool = funcconf.l_mess_project
l_mess: bool = True
l_record: bool = False

# OPEN THE SCRIPT LOG FILE
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C201_CREDITOR_TEST_DEV")
funcfile.writelog("------------------------------")
if l_debug:
    print("----------------------")
    print("C201_CREDITOR_TEST_DEV")
    print("----------------------")

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
funcfile.writelog("OPEN THE DATABASES")
if l_debug:
    print("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs_prev.sqlite' AS 'KFSPREV'")
funcfile.writelog("%t ATTACH DATABASE: KFS_PREV.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
funcfile.writelog("BEGIN OF SCRIPT")
if l_debug:
    print("BEGIN OF SCRIPT")

"""*****************************************************************************
PAYEE FISCAL OFFICER SAME
*****************************************************************************"""
funcfile.writelog("PAYEE FISCAL OFFICER SAME")
if l_debug:
    print("PAYEE FISCAL OFFICER SAME")

# DECLARE TEST VARIABLES
i_finding_after: int = 0
s_description = "Payee fiscal officer same person"
s_file_prefix: str = "X001f"
s_file_name: str = "payee_fiscal_officer_same"
s_finding: str = "PAYEE FISCAL OFFICER SAME"
s_report_file: str = "201_reported.txt"

# IDENTIFY PAYMENTS WHERE THE PAYEE IS ALSO THE FISCAL OFFICER
if l_debug:
    print("Identify payment transactions...")
sr_file: str = s_file_prefix + "a_" + s_file_name
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "Create Table " + sr_file + " As " + """
Select
    LIST.*
From
    KFSCURR.X001ad_Report_payments_accroute LIST
Where
    LIST.VENDOR_ID = LIST.ACCT_FSC_OFC_UID
Order By
    VENDOR_ID,
    PMT_DT    
;"""
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
if l_debug:
    so_conn.commit()

# IDENTIFY FINDINGS
if l_debug:
    print("Identify findings...")
sr_file = s_file_prefix + "b_finding"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    'NWU' As ORG,
    FIND.VENDOR_ID,
    FIND.PAYEE_NAME,
    FIND.TRAN_COUNT,
    FIND.AMOUNT_TOTAL,
    FIND.TRAN_VALUE
From
    %FILEP%%FILEN% FIND
;"""
s_sql = s_sql.replace("%FILEP%", s_file_prefix)
s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
if l_debug:
    so_conn.commit()

"""*****************************************************************************
END OF SCRIPT
*****************************************************************************"""
funcfile.writelog("END OF SCRIPT")
if l_debug:
    print("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("---------------------------------")
funcfile.writelog("COMPLETED: C201_CREDITOR_TEST_DEV")
