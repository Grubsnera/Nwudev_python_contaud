""" Script to build kfs creditor payment tests *********************************
Created on: 16 Apr 2019
Author: Albert J v Rensburg (NWU21162395)
*****************************************************************************"""

# IMPORT PYTHON MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcsys
from _my_modules import functest

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# OPEN THE SCRIPT LOG FILE
print("----------------------")
print("C201_CREDITOR_TEST_DEV")
print("----------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C201_CREDITOR_TEST_DEV")
funcfile.writelog("------------------------------")
ilog_severity = 1

# DECLARE VARIABLES
so_path = "W:/Kfs/"  # Source database path
so_file = "Kfs_test_creditor.sqlite"  # Source database
re_path = "R:/Kfs/"  # Results path
ed_path = "S:/_external_data/"  # External data path
l_export = True
l_mail = False
l_record = False

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

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
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

# TODO Develope script to test small split payments

# IDENTIFY AND SUMMARIZE SMALL PAYMENTS
# Payments between R4000 and R5000
print("Identify and summarize small payments...")
sr_file: str = "X001ba_Small_split_pay_summ"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "Create Table " + sr_file + " As " + """
Select
    SUMM.INIT_EMP_NO,
    SUMM.INIT_EMP_NAME,
    SUMM.VENDOR_ID,
    SUMM.VENDOR_NAME,
    Count(SUMM.EDOC) As Count_EDOC
From
    KFSCURR.X001ab_Report_payments_initiate SUMM
Where
    SUMM.INIT_EMP_NO <> "" And
    SUMM.NET_PMT_AMT >= 4000 And
    SUMM.NET_PMT_AMT <= 5000
Group By
    SUMM.INIT_EMP_NO,
    SUMM.VENDOR_ID,
    SUMM.NET_PMT_AMT
Having
    Count(SUMM.EDOC) > 1
;"""
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IDENTIFY PAYMENT TRANSACTIONS
print("Identify payment transactions...")
sr_file: str = "X001ba_Small_split_pay_list"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "Create Table " + sr_file + " As " + """
Select
    LIST.*
From
    KFSCURR.X001ab_Report_payments_initiate LIST Inner Join
    X001ba_Small_split_pay_summ SUMM On SUMM.INIT_EMP_NO = LIST.INIT_EMP_NO And SUMM.VENDOR_ID = LIST.VENDOR_ID
Order By
    LIST.INIT_EMP_NO,
    LIST.VENDOR_ID,
    LIST.PMT_DT    
;"""
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""

print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("---------------------------------")
funcfile.writelog("COMPLETED: C201_CREDITOR_TEST_DEV")
