""" Script to build kfs creditor payment tests *********************************
Created on: 16 Apr 2019
Author: Albert J v Rensburg (NWU21162395)
*****************************************************************************"""

# IMPORT PYTHON MODULES
import csv
import sqlite3
import datetime

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
print("Identify and summarize small payments...")
sr_file: str = "X001baa_Small_split_pay_summ"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "Create Table " + sr_file + " As " + """
Select
    a.ORG_NM,
    a.FIN_OBJ_CD_NM,
    a.VENDOR_ID,
    a.VENDOR_TYPE,
    a.DOC_TYPE,    
    a.PMT_DT,
    a.EDOC,
    Cast(Total(a.ACC_AMOUNT) As Real) As TOT_AMOUNT
From
    KFSCURR.X001ad_Report_payments_accroute a
Where
    a.VENDOR_TYPE = 'V' And
    a.DOC_TYPE = 'DV'
Group By
    a.ORG_NM,
    a.FIN_OBJ_CD_NM,    
    a.VENDOR_ID,
    a.PMT_DT,
    a.EDOC
Having
    TOT_AMOUNT > 5000 And
    TOT_AMOUNT < 100000    
Order By
    a.ORG_NM,
    a.FIN_OBJ_CD_NM,    
    a.VENDOR_ID,
    a.PMT_DT,
    a.EDOC
;"""
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IDENTIFY PAYMENT TRANSACTIONS
print("Identify payment transactions...")
sr_file: str = "X001bab_Small_split_pay_list"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "Create Table " + sr_file + " As " + """
Select
    a.ORG_NM,
    a.FIN_OBJ_CD_NM,
    a.VENDOR_ID,
    a.PMT_DT,
    Min(a.EDOC) As EDOC,
    Cast(Count(a.EDOC) As Int) As TRAN_COUNT,
    Cast(a.TOT_AMOUNT As Real) As TOT_AMOUNT
From
    X001baa_Small_split_pay_summ a
Group By
    a.ORG_NM,
    a.FIN_OBJ_CD_NM,    
    a.VENDOR_ID,
    a.PMT_DT
;"""
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IDENTIFY PAYMENT TRANSACTIONS
print("Identify payment transactions...")
sr_file: str = "X001bac_Small_split_pay_select"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "Create Table " + sr_file + " As " + """
Select
    a.ORG_NM,
    a.FIN_OBJ_CD_NM,
    a.VENDOR_ID,
    a.VENDOR_TYPE,
    a.DOC_TYPE,
    a.EDOC As EDOC_A,
    a.PMT_DT As PMT_DATE_A,
    a.TOT_AMOUNT As AMOUNT_PD_A,
    b.EDOC As EDOC_B,
    cast(julianday(b.PMT_DT) - julianday(a.PMT_DT) As int) As DAYS_AFTER,
    b.PMT_DT As PMT_DATE_B,
    b.TOT_AMOUNT As AMOUNT_PD_B,
    b.TRAN_COUNT,
    cast(a.TOT_AMOUNT + b.TOT_AMOUNT As real) As TOTAL_AMOUNT_PD
From
    X001baa_Small_split_pay_summ a Inner Join
    X001bab_Small_split_pay_list b On b.ORG_NM = a.ORG_NM
            And b.FIN_OBJ_CD_NM = a.FIN_OBJ_CD_NM
            And b.VENDOR_ID = a.VENDOR_ID
            And julianday(b.PMT_DT) - julianday(a.PMT_DT) >= 0
            And julianday(b.PMT_DT) - julianday(a.PMT_DT) <= 8
            And cast(a.TOT_AMOUNT + b.TOT_AMOUNT As real) > 100000
            And a.EDOC != b.EDOC
Order By
    a.ORG_NM,
    a.VENDOR_ID,
    a.PMT_DT
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
