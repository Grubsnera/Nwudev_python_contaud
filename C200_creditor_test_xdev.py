""" Script to build kfs creditor payment tests *********************************
Created on: 16 Apr 2019
Author: Albert J v Rensburg (NWU21162395)
*****************************************************************************"""

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
END OF SCRIPT
*****************************************************************************"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# IMPORT PYTHON MODULES
import csv
import datetime
import sqlite3
import sys

# ADD OWN MODULE PATH
sys.path.append('S:/_my_modules')

# IMPORT OWN MODULES
import funcfile
#import funccsv
#import funcdate
#import funcmail
#import funcmysql
#import funcpeople
#import funcstr
#import funcsys

# OPEN THE SCRIPT LOG FILE
print("----------------------")    
print("C200_CREDITOR_TEST_DEV")
print("----------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C200_CREDITOR_TEST_DEV")
funcfile.writelog("------------------------------")
ilog_severity = 1

# DECLARE VARIABLES
so_path = "W:/Kfs/" #Source database path
re_path = "R:/Kfs/" # Results path
ed_path = "S:/_external_data/" #external data path
so_file = "Kfs_test_creditor.sqlite" # Source database
s_sql = "" # SQL statements
l_export = False
l_mail = False
l_record = False

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

""" ****************************************************************************
TEST CREDITOR DUPLICATE PAYMENT
*****************************************************************************"""
print("TEST CREDITOR DUPLICATE PAYMENT")
funcfile.writelog("TEST CREDITOR DUPLICATE PAYMENT")

# BUILD CREDITOR PAYMENTS MASTER FILES - LAST 3 DAY
print("Build creditor payment master tables...")
sr_file = "X001_payment_totest"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    PAYMENT.VENDOR_ID,
    PAYMENT.CUST_PMT_DOC_NBR,
    PAYMENT.INV_NBR,
    PAYMENT.INV_DT,
    PAYMENT.NET_PMT_AMT,
    Cast(Trim(PAYMENT.INV_NBR,' -/ABCDEFGHIJKLMNOPQRSTUVWXYZ') As INT) As INV_CALC
From
    KFS.X001aa_Report_payments_curr PAYMENT
Where
    StrfTime('%Y-%m-%d',PAYMENT.PMT_DT) >= StrfTime('%Y-%m-%d','now','-3 day')
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD CREDITOR PAYMENTS MASTER FILES - LAST 3 DAY
print("Build creditor payment master tables...")
sr_file = "X001_payment_curr"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    PAYMENT.VENDOR_ID,
    PAYMENT.CUST_PMT_DOC_NBR,
    PAYMENT.INV_NBR,
    PAYMENT.INV_DT,
    PAYMENT.NET_PMT_AMT,
    Cast(Trim(PAYMENT.INV_NBR,' -/ABCDEFGHIJKLMNOPQRSTUVWXYZ') As INT) As INV_CALC
From
    KFS.X001aa_Report_payments_curr PAYMENT
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD CREDITOR PAYMENTS MASTER FILES - LAST 3 DAY
print("Build creditor payment master tables...")
sr_file = "X001_payment_prev"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    PAYMENT.VENDOR_ID,
    PAYMENT.CUST_PMT_DOC_NBR,
    PAYMENT.INV_NBR,
    PAYMENT.INV_DT,
    PAYMENT.NET_PMT_AMT,
    Cast(Trim(PAYMENT.INV_NBR,' -/ABCDEFGHIJKLMNOPQRSTUVWXYZ') As INT) As INV_CALC
From
    KFS.X001aa_Report_payments_prev PAYMENT
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

"""

Select
    TEST.VENDOR_ID As VENDOR,
    TEST.CUST_PMT_DOC_NBR As EDOC,
    TEST.INV_NBR As INVOICE,
    TEST.INV_DT As INVOICE_DATE,
    TEST.NET_PMT_AMT As AMOUNT,
    TEST.INV_CALC As CALC,
    BASE.CUST_PMT_DOC_NBR As DUP_EDOC,
    BASE.INV_NBR As DUP_INVOICE,
    BASE.INV_DT As DUP_INVOICE_DATE,
    BASE.NET_PMT_AMT As DUP_AMOUNT,
    BASE.INV_CALC As DUP_CALC
From
    X001_payment_totest TEST Inner Join
    X001_payment_prev BASE On BASE.VENDOR_ID = TEST.VENDOR_ID
            And BASE.NET_PMT_AMT = TEST.NET_PMT_AMT
            And BASE.INV_CALC = TEST.INV_CALC

"""

""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("---------------------------------")
funcfile.writelog("COMPLETED: C200_CREDITOR_TEST_DEV")
