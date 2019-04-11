""" Script to build standard KFS lists *****************************************
Created on: 11 Mar 2018
Author: Albert J v Rensburg (NWU21162395)
*************************************************************************** """

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
import funccsv
import funcdate
import funcfile
import funcmail
import funcmysql
import funcpeople
import funcstr
import funcsys

# OPEN THE SCRIPT LOG FILE
print("------------------")    
print("B002_KFS_LISTS_DEV")
print("------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B002_KFS_LISTS_DEV")
funcfile.writelog("--------------------------")
ilog_severity = 1

# DECLARE VARIABLES
so_path = "W:/Kfs/" #Source database path
re_path = "R:/Kfs/" # Results path
ed_path = "S:/_external_data/" #external data path
so_file = "Kfs.sqlite" # Source database
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
so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

# BUILD APPROVALS UNIQUE LIST
print("Build unique approvers list...")
sr_file = "X000_Approvers_last"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select Distinct
    ROUTE.ACTN_TKN_ID,
    ROUTE.DOC_HDR_ID,
    ROUTE.ACTN_DT,
    ROUTE.PRNCPL_ID,
    PERSON.NAME_ADDR,
    ROUTE.ACTN_CD,
    CASE
        WHEN ROUTE.ACTN_CD = 'a' THEN 'SUPER USER APPROVED'
        WHEN ROUTE.ACTN_CD = 'A' THEN 'APPROVED'
        WHEN ROUTE.ACTN_CD = 'B' THEN 'BLANKET APPROVED'
        WHEN ROUTE.ACTN_CD = 'r' THEN 'SUPER USER ROUTE LEVEL APPROVED'
        WHEN ROUTE.ACTN_CD = 'R' THEN 'SUPER USER ROUTE LEVEL APPROVED'
        WHEN ROUTE.ACTN_CD = 'v' THEN 'SUPER USER APPROVED'
       ELSE 'OTHER'
    END AS ACTN,
    Count(ROUTE.DOC_VER_NBR) As APP_COUNT
From
    KREW_ACTN_TKN_T_APP ROUTE Left Join
    PEOPLE.X002_PEOPLE_PREV_YEAR PERSON On PERSON.EMPLOYEE_NUMBER = ROUTE.PRNCPL_ID AND
        PERSON.EMP_START <= ROUTE.ACTN_DT AND
        PERSON.EMP_END >= ROUTE.ACTN_DT
Group By
    ROUTE.DOC_HDR_ID
Order By
    ROUTE.ACTN_DT,
    ROUTE.ACTN_TKN_ID
"""
so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD PAYMENTS
print("Build current year payments...")
sr_file = "X001aa_Report_payments_curr"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    PAYMENT.PMT_GRP_ID,
    PAYMENT.PAYEE_ID AS VENDOR_ID,
    PAYMENT.PMT_PAYEE_NM AS PAYEE_NAME,
    VENDOR.VNDR_NM AS VENDOR_NAME,
    VENDOR.VNDR_URL_ADDR AS VENDOR_REG_NR,
    VENDOR.VNDR_TAX_NBR AS VENDOR_TAX_NR,
    PAYEE.BNK_ACCT_NBR AS VENDOR_BANK_NR,
    PAYMENT.PAYEE_ID_TYP_CD AS VENDOR_TYPE,
    TYPE.PAYEE_TYP_DESC,
    PAYMENT.DISB_NBR,
    PAYMENT.DISB_TS,
    PAYMENT.PMT_DT,
    PAYMENT.PMT_STAT_CD,
    STATUS.PMT_STAT_CD_DESC AS PAYMENT_STATUS,
    DETAIL.CUST_PMT_DOC_NBR,
    DETAIL.INV_NBR,
    DETAIL.REQS_NBR,
    DETAIL.PO_NBR,
    DETAIL.INV_DT,
    DETAIL.ORIG_INV_AMT,
    DETAIL.NET_PMT_AMT
From
    PDP_PMT_GRP_T_CURR PAYMENT
    Left Join X000_VENDOR_MASTER VENDOR On VENDOR.VENDOR_ID = PAYMENT.PAYEE_ID
    Left Join PDP_PAYEE_ACH_ACCT_T PAYEE On PAYEE.PAYEE_ID_NBR = PAYMENT.PAYEE_ID And
        PAYEE.PAYEE_ID_TYP_CD = PAYMENT.PAYEE_ID_TYP_CD
    Left Join PDP_PAYEE_TYP_T TYPE ON TYPE.PAYEE_TYP_CD = PAYMENT.PAYEE_ID_TYP_CD
    Left Join PDP_PMT_STAT_CD_T STATUS On STATUS.PMT_STAT_CD = PAYMENT.PMT_STAT_CD
    Left Join PDP_PMT_DTL_T DETAIL On DETAIL.PMT_GRP_ID = PAYMENT.PMT_GRP_ID
"""
"""
    ROUTE.PRNCPL_ID AS APPROVER,
    CASE
        WHEN ACTN_CD = 'a' THEN 'APPROVED'
        WHEN ACTN_CD = 'A' THEN 'SUPER USED APPROVED'
        WHEN ACTN_CD = 'B' THEN 'BLANKET APPROVED'
        WHEN ACTN_CD = 'R' THEN 'SUPER USER ROUTE LEVEL APPROVED'
        ELSE 'OTHER'
    END AS APPROVE_STATUS,
    ROUTE.ACTN_DT AS APPROVE_DATE

    Left Join KREW_ACTN_TKN_T_APP ROUTE On ROUTE.DOC_HDR_ID = DETAIL.CUST_PMT_DOC_NBR
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
#print("Vacuum the database...")
so_conn.commit()
#so_conn.execute('VACUUM')
#funcfile.writelog("%t DATABASE: Vacuum")
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("-----------------------------")
funcfile.writelog("COMPLETED: B002_KFS_LISTS_DEV")
