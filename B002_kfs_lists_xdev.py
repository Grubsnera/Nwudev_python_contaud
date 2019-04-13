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

# BUILD CURR APPROVALS UNIQUE LIST
print("Build current approvers list...")
sr_file = "X000_Approvers_curr"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select Distinct
    ROUTE.ACTN_TKN_ID,
    ROUTE.DOC_HDR_ID,
    ROUTE.ACTN_DT,
    ROUTE.PRNCPL_ID,
    CASE
        WHEN ROUTE.PRNCPL_ID = '26807815' THEN 'KFS WORKFLOW SYSTEM USER'
        WHEN PERSON.NAME_ADDR IS NULL THEN 'UNKNOWN'
        ELSE PERSON.NAME_ADDR
    END AS NAME_ADDR,
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
    ROUTE.ANNOTN
From
    KREW_ACTN_TKN_T_APP ROUTE Left Join
    PEOPLE.X002_PEOPLE_CURR_YEAR PERSON On PERSON.EMPLOYEE_NUMBER = ROUTE.PRNCPL_ID
Order By
    ROUTE.ACTN_DT,
    ROUTE.ACTN_TKN_ID
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD CURR APPROVALS UNIQUE LIST
print("Build current payment full approvers list...")
sr_file = "X001ac_Report_payments_approute_curr"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    PAYMENT.PMT_GRP_ID,
    PAYMENT.VENDOR_ID,
    PAYMENT.PAYEE_NAME,
    PAYMENT.VENDOR_NAME,
    PAYMENT.VENDOR_REG_NR,
    PAYMENT.VENDOR_TAX_NR,
    PAYMENT.VENDOR_BANK_NR,
    PAYMENT.VENDOR_TYPE,
    PAYMENT.PAYEE_TYP_DESC,
    PAYMENT.DISB_NBR,
    PAYMENT.DISB_TS,
    PAYMENT.PMT_DT,
    PAYMENT.PMT_STAT_CD,
    PAYMENT.PAYMENT_STATUS,
    PAYMENT.CUST_PMT_DOC_NBR,
    PAYMENT.INV_NBR,
    PAYMENT.REQS_NBR,
    PAYMENT.PO_NBR,
    PAYMENT.INV_DT,
    PAYMENT.ORIG_INV_AMT,
    PAYMENT.NET_PMT_AMT,
    APPROVE.PRNCPL_ID AS APPROVE_EMP_NO,
    APPROVE.NAME_ADDR AS APPROVE_EMP_NAME,
    APPROVE.ACTN_DT AS APPROVE_DATE,
    APPROVE.ACTN AS APPROVE_STATUS,
    APPROVE.ANNOTN AS NOTE    
From
    X001aa_Report_payments_curr PAYMENT Left Join
    X000_Approvers_curr APPROVE On APPROVE.DOC_HDR_ID = PAYMENT.CUST_PMT_DOC_NBR
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD PREV APPROVALS UNIQUE LIST
print("Build previous approvers list...")
sr_file = "X000_Approvers_prev"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select Distinct
    ROUTE.ACTN_TKN_ID,
    ROUTE.DOC_HDR_ID,
    ROUTE.ACTN_DT,
    ROUTE.PRNCPL_ID,
    CASE
        WHEN ROUTE.PRNCPL_ID = '26807815' THEN 'KFS WORKFLOW SYSTEM USER'
        WHEN PERSON.NAME_ADDR IS NULL THEN 'UNKNOWN'
        ELSE PERSON.NAME_ADDR
    END AS NAME_ADDR,
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
    ROUTE.ANNOTN
From
    KREW_ACTN_TKN_T_APP ROUTE Left Join
    PEOPLE.X002_PEOPLE_PREV_YEAR PERSON On PERSON.EMPLOYEE_NUMBER = ROUTE.PRNCPL_ID
Order By
    ROUTE.ACTN_DT,
    ROUTE.ACTN_TKN_ID
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD PREV PAYMENTS FULL APPROVALS
print("Build previous payment full approvers list...")
sr_file = "X001ac_Report_payments_approute_prev"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    PAYMENT.PMT_GRP_ID,
    PAYMENT.VENDOR_ID,
    PAYMENT.PAYEE_NAME,
    PAYMENT.VENDOR_NAME,
    PAYMENT.VENDOR_REG_NR,
    PAYMENT.VENDOR_TAX_NR,
    PAYMENT.VENDOR_BANK_NR,
    PAYMENT.VENDOR_TYPE,
    PAYMENT.PAYEE_TYP_DESC,
    PAYMENT.DISB_NBR,
    PAYMENT.DISB_TS,
    PAYMENT.PMT_DT,
    PAYMENT.PMT_STAT_CD,
    PAYMENT.PAYMENT_STATUS,
    PAYMENT.CUST_PMT_DOC_NBR,
    PAYMENT.INV_NBR,
    PAYMENT.REQS_NBR,
    PAYMENT.PO_NBR,
    PAYMENT.INV_DT,
    PAYMENT.ORIG_INV_AMT,
    PAYMENT.NET_PMT_AMT,
    APPROVE.PRNCPL_ID AS APPROVE_EMP_NO,
    APPROVE.NAME_ADDR AS APPROVE_EMP_NAME,
    APPROVE.ACTN_DT AS APPROVE_DATE,
    APPROVE.ACTN AS APPROVE_STATUS,
    APPROVE.ANNOTN AS NOTE    
From
    X001aa_Report_payments_prev PAYMENT Left Join
    X000_Approvers_curr APPROVE On APPROVE.DOC_HDR_ID = PAYMENT.CUST_PMT_DOC_NBR
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
