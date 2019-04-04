""" Script to build kfs payments report ****************************************
Created on: 4 Apr 2019
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
print("---------------------------------")    
print("C201_REPORT_KFS_PAYMENTS_PREV_DEV")
print("---------------------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C201_REPORT_KFS_PAYMENTS_PREV_DEV")
funcfile.writelog("-----------------------------------------")
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

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

# BUILD PAYMENTS
print("Build previous year payments...")
sr_file = "X001aa_Report_payments_prev"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    PDP_PMT_GRP_T_PREV.PMT_GRP_ID,
    PDP_PMT_GRP_T_PREV.PAYEE_ID,
    PDP_PMT_GRP_T_PREV.PMT_PAYEE_NM,
    X000_Vendor.PAYEE_NM,
    X000_Vendor.VNDR_NM,
    X000_Vendor.VNDR_URL_ADDR,
    X000_Vendor.VNDR_TAX_NBR,
    X000_Vendor.BNK_ACCT_NBR,
    PDP_PMT_GRP_T_PREV.PAYEE_ID_TYP_CD,
    X000_Vendor.PAYEE_TYP_DESC,
    PDP_PMT_GRP_T_PREV.DISB_NBR,
    PDP_PMT_GRP_T_PREV.DISB_TS,
    PDP_PMT_GRP_T_PREV.PMT_DT,
    PDP_PMT_GRP_T_PREV.PMT_STAT_CD,
    PDP_PMT_STAT_CD_T.PMT_STAT_CD_DESC,
    PDP_PMT_DTL_T.CUST_PMT_DOC_NBR,
    PDP_PMT_DTL_T.INV_NBR,
    PDP_PMT_DTL_T.REQS_NBR,
    PDP_PMT_DTL_T.PO_NBR,
    PDP_PMT_DTL_T.INV_DT,
    PDP_PMT_DTL_T.ORIG_INV_AMT,
    PDP_PMT_DTL_T.NET_PMT_AMT
From
    PDP_PMT_GRP_T_PREV
    Left Join X000_Vendor On X000_Vendor.PAYEE_ID_NBR = PDP_PMT_GRP_T_PREV.PAYEE_ID
        And X000_Vendor.PAYEE_ID_TYP_CD = PDP_PMT_GRP_T_PREV.PAYEE_ID_TYP_CD
    Left Join PDP_PMT_STAT_CD_T On PDP_PMT_STAT_CD_T.PMT_STAT_CD = PDP_PMT_GRP_T_PREV.PMT_STAT_CD
    Left Join PDP_PMT_DTL_T On PDP_PMT_DTL_T.PMT_GRP_ID = PDP_PMT_GRP_T_PREV.PMT_GRP_ID
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# ADD DOCUMENT TYPE
print("Add document type...")
sr_file = "X001ab_Report_payments_prev_docs"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    X001_Report_payments_prev.*,
    X000_Documents.DOC_TYP_NM,
    X000_Documents.LBL
From
    X001_Report_payments_prev Left Join
    X000_Documents On X000_Documents.DOC_HDR_ID = X001_Report_payments_prev.CUST_PMT_DOC_NBR
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

"""
    PDP_PMT_STAT_CD_T.PMT_STAT_CD_DESC,
    PDP_PMT_DTL_T.CUST_PMT_DOC_NBR,
    PDP_PMT_DTL_T.INV_NBR,
    PDP_PMT_DTL_T.REQS_NBR,
    PDP_PMT_DTL_T.PO_NBR,
    PDP_PMT_DTL_T.INV_DT,
    PDP_PMT_DTL_T.ORIG_INV_AMT,
    PDP_PMT_DTL_T.NET_PMT_AMT
"""


"""
# BUILD PEOPLE BIRTHDAYS
print("Build previous year payments report...")
sr_file = "X001_Report_payment_prev"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
"""
select 
  paygroup.pmt_grp_id,
  paygroup.payee_id,
  paygroup.pmt_payee_nm,
  payee_account.payee_nm current_payee_nm,
  vendor_detail.vndr_nm vendor_name,
  vendor_detail.vndr_url_addr reg_number,
  vendor_header.vndr_tax_nbr,
  paygroup.payee_id_typ_cd,
  payee_type.payee_typ_desc,
  paygroup.disb_nbr,
  paygroup.disb_ts,
  paygroup.pmt_dt,
  paygroup.pmt_stat_cd,
  payment_status.pmt_stat_cd_desc,
  payment_detail.cust_pmt_doc_nbr,
  doc_type.doc_typ_nm,
  doc_type.lbl,
  payment_detail.inv_nbr,
  payment_detail.reqs_nbr,
  payment_detail.po_nbr,
  payment_detail.inv_dt,
  payment_detail.orig_inv_amt,
  payment_detail.net_pmt_amt
from 
  PDP_PMT_GRP_T_PREV paygroup
  
  left join pdp_payee_typ_t payee_type on payee_type.payee_typ_cd = paygroup.payee_id_typ_cd
  left join pdp_pmt_stat_cd_t payment_status on payment_status.pmt_stat_cd = paygroup.pmt_stat_cd
  left join PDP_PMT_DTL_T payment_detail on  payment_detail.pmt_grp_id = paygroup.pmt_grp_id
  left join PDP_PAYEE_ACH_ACCT_T payee_account on trim(payee_account.payee_id_nbr) = trim(paygroup.payee_id) and
    trim(payee_account.payee_id_typ_cd) = trim(paygroup.payee_id_typ_cd)
  left outer join pur_vndr_dtl_t vendor_detail on vendor_detail.vndr_hdr_gnrtd_id||'-'||vendor_detail.vndr_dtl_asnd_id = paygroup.payee_id
  left outer join pur_vndr_hdr_t vendor_header on vendor_header.vndr_hdr_gnrtd_id = substr(paygroup.payee_id,1,8)
  left outer join krew_doc_hdr_t doc_header on doc_header.doc_hdr_id  = payment_detail.cust_pmt_doc_nbr
  left outer join krew_doc_typ_t doc_type on doc_type.doc_typ_id = doc_header.doc_typ_id
"""
"""so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file) 
"""

""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("--------------------------------------------")
funcfile.writelog("COMPLETED: C201_REPORT_KFS_PAYMENTS_PREV_DEV")
