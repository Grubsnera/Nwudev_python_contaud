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

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

""" ****************************************************************************
BUILD VENDORS
*****************************************************************************"""
print("BUILD VENDORS")
funcfile.writelog("BUILD VENDORS")

# BUILD TABLE WITH VENDOR REMITTANCE ADDRESSES
print("Build vendor remittance addresses...")
sr_file = "X001aa_vendor_rm_address"
s_sql = "CREATE VIEW "+sr_file+" AS " + """
Select
    CAST(TRIM(UPPER(PUR_VNDR_ADDR_T.VNDR_HDR_GNRTD_ID))||'-'||TRIM(UPPER(PUR_VNDR_ADDR_T.VNDR_DTL_ASND_ID)) AS TEXT) VENDOR_ID,
    PUR_VNDR_ADDR_T.VNDR_ST_CD,
    PUR_VNDR_ADDR_T.VNDR_CNTRY_CD,
    PUR_VNDR_ADDR_T.VNDR_ADDR_EMAIL_ADDR,
    PUR_VNDR_ADDR_T.VNDR_B2B_URL_ADDR,
    PUR_VNDR_ADDR_T.VNDR_FAX_NBR,
    TRIM(UPPER(PUR_VNDR_ADDR_T.VNDR_DFLT_ADDR_IND))||'~'||
    TRIM(UPPER(PUR_VNDR_ADDR_T.VNDR_ATTN_NM))||'~'||
    TRIM(UPPER(PUR_VNDR_ADDR_T.VNDR_LN1_ADDR))||'~'||
    TRIM(UPPER(PUR_VNDR_ADDR_T.VNDR_LN2_ADDR))||'~'||
    TRIM(UPPER(PUR_VNDR_ADDR_T.VNDR_CTY_NM))||'~'||
    TRIM(UPPER(PUR_VNDR_ADDR_T.VNDR_ZIP_CD))||'~'||
    TRIM(UPPER(PUR_VNDR_ADDR_T.VNDR_CNTRY_CD))
    ADDRESS_RM
From
    PUR_VNDR_ADDR_T
Where
    PUR_VNDR_ADDR_T.VNDR_ADDR_TYP_CD = 'RM' And
    PUR_VNDR_ADDR_T.VNDR_DFLT_ADDR_IND = 'Y'
Group By
    PUR_VNDR_ADDR_T.VNDR_HDR_GNRTD_ID,
    PUR_VNDR_ADDR_T.VNDR_DTL_ASND_ID    
"""
so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: "+sr_file)

# BUILD TABLE WITH VENDOR PURCHASE ORDER ADDRESSES
print("Build vendor purchase order addresses...")
sr_file = "X001ab_vendor_po_address"
s_sql = "CREATE VIEW "+sr_file+" AS " + """
Select
    CAST(TRIM(UPPER(PUR_VNDR_ADDR_T.VNDR_HDR_GNRTD_ID))||'-'||TRIM(UPPER(PUR_VNDR_ADDR_T.VNDR_DTL_ASND_ID)) AS TEXT) VENDOR_ID,
    PUR_VNDR_ADDR_T.VNDR_ST_CD,
    PUR_VNDR_ADDR_T.VNDR_CNTRY_CD,
    PUR_VNDR_ADDR_T.VNDR_ADDR_EMAIL_ADDR,
    PUR_VNDR_ADDR_T.VNDR_B2B_URL_ADDR,
    PUR_VNDR_ADDR_T.VNDR_FAX_NBR,
    TRIM(UPPER(PUR_VNDR_ADDR_T.VNDR_DFLT_ADDR_IND))||'~'||
    TRIM(UPPER(PUR_VNDR_ADDR_T.VNDR_ATTN_NM))||'~'||
    TRIM(UPPER(PUR_VNDR_ADDR_T.VNDR_LN1_ADDR))||'~'||
    TRIM(UPPER(PUR_VNDR_ADDR_T.VNDR_LN2_ADDR))||'~'||
    TRIM(UPPER(PUR_VNDR_ADDR_T.VNDR_CTY_NM))||'~'||
    TRIM(UPPER(PUR_VNDR_ADDR_T.VNDR_ZIP_CD))||'~'||
    TRIM(UPPER(PUR_VNDR_ADDR_T.VNDR_CNTRY_CD))
    ADDRESS_PO
From
    PUR_VNDR_ADDR_T
Where
    PUR_VNDR_ADDR_T.VNDR_ADDR_TYP_CD = 'PO' And
    PUR_VNDR_ADDR_T.VNDR_DFLT_ADDR_IND = 'Y'
Group By
    PUR_VNDR_ADDR_T.VNDR_HDR_GNRTD_ID,
    PUR_VNDR_ADDR_T.VNDR_DTL_ASND_ID    
"""
so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: "+sr_file)

# BUILD TABLE WITH VENDOR PURCHASE ORDER ADDRESSES
print("Build vendor master file...")
sr_file = "X001ac_vendor_address_comb"
s_sql = "CREATE VIEW "+sr_file+" AS " + """
Select
    PUR_VNDR_DTL_T.VNDR_ID As VENDOR_ID,
    Case
        When X001aa_vendor_rm_address.VNDR_ST_CD <> '' Then X001aa_vendor_rm_address.VNDR_ST_CD
        Else X001ab_vendor_po_address.VNDR_ST_CD
    End as STATE_CD,
    Case
        When X001aa_vendor_rm_address.VNDR_CNTRY_CD <> '' Then X001aa_vendor_rm_address.VNDR_CNTRY_CD
        Else X001ab_vendor_po_address.VNDR_CNTRY_CD
    End as COUNTRY_CD,
    Case
        When X001aa_vendor_rm_address.VNDR_ADDR_EMAIL_ADDR <> '' Then Lower(X001aa_vendor_rm_address.VNDR_ADDR_EMAIL_ADDR)
        Else Lower(X001ab_vendor_po_address.VNDR_ADDR_EMAIL_ADDR)
    End as EMAIL,
    Case
        When X001aa_vendor_rm_address.VNDR_B2B_URL_ADDR <> '' Then Lower(X001aa_vendor_rm_address.VNDR_B2B_URL_ADDR)
        Else Lower(X001ab_vendor_po_address.VNDR_B2B_URL_ADDR)
    End as URL,
    Case
        When X001aa_vendor_rm_address.VNDR_FAX_NBR <> '' Then X001aa_vendor_rm_address.VNDR_FAX_NBR
        Else X001ab_vendor_po_address.VNDR_FAX_NBR
    End as FAX,
    Case
        When X001aa_vendor_rm_address.ADDRESS_RM <> '' Then Upper(X001aa_vendor_rm_address.ADDRESS_RM)
        Else Upper(X001ab_vendor_po_address.ADDRESS_PO)
    End as ADDRESS
From
    PUR_VNDR_DTL_T Left Join
    X001aa_vendor_rm_address On X001aa_vendor_rm_address.VENDOR_ID = PUR_VNDR_DTL_T.VNDR_ID Left Join
    X001ab_vendor_po_address On X001ab_vendor_po_address.VENDOR_ID = PUR_VNDR_DTL_T.VNDR_ID
"""
so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: "+sr_file)

# BUILD TABLE WITH VENDOR PURCHASE ORDER ADDRESSES
print("Build vendor master file...")
sr_file = "X000_VENDOR_MASTER"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    PUR_VNDR_DTL_T.VNDR_ID As VENDOR_ID,
    PUR_VNDR_DTL_T.VNDR_NM,
    PUR_VNDR_DTL_T.VNDR_URL_ADDR,
    PUR_VNDR_HDR_T.VNDR_TAX_NBR,
    X001ac_vendor_address_comb.FAX,
    X001ac_vendor_address_comb.EMAIL,
    X001ac_vendor_address_comb.ADDRESS,
    X001ac_vendor_address_comb.URL,
    X001ac_vendor_address_comb.STATE_CD,
    X001ac_vendor_address_comb.COUNTRY_CD,
    PUR_VNDR_HDR_T.VNDR_TAX_TYP_CD,
    PUR_VNDR_HDR_T.VNDR_TYP_CD,
    PUR_VNDR_DTL_T.VNDR_PMT_TERM_CD,
    PUR_VNDR_DTL_T.VNDR_SHP_TTL_CD,
    PUR_VNDR_DTL_T.VNDR_PARENT_IND,
    PUR_VNDR_DTL_T.VNDR_1ST_LST_NM_IND,
    PUR_VNDR_DTL_T.COLLECT_TAX_IND,
    PUR_VNDR_HDR_T.VNDR_FRGN_IND,
    PUR_VNDR_DTL_T.VNDR_CNFM_IND,
    PUR_VNDR_DTL_T.VNDR_PRPYMT_IND,
    PUR_VNDR_DTL_T.VNDR_CCRD_IND,
    PUR_VNDR_DTL_T.DOBJ_MAINT_CD_ACTV_IND,
    PUR_VNDR_DTL_T.VNDR_INACTV_REAS_CD
From
    PUR_VNDR_DTL_T Left Join
    PUR_VNDR_HDR_T On PUR_VNDR_HDR_T.VNDR_HDR_GNRTD_ID = PUR_VNDR_DTL_T.VNDR_HDR_GNRTD_ID Left Join
    X001ac_vendor_address_comb On X001ac_vendor_address_comb.VENDOR_ID = PUR_VNDR_DTL_T.VNDR_ID
"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

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
