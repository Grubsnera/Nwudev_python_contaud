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

# BUILD VENDOR TABLE
print("Build vendor master file...")
sr_file = "X000_VENDOR_MASTER"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
Select
    PUR_VNDR_DTL_T.VNDR_ID As VENDOR_ID,
    UPPER(PUR_VNDR_DTL_T.VNDR_NM) AS VNDR_NM,
    PUR_VNDR_DTL_T.VNDR_URL_ADDR,
    PUR_VNDR_HDR_T.VNDR_TAX_NBR,
    X001ad_vendor_bankacc.VEND_BANK,
    X001ad_vendor_bankacc.VEND_MAIL,
    X001ad_vendor_bankacc.EMPL_BANK,
    X001ad_vendor_bankacc.STUD_BANK,
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
    X001ac_vendor_address_comb On X001ac_vendor_address_comb.VENDOR_ID = PUR_VNDR_DTL_T.VNDR_ID Left Join
    X001ad_vendor_bankacc On X001ad_vendor_bankacc.VENDOR_ID = PUR_VNDR_DTL_T.VNDR_ID
Order by
    VNDR_NM,
    VENDOR_ID
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
