"""
Script to build standard KFS lists
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funccsv
from _my_modules import funcfile
from _my_modules import funcdate

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
END OF SCRIPT
*****************************************************************************"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# DECLARE VARIABLES
l_debug: bool = True
l_export: bool = True
so_file: str = ""
s_period: str = "curr"
s_year: str = s_period
so_path = "W:/Kfs/"  # Source database path
if s_period == "curr":
    so_file = "Kfs_curr.sqlite"  # Source database
    s_year = funcdate.cur_year()
elif s_period == "prev":
    so_file = "Kfs_prev.sqlite"  # Source database
    s_year = funcdate.prev_year()
else:
    so_file = "Kfs_" + s_year + ".sqlite"  # Source database
re_path = "R:/Kfs/"  # Results path
ed_path = "S:/_external_data/"  # external data path

# OPEN THE SCRIPT LOG FILE
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B006_KFS_PERIOD_LIST_DEV")
funcfile.writelog("--------------------------------")
if l_debug:
    print("------------------------")
    print("B006_KFS_PERIOD_LIST_DEV")
    print("------------------------")

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
if s_period == "curr":
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

    """ ****************************************************************************
    PURCHASE ORDER MASTER LIST
    *****************************************************************************"""
    funcfile.writelog("PURCHASE ORDER MASTER LIST")
    if l_debug:
        print("PURCHASE ORDER MASTER LIST")

    # BUILD GL TRANSACTION LIST
    print("Build po master list...")
    sr_file = "X000_PO_master"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        POR.FDOC_NBR,
        POR.PO_ID,
        POR.VNDR_HDR_GNRTD_ID,
        POR.VNDR_DTL_ASND_ID,
        POR.VNDR_NM,
        POR.VNDR_PMT_TERM_CD,
        POR.PO_VNDR_CHC_CD,
        upper(PO_VNDR_CHC_DESC) As PO_VNDR_CHC_DESC,
        POR.VNDR_CONTR_GNRTD_ID,
        POR.REQS_ID,
        POR.REQS_SRC_CD,
        POR.FND_SRC_CD,
        POR.PO_CST_SRC_CD,
        POR.PO_TRNS_MTHD_CD,
        POR.FIN_COA_CD,
        POR.ORG_CD,
        POR.CONTR_MGR_CD,
        POR.PO_CRTE_DT,
        POR.PO_QT_DUE_DT,
        POR.PO_INIT_OPEN_DT,
        POR.PO_1ST_TRNS_DT,
        POR.PO_LST_TRNS_DT,
        POR.AP_PUR_DOC_LNK_ID,
        POR.PO_QT_INITLZTN_DT,   
        POR.PO_CUR_IND,
        POR.PEND_ACTN_IND,
        POR.RCVNG_DOC_REQ_IND,
        POR.PMT_RQST_PSTV_APRVL_IND,
        POR.RQSTR_PRSN_EMAIL_ADDR,
        POR.DLVY_TO_EMAIL_ADDR,
        upper(POR.VNDR_NTE_TXT) As VNDR_NTE_TXT,
        upper(POR.DLVY_INSTRC_TXT) As DLVY_INSTRC_TXT
    From
        PUR_PO_T POR Left Join
        KFS.PUR_PO_VNDR_CHC_T CHC On CHC.PO_VNDR_CHC_CD = POR.PO_VNDR_CHC_CD
    Order By
        POR.FDOC_NBR
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: PO Master list")

""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""
funcfile.writelog("END OF SCRIPT")
if l_debug:
    print("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.commit()
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("-----------------------------------")
funcfile.writelog("COMPLETED: B006_KFS_PERIOD_LIST_DEV")
