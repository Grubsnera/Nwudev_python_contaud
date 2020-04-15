"""
Script to build standard KFS transaction lists
Created on: 27 Aug 2019
Copyright: Albert J v Rensburg (NWU:21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcsms
from _my_modules import funcsys

""" IMPORTANT NOTE *************************************************************
1. This script rely on data from the previous year payments. This script for the
    previous year must be run before running this script for the current year.
    Refer to Kfs_prev.sqlite
*****************************************************************************"""

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
GL TRANSACTION LIST
PAYMENT SUMMARY LIST
PAYMENT INITIATE LIST
PAYMENT APPROVE LIST
PAYMENT ACCOUNTING LINE
PAYMENT NOTES
PAYMENT ATTACHMENTS
PAYMENTS DETAIL
PAYMENTS SUMMARY
END OF SCRIPT
*****************************************************************************"""


def kfs_period_list(s_period="curr", s_yyyy=""):
    """
    Script to build standard KFS lists
    :type s_period: str: The financial period (curr, prev or year)
    :type s_yyyy: str: The financial year
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # OPEN THE LOG WRITER
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: B006_KFS_PERIOD_LIST")
    funcfile.writelog("----------------------------")
    print("--------------------")
    print("B006_KFS_PERIOD_LIST")
    print("--------------------")

    # DECLARE VARIABLES
    s_year: str = s_yyyy
    so_path = "W:/Kfs/"  # Source database path
    if s_period == "curr":
        s_year = funcdate.cur_year()
        so_file = "Kfs_curr.sqlite"  # Source database
    elif s_period == "prev":
        s_year = funcdate.prev_year()
        so_file = "Kfs_prev.sqlite"  # Source database
    else:
        so_file = "Kfs_" + s_year + ".sqlite"  # Source database

    # MESSAGE
    if funcconf.l_mess_project:
        funcsms.send_telegram("", "administrator", "<b>B006 Kfs " + s_year + " period lists</b>")

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
    if s_period == "curr":
        so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs_prev.sqlite' AS 'KFSPREV'")
        funcfile.writelog("%t ATTACH DATABASE: KFS_PREV.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """ ****************************************************************************
    GL TRANSACTION LIST
    *****************************************************************************"""
    print("GL TRANSACTION LIST")
    funcfile.writelog("GL TRANSACTION LIST")

    # BUILD GL TRANSACTION LIST
    print("Build gl transaction list...")
    sr_file = "X000_GL_trans"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        GLC.UNIV_FISCAL_YR,
        GLC.UNIV_FISCAL_PRD_CD,
        GLC.CALC_COST_STRING,
        ACC.ORG_NM,
        ACC.ACCOUNT_NM,
        OBJ.FIN_OBJ_CD_NM,
        GLC.TRANSACTION_DT,
        GLC.FDOC_NBR,
        GLC.CALC_AMOUNT,
        GLC.TRN_LDGR_ENTR_DESC,
        ACC.ACCT_TYP_NM,
        GLC.TRN_POST_DT,
        GLC."TIMESTAMP",
        GLC.FIN_COA_CD,
        GLC.ACCOUNT_NBR,
        GLC.FIN_OBJECT_CD,
        GLC.FIN_BALANCE_TYP_CD,
        GLC.FIN_OBJ_TYP_CD,
        GLC.FDOC_TYP_CD,
        GLC.FS_ORIGIN_CD,
        ORI.FS_DATABASE_DESC,
        GLC.TRN_ENTR_SEQ_NBR,
        GLC.FDOC_REF_TYP_CD,
        GLC.FS_REF_ORIGIN_CD,
        GLC.FDOC_REF_NBR,
        GLC.FDOC_REVERSAL_DT,
        GLC.TRN_ENCUM_UPDT_CD
    FROM
        GL_ENTRY_T GLC Left Join
        KFS.X000_Account ACC ON ACC.FIN_COA_CD = GLC.FIN_COA_CD AND ACC.ACCOUNT_NBR = GLC.ACCOUNT_NBR Left Join
        KFS.CA_OBJECT_CODE_T OBJ ON OBJ.UNIV_FISCAL_YR = GLC.UNIV_FISCAL_YR AND
            OBJ.FIN_COA_CD = GLC.FIN_COA_CD AND
            OBJ.FIN_OBJECT_CD = GLC.FIN_OBJECT_CD Left Join
        KFS.FS_ORIGIN_CODE_T ORI ON ORI.FS_ORIGIN_CD = GLC.FS_ORIGIN_CD
    ORDER BY
        GLC.CALC_COST_STRING,
        GLC.UNIV_FISCAL_PRD_CD
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: GL Transaction list")

    # MESSAGE
    if funcconf.l_mess_project:
        i = funcsys.tablerowcount(so_curs, sr_file)
        funcsms.send_telegram("", "administrator", "<b> " + str(i) + "</b> GL transactions")

    """ ****************************************************************************
    PAYMENT SUMMARY LIST
    *****************************************************************************"""
    print("PAYMENT INITIATE LIST")
    funcfile.writelog("PAYMENT INITIATE LIST")

    # BUILD PAYMENTS SUMMARY LIST
    print("Build payments...")
    sr_file = "X000_Payments"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        DETAIL.CUST_PMT_DOC_NBR As EDOC,
        PAYMENT.PMT_DT,
        DETAIL.REQS_NBR,
        DETAIL.PO_NBR,
        DETAIL.INV_NBR,
        DETAIL.INV_DT,
        DETAIL.ORIG_INV_AMT,
        PAYMENT.PAYEE_ID AS VENDOR_ID,
        PAYMENT.PAYEE_ID_TYP_CD AS VENDOR_TYPE,
        DETAIL.NET_PMT_AMT,
        PAYMENT.PMT_GRP_ID,
        PAYMENT.DISB_NBR,
        PAYMENT.DISB_TS,
        PAYMENT.PMT_STAT_CD,
        DOC.DOC_TYP_NM As DOC_TYPE,
        Upper(DOC.LBL) As DOC_LABEL
    From
        PDP_PMT_GRP_T PAYMENT Left Join
        KFS.PDP_PMT_DTL_T DETAIL On DETAIL.PMT_GRP_ID = PAYMENT.PMT_GRP_ID Left Join
        KFS.X000_Document DOC On DOC.DOC_HDR_ID = DETAIL.CUST_PMT_DOC_NBR
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if funcconf.l_mess_project:
        i = funcsys.tablerowcount(so_curs, sr_file)
        funcsms.send_telegram("", "administrator", "<b> " + str(i) + "</b> Payments")

    """ ****************************************************************************
    PAYMENT INITIATE LIST
    *****************************************************************************"""
    print("PAYMENT INITIATE LIST")
    funcfile.writelog("PAYMENT INITIATE LIST")

    # BUILD INITIATOR LIST
    print("Build initiate list...")
    sr_file = "X000_Initiate"
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
            WHEN ROUTE.ACTN_CD = 'C' THEN 'COMPLETED'
            ELSE 'OTHER'
        END AS ACTN,
        ROUTE.ANNOTN
    From
        KREW_ACTN_TKN_T_COM ROUTE Left Join
        PEOPLE.X002_PEOPLE_CURR_YEAR PERSON On PERSON.EMPLOYEE_NUMBER = ROUTE.PRNCPL_ID
    Order By
        ROUTE.ACTN_DT,
        ROUTE.ACTN_TKN_ID
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD UNIQUE INITIATOR LIST
    print("Build unique initiate list...")
    sr_file = "X000_Initiate_unique"
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
            WHEN ROUTE.ACTN_CD = 'C' THEN 'COMPLETED'
            ELSE 'OTHER'
        END AS ACTN,
        ROUTE.ANNOTN,
        Count(ROUTE.DOC_VER_NBR) As COM_COUNT
    From
        KREW_ACTN_TKN_T_COM ROUTE Left Join
        PEOPLE.X002_PEOPLE_CURR_YEAR PERSON On PERSON.EMPLOYEE_NUMBER = ROUTE.PRNCPL_ID
    Group By
        ROUTE.DOC_HDR_ID
    Order By
        ROUTE.ACTN_DT,
        ROUTE.ACTN_TKN_ID
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    PAYMENT APPROVE LIST
    *****************************************************************************"""
    print("PAYMENT APPROVE LIST")
    funcfile.writelog("PAYMENT APPROVE LIST")

    # BUILD CURR APPROVALS ALL APPROVERS
    print("Build approve list...")
    sr_file = "X000_Approve"
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

    # BUILD CURR APPROVALS UNIQUE LIST OF LAST APPROVER
    print("Build unique approve list...")
    sr_file = "X000_Approve_unique"
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
        ROUTE.ANNOTN,
        Count(ROUTE.DOC_VER_NBR) As APP_COUNT
    From
        KREW_ACTN_TKN_T_APP ROUTE Left Join
        PEOPLE.X002_PEOPLE_CURR_YEAR PERSON On PERSON.EMPLOYEE_NUMBER = ROUTE.PRNCPL_ID
    Group By
        ROUTE.DOC_HDR_ID
    Order By
        ROUTE.ACTN_DT,
        ROUTE.ACTN_TKN_ID
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    PAYMENT ACCOUNTING LINE
    *****************************************************************************"""
    print("PAYMENT ACCOUNTING LINE")
    funcfile.writelog("PAYMENT ACCOUNTING LINE")

    # BUILD ACCOUNTING LINES
    print("Build account lines...")
    sr_file = "X000_Account_line"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        LINE.FDOC_NBR,
        LINE.FDOC_LINE_NBR,
        LINE.FDOC_POST_YR,
        Trim(LINE.FIN_COA_CD)||'.'||Trim(LINE.ACCOUNT_NBR)||'.'||Trim(LINE.FIN_OBJECT_CD) As COST_STRING,
        Case
            When LINE.FDOC_LINE_DBCR_CD = "C" Then LINE.FDOC_LINE_AMT * -1
            Else LINE.FDOC_LINE_AMT
        End As AMOUNT,
        LINE.VATABLE,
        LINE.FDOC_LINE_DESC,
        ACC.ORG_NM,
        ACC.ACCOUNT_NM,
        OBJ.FIN_OBJ_CD_NM,
        ACC.ACCT_TYP_NM,
        ACC.ACCT_FSC_OFC_UID
    From
        FP_ACCT_LINES_T LINE Left Join
        KFS.X000_Account ACC ON ACC.FIN_COA_CD = LINE.FIN_COA_CD And
            ACC.ACCOUNT_NBR = LINE.ACCOUNT_NBR Left Join
        KFS.CA_OBJECT_CODE_T OBJ ON OBJ.UNIV_FISCAL_YR = LINE.FDOC_POST_YR And
            OBJ.FIN_COA_CD = LINE.FIN_COA_CD And
            OBJ.FIN_OBJECT_CD = LINE.FIN_OBJECT_CD        
    Order By
        LINE.FDOC_NBR,
        LINE.FDOC_LINE_NBR,
        LINE.FDOC_LINE_AMT
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD UNIQUE ACCOUNT LINE
    print("Build unique account lines...")
    sr_file = "X000_Account_line_unique"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        FIND.FDOC_NBR,
        FIND.FDOC_POST_YR,
        FIND.COST_STRING,
        FIND.AMOUNT,
        FIND.FDOC_LINE_DESC,
        Count(FIND.VATABLE) As COUNT_LINES
    From
        X000_Account_line FIND
    Group By
        FIND.FDOC_NBR
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    PAYMENT NOTES
    *****************************************************************************"""
    print("PAYMENT NOTES")
    funcfile.writelog("PAYMENT NOTES")

    # BUILD PAYMENT NOTE
    print("Build payment note...")
    sr_file = "X000_Note"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PAY.EDOC,
        HDR.FDOC_DESC As NOTE_DESC,
        NTE.TXT As NOTE_TXT
    From
        X000_Payments PAY Left Join
        KFS.KRNS_DOC_HDR_T HDR On HDR.DOC_HDR_ID = PAY.EDOC Inner Join
        KFS.KRNS_NTE_T NTE On NTE.RMT_OBJ_ID = HDR.OBJ_ID Left Join
        KFS.KRNS_ATT_T ATT On ATT.NTE_ID = NTE.NTE_ID
    Where
        ATT.FILE_NM IS NULL
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD PAYMENT UNIQUE NOTE
    print("Build payment unique note...")
    sr_file = "X000_Note_unique"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        NTE.EDOC,
        Count(NTE.EDOC) As NOTE_COUNT,
        NTE.NOTE_DESC,
        NTE.NOTE_TXT
    From
        X000_Note NTE
    Group By
        NTE.EDOC
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    PAYMENT ATTACHMENTS
    *****************************************************************************"""
    print("PAYMENT ATTACHMENTS")
    funcfile.writelog("PAYMENT ATTACHMENTS")

    # BUILD PAYMENT ATTACHMENTS
    print("Build payment attachments...")
    sr_file = "X000_Attachment"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PAY.EDOC,
        HDR.FDOC_DESC As ATTACH_DESC,
        NTE.TXT As ATTACH_TXT,
        ATT.FILE_NM As ATTACH_FILE
    From
        X000_Payments PAY Left Join
        KFS.KRNS_DOC_HDR_T HDR On HDR.DOC_HDR_ID = PAY.EDOC Inner Join
        KFS.KRNS_NTE_T NTE On NTE.RMT_OBJ_ID = HDR.OBJ_ID Inner Join
        KFS.KRNS_ATT_T ATT On ATT.NTE_ID = NTE.NTE_ID
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD PAYMENT UNIQUE ATTACHMENT
    print("Build payment unique attachment...")
    sr_file = "X000_Attachment_unique"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        ATT.EDOC,
        Count(ATT.EDOC) As ATTACH_COUNT,
        ATT.ATTACH_DESC,
        ATT.ATTACH_TXT,
        ATT.ATTACH_FILE
    From
        X000_Attachment ATT
    Group By
        ATT.EDOC
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    PAYMENTS DETAIL
    *****************************************************************************"""
    print("PAYMENTS DETAIL")
    funcfile.writelog("PAYMENTS DETAIL")

    # BUILD PAYMENTS WITH LAST INITIATOR AND APPROVER
    print("Build payments...")
    sr_file = "X001aa_Report_payments"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        DETAIL.CUST_PMT_DOC_NBR As EDOC,
        DETAIL.CUST_PMT_DOC_NBR,        
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
        DETAIL.INV_NBR,
        DETAIL.REQS_NBR,
        DETAIL.PO_NBR,
        DETAIL.INV_DT,
        DETAIL.ORIG_INV_AMT,
        DETAIL.NET_PMT_AMT,
        DOC.DOC_TYP_NM As DOC_TYPE,
        Upper(DOC.LBL) As DOC_LABEL,        
        INITIATE.PRNCPL_ID AS COMPLETE_EMP_NO,
        INITIATE.NAME_ADDR AS COMPLETE_EMP_NAME,
        INITIATE.ACTN_DT AS COMPLETE_DATE,
        INITIATE.ACTN AS COMPLETE_STATUS,
        Cast(INITIATE.COM_COUNT As INT) As I_COUNT,
        INITIATE.ANNOTN AS COMPLETE_NOTE,
        APPROVE.PRNCPL_ID AS APPROVE_EMP_NO,
        APPROVE.NAME_ADDR AS APPROVE_EMP_NAME,
        APPROVE.ACTN_DT AS APPROVE_DATE,
        APPROVE.ACTN AS APPROVE_STATUS,
        Cast(APPROVE.APP_COUNT As INT) As A_COUNT,
        APPROVE.ANNOTN AS APPROVE_NOTE,
        CASE
            WHEN ACC.COUNT_LINES = 1 THEN ACC.COST_STRING 
            ELSE Cast(ACC.COUNT_LINES As TEXT)
        END As ACC_COST_STRING,
        ACC.FDOC_LINE_DESC As ACC_DESC,
        NTE.NOTE_DESC,
        NTE.NOTE_TXT,
        NTE.NOTE_COUNT,
        ATT.ATTACH_DESC,
        ATT.ATTACH_TXT,
        ATT.ATTACH_FILE,
        ATT.ATTACH_COUNT
    From
        PDP_PMT_GRP_T PAYMENT Left Join
        KFS.X000_Vendor VENDOR On VENDOR.VENDOR_ID = PAYMENT.PAYEE_ID Left Join
        KFS.PDP_PAYEE_ACH_ACCT_T PAYEE On PAYEE.PAYEE_ID_NBR = PAYMENT.PAYEE_ID And
            PAYEE.PAYEE_ID_TYP_CD = PAYMENT.PAYEE_ID_TYP_CD Left Join
        KFS.PDP_PAYEE_TYP_T TYPE ON TYPE.PAYEE_TYP_CD = PAYMENT.PAYEE_ID_TYP_CD Left Join
        KFS.PDP_PMT_STAT_CD_T STATUS On STATUS.PMT_STAT_CD = PAYMENT.PMT_STAT_CD Left Join
        KFS.PDP_PMT_DTL_T DETAIL On DETAIL.PMT_GRP_ID = PAYMENT.PMT_GRP_ID Left Join
        KFS.X000_Document DOC On DOC.DOC_HDR_ID = DETAIL.CUST_PMT_DOC_NBR Left Join
        X000_Account_line_unique ACC On ACC.FDOC_NBR = DETAIL.CUST_PMT_DOC_NBR Left Join
        X000_Initiate_unique INITIATE On INITIATE.DOC_HDR_ID = DETAIL.CUST_PMT_DOC_NBR Left Join    
        X000_Approve_unique APPROVE On APPROVE.DOC_HDR_ID = DETAIL.CUST_PMT_DOC_NBR Left Join
        X000_Note_unique NTE On NTE.EDOC = DETAIL.CUST_PMT_DOC_NBR Left Join
        X000_Attachment_unique ATT On ATT.EDOC = DETAIL.CUST_PMT_DOC_NBR
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD PAYMENT LIST WITH ALL INITIATORS
    print("Build payments initiate...")
    sr_file = "X001ab_Report_payments_initiate"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PAYMENT.EDOC,
        PAYMENT.CUST_PMT_DOC_NBR,        
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
        PAYMENT.INV_NBR,
        PAYMENT.REQS_NBR,
        PAYMENT.PO_NBR,
        PAYMENT.INV_DT,
        PAYMENT.ORIG_INV_AMT,
        PAYMENT.NET_PMT_AMT,
        DOC.DOC_TYP_NM As DOC_TYPE,
        Upper(DOC.LBL) As DOC_LABEL,        
        INIT.PRNCPL_ID AS INIT_EMP_NO,
        INIT.NAME_ADDR AS INIT_EMP_NAME,
        INIT.ACTN_DT AS INIT_DATE,
        INIT.ACTN AS INIT_STATUS,
        INIT.ANNOTN AS NOTE,
        APPROVE.PRNCPL_ID AS APPROVE_EMP_NO,
        APPROVE.NAME_ADDR AS APPROVE_EMP_NAME,
        APPROVE.ACTN_DT AS APPROVE_DATE,
        APPROVE.ACTN AS APPROVE_STATUS,
        Cast(APPROVE.APP_COUNT As INT) As A_COUNT,
        APPROVE.ANNOTN AS APPROVE_NOTE,
        CASE
            WHEN ACC.COUNT_LINES = 1 THEN ACC.COST_STRING 
            ELSE Cast(ACC.COUNT_LINES As TEXT)
        END As ACC_COST_STRING,
        ACC.FDOC_LINE_DESC As ACC_DESC
    From
        X001aa_Report_payments PAYMENT Left Join
        KFS.X000_Document DOC On DOC.DOC_HDR_ID = PAYMENT.EDOC Left Join
        X000_Account_line_unique ACC On ACC.FDOC_NBR = PAYMENT.EDOC Left Join       
        X000_Initiate INIT On INIT.DOC_HDR_ID = PAYMENT.EDOC Left Join
        X000_Approve_unique APPROVE On APPROVE.DOC_HDR_ID = PAYMENT.EDOC    
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD PAYMENT LIST WITH ALL APPROVED
    print("Build payments approved...")
    sr_file = "X001ac_Report_payments_approve"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PAYMENT.EDOC,
        PAYMENT.CUST_PMT_DOC_NBR,        
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
        PAYMENT.INV_NBR,
        PAYMENT.REQS_NBR,
        PAYMENT.PO_NBR,
        PAYMENT.INV_DT,
        PAYMENT.ORIG_INV_AMT,
        PAYMENT.NET_PMT_AMT,
        DOC.DOC_TYP_NM As DOC_TYPE,
        Upper(DOC.LBL) As DOC_LABEL,
        INITIATE.PRNCPL_ID AS COMPLETE_EMP_NO,
        INITIATE.NAME_ADDR AS COMPLETE_EMP_NAME,
        INITIATE.ACTN_DT AS COMPLETE_DATE,
        INITIATE.ACTN AS COMPLETE_STATUS,
        Cast(INITIATE.COM_COUNT As INT) As I_COUNT,
        APPROVE.PRNCPL_ID AS APPROVE_EMP_NO,
        APPROVE.NAME_ADDR AS APPROVE_EMP_NAME,
        APPROVE.ACTN_DT AS APPROVE_DATE,
        APPROVE.ACTN AS APPROVE_STATUS,
        PAYMENT.A_COUNT,
        APPROVE.ANNOTN AS NOTE,
        CASE
            WHEN ACC.COUNT_LINES = 1 THEN ACC.COST_STRING 
            ELSE Cast(ACC.COUNT_LINES As TEXT)
        END As ACC_COST_STRING,
        ACC.FDOC_LINE_DESC As ACC_DESC
    From
        X001aa_Report_payments PAYMENT Left Join
        KFS.X000_Document DOC On DOC.DOC_HDR_ID = PAYMENT.EDOC Left Join
        X000_Account_line_unique ACC On ACC.FDOC_NBR = PAYMENT.EDOC Left Join       
        X000_Approve APPROVE On APPROVE.DOC_HDR_ID = PAYMENT.EDOC Left Join
        X000_Initiate_unique INITIATE On INITIATE.DOC_HDR_ID = PAYMENT.EDOC    
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD PAYMENT LIST WITH ALL ACCOUNT LINES
    print("Build payments account line...")
    sr_file = "X001ad_Report_payments_accroute"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PAYMENT.EDOC,
        PAYMENT.CUST_PMT_DOC_NBR,        
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
        PAYMENT.INV_NBR,
        PAYMENT.REQS_NBR,
        PAYMENT.PO_NBR,
        PAYMENT.INV_DT,
        PAYMENT.ORIG_INV_AMT,
        PAYMENT.NET_PMT_AMT,
        DOC.DOC_TYP_NM As DOC_TYPE,
        Upper(DOC.LBL) As DOC_LABEL,        
        INIT.PRNCPL_ID AS INIT_EMP_NO,
        INIT.NAME_ADDR AS INIT_EMP_NAME,
        INIT.ACTN_DT AS INIT_DATE,
        INIT.ACTN AS INIT_STATUS,
        Cast(INIT.COM_COUNT As INT) As I_COUNT,
        INIT.ANNOTN AS COMPLETE_NOTE,
        APPROVE.PRNCPL_ID AS APPROVE_EMP_NO,
        APPROVE.NAME_ADDR AS APPROVE_EMP_NAME,
        APPROVE.ACTN_DT AS APPROVE_DATE,
        APPROVE.ACTN AS APPROVE_STATUS,
        Cast(APPROVE.APP_COUNT As INT) As A_COUNT,
        APPROVE.ANNOTN AS APPROVE_NOTE,
        ACC.FDOC_LINE_NBR As ACC_LINE,
        ACC.COST_STRING As ACC_COST_STRING,
        ACC.AMOUNT As ACC_AMOUNT,
        ACC.FDOC_LINE_DESC As ACC_DESC,
        ACC.ORG_NM,
        ACC.ACCOUNT_NM,
        ACC.FIN_OBJ_CD_NM,
        ACC.ACCT_TYP_NM,
        ACC.ACCT_FSC_OFC_UID
    From
        X001aa_Report_payments PAYMENT Left Join
        KFS.X000_Document DOC On DOC.DOC_HDR_ID = PAYMENT.EDOC Left Join
        X000_Account_line ACC On ACC.FDOC_NBR = PAYMENT.EDOC Left Join       
        X000_Initiate_unique INIT On INIT.DOC_HDR_ID = PAYMENT.EDOC Left Join
        X000_Approve_unique APPROVE On APPROVE.DOC_HDR_ID = PAYMENT.EDOC
    Order By
        VENDOR_NAME,
        PAYEE_NAME,
        PMT_DT,
        EDOC,
        FDOC_LINE_NBR     
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    PAYMENTS SUMMARY
    *****************************************************************************"""
    print("PAYMENTS SUMMARY")
    funcfile.writelog("PAYMENTS SUMMARY")

    # BUILD VENDOR PAYMENTS SUMMARY
    print("Build vendor payments summary...")
    sr_file = "X002aa_Report_payments_summary"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PAY.VENDOR_ID,
        Max(PAY.PMT_DT) As Max_PMT_DT,
        Sum(PAY.NET_PMT_AMT) As Sum_NET_PMT_AMT,
        Count(PAY.VENDOR_ID) As Count_TRAN
    From
        X001aa_Report_payments PAY
    Group By
        PAY.VENDOR_ID
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD PAYEE TYPE SUMMARY OF PAYMENTS
    print("Build payee type payment summary per month...")
    sr_file = "X002ab_Report_payments_typemon"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        '%PERIOD_TEXT%' As YEAR,
        PAYM.PAYEE_TYP_DESC As TYPE,
        PAYM.DOC_LABEL,
        SubStr(PAYM.PMT_DT, 6, 2) As MONTH,
        Sum(PAYM.NET_PMT_AMT) As Sum_NET_PMT_AMT
    From
        X001aa_Report_payments PAYM
    Group By
        PAYM.PAYEE_TYP_DESC,
        PAYM.DOC_LABEL,    
        SubStr(PAYM.PMT_DT, 6, 2)
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if s_period == "curr":
        s_sql = s_sql.replace("%PERIOD_TEXT%",'CURRENT')
    elif s_period == "prev":
        s_sql = s_sql.replace("%PERIOD_TEXT%",'PREVIOUS')
    else:
        s_sql = s_sql.replace("%PERIOD_TEXT%", 'PERIOD')
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD SUMMARY OF PAYMENTS
    if s_period == "curr":
        print("Combine payee type payments summary per month...")
        sr_file = "X002ac_Report_typemon_summary"
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.TYPE As TYPE,
            PREV.DOC_LABEL As TYPE_DOC,
            PREV.MONTH As MONTH,
            PREV.Sum_NET_PMT_AMT As 'PREVIOUS',
            CURR.Sum_NET_PMT_AMT As 'CURRENT'
        From
            KFSPREV.X002ab_Report_payments_typemon PREV Left Join
            X002ab_Report_payments_typemon CURR On CURR.TYPE = PREV.TYPE And
                CURR.DOC_LABEL = PREV.DOC_LABEL And
                CURR.MONTH = PREV.MONTH
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

    so_conn.commit()
    so_conn.close()

    # Close the log writer *********************************************************
    funcfile.writelog("-------------------------------")
    funcfile.writelog("COMPLETED: B006_KFS_PERIOD_LIST")

    return


if __name__ == '__main__':
    try:
        kfs_period_list()
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, "B006_kfs_period_list", "B006_kfs_period_list")
