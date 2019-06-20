"""
Script to build standard KFS lists
Created on: 11 Mar 2018
Copyright: Albert J v Rensburg
"""

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
VENDOR MASTER LIST
DOCUMENTS MASTER LIST
BUILD ACCOUNTING LINES
PAYMENT COMPLETED LISTS
PAYMENT APPROVERS LISTS
BUILD CURRENT YEAR PAYMENTS
BUILD PREVIOUS YEAR PAYMENTS
BUILD PAYMENT TYPE SUMMARY PER MONTH
END OF SCRIPT
*****************************************************************************"""

def Kfs_lists():

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # Import python modules
    import datetime
    import sqlite3
    import sys

    # Add own module path
    sys.path.append('S:\\_my_modules')

    # Import own modules
    import funcdate
    import funccsv
    import funcfile

    # Open the script log file ******************************************************

    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: B002_KFS_LISTS")
    funcfile.writelog("----------------------")
    print("--------------")
    print("B002_KFS_LISTS")
    print("--------------")
    ilog_severity = 1

    # Declare variables
    so_path = "W:/Kfs/" #Source database path
    so_file = "Kfs.sqlite" #Source database
    s_sql = "" #SQL statements
    l_vacuum = False # Vacuum database

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

    # Add calculated fields to the current gl transaction list *********************

    print("Calculate current gl columns...")

    # Calc combined cost string
    if "CALC_COST_STRING" not in funccsv.get_colnames_sqlite(so_curs,"GL_ENTRY_T_CURR"):
        so_curs.execute("ALTER TABLE GL_ENTRY_T_CURR ADD COLUMN CALC_COST_STRING TEXT;")
        so_curs.execute("UPDATE GL_ENTRY_T_CURR SET CALC_COST_STRING = Trim(FIN_COA_CD) || '.' || Trim(ACCOUNT_NBR) || '.' || Trim(FIN_OBJECT_CD);")
        so_conn.commit()
        funcfile.writelog("%t CALC COLUMNS: Combined cost string")

    # Calc amount
    if "CALC_AMOUNT" not in funccsv.get_colnames_sqlite(so_curs,"GL_ENTRY_T_CURR"):
        so_curs.execute("ALTER TABLE GL_ENTRY_T_CURR ADD COLUMN CALC_AMOUNT REAL;")
        so_curs.execute("UPDATE GL_ENTRY_T_CURR " + """
                        SET CALC_AMOUNT = 
                        CASE
                           WHEN TRN_DEBIT_CRDT_CD = "C" THEN TRN_LDGR_ENTR_AMT * -1
                           ELSE TRN_LDGR_ENTR_AMT
                        END
                        ;""")
        so_conn.commit()
        funcfile.writelog("%t CALC COLUMNS: Amount")

    # Add calculated fields to the previous gl transaction list ********************

    print("Calculate previous gl columns...")

    # Calc combined cost string
    if "CALC_COST_STRING" not in funccsv.get_colnames_sqlite(so_curs,"GL_ENTRY_T_PREV"):
        so_curs.execute("ALTER TABLE GL_ENTRY_T_PREV ADD COLUMN CALC_COST_STRING TEXT;")
        so_curs.execute("UPDATE GL_ENTRY_T_PREV SET CALC_COST_STRING = Trim(FIN_COA_CD) || '.' || Trim(ACCOUNT_NBR) || '.' || Trim(FIN_OBJECT_CD);")
        so_conn.commit()
        funcfile.writelog("%t CALC COLUMNS: Combined prev cost string")

    # Calc amount
    if "CALC_AMOUNT" not in funccsv.get_colnames_sqlite(so_curs,"GL_ENTRY_T_PREV"):
        so_curs.execute("ALTER TABLE GL_ENTRY_T_PREV ADD COLUMN CALC_AMOUNT REAL;")
        so_curs.execute("UPDATE GL_ENTRY_T_PREV " + """
                        SET CALC_AMOUNT = 
                        CASE
                           WHEN TRN_DEBIT_CRDT_CD = "C" THEN TRN_LDGR_ENTR_AMT * -1
                           ELSE TRN_LDGR_ENTR_AMT
                        END
                        ;""")
        so_conn.commit()
        funcfile.writelog("%t CALC COLUMNS: Prev amount")

    # Build organization list ******************************************************

    print("Build organization...")

    s_sql = "CREATE TABLE X000_Organization AS " + """
    SELECT
      CA_ORG_T.FIN_COA_CD,
      CA_ORG_T.ORG_CD,
      CA_ORG_T.ORG_TYP_CD,
      CA_ORG_TYPE_T.ORG_TYP_NM,
      CA_ORG_T.ORG_NM,
      CA_ORG_T.ORG_BEGIN_DT,
      CA_ORG_T.ORG_END_DT,
      CA_ORG_T.OBJ_ID,
      CA_ORG_T.VER_NBR,
      CA_ORG_T.ORG_MGR_UNVL_ID,
      CA_ORG_T.RC_CD,
      CA_ORG_T.ORG_PHYS_CMP_CD,
      CA_ORG_T.ORG_DFLT_ACCT_NBR,
      CA_ORG_T.ORG_LN1_ADDR,
      CA_ORG_T.ORG_LN2_ADDR,
      CA_ORG_T.ORG_CITY_NM,
      CA_ORG_T.ORG_STATE_CD,
      CA_ORG_T.ORG_ZIP_CD,
      CA_ORG_T.ORG_CNTRY_CD,
      CA_ORG_T.RPTS_TO_FIN_COA_CD,
      CA_ORG_T.RPTS_TO_ORG_CD,
      CA_ORG_T.ORG_ACTIVE_CD,
      CA_ORG_T.ORG_IN_FP_CD,
      CA_ORG_T.ORG_PLNT_ACCT_NBR,
      CA_ORG_T.CMP_PLNT_ACCT_NBR,
      CA_ORG_T.ORG_PLNT_COA_CD,
      CA_ORG_T.CMP_PLNT_COA_CD,
      CA_ORG_T.ORG_LVL
    FROM
      CA_ORG_T
      LEFT JOIN CA_ORG_TYPE_T ON CA_ORG_TYPE_T.ORG_TYP_CD = CA_ORG_T.ORG_TYP_CD
    ORDER BY
      CA_ORG_T.FIN_COA_CD,
      CA_ORG_T.ORG_LVL,
      CA_ORG_TYPE_T.ORG_TYP_NM,
      CA_ORG_T.ORG_NM,
      CA_ORG_T.ORG_BEGIN_DT,
      CA_ORG_T.ORG_END_DT
    """
    so_curs.execute("DROP TABLE IF EXISTS X000_Organization")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: Organization list")

    # Build organization structure *************************************************

    print("Build organization structure...")

    s_sql = "CREATE TABLE X000_Organization_struct AS " + """
    SELECT
      X000_Organization.FIN_COA_CD,
      X000_Organization.ORG_CD,
      X000_Organization.ORG_TYP_CD,
      X000_Organization.ORG_TYP_NM,
      X000_Organization.ORG_NM,
      X000_Organization.ORG_MGR_UNVL_ID,
      X000_Organization.ORG_LVL,
      X000_Organization1.FIN_COA_CD AS FIN_COA_CD1,
      X000_Organization1.ORG_CD AS ORG_CD1,
      X000_Organization1.ORG_TYP_CD AS ORG_TYP_CD1,
      X000_Organization1.ORG_TYP_NM AS ORG_TYP_NM1,
      X000_Organization1.ORG_NM AS ORG_NM1,
      X000_Organization1.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID1,
      X000_Organization1.ORG_LVL AS ORG_LVL1,
      X000_Organization2.FIN_COA_CD AS FIN_COA_CD2,
      X000_Organization2.ORG_CD AS ORG_CD2,
      X000_Organization2.ORG_TYP_CD AS ORG_TYP_CD2,
      X000_Organization2.ORG_TYP_NM AS ORG_TYP_NM2,
      X000_Organization2.ORG_NM AS ORG_NM2,
      X000_Organization2.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID2,
      X000_Organization2.ORG_LVL AS ORG_LVL2,
      X000_Organization3.FIN_COA_CD AS FIN_COA_CD3,
      X000_Organization3.ORG_CD AS ORG_CD3,
      X000_Organization3.ORG_TYP_CD AS ORG_TYP_CD3,
      X000_Organization3.ORG_TYP_NM AS ORG_TYP_NM3,
      X000_Organization3.ORG_NM AS ORG_NM3,
      X000_Organization3.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID3,
      X000_Organization3.ORG_LVL AS ORG_LVL3,
      X000_Organization4.FIN_COA_CD AS FIN_COA_CD4,
      X000_Organization4.ORG_CD AS ORG_CD4,
      X000_Organization4.ORG_TYP_CD AS ORG_TYP_CD4,
      X000_Organization4.ORG_TYP_NM AS ORG_TYP_NM4,
      X000_Organization4.ORG_NM AS ORG_NM4,
      X000_Organization4.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID4,
      X000_Organization4.ORG_LVL AS ORG_LVL4,
      X000_Organization5.FIN_COA_CD AS FIN_COA_CD5,
      X000_Organization5.ORG_CD AS ORG_CD5,
      X000_Organization5.ORG_TYP_CD AS ORG_TYP_CD5,
      X000_Organization5.ORG_TYP_NM AS ORG_TYP_NM5,
      X000_Organization5.ORG_NM AS ORG_NM5,
      X000_Organization5.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID5,
      X000_Organization5.ORG_LVL AS ORG_LVL5,
      X000_Organization6.FIN_COA_CD AS FIN_COA_CD6,
      X000_Organization6.ORG_CD AS ORG_CD6,
      X000_Organization6.ORG_TYP_CD AS ORG_TYP_CD6,
      X000_Organization6.ORG_TYP_NM AS ORG_TYP_NM6,
      X000_Organization6.ORG_NM AS ORG_NM6,
      X000_Organization6.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID6,
      X000_Organization6.ORG_LVL AS ORG_LVL6,
      X000_Organization7.FIN_COA_CD AS FIN_COA_CD7,
      X000_Organization7.ORG_CD AS ORG_CD7,
      X000_Organization7.ORG_TYP_CD AS ORG_TYP_CD7,
      X000_Organization7.ORG_TYP_NM AS ORG_TYP_NM7,
      X000_Organization7.ORG_NM AS ORG_NM7,
      X000_Organization7.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID7,
      X000_Organization7.ORG_LVL AS ORG_LVL7
    FROM
      X000_Organization
      LEFT JOIN X000_Organization X000_Organization1 ON X000_Organization1.FIN_COA_CD = X000_Organization.RPTS_TO_FIN_COA_CD
        AND X000_Organization1.ORG_CD = X000_Organization.RPTS_TO_ORG_CD
      LEFT JOIN X000_Organization X000_Organization2 ON
        X000_Organization2.FIN_COA_CD = X000_Organization1.RPTS_TO_FIN_COA_CD AND X000_Organization2.ORG_CD =
        X000_Organization1.RPTS_TO_ORG_CD
      LEFT JOIN X000_Organization X000_Organization3 ON
        X000_Organization3.FIN_COA_CD = X000_Organization2.RPTS_TO_FIN_COA_CD AND X000_Organization3.ORG_CD =
        X000_Organization2.RPTS_TO_ORG_CD
      LEFT JOIN X000_Organization X000_Organization4 ON
        X000_Organization4.FIN_COA_CD = X000_Organization3.RPTS_TO_FIN_COA_CD AND X000_Organization4.ORG_CD =
        X000_Organization3.RPTS_TO_ORG_CD
      LEFT JOIN X000_Organization X000_Organization5 ON
        X000_Organization5.FIN_COA_CD = X000_Organization4.RPTS_TO_FIN_COA_CD AND X000_Organization5.ORG_CD =
        X000_Organization4.RPTS_TO_ORG_CD
      LEFT JOIN X000_Organization X000_Organization6 ON
        X000_Organization6.FIN_COA_CD = X000_Organization5.RPTS_TO_FIN_COA_CD AND X000_Organization6.ORG_CD =
        X000_Organization5.RPTS_TO_ORG_CD
      LEFT JOIN X000_Organization X000_Organization7 ON
        X000_Organization7.FIN_COA_CD = X000_Organization6.RPTS_TO_FIN_COA_CD AND X000_Organization7.ORG_CD =
        X000_Organization6.RPTS_TO_ORG_CD
    WHERE
      X000_Organization.ORG_BEGIN_DT >= Date("2018-01-01")
    ORDER BY
      X000_Organization.ORG_LVL,
      X000_Organization.ORG_NM
    """
    so_curs.execute("DROP TABLE IF EXISTS X000_Organization_struct")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: Organization structure")

    # Build account list ***********************************************************

    print("Build account list...")

    s_sql = "CREATE TABLE X000_Account AS " + """
    SELECT
      CA_ACCOUNT_T.FIN_COA_CD,
      CA_ACCOUNT_T.ACCOUNT_NBR,
      CA_ACCOUNT_TYPE_T.ACCT_TYP_NM,
      X000_Organization.ORG_NM,
      CA_ACCOUNT_T.ACCOUNT_NM,
      CA_ACCOUNT_T.ACCT_FSC_OFC_UID,
      CA_ACCOUNT_T.ACCT_SPVSR_UNVL_ID,
      CA_ACCOUNT_T.ACCT_MGR_UNVL_ID,
      CA_ACCOUNT_T.ORG_CD,
      CA_ACCOUNT_T.ACCT_TYP_CD,
      CA_ACCOUNT_T.ACCT_PHYS_CMP_CD,
      CA_ACCOUNT_T.ACCT_FRNG_BNFT_CD,
      CA_ACCOUNT_T.FIN_HGH_ED_FUNC_CD,
      CA_ACCOUNT_T.SUB_FUND_GRP_CD,
      CA_ACCOUNT_T.ACCT_RSTRC_STAT_CD,
      CA_ACCOUNT_T.ACCT_RSTRC_STAT_DT,
      CA_ACCOUNT_T.ACCT_CITY_NM,
      CA_ACCOUNT_T.ACCT_STATE_CD,
      CA_ACCOUNT_T.ACCT_STREET_ADDR,
      CA_ACCOUNT_T.ACCT_ZIP_CD,
      CA_ACCOUNT_T.RPTS_TO_FIN_COA_CD,
      CA_ACCOUNT_T.RPTS_TO_ACCT_NBR,
      CA_ACCOUNT_T.ACCT_CREATE_DT,
      CA_ACCOUNT_T.ACCT_EFFECT_DT,
      CA_ACCOUNT_T.ACCT_EXPIRATION_DT,
      CA_ACCOUNT_T.CONT_FIN_COA_CD,
      CA_ACCOUNT_T.CONT_ACCOUNT_NBR,
      CA_ACCOUNT_T.ACCT_CLOSED_IND,
      CA_ACCOUNT_T.OBJ_ID,
      CA_ACCOUNT_T.VER_NBR
    FROM
      CA_ACCOUNT_T
      LEFT JOIN X000_Organization ON X000_Organization.FIN_COA_CD = CA_ACCOUNT_T.FIN_COA_CD AND X000_Organization.ORG_CD =
        CA_ACCOUNT_T.ORG_CD
      LEFT JOIN CA_ACCOUNT_TYPE_T ON CA_ACCOUNT_TYPE_T.ACCT_TYP_CD = CA_ACCOUNT_T.ACCT_TYP_CD
    """
    so_curs.execute("DROP TABLE IF EXISTS X000_Account")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: Account list")

    # Build current transaction list ***********************************************

    print("Build current yr transaction list...")

    s_sql = "CREATE VIEW X000_GL_trans_curr AS " + """
    SELECT
      GL_ENTRY_T_CURR.UNIV_FISCAL_YR,
      GL_ENTRY_T_CURR.UNIV_FISCAL_PRD_CD,
      GL_ENTRY_T_CURR.CALC_COST_STRING,
      X000_Account.ORG_NM,
      X000_Account.ACCOUNT_NM,
      CA_OBJECT_CODE_T.FIN_OBJ_CD_NM,
      GL_ENTRY_T_CURR.TRANSACTION_DT,
      GL_ENTRY_T_CURR.FDOC_NBR,
      GL_ENTRY_T_CURR.CALC_AMOUNT,
      GL_ENTRY_T_CURR.TRN_LDGR_ENTR_DESC,
      X000_Account.ACCT_TYP_NM,
      GL_ENTRY_T_CURR.TRN_POST_DT,
      GL_ENTRY_T_CURR."TIMESTAMP",
      GL_ENTRY_T_CURR.FIN_COA_CD,
      GL_ENTRY_T_CURR.ACCOUNT_NBR,
      GL_ENTRY_T_CURR.FIN_OBJECT_CD,
      GL_ENTRY_T_CURR.FIN_BALANCE_TYP_CD,
      GL_ENTRY_T_CURR.FIN_OBJ_TYP_CD,
      GL_ENTRY_T_CURR.FDOC_TYP_CD,
      GL_ENTRY_T_CURR.FS_ORIGIN_CD,
      FS_ORIGIN_CODE_T.FS_DATABASE_DESC,
      GL_ENTRY_T_CURR.TRN_ENTR_SEQ_NBR,
      GL_ENTRY_T_CURR.FDOC_REF_TYP_CD,
      GL_ENTRY_T_CURR.FS_REF_ORIGIN_CD,
      GL_ENTRY_T_CURR.FDOC_REF_NBR,
      GL_ENTRY_T_CURR.FDOC_REVERSAL_DT,
      GL_ENTRY_T_CURR.TRN_ENCUM_UPDT_CD
    FROM
      GL_ENTRY_T_CURR
      LEFT JOIN X000_Account ON X000_Account.FIN_COA_CD = GL_ENTRY_T_CURR.FIN_COA_CD AND X000_Account.ACCOUNT_NBR =
        GL_ENTRY_T_CURR.ACCOUNT_NBR
      LEFT JOIN CA_OBJECT_CODE_T ON CA_OBJECT_CODE_T.UNIV_FISCAL_YR = GL_ENTRY_T_CURR.UNIV_FISCAL_YR AND
        CA_OBJECT_CODE_T.FIN_COA_CD = GL_ENTRY_T_CURR.FIN_COA_CD AND CA_OBJECT_CODE_T.FIN_OBJECT_CD =
        GL_ENTRY_T_CURR.FIN_OBJECT_CD
      LEFT JOIN FS_ORIGIN_CODE_T ON FS_ORIGIN_CODE_T.FS_ORIGIN_CD = GL_ENTRY_T_CURR.FS_ORIGIN_CD
    ORDER BY
      GL_ENTRY_T_CURR.CALC_COST_STRING,
      GL_ENTRY_T_CURR.UNIV_FISCAL_PRD_CD
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_GL_trans_curr")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: Current yr transaction list")

    # Build current transaction list ***********************************************

    print("Build previous yr transaction list...")

    s_sql = "CREATE VIEW X000_GL_trans_prev AS " + """
    SELECT
      GL_ENTRY_T_PREV.UNIV_FISCAL_YR,
      GL_ENTRY_T_PREV.UNIV_FISCAL_PRD_CD,
      GL_ENTRY_T_PREV.CALC_COST_STRING,
      X000_Account.ORG_NM,
      X000_Account.ACCOUNT_NM,
      CA_OBJECT_CODE_T.FIN_OBJ_CD_NM,
      GL_ENTRY_T_PREV.TRANSACTION_DT,
      GL_ENTRY_T_PREV.FDOC_NBR,
      GL_ENTRY_T_PREV.CALC_AMOUNT,
      GL_ENTRY_T_PREV.TRN_LDGR_ENTR_DESC,
      X000_Account.ACCT_TYP_NM,
      GL_ENTRY_T_PREV.TRN_POST_DT,
      GL_ENTRY_T_PREV."TIMESTAMP",
      GL_ENTRY_T_PREV.FIN_COA_CD,
      GL_ENTRY_T_PREV.ACCOUNT_NBR,
      GL_ENTRY_T_PREV.FIN_OBJECT_CD,
      GL_ENTRY_T_PREV.FIN_BALANCE_TYP_CD,
      GL_ENTRY_T_PREV.FIN_OBJ_TYP_CD,
      GL_ENTRY_T_PREV.FDOC_TYP_CD,
      GL_ENTRY_T_PREV.FS_ORIGIN_CD,
      FS_ORIGIN_CODE_T.FS_DATABASE_DESC,
      GL_ENTRY_T_PREV.TRN_ENTR_SEQ_NBR,
      GL_ENTRY_T_PREV.FDOC_REF_TYP_CD,
      GL_ENTRY_T_PREV.FS_REF_ORIGIN_CD,
      GL_ENTRY_T_PREV.FDOC_REF_NBR,
      GL_ENTRY_T_PREV.FDOC_REVERSAL_DT,
      GL_ENTRY_T_PREV.TRN_ENCUM_UPDT_CD
    FROM
      GL_ENTRY_T_PREV
      LEFT JOIN X000_Account ON X000_Account.FIN_COA_CD = GL_ENTRY_T_PREV.FIN_COA_CD AND X000_Account.ACCOUNT_NBR =
        GL_ENTRY_T_PREV.ACCOUNT_NBR
      LEFT JOIN CA_OBJECT_CODE_T ON CA_OBJECT_CODE_T.UNIV_FISCAL_YR = GL_ENTRY_T_PREV.UNIV_FISCAL_YR AND
        CA_OBJECT_CODE_T.FIN_COA_CD = GL_ENTRY_T_PREV.FIN_COA_CD AND CA_OBJECT_CODE_T.FIN_OBJECT_CD =
        GL_ENTRY_T_PREV.FIN_OBJECT_CD
      LEFT JOIN FS_ORIGIN_CODE_T ON FS_ORIGIN_CODE_T.FS_ORIGIN_CD = GL_ENTRY_T_PREV.FS_ORIGIN_CD
    ORDER BY
      GL_ENTRY_T_PREV.CALC_COST_STRING,
      GL_ENTRY_T_PREV.UNIV_FISCAL_PRD_CD
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_GL_trans_prev")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: Previous yr transaction list")

    """ ****************************************************************************
    VENDOR MASTER LIST
    *****************************************************************************"""
    print("VENDOR MASTER LIST")
    funcfile.writelog("VENDOR MASTER LIST")

    # BUILD VENDOR MASTER LIST (DELETE)
    sr_file = "X000_VENDOR"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)

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

    # JOIN VENDOR RM AND PO ADDRESSES
    print("Build vendor address master file...")
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

    # BUILD VENDOR BANK ACCOUNT TABLE
    print("Build vendor bank account table...")
    sr_file = "X001ad_vendor_bankacc"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    Select Distinct
        BANK.PAYEE_ID_NBR As VENDOR_ID,
        STUD.BNK_ACCT_NBR As STUD_BANK,
        STUDB.BNK_BRANCH_CD As STUD_BRANCH,
        STUD.PAYEE_ID_TYP_CD As STUD_TYPE,
        STUD.PAYEE_EMAIL_ADDR As STUD_MAIL,
        VEND.BNK_ACCT_NBR As VEND_BANK,
        VENDB.BNK_BRANCH_CD As VEND_BRANCH,
        VEND.PAYEE_ID_TYP_CD As VEND_TYPE,
        VEND.PAYEE_EMAIL_ADDR As VEND_MAIL,
        EMPL.BNK_ACCT_NBR As EMPL_BANK,
        EMPLB.BNK_BRANCH_CD As EMPL_BRANCH,
        EMPL.PAYEE_ID_TYP_CD As EMPL_TYPE,
        EMPL.PAYEE_EMAIL_ADDR As EMPL_MAIL
    From
        PDP_PAYEE_ACH_ACCT_T BANK
        Left Join PDP_PAYEE_ACH_ACCT_T STUD On STUD.PAYEE_ID_NBR = BANK.PAYEE_ID_NBR And STUD.PAYEE_ID_TYP_CD = 'S' And STUD.ROW_ACTV_IND = 'Y'
        Left Join PDP_PAYEE_ACH_ACCT_EXT_T STUDB On STUDB.ACH_ACCT_GNRTD_ID = STUD.ACH_ACCT_GNRTD_ID
        Left Join PDP_PAYEE_ACH_ACCT_T VEND On VEND.PAYEE_ID_NBR = BANK.PAYEE_ID_NBR And VEND.PAYEE_ID_TYP_CD = 'V' And VEND.ROW_ACTV_IND = 'Y'
        Left Join PDP_PAYEE_ACH_ACCT_EXT_T VENDB On VENDB.ACH_ACCT_GNRTD_ID = VEND.ACH_ACCT_GNRTD_ID
        Left Join PDP_PAYEE_ACH_ACCT_T EMPL On EMPL.PAYEE_ID_NBR = BANK.PAYEE_ID_NBR And EMPL.PAYEE_ID_TYP_CD = 'E' And EMPL.ROW_ACTV_IND = 'Y'
        Left Join PDP_PAYEE_ACH_ACCT_EXT_T EMPLB On EMPLB.ACH_ACCT_GNRTD_ID = EMPL.ACH_ACCT_GNRTD_ID
    Where
        BANK.ROW_ACTV_IND = 'Y'
    """
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)

    # BUILD VENDOR TABLE
    print("Build vendor master file...")
    sr_file = "X000_Vendor_master"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        DETAIL.VNDR_ID As VENDOR_ID,
        UPPER(DETAIL.VNDR_NM) AS VNDR_NM,
        DETAIL.VNDR_URL_ADDR,
        HEADER.VNDR_TAX_NBR,
        BANK.VEND_BANK,
        BANK.VEND_BRANCH,
        BANK.VEND_MAIL,
        BANK.EMPL_BANK,
        BANK.STUD_BANK,
        ADDR.FAX,
        ADDR.EMAIL,
        ADDR.ADDRESS,
        ADDR.URL,
        ADDR.STATE_CD,
        ADDR.COUNTRY_CD,
        HEADER.VNDR_TAX_TYP_CD,
        HEADER.VNDR_TYP_CD,
        DETAIL.VNDR_PMT_TERM_CD,
        DETAIL.VNDR_SHP_TTL_CD,
        DETAIL.VNDR_PARENT_IND,
        DETAIL.VNDR_1ST_LST_NM_IND,
        DETAIL.COLLECT_TAX_IND,
        HEADER.VNDR_FRGN_IND,
        DETAIL.VNDR_CNFM_IND,
        DETAIL.VNDR_PRPYMT_IND,
        DETAIL.VNDR_CCRD_IND,
        DETAIL.DOBJ_MAINT_CD_ACTV_IND,
        DETAIL.VNDR_INACTV_REAS_CD
    From
        PUR_VNDR_DTL_T DETAIL Left Join
        PUR_VNDR_HDR_T HEADER On HEADER.VNDR_HDR_GNRTD_ID = DETAIL.VNDR_HDR_GNRTD_ID Left Join
        X001ac_vendor_address_comb ADDR On ADDR.VENDOR_ID = DETAIL.VNDR_ID Left Join
        X001ad_vendor_bankacc BANK On BANK.VENDOR_ID = DETAIL.VNDR_ID
    Order by
        VNDR_NM,
        VENDOR_ID
    """
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)    

    """ ****************************************************************************
    DOCUMENTS MASTER LIST
    *****************************************************************************"""
    print("DOCUMENTS MASTER LIST")
    funcfile.writelog("DOCUMENTS MASTER LIST")

    # BUILD DOCS MASTER LIST
    print("Build docs master list...")
    sr_file = "X000_Documents"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        KREW_DOC_HDR_T.DOC_HDR_ID,
        KREW_DOC_HDR_T.DOC_TYP_ID,
        KREW_DOC_TYP_T.DOC_TYP_NM,
        KREW_DOC_TYP_T.LBL
    From
        KREW_DOC_HDR_T Inner Join
        KREW_DOC_TYP_T On KREW_DOC_TYP_T.DOC_TYP_ID = KREW_DOC_HDR_T.DOC_TYP_ID
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    BUILD ACCOUNTING LINES
    *****************************************************************************"""
    print("BUILD ACCOUNTING LINES")
    funcfile.writelog("BUILD ACCOUNTING LINES")

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
        LINE.FDOC_LINE_DESC
    From
        FP_ACCT_LINES_T LINE
    Where
        LINE.FDOC_POST_YR >= %PYEAR%
    Order By
        LINE.FDOC_NBR,
        LINE.FDOC_LINE_AMT
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%PYEAR%",funcdate.prev_year())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD ACCOUNTING LINES
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
    PAYMENT COMPLETED LISTS
    *****************************************************************************"""
    print("BUILD PAYMENT COMPLETED LISTS")
    funcfile.writelog("BUILD PAYMENT COMPLETED LISTS")

    # BUILD CURR UNIQUE LIST OF LAST COMPLETED
    print("Build current unique completed list...")
    sr_file = "X000_Completed_curr_last"
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
        Count(ROUTE.DOC_VER_NBR) As APP_COUNT
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

    # BUILD CURR APPROVALS ALL APPROVERS
    print("Build current completed list...")
    sr_file = "X000_Completed_curr"
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

    # BUILD PREV UNIQUE LIST OF LAST COMPLETED
    print("Build previous unique completed list...")
    sr_file = "X000_Completed_prev_last"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select Distinct
        ROUTE.ACTN_TKN_ID,
        ROUTE.DOC_HDR_ID,
        ROUTE.ACTN_DT,
        ROUTE.PRNCPL_ID,
        CASE
            WHEN PERSON.NAME_ADDR IS NULL THEN 'UNKNOWN'
            ELSE PERSON.NAME_ADDR
        END AS NAME_ADDR,
        ROUTE.ACTN_CD,
        CASE
            WHEN ROUTE.ACTN_CD = 'C' THEN 'COMPLETED'
            ELSE 'OTHER'
        END AS ACTN,
        ROUTE.ANNOTN,
        Count(ROUTE.DOC_VER_NBR) As APP_COUNT
    From
        KREW_ACTN_TKN_T_COM ROUTE Left Join
        PEOPLE.X002_PEOPLE_PREV_YEAR PERSON On PERSON.EMPLOYEE_NUMBER = ROUTE.PRNCPL_ID
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

    # BUILD PREV LIST WITH ALL COMPLETED
    print("Build previous completed list...")
    sr_file = "X000_Completed_prev"
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
        PEOPLE.X002_PEOPLE_PREV_YEAR PERSON On PERSON.EMPLOYEE_NUMBER = ROUTE.PRNCPL_ID
    Order By
        ROUTE.ACTN_DT,
        ROUTE.ACTN_TKN_ID
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    PAYMENT APPROVED LISTS
    *****************************************************************************"""
    print("BUILD PAYMENT APPROVED LISTS")
    funcfile.writelog("BUILD PAYMENT APPROVED LISTS")

    # BUILD CURR APPROVALS UNIQUE LIST OF LAST APPROVER
    print("Build current unique approvers list...")
    sr_file = "X000_Approvers_curr_last"
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

    # BUILD CURR APPROVALS ALL APPROVERS
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

    # BUILD PREV APPROVALS UNIQUE LIST WITH LAST APPROVER
    print("Build previous year unique approvers list...")
    sr_file = "X000_Approvers_prev_last"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select Distinct
        ROUTE.ACTN_TKN_ID,
        ROUTE.DOC_HDR_ID,
        ROUTE.ACTN_DT,
        ROUTE.PRNCPL_ID,
        CASE
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
        PEOPLE.X002_PEOPLE_PREV_YEAR PERSON On PERSON.EMPLOYEE_NUMBER = ROUTE.PRNCPL_ID
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

    # BUILD PREV APPROVALS WITH ALL APPROVERS
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

    """ ****************************************************************************
    BUILD CURRENT YEAR PAYMENTS
    *****************************************************************************"""
    print("BUILD CURRENT YEAR PAYMENTS")
    funcfile.writelog("BUILD CURRENT YEAR PAYMENTS")

    # BUILD PAYMENTS WITH LAST INITIATOR AND APPROVER
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
        DETAIL.NET_PMT_AMT,
        DOC.DOC_TYP_NM As DOC_TYPE,
        Upper(DOC.LBL) As DOC_LABEL,        
        COMP.PRNCPL_ID AS COMPLETE_EMP_NO,
        COMP.NAME_ADDR AS COMPLETE_EMP_NAME,
        COMP.ACTN_DT AS COMPLETE_DATE,
        COMP.ACTN AS COMPLETE_STATUS,
        COMP.ANNOTN AS COMPLETE_NOTE,
        APPROVE.PRNCPL_ID AS APPROVE_EMP_NO,
        APPROVE.NAME_ADDR AS APPROVE_EMP_NAME,
        APPROVE.ACTN_DT AS APPROVE_DATE,
        APPROVE.ACTN AS APPROVE_STATUS,
        APPROVE.ANNOTN AS APPROVE_NOTE,
        ACC.COST_STRING As ACC_COST_STRING,
        ACC.FDOC_LINE_DESC As ACC_DESC,
        ACC.COUNT_LINES As ACC_LINE_COUNT
    From
        PDP_PMT_GRP_T_CURR PAYMENT
        Left Join X000_VENDOR_MASTER VENDOR On VENDOR.VENDOR_ID = PAYMENT.PAYEE_ID
        Left Join PDP_PAYEE_ACH_ACCT_T PAYEE On PAYEE.PAYEE_ID_NBR = PAYMENT.PAYEE_ID And
            PAYEE.PAYEE_ID_TYP_CD = PAYMENT.PAYEE_ID_TYP_CD
        Left Join PDP_PAYEE_TYP_T TYPE ON TYPE.PAYEE_TYP_CD = PAYMENT.PAYEE_ID_TYP_CD
        Left Join PDP_PMT_STAT_CD_T STATUS On STATUS.PMT_STAT_CD = PAYMENT.PMT_STAT_CD
        Left Join PDP_PMT_DTL_T DETAIL On DETAIL.PMT_GRP_ID = PAYMENT.PMT_GRP_ID
        Left Join X000_Documents DOC On DOC.DOC_HDR_ID = DETAIL.CUST_PMT_DOC_NBR
        Left Join X000_Account_line_unique ACC On ACC.FDOC_NBR = DETAIL.CUST_PMT_DOC_NBR
        Left Join X000_Completed_curr_last COMP On COMP.DOC_HDR_ID = DETAIL.CUST_PMT_DOC_NBR    
        Left Join X000_Approvers_prev_last APPROVE On APPROVE.DOC_HDR_ID = DETAIL.CUST_PMT_DOC_NBR
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)    

    # BUILD PAYMENTS SUMMARY
    print("Build current year payments summary...")
    sr_file = "X001ab_Report_payments_curr_summ"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        X001aa_Report_payments_curr.VENDOR_ID,
        Max(X001aa_Report_payments_curr.PMT_DT) As Max_PMT_DT,
        Sum(X001aa_Report_payments_curr.NET_PMT_AMT) As Sum_NET_PMT_AMT,
        Count(X001aa_Report_payments_curr.VENDOR_ID) As Count_TRAN
    From
        X001aa_Report_payments_curr
    Group By
        X001aa_Report_payments_curr.VENDOR_ID
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD PAYMENT LIST WITH ALL APPROVERS
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
        DOC.DOC_TYP_NM As DOC_TYPE,
        Upper(DOC.LBL) As DOC_LABEL,        
        APPROVE.PRNCPL_ID AS APPROVE_EMP_NO,
        APPROVE.NAME_ADDR AS APPROVE_EMP_NAME,
        APPROVE.ACTN_DT AS APPROVE_DATE,
        APPROVE.ACTN AS APPROVE_STATUS,
        APPROVE.ANNOTN AS NOTE,
        ACC.COST_STRING As ACC_COST_STRING,
        ACC.FDOC_LINE_DESC As ACC_DESC,
        ACC.COUNT_LINES As ACC_LINE_COUNT        
    From
        X001aa_Report_payments_curr PAYMENT Left Join
        X000_Documents DOC On DOC.DOC_HDR_ID = PAYMENT.CUST_PMT_DOC_NBR Left Join
        X000_Account_line_unique ACC On ACC.FDOC_NBR = PAYMENT.CUST_PMT_DOC_NBR Left Join       
        X000_Approvers_curr APPROVE On APPROVE.DOC_HDR_ID = PAYMENT.CUST_PMT_DOC_NBR
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD PAYMENT LIST WITH ALL INITIATORS
    print("Build current payment full initiators list...")
    sr_file = "X001ad_Report_payments_initroute_curr"
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
        DOC.DOC_TYP_NM As DOC_TYPE,
        Upper(DOC.LBL) As DOC_LABEL,        
        INIT.PRNCPL_ID AS INIT_EMP_NO,
        INIT.NAME_ADDR AS INIT_EMP_NAME,
        INIT.ACTN_DT AS INIT_DATE,
        INIT.ACTN AS INIT_STATUS,
        INIT.ANNOTN AS NOTE,
        ACC.COST_STRING As ACC_COST_STRING,
        ACC.FDOC_LINE_DESC As ACC_DESC,
        ACC.COUNT_LINES As ACC_LINE_COUNT        
    From
        X001aa_Report_payments_curr PAYMENT Left Join
        X000_Documents DOC On DOC.DOC_HDR_ID = PAYMENT.CUST_PMT_DOC_NBR Left Join
        X000_Account_line_unique ACC On ACC.FDOC_NBR = PAYMENT.CUST_PMT_DOC_NBR Left Join       
        X000_Completed_curr INIT On INIT.DOC_HDR_ID = PAYMENT.CUST_PMT_DOC_NBR
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    BUILD PREVIOUS YEAR PAYMENTS
    *****************************************************************************"""
    print("BUILD PREVIOUS YEAR PAYMENTS")
    funcfile.writelog("BUILD PREVIOUS YEAR PAYMENTS")

    # BUILD PAYMENTS WITH LAST INITIATOR AND APPROVER
    print("Build previous year payments...")
    sr_file = "X001aa_Report_payments_prev"
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
        DETAIL.NET_PMT_AMT,
        DOC.DOC_TYP_NM As DOC_TYPE,
        Upper(DOC.LBL) As DOC_LABEL,        
        COMP.PRNCPL_ID AS COMPLETE_EMP_NO,
        COMP.NAME_ADDR AS COMPLETE_EMP_NAME,
        COMP.ACTN_DT AS COMPLETE_DATE,
        COMP.ACTN AS COMPLETE_STATUS,
        COMP.ANNOTN AS COMPLETE_NOTE,
        APPROVE.PRNCPL_ID AS APPROVE_EMP_NO,
        APPROVE.NAME_ADDR AS APPROVE_EMP_NAME,
        APPROVE.ACTN_DT AS APPROVE_DATE,
        APPROVE.ACTN AS APPROVE_STATUS,
        APPROVE.ANNOTN AS APPROVE_NOTE,
        ACC.COST_STRING As ACC_COST_STRING,
        ACC.FDOC_LINE_DESC As ACC_DESC,
        ACC.COUNT_LINES As ACC_LINE_COUNT        
    From
        PDP_PMT_GRP_T_PREV PAYMENT
        Left Join X000_VENDOR_MASTER VENDOR On VENDOR.VENDOR_ID = PAYMENT.PAYEE_ID
        Left Join PDP_PAYEE_ACH_ACCT_T PAYEE On PAYEE.PAYEE_ID_NBR = PAYMENT.PAYEE_ID And
            PAYEE.PAYEE_ID_TYP_CD = PAYMENT.PAYEE_ID_TYP_CD
        Left Join PDP_PAYEE_TYP_T TYPE ON TYPE.PAYEE_TYP_CD = PAYMENT.PAYEE_ID_TYP_CD
        Left Join PDP_PMT_STAT_CD_T STATUS On STATUS.PMT_STAT_CD = PAYMENT.PMT_STAT_CD
        Left Join PDP_PMT_DTL_T DETAIL On DETAIL.PMT_GRP_ID = PAYMENT.PMT_GRP_ID
        Left Join X000_Documents DOC On DOC.DOC_HDR_ID = DETAIL.CUST_PMT_DOC_NBR
        Left Join X000_Account_line_unique ACC On ACC.FDOC_NBR = DETAIL.CUST_PMT_DOC_NBR        
        Left Join X000_Completed_prev_last COMP On COMP.DOC_HDR_ID = DETAIL.CUST_PMT_DOC_NBR    
        Left Join X000_Approvers_prev_last APPROVE On APPROVE.DOC_HDR_ID = DETAIL.CUST_PMT_DOC_NBR
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD PAYMENTS SUMMARY
    print("Build previous year payments summary...")
    sr_file = "X001ab_Report_payments_prev_summ"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        X001aa_Report_payments_prev.VENDOR_ID,
        Max(X001aa_Report_payments_prev.PMT_DT) As Max_PMT_DT,
        Sum(X001aa_Report_payments_prev.NET_PMT_AMT) As Sum_NET_PMT_AMT,
        Count(X001aa_Report_payments_prev.VENDOR_ID) As Count_TRAN    
    From
        X001aa_Report_payments_prev
    Group By
        X001aa_Report_payments_prev.VENDOR_ID
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
        DOC.DOC_TYP_NM As DOC_TYPE,
        Upper(DOC.LBL) As DOC_LABEL,        
        APPROVE.PRNCPL_ID AS APPROVE_EMP_NO,
        APPROVE.NAME_ADDR AS APPROVE_EMP_NAME,
        APPROVE.ACTN_DT AS APPROVE_DATE,
        APPROVE.ACTN AS APPROVE_STATUS,
        APPROVE.ANNOTN AS NOTE,
        ACC.COST_STRING As ACC_COST_STRING,
        ACC.FDOC_LINE_DESC As ACC_DESC,
        ACC.COUNT_LINES As ACC_LINE_COUNT        
    From
        X001aa_Report_payments_prev PAYMENT Left Join
        X000_Documents DOC On DOC.DOC_HDR_ID = PAYMENT.CUST_PMT_DOC_NBR Left Join
        X000_Account_line_unique ACC On ACC.FDOC_NBR = PAYMENT.CUST_PMT_DOC_NBR Left Join       
        X000_Approvers_curr APPROVE On APPROVE.DOC_HDR_ID = PAYMENT.CUST_PMT_DOC_NBR
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    BUILD PAYMENT TYPE SUMMARY PER MONTH
    *****************************************************************************"""
    print("BUILD PAYMENT TYPE SUMMARY PER MONTH")
    funcfile.writelog("BUILD PAYMENT TYPE SUMMARY PER MONTH")

    # BUILD SUMMARY OF CURRENT YEAR PAYMENTS
    print("Build summary of current year payments...")
    sr_file = "X002aa_Report_paym_typemon_curr"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'CURRENT' As YEAR,
        PAYM.PAYEE_TYP_DESC As TYPE,
        PAYM.DOC_LABEL,
        SubStr(PAYM.PMT_DT, 6, 2) As MONTH,
        Sum(PAYM.NET_PMT_AMT) As Sum_NET_PMT_AMT
    From
        X001aa_Report_payments_curr PAYM
    Group By
        PAYM.PAYEE_TYP_DESC,
        PAYM.DOC_LABEL,    
        SubStr(PAYM.PMT_DT, 6, 2)
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD SUMMARY OF PREVIOUS YEAR PAYMENTS
    print("Build summary of previous year payments...")
    sr_file = "X002ab_Report_paym_typemon_prev"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'PREVIOUS' As YEAR,
        PAYM.PAYEE_TYP_DESC As TYPE,
        PAYM.DOC_LABEL,
        SubStr(PAYM.PMT_DT, 6, 2) As MONTH,
        Sum(PAYM.NET_PMT_AMT) As Sum_NET_PMT_AMT
    From
        X001aa_Report_payments_prev PAYM
    Group By
        PAYM.PAYEE_TYP_DESC,
        PAYM.DOC_LABEL,    
        SubStr(PAYM.PMT_DT, 6, 2)
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD SUMMARY OF PAYMENTS
    print("Build summary of payments per month...")
    sr_file = "X002ac_Report_payment_month"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.TYPE As TYPE,
        PREV.MONTH As MONTH,
        PREV.Sum_NET_PMT_AMT As 'PREVIOUS',
        CURR.Sum_NET_PMT_AMT As 'CURRENT'
    From
        X002ab_Report_paym_typemon_prev PREV Left Join
        X002aa_Report_paym_typemon_curr CURR On CURR.TYPE = PREV.TYPE
                And CURR.MONTH = PREV.MONTH
    """
    so_curs.execute("DROP TABLE IF EXISTS X002aa_Report_payment_month")
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
    if l_vacuum == True:
        print("Vacuum the database...")
        so_conn.commit()
        so_conn.execute('VACUUM')
        funcfile.writelog("%t DATABASE: Vacuum kfs")    
    so_conn.commit()
    so_conn.close()

    # Close the log writer *********************************************************
    funcfile.writelog("-------------------------")
    funcfile.writelog("COMPLETED: B002_KFS_LISTS")

    return
