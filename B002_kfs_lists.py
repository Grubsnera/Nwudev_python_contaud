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
BUILD CURRENT YEAR PAYMENTS
BUILD PREVIOUS YEAR PAYMENTS
END OF SCRIPT
*****************************************************************************"""

def Kfs_lists():

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

    # Open the SOURCE file
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()

    funcfile.writelog("%t OPEN DATABASE: KFS")

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
        PDP_PAYEE_ACH_ACCT_T.PAYEE_ID_NBR As VENDOR_ID,
        STUD.BNK_ACCT_NBR As STUD_BANK,
        STUD.PAYEE_ID_TYP_CD As STUD_TYPE,
        STUD.PAYEE_EMAIL_ADDR As STUD_MAIL,
        VEND.BNK_ACCT_NBR As VEND_BANK,
        VEND.PAYEE_ID_TYP_CD As VEND_TYPE,
        VEND.PAYEE_EMAIL_ADDR As VEND_MAIL,
        EMPL.BNK_ACCT_NBR As EMPL_BANK,
        EMPL.PAYEE_ID_TYP_CD As EMPL_TYPE,
        EMPL.PAYEE_EMAIL_ADDR As EMPL_MAIL
    From
        PDP_PAYEE_ACH_ACCT_T
        Left Join PDP_PAYEE_ACH_ACCT_T STUD On STUD.PAYEE_ID_NBR = PDP_PAYEE_ACH_ACCT_T.PAYEE_ID_NBR And STUD.PAYEE_ID_TYP_CD = 'S' And STUD.ROW_ACTV_IND = 'Y'
        Left Join PDP_PAYEE_ACH_ACCT_T VEND On VEND.PAYEE_ID_NBR = PDP_PAYEE_ACH_ACCT_T.PAYEE_ID_NBR And VEND.PAYEE_ID_TYP_CD = 'V' And VEND.ROW_ACTV_IND = 'Y'
        Left Join PDP_PAYEE_ACH_ACCT_T EMPL On EMPL.PAYEE_ID_NBR = PDP_PAYEE_ACH_ACCT_T.PAYEE_ID_NBR And EMPL.PAYEE_ID_TYP_CD = 'E' And EMPL.ROW_ACTV_IND = 'Y'
    Where
        PDP_PAYEE_ACH_ACCT_T.ROW_ACTV_IND = 'Y'
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
    BUILD CURRENT YEAR PAYMENTS
    *****************************************************************************"""
    print("BUILD CURRENT YEAR PAYMENTS")
    funcfile.writelog("BUILD CURRENT YEAR PAYMENTS")

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

    """ ****************************************************************************
    BUILD PREVIOUS YEAR PAYMENTS
    *****************************************************************************"""
    print("BUILD PREVIOUS YEAR PAYMENTS")
    funcfile.writelog("BUILD PREVIOUS YEAR PAYMENTS")

    # BUILD PAYMENTS
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
        DETAIL.NET_PMT_AMT
    From
        PDP_PMT_GRP_T_PREV PAYMENT
        Left Join X000_VENDOR_MASTER VENDOR On VENDOR.VENDOR_ID = PAYMENT.PAYEE_ID
        Left Join PDP_PAYEE_ACH_ACCT_T PAYEE On PAYEE.PAYEE_ID_NBR = PAYMENT.PAYEE_ID And
            PAYEE.PAYEE_ID_TYP_CD = PAYMENT.PAYEE_ID_TYP_CD
        Left Join PDP_PAYEE_TYP_T TYPE ON TYPE.PAYEE_TYP_CD = PAYMENT.PAYEE_ID_TYP_CD
        Left Join PDP_PMT_STAT_CD_T STATUS On STATUS.PMT_STAT_CD = PAYMENT.PMT_STAT_CD
        Left Join PDP_PMT_DTL_T DETAIL On DETAIL.PMT_GRP_ID = PAYMENT.PMT_GRP_ID
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
