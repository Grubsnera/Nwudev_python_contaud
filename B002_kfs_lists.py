"""
Script to build standard KFS lists
Created on: 11 Mar 2018
Copyright: Albert J v Rensburg (NWU:21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcfile

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
ORGANIZATION MASTER LIST
ACCOUNT MASTER LIST
VENDOR MASTER LIST
DOCUMENT MASTER LIST
END OF SCRIPT
*****************************************************************************"""


def kfs_lists():
    """
    Script to build standard KFS lists
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # OPEN THE LOG WRITER
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: B002_KFS_LISTS")
    funcfile.writelog("----------------------")
    print("--------------")
    print("B002_KFS_LISTS")
    print("--------------")

    # DECLARE VARIABLES
    so_file = "Kfs.sqlite"  # Source database
    so_path = "W:/Kfs/"  # Source database path
    l_vacuum = False  # Vacuum database

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
    so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """ ****************************************************************************
    ORGANIZATION MASTER LIST
    *****************************************************************************"""
    print("ORGANIZATION MASTER LIST")
    funcfile.writelog("ORGANIZATION MASTER LIST")

    # BUILD ORGANIZATION LIST
    print("Build organization...")
    sr_file = "X000_Organization"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        ORG.FIN_COA_CD,
        ORG.ORG_CD,
        ORG.ORG_TYP_CD,
        TYP.ORG_TYP_NM,
        ORG.ORG_NM,
        ORG.ORG_BEGIN_DT,
        ORG.ORG_END_DT,
        ORG.OBJ_ID,
        ORG.VER_NBR,
        ORG.ORG_MGR_UNVL_ID,
        ORG.RC_CD,
        ORG.ORG_PHYS_CMP_CD,
        ORG.ORG_DFLT_ACCT_NBR,
        ORG.ORG_LN1_ADDR,
        ORG.ORG_LN2_ADDR,
        ORG.ORG_CITY_NM,
        ORG.ORG_STATE_CD,
        ORG.ORG_ZIP_CD,
        ORG.ORG_CNTRY_CD,
        ORG.RPTS_TO_FIN_COA_CD,
        ORG.RPTS_TO_ORG_CD,
        ORG.ORG_ACTIVE_CD,
        ORG.ORG_IN_FP_CD,
        ORG.ORG_PLNT_ACCT_NBR,
        ORG.CMP_PLNT_ACCT_NBR,
        ORG.ORG_PLNT_COA_CD,
        ORG.CMP_PLNT_COA_CD,
        ORG.ORG_LVL
    From
       CA_ORG_T ORG Left Join
       CA_ORG_TYPE_T TYP ON TYP.ORG_TYP_CD = ORG.ORG_TYP_CD
    Order By
       ORG.FIN_COA_CD,
       ORG.ORG_LVL,
       TYP.ORG_TYP_NM,
       ORG.ORG_NM,
       ORG.ORG_BEGIN_DT,
       ORG.ORG_END_DT
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: Organization list")

    # BUILD ORGANIZATION STRUCTURE
    print("Build organization structure...")
    sr_file = "X000_Organization_struct"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        ORS.FIN_COA_CD,
        ORS.ORG_CD,
        ORS.ORG_TYP_CD,
        ORS.ORG_TYP_NM,
        ORS.ORG_NM,
        ORS.ORG_MGR_UNVL_ID,
        ORS.ORG_LVL,
        ORA.FIN_COA_CD AS FIN_COA_CD1,
        ORA.ORG_CD AS ORG_CD1,
        ORA.ORG_TYP_CD AS ORG_TYP_CD1,
        ORA.ORG_TYP_NM AS ORG_TYP_NM1,
        ORA.ORG_NM AS ORG_NM1,
        ORA.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID1,
        ORA.ORG_LVL AS ORG_LVL1,
        ORB.FIN_COA_CD AS FIN_COA_CD2,
        ORB.ORG_CD AS ORG_CD2,
        ORB.ORG_TYP_CD AS ORG_TYP_CD2,
        ORB.ORG_TYP_NM AS ORG_TYP_NM2,
        ORB.ORG_NM AS ORG_NM2,
        ORB.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID2,
        ORB.ORG_LVL AS ORG_LVL2,
        ORC.FIN_COA_CD AS FIN_COA_CD3,
        ORC.ORG_CD AS ORG_CD3,
        ORC.ORG_TYP_CD AS ORG_TYP_CD3,
        ORC.ORG_TYP_NM AS ORG_TYP_NM3,
        ORC.ORG_NM AS ORG_NM3,
        ORC.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID3,
        ORC.ORG_LVL AS ORG_LVL3,
        ORD.FIN_COA_CD AS FIN_COA_CD4,
        ORD.ORG_CD AS ORG_CD4,
        ORD.ORG_TYP_CD AS ORG_TYP_CD4,
        ORD.ORG_TYP_NM AS ORG_TYP_NM4,
        ORD.ORG_NM AS ORG_NM4,
        ORD.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID4,
        ORD.ORG_LVL AS ORG_LVL4,
        ORE.FIN_COA_CD AS FIN_COA_CD5,
        ORE.ORG_CD AS ORG_CD5,
        ORE.ORG_TYP_CD AS ORG_TYP_CD5,
        ORE.ORG_TYP_NM AS ORG_TYP_NM5,
        ORE.ORG_NM AS ORG_NM5,
        ORE.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID5,
        ORE.ORG_LVL AS ORG_LVL5,
        ORF.FIN_COA_CD AS FIN_COA_CD6,
        ORF.ORG_CD AS ORG_CD6,
        ORF.ORG_TYP_CD AS ORG_TYP_CD6,
        ORF.ORG_TYP_NM AS ORG_TYP_NM6,
        ORF.ORG_NM AS ORG_NM6,
        ORF.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID6,
        ORF.ORG_LVL AS ORG_LVL6,
        ORG.FIN_COA_CD AS FIN_COA_CD7,
        ORG.ORG_CD AS ORG_CD7,
        ORG.ORG_TYP_CD AS ORG_TYP_CD7,
        ORG.ORG_TYP_NM AS ORG_TYP_NM7,
        ORG.ORG_NM AS ORG_NM7,
        ORG.ORG_MGR_UNVL_ID AS ORG_MGR_UNVL_ID7,
        ORG.ORG_LVL AS ORG_LVL7
    From
        X000_Organization ORS Left Join
        X000_Organization ORA ON ORA.FIN_COA_CD = ORS.RPTS_TO_FIN_COA_CD And
            ORA.ORG_CD = ORS.RPTS_TO_ORG_CD Left Join
        X000_Organization ORB ON ORB.FIN_COA_CD = ORA.RPTS_TO_FIN_COA_CD AND
            ORB.ORG_CD = ORA.RPTS_TO_ORG_CD Left Join
        X000_Organization ORC ON ORC.FIN_COA_CD = ORB.RPTS_TO_FIN_COA_CD AND
            ORC.ORG_CD = ORB.RPTS_TO_ORG_CD Left Join
        X000_Organization ORD ON ORD.FIN_COA_CD = ORC.RPTS_TO_FIN_COA_CD AND
            ORD.ORG_CD = ORC.RPTS_TO_ORG_CD Left Join
        X000_Organization ORE ON ORE.FIN_COA_CD = ORD.RPTS_TO_FIN_COA_CD AND
            ORE.ORG_CD = ORD.RPTS_TO_ORG_CD Left Join
        X000_Organization ORF ON ORF.FIN_COA_CD = ORE.RPTS_TO_FIN_COA_CD AND
            ORF.ORG_CD = ORE.RPTS_TO_ORG_CD Left Join
        X000_Organization ORG ON ORG.FIN_COA_CD = ORF.RPTS_TO_FIN_COA_CD AND
            ORG.ORG_CD = ORF.RPTS_TO_ORG_CD
    Where
        ORS.ORG_BEGIN_DT >= Date("2018-01-01")
    Order By
        ORG.ORG_LVL,
        ORG.ORG_NM
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: Organization structure")

    """ ****************************************************************************
    ACCOUNT MASTER LIST
    *****************************************************************************"""
    print("ACCOUNT MASTER LIST")
    funcfile.writelog("ACCOUNT MASTER LIST")

    # BUILD ACCOUNT LIST
    print("Build account list...")
    sr_file = "X000_Account"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        ACC.FIN_COA_CD,
        ACC.ACCOUNT_NBR,
        TYP.ACCT_TYP_NM,
        ORG.ORG_NM,
        ACC.ACCOUNT_NM,
        ACC.ACCT_FSC_OFC_UID,
        ACC.ACCT_SPVSR_UNVL_ID,
        ACC.ACCT_MGR_UNVL_ID,
        ACC.ORG_CD,
        ACC.ACCT_TYP_CD,
        ACC.ACCT_PHYS_CMP_CD,
        ACC.ACCT_FRNG_BNFT_CD,
        ACC.FIN_HGH_ED_FUNC_CD,
        ACC.SUB_FUND_GRP_CD,
        ACC.ACCT_RSTRC_STAT_CD,
        ACC.ACCT_RSTRC_STAT_DT,
        ACC.ACCT_CITY_NM,
        ACC.ACCT_STATE_CD,
        ACC.ACCT_STREET_ADDR,
        ACC.ACCT_ZIP_CD,
        ACC.RPTS_TO_FIN_COA_CD,
        ACC.RPTS_TO_ACCT_NBR,
        ACC.ACCT_CREATE_DT,
        ACC.ACCT_EFFECT_DT,
        ACC.ACCT_EXPIRATION_DT,
        ACC.CONT_FIN_COA_CD,
        ACC.CONT_ACCOUNT_NBR,
        ACC.ACCT_CLOSED_IND,
        ACC.OBJ_ID,
        ACC.VER_NBR
    From
        CA_ACCOUNT_T ACC Left Join
        X000_Organization ORG ON ORG.FIN_COA_CD = ACC.FIN_COA_CD AND ORG.ORG_CD = ACC.ORG_CD Left Join
        CA_ACCOUNT_TYPE_T TYP ON TYP.ACCT_TYP_CD = ACC.ACCT_TYP_CD
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: Account list")

    """ ****************************************************************************
    VENDOR MASTER LIST
    *****************************************************************************"""
    print("VENDOR MASTER LIST")
    funcfile.writelog("VENDOR MASTER LIST")

    # BUILD TABLE WITH VENDOR REMITTANCE ADDRESSES
    print("Build vendor remittance addresses...")
    sr_file = "X001aa_vendor_rm_address"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        CAST(TRIM(UPPER(ADDR.VNDR_HDR_GNRTD_ID))||'-'||TRIM(UPPER(ADDR.VNDR_DTL_ASND_ID)) AS
            TEXT) VENDOR_ID,
        ADDR.VNDR_ST_CD,
        ADDR.VNDR_CNTRY_CD,
        ADDR.VNDR_ADDR_EMAIL_ADDR,
        ADDR.VNDR_B2B_URL_ADDR,
        ADDR.VNDR_FAX_NBR,
        TRIM(UPPER(ADDR.VNDR_DFLT_ADDR_IND))||'~'||
        TRIM(UPPER(ADDR.VNDR_ATTN_NM))||'~'||
        TRIM(UPPER(ADDR.VNDR_LN1_ADDR))||'~'||
        TRIM(UPPER(ADDR.VNDR_LN2_ADDR))||'~'||
        TRIM(UPPER(ADDR.VNDR_CTY_NM))||'~'||
        TRIM(UPPER(ADDR.VNDR_ZIP_CD))||'~'||
        TRIM(UPPER(ADDR.VNDR_CNTRY_CD))
        ADDRESS_RM
    From
        PUR_VNDR_ADDR_T ADDR
    Where
        ADDR.VNDR_ADDR_TYP_CD = 'RM' And
        ADDR.VNDR_DFLT_ADDR_IND = 'Y'
    Group By
        ADDR.VNDR_HDR_GNRTD_ID,
        ADDR.VNDR_DTL_ASND_ID    
    """
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD TABLE WITH VENDOR PURCHASE ORDER ADDRESSES
    print("Build vendor purchase order addresses...")
    sr_file = "X001ab_vendor_po_address"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        CAST(TRIM(UPPER(ADDR.VNDR_HDR_GNRTD_ID))||'-'||TRIM(UPPER(ADDR.VNDR_DTL_ASND_ID))
            AS TEXT) VENDOR_ID,
        ADDR.VNDR_ST_CD,
        ADDR.VNDR_CNTRY_CD,
        ADDR.VNDR_ADDR_EMAIL_ADDR,
        ADDR.VNDR_B2B_URL_ADDR,
        ADDR.VNDR_FAX_NBR,
        TRIM(UPPER(ADDR.VNDR_DFLT_ADDR_IND))||'~'||
        TRIM(UPPER(ADDR.VNDR_ATTN_NM))||'~'||
        TRIM(UPPER(ADDR.VNDR_LN1_ADDR))||'~'||
        TRIM(UPPER(ADDR.VNDR_LN2_ADDR))||'~'||
        TRIM(UPPER(ADDR.VNDR_CTY_NM))||'~'||
        TRIM(UPPER(ADDR.VNDR_ZIP_CD))||'~'||
        TRIM(UPPER(ADDR.VNDR_CNTRY_CD))
        ADDRESS_PO
    From
        PUR_VNDR_ADDR_T ADDR
    Where
        ADDR.VNDR_ADDR_TYP_CD = 'PO' And
        ADDR.VNDR_DFLT_ADDR_IND = 'Y'
    Group By
        ADDR.VNDR_HDR_GNRTD_ID,
        ADDR.VNDR_DTL_ASND_ID    
    """
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # JOIN VENDOR RM AND PO ADDRESSES
    print("Build vendor address master file...")
    sr_file = "X001ac_vendor_address_comb"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        VENDOR.VNDR_ID As VENDOR_ID,
        Case
            When ADDRM.VNDR_ST_CD <> '' Then ADDRM.VNDR_ST_CD
            Else ADDPO.VNDR_ST_CD
        End as STATE_CD,
        Case
            When ADDRM.VNDR_CNTRY_CD <> '' Then ADDRM.VNDR_CNTRY_CD
            Else ADDPO.VNDR_CNTRY_CD
        End as COUNTRY_CD,
        Case
            When ADDRM.VNDR_ADDR_EMAIL_ADDR <> '' Then
                Lower(ADDRM.VNDR_ADDR_EMAIL_ADDR)
            Else Lower(ADDPO.VNDR_ADDR_EMAIL_ADDR)
        End as EMAIL,
        Case
            When ADDRM.VNDR_B2B_URL_ADDR <> '' Then Lower(ADDRM.VNDR_B2B_URL_ADDR)
            Else Lower(ADDPO.VNDR_B2B_URL_ADDR)
        End as URL,
        Case
            When ADDRM.VNDR_FAX_NBR <> '' Then ADDRM.VNDR_FAX_NBR
            Else ADDPO.VNDR_FAX_NBR
        End as FAX,
        Case
            When ADDRM.ADDRESS_RM <> '' Then Upper(ADDRM.ADDRESS_RM)
            Else Upper(ADDPO.ADDRESS_PO)
        End as ADDRESS
    From
        PUR_VNDR_DTL_T VENDOR Left Join
        X001aa_vendor_rm_address ADDRM On ADDRM.VENDOR_ID = VENDOR.VNDR_ID Left Join
        X001ab_vendor_po_address ADDPO On ADDPO.VENDOR_ID = VENDOR.VNDR_ID
    """
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD VENDOR BANK ACCOUNT TABLE
    print("Build vendor bank account table...")
    sr_file = "X001ad_vendor_bankacc"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
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
        Left Join PDP_PAYEE_ACH_ACCT_T STUD On STUD.PAYEE_ID_NBR = BANK.PAYEE_ID_NBR And STUD.PAYEE_ID_TYP_CD = 'S' And
            STUD.ROW_ACTV_IND = 'Y'
        Left Join PDP_PAYEE_ACH_ACCT_EXT_T STUDB On STUDB.ACH_ACCT_GNRTD_ID = STUD.ACH_ACCT_GNRTD_ID
        Left Join PDP_PAYEE_ACH_ACCT_T VEND On VEND.PAYEE_ID_NBR = BANK.PAYEE_ID_NBR And VEND.PAYEE_ID_TYP_CD = 'V' And
            VEND.ROW_ACTV_IND = 'Y'
        Left Join PDP_PAYEE_ACH_ACCT_EXT_T VENDB On VENDB.ACH_ACCT_GNRTD_ID = VEND.ACH_ACCT_GNRTD_ID
        Left Join PDP_PAYEE_ACH_ACCT_T EMPL On EMPL.PAYEE_ID_NBR = BANK.PAYEE_ID_NBR And EMPL.PAYEE_ID_TYP_CD = 'E' And
            EMPL.ROW_ACTV_IND = 'Y'
        Left Join PDP_PAYEE_ACH_ACCT_EXT_T EMPLB On EMPLB.ACH_ACCT_GNRTD_ID = EMPL.ACH_ACCT_GNRTD_ID
    Where
        BANK.ROW_ACTV_IND = 'Y'
    """
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD CONTACT NAMES EMAIL PHONE MOBILE LIST
    print("Build contact email phone and mobile list...")
    sr_file = "X001ae_vendor_contact"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "Create View " + sr_file + " As" + """
    Select
        CONT.VNDR_HDR_GNRTD_ID,
        CONT.VNDR_CNTCT_GNRTD_ID,
        CONT.VNDR_CNTCT_TYP_CD As CONTACT_TYPE,
        Trim(Upper(CONT.VNDR_CNTCT_NM)) As CONTACT,
        Trim(Upper(CONT.VNDR_ATTN_NM)) As ATTENTION,
        Lower(CONT.VNDR_CNTCT_EMAIL_ADDR) As EMAIL,
        PHON.VNDR_PHN_NBR As PHONE,
        MOBI.VNDR_PHN_NBR As MOBILE
    From
        PUR_VNDR_CNTCT_T CONT Left Join
        PUR_VNDR_CNTCT_PHN_NBR_T PHON On PHON.VNDR_CNTCT_GNRTD_ID = CONT.VNDR_CNTCT_GNRTD_ID And PHON.VNDR_PHN_TYP_CD = 'PH' Left Join
        PUR_VNDR_CNTCT_PHN_NBR_T MOBI On MOBI.VNDR_CNTCT_GNRTD_ID = CONT.VNDR_CNTCT_GNRTD_ID And MOBI.VNDR_PHN_TYP_CD = 'MB'
    Group By
        CONT.VNDR_HDR_GNRTD_ID
    Order By
        CONT.VNDR_HDR_GNRTD_ID,
        CONTACT_TYPE
    ;"""
    # s_sql = s_sql.replace("%PERIOD%", s_period)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD VENDOR PHONE MOBILE LIST
    print("Build vendor phone and mobile list...")
    sr_file = "X001af_vendor_phone"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "Create View " + sr_file + " As" + """
    Select
        PHON.VNDR_HDR_GNRTD_ID,
        PHON.VNDR_PHN_GNRTD_ID,
        LAND.VNDR_PHN_NBR As PHONE,
        MOBI.VNDR_PHN_NBR As MOBILE
    From
        PUR_VNDR_PHN_NBR_T PHON Left Join
        PUR_VNDR_PHN_NBR_T LAND On LAND.VNDR_PHN_GNRTD_ID = PHON.VNDR_PHN_GNRTD_ID And LAND.VNDR_PHN_TYP_CD = 'PH' Left Join
        PUR_VNDR_PHN_NBR_T MOBI On MOBI.VNDR_PHN_GNRTD_ID = PHON.VNDR_PHN_GNRTD_ID And MOBI.VNDR_PHN_TYP_CD = 'MB'
    Group By
        PHON.VNDR_HDR_GNRTD_ID
    Order By
        PHON.VNDR_HDR_GNRTD_ID
    ;"""
    # s_sql = s_sql.replace("%PERIOD%", s_period)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD VENDOR MASTER CONTACT LIST
    print("Build vendor master contact list...")
    sr_file = "X001ag_contact_comb"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "Create View " + sr_file + " As" + """
    Select
        VEND.VNDR_ID As VENDOR_ID,
        PHON.PHONE,
        PHON.MOBILE,
        CONT.CONTACT,
        CONT.ATTENTION,
        CONT.EMAIL,
        CONT.PHONE As PHONEC,
        CONT.MOBILE As MOBILEC
    From
        PUR_VNDR_DTL_T VEND Left Join
        X001af_vendor_phone PHON On PHON.VNDR_HDR_GNRTD_ID = VEND.VNDR_HDR_GNRTD_ID Left Join
        X001ae_vendor_contact CONT On CONT.VNDR_HDR_GNRTD_ID = VEND.VNDR_HDR_GNRTD_ID
    ;"""
    # s_sql = s_sql.replace("%PERIOD%", s_period)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD VENDOR MASTER COMBINED CONTACT LIST
    print("Build vendor master combined contact list...")
    sr_file = "X000_Contact"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As" + """
    Select
        CONT.VENDOR_ID AS VENDOR_ID,
        CONT.CONTACT,
        CONT.ATTENTION,
        CONT.EMAIL,
        CONT.PHONE,
        CONT.MOBILE,
        CONT.PHONEC AS PHONEC,
        CONT.MOBILEC AS MOBILEC,
        CASE
            WHEN CONT.PHONE != '' THEN Replace(Trim(CONT.PHONE),' ','') || '~'
            ELSE ''
        END As NUMBERS
    From
        X001ag_contact_comb CONT
    Order By
        VENDOR_ID    
    ;"""
    # s_sql = s_sql.replace("%PERIOD%", s_period)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # UPDATE NUMBERS COLUMN WITH MOBILE
    print("Update numbers with mobile...")
    so_curs.execute("Update X000_Contact " + """
                    Set NUMBERS = 
                    Case
                        When Trim(MOBILE) != '' And Instr(NUMBERS, Replace(Trim(MOBILE), ' ', '')) != 0 THEN NUMBERS
                        When Trim(MOBILE) != '' THEN NUMBERS || Replace(Trim(MOBILE),' ','') || '~'
                        Else NUMBERS
                    End
                    ;""")

    # UPDATE NUMBERS COLUMN WITH PHONEC
    print("Update numbers with phonec...")
    so_curs.execute("Update X000_Contact " + """
                    Set NUMBERS = 
                    Case
                        When Trim(PHONEC) != '' And Instr(NUMBERS, Replace(Trim(PHONEC), ' ', '')) != 0 THEN NUMBERS
                        When Trim(PHONEC) != '' THEN NUMBERS || Replace(Trim(PHONEC),' ','') || '~'
                        Else NUMBERS
                    End
                    ;""")

    # UPDATE NUMBERS COLUMN WITH MOBILEC
    print("Update numbers with mobilec...")
    so_curs.execute("Update X000_Contact " + """
                    Set NUMBERS = 
                    Case
                        When Trim(MOBILEC) != '' And Instr(NUMBERS, Replace(Trim(MOBILEC), ' ', '')) != 0 THEN NUMBERS
                        When Trim(MOBILEC) != '' THEN NUMBERS || Replace(Trim(MOBILEC),' ','') || '~'
                        Else NUMBERS
                    End
                    ;""")

    # UPDATE NUMBERS REMOVE SPECIAL CHARACTERS FROM NUMBERS
    for i in range(5):
        print("Remove special characters...")
        so_curs.execute("Update X000_Contact " + """
                        Set NUMBERS = 
                        Case
                            When NUMBERS Like('%-%') Then Replace(NUMBERS, '-', '')
                            When NUMBERS Like('%(%') Then Replace(NUMBERS, '(', '')
                            When NUMBERS Like('%)%') Then Replace(NUMBERS, ')', '')
                            When NUMBERS Like('%*%') Then Replace(NUMBERS, '*', '')
                            When NUMBERS Like('%;%') Then Replace(NUMBERS, ';', '')                        
                            When NUMBERS Like('%.%') Then Replace(NUMBERS, '.', '')                        
                            When NUMBERS Like('%+27%') Then Replace(NUMBERS, '+27', '0')
                            When NUMBERS Like('%UNKNOWN%') Then Replace(NUMBERS, 'UNKNOWN'||'~', '')
                            When NUMBERS Like('%geennommerbeskikbaar%') Then Replace(NUMBERS, 'geennommerbeskikbaar'||'~', '')
                            When NUMBERS Like('%NONE%') Then Replace(NUMBERS, 'NONE'||'~', '')
                            When NUMBERS Like('%N/A%') Then Replace(NUMBERS, 'N/A'||'~', '')
                            When NUMBERS Like('%Fon:%') Then Replace(NUMBERS, 'Fon:', '')
                            When NUMBERS Like('%Tel:%') Then Replace(NUMBERS, 'Tel:', '')
                            When Trim(EMAIL) != '' And Instr(NUMBERS,EMAIL) > 0 THEN Replace(NUMBERS, EMAIL || '~', '')
                            When NUMBERS Like('%O%') Then Replace(NUMBERS, 'O', '0')
                            Else NUMBERS
                        End
                        ;""")

    # TRIM UNWANTED CHARACTERS
    print("Trim unwanted characters...")
    so_curs.execute("Update X000_Contact " + """
                    Set NUMBERS = Trim(NUMBERS,'~')
                    ;""")

    # BUILD VENDOR TABLE
    print("Build vendor master file...")
    sr_file = "X000_Vendor"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
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
        CONT.NUMBERS,
        ADDR.FAX,
        ADDR.EMAIL,
        CONT.CONTACT,
        CONT.ATTENTION,
        CONT.EMAIL AS EMAIL_CONTACT,
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
        X001ad_vendor_bankacc BANK On BANK.VENDOR_ID = DETAIL.VNDR_ID Left Join
        X000_Contact CONT On CONT.VENDOR_ID = DETAIL.VNDR_ID
    Order by
        VNDR_NM,
        VENDOR_ID
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    DOCUMENT MASTER LIST
    *****************************************************************************"""
    print("DOCUMENTS MASTER LIST")
    funcfile.writelog("DOCUMENTS MASTER LIST")

    # BUILD DOCS MASTER LIST
    print("Build docs master list...")
    sr_file = "X000_Document"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        DOC.DOC_HDR_ID,
        DOC.DOC_TYP_ID,
        TYP.DOC_TYP_NM,
        TYP.LBL
    From
        KREW_DOC_HDR_T DOC Inner Join
        KREW_DOC_TYP_T TYP On TYP.DOC_TYP_ID = DOC.DOC_TYP_ID
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
    if l_vacuum:
        print("Vacuum the database...")
        so_conn.commit()
        so_conn.execute('VACUUM')
        funcfile.writelog("%t VACUUM DATABASE: " + so_file)
    so_conn.commit()
    so_conn.close()

    # Close the log writer *********************************************************
    funcfile.writelog("-------------------------")
    funcfile.writelog("COMPLETED: B002_KFS_LISTS")

    return
