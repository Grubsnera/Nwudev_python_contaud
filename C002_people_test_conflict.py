"""
Script to test PEOPLE conflict of interest
Created on: 8 Apr 2019
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcsms
from _my_modules import funcsys
from _my_modules import functest

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
BUILD CONFLICT MASTER TABLES
BUILD ANNUAL TABLES
BUILD DASHBOARD TABLES
BANK NUMBER MASTER FILES
TEST EMPLOYEE VENDOR SHARE BANK ACCOUNT (V1.1.2)
TEST EMPLOYEE VENDOR SHARE EMAIL ADDRESS (V2.0.3)
TEST EMPLOYEE NO DECLARATION (V2.0.4)
END OF SCRIPT
*****************************************************************************"""


def people_test_conflict():
    """
    Script to test PEOPLE conflict of interest
    :return:
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # DECLARE VARIABLES
    so_path = "W:/People_conflict/"   # Source database path
    re_path = "R:/People/" + funcdate.cur_year() + "/"  # Results path
    ed_path = "S:/_external_data/"   # external data path
    so_file = "People_conflict.sqlite"  # Source database
    l_debug: bool = False
    l_export: bool = False
    l_mail: bool = False
    l_mess: bool = True
    l_record: bool = True

    # OPEN THE SCRIPT LOG FILE
    print("-----------------------------")
    print("C002_PEOPLE_TEST_CONFLICT_DEV")
    print("-----------------------------")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C002_PEOPLE_TEST_CONFLICT_DEV")
    funcfile.writelog("-------------------------------------")

    if l_mess:
        funcsms.send_telegram('', 'administrator', '<b>C002 People conflict interest tests</b>')

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN THE WORKING DATABASE
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH DATA SOURCES
    so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
    funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss_curr.sqlite' AS 'VSSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: VSS_CURR.SQLITE")

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """ ****************************************************************************
    BUILD CONFLICT MASTER TABLES
    *****************************************************************************"""
    print("CONFLICT MASTER TABLES")
    funcfile.writelog("CONFLICT MASTER TABLES")

    # BUILD DECLARATIONS MASTER TABLE
    print("Build declarations master table...")
    sr_file = "X000_declarations_all"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        COI_DECLARATIONS.DECLARATION_ID,
        COI_DECLARATIONS.EMPLOYEE_NUMBER,
        COI_DECLARATIONS.DECLARATION_DATE,
        COI_DECLARATIONS.UNDERSTAND_POLICY_FLAG,
        COI_DECLARATIONS.INTEREST_TO_DECLARE_FLAG,
        COI_DECLARATIONS.FULL_DISCLOSURE_FLAG,
        PEOPLE.HR_LOOKUPS.MEANING AS STATUS,
        COI_DECLARATIONS.LINE_MANAGER,
        COI_DECLARATIONS.REJECTION_REASON,
        COI_DECLARATIONS.CREATION_DATE,
        COI_DECLARATIONS.AUDIT_USER,
        COI_DECLARATIONS.LAST_UPDATE_DATE,
        COI_DECLARATIONS.LAST_UPDATED_BY,
        COI_DECLARATIONS.EXTERNAL_REFERENCE
    From
      COI_DECLARATIONS
      Left Join PEOPLE.HR_LOOKUPS ON PEOPLE.HR_LOOKUPS.LOOKUP_CODE = COI_DECLARATIONS.STATUS_ID And
          HR_LOOKUPS.LOOKUP_TYPE = "NWU_COI_STATUS" And
          PEOPLE.HR_LOOKUPS.START_DATE_ACTIVE <= COI_DECLARATIONS.DECLARATION_DATE And
          PEOPLE.HR_LOOKUPS.END_DATE_ACTIVE >= COI_DECLARATIONS.DECLARATION_DATE
    Order By
      COI_DECLARATIONS.EMPLOYEE_NUMBER,
      COI_DECLARATIONS.DECLARATION_DATE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # BUILD INTERESTS MASTER TABLE
    print("Build interests master table...")
    sr_file = "X000_interests_all"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        COI_INTERESTS.INTEREST_ID,
        COI_INTERESTS.DECLARATION_ID,
        COI_DECLARATIONS.EMPLOYEE_NUMBER,
        COI_DECLARATIONS.DECLARATION_DATE,
        COI_INTERESTS.CONFLICT_TYPE_ID,
        HR_LOOKUPS2.MEANING AS CONFLICT_TYPE,
        COI_INTERESTS.INTEREST_TYPE_ID,
        HR_LOOKUPS1.MEANING AS INTEREST_TYPE,
        COI_INTERESTS.STATUS_ID,
        HR_LOOKUPS3.MEANING AS INTEREST_STATUS,
        COI_INTERESTS.PERC_SHARE_INTEREST,
        COI_INTERESTS.ENTITY_NAME,
        COI_INTERESTS.ENTITY_REGISTRATION_NUMBER,
        COI_INTERESTS.OFFICE_ADDRESS,
        COI_INTERESTS.DESCRIPTION,
        COI_INTERESTS.DIR_APPOINTMENT_DATE,
        COI_INTERESTS.LINE_MANAGER,
        COI_INTERESTS.NEXT_LINE_MANAGER,
        COI_INTERESTS.INDUSTRY_CLASS_ID,
        PEOPLE.HR_LOOKUPS.MEANING AS INDUSTRY_TYPE,
        COI_INTERESTS.TASK_PERF_AGREEMENT,
        COI_INTERESTS.MITIGATION_AGREEMENT,
        COI_INTERESTS.REJECTION_REASON,
        COI_INTERESTS.CREATION_DATE,
        COI_INTERESTS.AUDIT_USER,
        COI_INTERESTS.LAST_UPDATE_DATE,
        COI_INTERESTS.LAST_UPDATED_BY,
        COI_INTERESTS.EXTERNAL_REFERENCE
    From
      COI_INTERESTS
      Left Join COI_DECLARATIONS ON COI_DECLARATIONS.DECLARATION_ID = COI_INTERESTS.DECLARATION_ID
      Left Join PEOPLE.HR_LOOKUPS ON PEOPLE.HR_LOOKUPS.LOOKUP_CODE = COI_INTERESTS.INTEREST_TYPE_ID AND
          HR_LOOKUPS.LOOKUP_TYPE = "NWU_COI_INDUSTRY_CLASS"
      Left Join PEOPLE.HR_LOOKUPS HR_LOOKUPS1 ON HR_LOOKUPS1.LOOKUP_CODE = COI_INTERESTS.INTEREST_TYPE_ID AND
          HR_LOOKUPS1.LOOKUP_TYPE = "NWU_COI_INTEREST_TYPES"
      Left Join PEOPLE.HR_LOOKUPS HR_LOOKUPS2 ON HR_LOOKUPS2.LOOKUP_CODE = COI_INTERESTS.CONFLICT_TYPE_ID AND
          HR_LOOKUPS2.LOOKUP_TYPE = "NWU_COI_CONFLICT_TYPE"
      Left Join PEOPLE.HR_LOOKUPS HR_LOOKUPS3 ON HR_LOOKUPS3.LOOKUP_CODE = COI_INTERESTS.STATUS_ID AND
          HR_LOOKUPS3.LOOKUP_TYPE = "NWU_COI_STATUS"
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    """ ****************************************************************************
    BUILD ANNUAL TABLES
    *****************************************************************************"""
    print("CURRENT YEAR DECLARATIONS")
    funcfile.writelog("CURRENT YEAR DECLARATIONS")

    # BUILD CURRENT YEAR DECLARATIONS
    print("Build current declarations...")
    sr_file = "X001_declarations_curr"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X000_declarations_all.DECLARATION_ID,
        X000_declarations_all.EMPLOYEE_NUMBER AS EMPLOYEE,
        EMPLOYEE.LAST_NAME AS EMP_SURNAME,
        EMPLOYEE.NAME_ADDR AS EMP_NAME,
        EMPLOYEE.ADDRESS_SARS AS EMP_ADD_SARS,
        EMPLOYEE.ADDRESS_POST AS EMP_ADD_POST,
        EMPLOYEE.EMP_ACTIVE,    
        EMPLOYEE.EMAIL_ADDRESS AS EMP_MAIL,
        EMPLOYEE.POSITION_FULL AS EMP_POSITION,    
        X000_declarations_all.DECLARATION_DATE,
        X000_declarations_all.UNDERSTAND_POLICY_FLAG,
        X000_declarations_all.INTEREST_TO_DECLARE_FLAG,
        X000_declarations_all.FULL_DISCLOSURE_FLAG,
        X000_declarations_all.STATUS,
        X000_declarations_all.LINE_MANAGER,
        MANAGER.LAST_NAME AS MAN_SURNAME,
        MANAGER.NAME_ADDR AS MAN_NAME,
        MANAGER.ADDRESS_SARS AS MAN_ADD_SARS,
        MANAGER.ADDRESS_POST AS MAN_ADD_POST,
        MANAGER.EMP_ACTIVE AS MAN_ACTIVE,    
        MANAGER.EMAIL_ADDRESS AS MAN_MAIL,
        MANAGER.POSITION_FULL AS MAN_POSITION,    
        X000_declarations_all.REJECTION_REASON,
        X000_declarations_all.CREATION_DATE,
        X000_declarations_all.AUDIT_USER,
        X000_declarations_all.LAST_UPDATE_DATE,
        X000_declarations_all.LAST_UPDATED_BY,
        X000_declarations_all.EXTERNAL_REFERENCE,
        EMPLOYEE.SUPERVISOR,
        SUPERVISOR.LAST_NAME AS SUP_SURNAME,
        SUPERVISOR.NAME_ADDR AS SUP_NAME,
        SUPERVISOR.EMP_ACTIVE AS SUP_ACTIVE,    
        SUPERVISOR.EMAIL_ADDRESS AS SUP_MAIL,
        SUPERVISOR.POSITION_FULL AS SUP_POSITION
    From
        X000_declarations_all Left Join
        PEOPLE.X002_PEOPLE_CURR_YEAR EMPLOYEE ON EMPLOYEE.EMPLOYEE_NUMBER = X000_declarations_all.EMPLOYEE_NUMBER Left join
        PEOPLE.X002_PEOPLE_CURR_YEAR MANAGER ON MANAGER.EMPLOYEE_NUMBER = X000_declarations_all.LINE_MANAGER Left Join
        PEOPLE.X002_PEOPLE_CURR_YEAR SUPERVISOR ON SUPERVISOR.EMPLOYEE_NUMBER = EMPLOYEE.SUPERVISOR
    Where
        X000_declarations_all.DECLARATION_DATE >= Date("%CYEARB%") AND
        X000_declarations_all.DECLARATION_DATE <= Date("%CYEARE%")
    Order By
        EMPLOYEE.LAST_NAME,
        EMPLOYEE.NAME_ADDR,
        X000_declarations_all.LAST_UPDATE_DATE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    s_sqlp = s_sql
    s_sql = s_sql.replace("%CYEARB%", funcdate.cur_yearbegin())
    s_sql = s_sql.replace("%CYEARE%", funcdate.cur_yearend())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # BUILD PREVIOUS YEAR DECLARATIONS
    print("Build previous declarations...")
    sr_file = "X001_declarations_prev"
    s_sql = s_sqlp
    s_sql = s_sql.replace("X001_declarations_curr", sr_file)
    s_sql = s_sql.replace("X002_PEOPLE_CURR_YEAR", "X002_PEOPLE_PREV_YEAR")
    s_sql = s_sql.replace("%CYEARB%", funcdate.prev_yearbegin())
    s_sql = s_sql.replace("%CYEARE%", funcdate.prev_yearend())
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # BUILD THE CURRENT YEAR INTERESTS
    print("Build current interests...")
    sr_file = "X002_interests_curr"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X000_interests_all.DECLARATION_ID,
        X000_interests_all.INTEREST_ID,
        X000_interests_all.EMPLOYEE_NUMBER,
        EMPLOYEE.LAST_NAME AS EMP_SURNAME,
        EMPLOYEE.NAME_ADDR AS EMP_NAME,
        EMPLOYEE.ADDRESS_SARS AS EMP_ADD_SARS,
        EMPLOYEE.ADDRESS_POST AS EMP_ADD_POST,
        EMPLOYEE.EMP_ACTIVE,    
        EMPLOYEE.EMAIL_ADDRESS AS EMP_MAIL,
        EMPLOYEE.POSITION_FULL AS EMP_POSITION,    
        X000_interests_all.DECLARATION_DATE,
        X000_interests_all.CONFLICT_TYPE,
        X000_interests_all.INTEREST_TYPE,
        X000_interests_all.PERC_SHARE_INTEREST,
        X000_interests_all.INDUSTRY_TYPE,
        X000_interests_all.ENTITY_NAME,
        X000_interests_all.ENTITY_REGISTRATION_NUMBER,
        X000_interests_all.OFFICE_ADDRESS,
        X000_interests_all.DESCRIPTION,
        X000_interests_all.INTEREST_STATUS,
        X000_interests_all.LINE_MANAGER,
        MANAGER.LAST_NAME AS MAN_SURNAME,
        MANAGER.NAME_ADDR AS MAN_NAME,
        MANAGER.ADDRESS_SARS AS MAN_ADD_SARS,
        MANAGER.ADDRESS_POST AS MAN_ADD_POST,
        MANAGER.EMP_ACTIVE AS MAN_ACTIVE,    
        MANAGER.EMAIL_ADDRESS AS MAN_MAIL,
        MANAGER.POSITION_FULL AS MAN_POSITION,    
        X000_interests_all.NEXT_LINE_MANAGER,
        NEXTMANAGER.LAST_NAME AS NMAN_SURNAME,
        NEXTMANAGER.NAME_ADDR AS NMAN_NAME,
        NEXTMANAGER.ADDRESS_SARS AS NMAN_ADD_SARS,
        NEXTMANAGER.ADDRESS_POST AS NMAN_ADD_POST,
        NEXTMANAGER.EMP_ACTIVE AS NMAN_ACTIVE,    
        NEXTMANAGER.EMAIL_ADDRESS AS NMAN_MAIL,
        NEXTMANAGER.POSITION_FULL AS NMAN_POSITION,    
        X000_interests_all.DIR_APPOINTMENT_DATE,
        X000_interests_all.TASK_PERF_AGREEMENT,
        X000_interests_all.MITIGATION_AGREEMENT,
        X000_interests_all.REJECTION_REASON,
        X000_interests_all.CREATION_DATE,
        X000_interests_all.AUDIT_USER,
        X000_interests_all.LAST_UPDATE_DATE,
        X000_interests_all.LAST_UPDATED_BY,
        X000_interests_all.EXTERNAL_REFERENCE,
        EMPLOYEE.SUPERVISOR,
        SUPERVISOR.LAST_NAME AS SUP_SURNAME,
        SUPERVISOR.NAME_ADDR AS SUP_NAME,
        SUPERVISOR.EMP_ACTIVE AS SUP_ACTIVE,    
        SUPERVISOR.EMAIL_ADDRESS AS SUP_MAIL,
        SUPERVISOR.POSITION_FULL AS SUP_POSITION
    From
        X001_declarations_curr
        Inner Join X000_interests_all ON X000_interests_all.DECLARATION_ID = X001_declarations_curr.DECLARATION_ID
        Left Join PEOPLE.X002_PEOPLE_CURR_YEAR EMPLOYEE ON EMPLOYEE.EMPLOYEE_NUMBER = X000_interests_all.EMPLOYEE_NUMBER
        Left Join PEOPLE.X002_PEOPLE_CURR_YEAR MANAGER ON MANAGER.EMPLOYEE_NUMBER = X000_interests_all.LINE_MANAGER
        Left Join PEOPLE.X002_PEOPLE_CURR_YEAR NEXTMANAGER ON NEXTMANAGER.EMPLOYEE_NUMBER = X000_interests_all.NEXT_LINE_MANAGER
        Left Join PEOPLE.X002_PEOPLE_CURR_YEAR SUPERVISOR ON SUPERVISOR.EMPLOYEE_NUMBER = EMPLOYEE.SUPERVISOR
    Where
        X001_declarations_curr.INTEREST_TO_DECLARE_FLAG == "Y"
    Order By
        EMPLOYEE.LAST_NAME,
        EMPLOYEE.NAME_ADDR,
        X000_interests_all.LAST_UPDATE_DATE
    """
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    s_sqlp = s_sql
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # BUILD PREVIOUS YEAR INTERESTS
    print("Build previous interests...")
    sr_file = "X002_interests_prev"
    s_sql = s_sqlp
    s_sql = s_sql.replace("X002_interests_curr", sr_file)
    s_sql = s_sql.replace("X001_declarations_curr", "X001_declarations_prev")
    s_sql = s_sql.replace("X002_PEOPLE_CURR_YEAR", "X002_PEOPLE_PREV_YEAR")
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    """ ****************************************************************************
    BUILD DASHBOARD TABLES
    *****************************************************************************"""
    print("BUILD DASHBOARD TABLES")
    funcfile.writelog("BUILD DASHBOARD TABLES")

    # BUILD CURRENT DECLARATION DASHBOARD PEOPLE
    print("Build current declaration dashboard people data...")
    sr_file = "X003aa_people_curr"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        PERSON.EMPLOYEE_NUMBER AS EMPLOYEE,
        PERSON.NAME_ADDR AS NAME,
        Case
            When PERSON.SEX = 'F' Then 'FEMALE'
            Else 'MALE'
        End As GENDER,
        Upper(PERSON.RACE_DESC) As RACE,
        Upper(PERSON.LANG_DESC) As LANGUAGE,
        Upper(PERSON.LOCATION_DESCRIPTION) As LOCATION,
        Upper(PERSON.ACAD_SUPP) As ACAD_SUPP,
        Upper(PERSON.FACULTY) As FACULTY,
        Upper(PERSON.DIVISION) As DIVISION,
        PERSON.EMPLOYMENT_CATEGORY As CATEGORY,
        Upper(PERSON.GRADE_CALC) As POS_GRADE,
        Upper(PERSON.JOB_NAME) As JOB_NAME,
        Upper(PERSON.PERSON_TYPE) As PERSON_TYPE,
        PERSON.AGE,
        PERSON.SUPERVISOR,
        PERSON.EMP_START
    From
        PEOPLE.X002_PEOPLE_CURR PERSON 
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # BUILD CURRENT DECLARATION DASHBOARD DECLARATION DATA
    print("Build current declaration dashboard unique declarations...")
    sr_file = "X003ab_declarations_curr"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        DECLARE.DECLARATION_ID,
        DECLARE.EMPLOYEE,
        DECLARE.DECLARATION_DATE,
        Max(DECLARE.CREATION_DATE) As CREATION_DATE,
        Case
            When UNDERSTAND_POLICY_FLAG = 'N' Then 'NO'
            When UNDERSTAND_POLICY_FLAG = 'Y' Then 'YES'
            Else 'NA'
        End As UNDERSTAND_POLICY,
        Case
            When INTEREST_TO_DECLARE_FLAG = 'N' Then 'NO'
            When INTEREST_TO_DECLARE_FLAG = 'Y' Then 'YES'
            Else 'NA'
        End As INTERESTS_TO_DECLARE,
        Case
            When FULL_DISCLOSURE_FLAG = 'N' Then 'NO'
            When FULL_DISCLOSURE_FLAG = 'Y' Then 'YES'
            Else 'NA'
        End As FULL_DISCLOSURE,
        Upper(STATUS) AS DECLARE_STATUS,
        DECLARE.LINE_MANAGER,
        Upper(DECLARE.REJECTION_REASON) As REJECTION_REASON
    From
        X001_declarations_curr DECLARE
    Group By
        DECLARE.EMPLOYEE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # BUILD CURRENT DECLARATION DASHBOARD DATA
    print("Build current declaration dashboard data...")
    sr_file = "X003_dashboard_curr"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        *,
        Case
            When DECLARE.DECLARE_STATUS IS NOT NULL Then DECLARE.DECLARE_STATUS
            Else 'NO DECLARATION'
        End As DECLARED
    From
        X003aa_people_curr PEOPLE Left Join
        X003ab_declarations_curr DECLARE On DECLARE.EMPLOYEE = PEOPLE.EMPLOYEE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # BUILD PREVIOUS DECLARATION DASHBOARD PEOPLE
    print("Build previous declaration dashboard people data...")
    sr_file = "X003aa_people_prev"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        PERSON.EMPLOYEE_NUMBER AS EMPLOYEE,
        PERSON.NAME_ADDR AS NAME,
        Case
            When PERSON.SEX = 'F' Then 'FEMALE'
            Else 'MALE'
        End As GENDER,
        Upper(PERSON.RACE_DESC) As RACE,
        Upper(PERSON.LANG_DESC) As LANGUAGE,
        Upper(PERSON.LOCATION_DESCRIPTION) As LOCATION,
        Upper(PERSON.ACAD_SUPP) As ACAD_SUPP,
        Upper(PERSON.FACULTY) As FACULTY,
        Upper(PERSON.DIVISION) As DIVISION,
        PERSON.EMPLOYMENT_CATEGORY As CATEGORY,
        Upper(PERSON.GRADE_CALC) As POS_GRADE,
        Upper(PERSON.JOB_NAME) As JOB_NAME,
        Upper(PERSON.PERSON_TYPE) As PERSON_TYPE,
        PERSON.AGE,
        PERSON.SUPERVISOR,
        PERSON.EMP_START
    From
        PEOPLE.X002_PEOPLE_PREV_YEAR PERSON
    Where
        PERSON.EMP_ACTIVE = 'Y'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # BUILD PREVIOUS DECLARATION DASHBOARD DECLARATION DATA
    print("Build previous declaration dashboard unique declarations...")
    sr_file = "X003ab_declarations_prev"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        DECLARE.DECLARATION_ID,
        DECLARE.EMPLOYEE,
        DECLARE.DECLARATION_DATE,
        Max(DECLARE.CREATION_DATE) As CREATION_DATE,
        Case
            When UNDERSTAND_POLICY_FLAG = 'N' Then 'NO'
            When UNDERSTAND_POLICY_FLAG = 'Y' Then 'YES'
            Else 'NA'
        End As UNDERSTAND_POLICY,
        Case
            When INTEREST_TO_DECLARE_FLAG = 'N' Then 'NO'
            When INTEREST_TO_DECLARE_FLAG = 'Y' Then 'YES'
            Else 'NA'
        End As INTERESTS_TO_DECLARE,
        Case
            When FULL_DISCLOSURE_FLAG = 'N' Then 'NO'
            When FULL_DISCLOSURE_FLAG = 'Y' Then 'YES'
            Else 'NA'
        End As FULL_DISCLOSURE,
        Upper(STATUS) AS DECLARE_STATUS,
        DECLARE.LINE_MANAGER,
        Upper(DECLARE.REJECTION_REASON) As REJECTION_REASON
    From
        X001_declarations_prev DECLARE
    Group By
        DECLARE.EMPLOYEE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # BUILD PREVIOUS DECLARATION DASHBOARD DATA
    print("Build previous declaration dashboard data...")
    sr_file = "X003_dashboard_prev"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        *,
        Case
            When DECLARE.DECLARE_STATUS IS NOT NULL Then DECLARE.DECLARE_STATUS
            Else 'NO DECLARATION'
        End As DECLARED
    From
        X003aa_people_prev PEOPLE Left Join
        X003ab_declarations_prev DECLARE On DECLARE.EMPLOYEE = PEOPLE.EMPLOYEE
    Where
        PEOPLE.EMPLOYEE IS NOT NULL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # DELETE CALCULATION FILES
    so_curs.execute("DROP TABLE IF EXISTS X003aa_people_curr")
    so_curs.execute("DROP TABLE IF EXISTS X003ab_declarations_curr")
    so_curs.execute("DROP TABLE IF EXISTS X003aa_people_prev")
    so_curs.execute("DROP TABLE IF EXISTS X003ab_declarations_prev")

    """ ****************************************************************************
    BANK NUMBER MASTER FILES
    *****************************************************************************"""

    # BUILD TABLE WITH EMPLOYEE BANK ACCOUNT NUMBERS
    print("Obtain master list of all employees...")
    sr_file = "X100_bank_emp"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        'NWU' AS ORG,
        Substr(LOCATION_DESCRIPTION,1,3) As LOC,
        PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER AS EMP,
        PEOPLE.X002_PEOPLE_CURR.ACC_NUMBER AS EMP_BANK
    From
        PEOPLE.X002_PEOPLE_CURR
    Where
        PEOPLE.X002_PEOPLE_CURR.ACC_NUMBER <> ''
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # BUILD TABLE WITH VENDOR BANK ACCOUNT NUMBERS
    print("Obtain master list of all vendors...")
    sr_file = "X100_bank_ven"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        KFS.X000_Vendor.VENDOR_ID,
        KFS.X000_Vendor.VEND_BANK AS VENDOR_BANK,
        KFS.X000_Vendor.VNDR_NM
    From
        KFS.X000_Vendor
    Where
        KFS.X000_Vendor.VEND_BANK <> '' AND
        KFS.X000_Vendor.DOBJ_MAINT_CD_ACTV_IND = 'Y' AND
        Instr(KFS.X000_Vendor.VNDR_NM, "DT CARD") = 0         
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    """ ****************************************************************************
    TEST EMPLOYEE VENDOR SHARE BANK ACCOUNT
    *****************************************************************************"""
    print("TEST EMPLOYEE VENDOR COMMON BANK")
    funcfile.writelog("TEST EMPLOYEE VENDOR COMMON BANK")

    # DECLARE TEST VARIABLES
    i_coun: int = 0  # Number of new findings to report
    s_desc: str = "Employee vendor share bank acc"

    # BUILD TABLE WITH VENDOR BANK ACCOUNT NUMBERS
    print("Merge employees and vendors on bank account...")
    sr_file = "X100aa_bank_empven"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        BANK.*,
        VEND.*   
    From
        X100_bank_emp BANK Inner Join
        X100_bank_ven VEND On VEND.VENDOR_BANK = BANK.EMP_BANK And
            Instr(VEND.VENDOR_ID, BANK.EMP) = 0
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD TABLE WITH VENDOR BANK ACCOUNT NUMBERS
    print("Compile list of shared bank accounts...")
    sr_file = "X100ab_bank_empven"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        FINDING.ORG,
        FINDING.LOC,
        FINDING.EMP,
        FINDING.EMP_BANK,
        FINDING.VENDOR_ID,
        FINDING.VENDOR_BANK
    From
        X100aa_bank_empven FINDING
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_find) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_find) + " EMPL BANK conflict finding(s)")

    # GET PREVIOUS FINDINGS
    if i_find > 0:
        functest.get_previous_finding(so_curs, ed_path, "002_reported.txt", "bank_share_emp_ven", "ITTTT")
        so_conn.commit()

    # SET PREVIOUS FINDINGS
    if i_find > 0:
        functest.set_previous_finding(so_curs)
        so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = "X100ad_bank_addprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        SELECT
            FIND.*,
            'bank_share_emp_ven' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%TODAYPLUS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        FROM
            X100ab_bank_empven FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = FIND.EMP AND PREV.FIELD2 = FIND.EMP_BANK
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X100ae_bank_newprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
            PREV.PROCESS,
            PREV.EMP AS FIELD1,
            PREV.EMP_BANK AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        FROM
            X100ad_bank_addprev PREV
        WHERE
            PREV.PREV_PROCESS IS NULL Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(so_curs, sr_file)
        if i_coun > 0:
            print("*** " + str(i_coun) + " Finding(s) to report ***")
            sr_filet = sr_file
            sx_path = ed_path
            sx_file = "002_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_coun) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_find > 0 and i_coun > 0:
        functest.get_officer(so_curs, "HR", "TEST_BANKACC_CONFLICT_VENDOR_OFFICER")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_find > 0 and i_coun > 0:
        functest.get_supervisor(so_curs, "HR", "TEST_BANKACC_CONFLICT_VENDOR_SUPERVISOR")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X100ah_bank_addempven"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.*,
            PERSON.NAME_ADDR AS EMP_NAME,
            PERSON.ACC_TYPE AS BANKACC_TYPE,
            PERSON.ACC_BRANCH AS BANKACC_BRANCH,
            PERSON.ACC_RELATION AS BANKACC_RELATION,
            PERSON.PERSON_TYPE,
            VENDOR.VNDR_NM AS VENDOR_NAME,
            DECLARE.DECLARATION_DATE AS DECLARE_DATE,
            DECLARE.STATUS AS DECLARE_STATUS,
            DECLARE.INTEREST_TO_DECLARE_FLAG AS DECLARE_INTEREST,
            PAYMENTS.LAST_PMT_DT AS PAY_DATE_LAST,
            PAYMENTS.TRAN_COUNT AS PAY_NO_TRAN,
            PAYMENTS.NET_PMT_AMT AS PAY_TOTAL_AMOUNT,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL,
            AUD_OFF.EMPLOYEE_NUMBER As AUD_OFF_NUMB,
            AUD_OFF.NAME_ADDR As AUD_OFF_NAME,
            AUD_OFF.EMAIL_ADDRESS As AUD_OFF_MAIL,
            AUD_SUP.EMPLOYEE_NUMBER As AUD_SUP_NUMB,
            AUD_SUP.NAME_ADDR As AUD_SUP_NAME,
            AUD_SUP.EMAIL_ADDRESS As AUD_SUP_MAIL
        From
            X100ad_bank_addprev PREV
            Left Join PEOPLE.X002_PEOPLE_CURR PERSON On PERSON.EMPLOYEE_NUMBER = PREV.EMP
            Left Join KFS.X000_Vendor VENDOR On VENDOR.VENDOR_ID = PREV.VENDOR_ID
            Left Join KFSCURR.X002aa_Report_payments_summary PAYMENTS On PAYMENTS.VENDOR_ID = PREV.VENDOR_ID
            Left Join X001_declarations_curr DECLARE On DECLARE.EMPLOYEE = PREV.EMP
            Left Join Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC
            Left Join Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
            Left Join Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD'
            Left Join Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC
            Left Join Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
            Left Join Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD'
        WHERE
            PREV.PREV_PROCESS IS NULL Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""    
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X100ax_bank_emp_vend"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_find > 0 and i_coun > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'EMPL VENDOR SAME BANK ACC' As AUDIT_FINDING,
            FIND.ORG AS ORGANIZATION,
            FIND.LOC AS LOCATION,
            FIND.EMP AS EMPLOYEE_NUMBER,
            FIND.EMP_NAME AS EMPLOYEE_NAME,
            FIND.PERSON_TYPE,
            FIND.EMP_BANK AS EMPLOYEE_BANK,
            FIND.BANKACC_TYPE,
            FIND.BANKACC_BRANCH,
            FIND.BANKACC_RELATION,
            FIND.DECLARE_DATE,
            FIND.DECLARE_STATUS,
            FIND.DECLARE_INTEREST,
            FIND.VENDOR_ID,
            FIND.VENDOR_NAME,
            FIND.VENDOR_BANK,
            FIND.PAY_DATE_LAST,
            FIND.PAY_NO_TRAN,
            FIND.PAY_TOTAL_AMOUNT,
            FIND.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
            FIND.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
            FIND.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
            FIND.CAMP_SUP_NAME AS SUPERVISOR,
            FIND.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
            FIND.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
            FIND.ORG_OFF_NAME AS ORG_OFFICER,
            FIND.ORG_OFF_NUMB AS ORG_OFFICER_NUMB,
            FIND.ORG_OFF_MAIL AS ORG_OFFICER_MAIL,
            FIND.ORG_SUP_NAME AS ORG_SUPERVISOR,
            FIND.ORG_SUP_NUMB AS ORG_SUPERVISOR_NUMB,
            FIND.ORG_SUP_MAIL AS ORG_SUPERVISOR_MAIL,
            FIND.AUD_OFF_NAME AS AUDIT_OFFICER,
            FIND.AUD_OFF_NUMB AS AUDIT_OFFICER_NUMB,
            FIND.AUD_OFF_MAIL AS AUDIT_OFFICER_MAIL,
            FIND.AUD_SUP_NAME AS AUDIT_SUPERVISOR,
            FIND.AUD_SUP_NUMB AS AUDIT_SUPERVISOR_NUMB,
            FIND.AUD_SUP_MAIL AS AUDIT_SUPERVISOR_MAIL
        From
            X100ah_bank_addempven FIND
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sr_filet = sr_file
            sx_path = re_path
            sx_file = "People_test_100ax_bank_emp_vend_"
            sx_filet = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    TEST EMPLOYEE VENDOR SHARE EMAIL ADDRESS
    *****************************************************************************"""
    print("TEST EMPLOYEE VENDOR SHARE EMAIL ADDRESS")
    funcfile.writelog("TEST EMPLOYEE VENDOR SHARE EMAIL ADDRESS")

    # FILES NEEDED
    # X020bx_Student_master_sort

    # DECLARE TEST VARIABLES
    i_finding_after: int = 0
    s_description = "Employee vendor share email add"
    s_file_name: str = "employee_vendor_share_email"
    s_file_prefix: str = "X100b"
    s_finding: str = "VENDOR EMAIL INVALID"
    s_report_file: str = "002_reported.txt"

    # OBTAIN TEST DATA FOR CURRENT ACTIVE VENDORS - NEW VENDORS
    print("Obtain test data...")
    sr_file: str = s_file_prefix + "aaa_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        VEND.VENDOR_ID,
        Lower(VEND.VEND_MAIL) As VENDOR_MAIL
    From
        KFS.X000_Vendor VEND
    Where
        VEND.DOBJ_MAINT_CD_ACTV_IND = 'Y'
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # GET PREVIOUS VENDORS - NEW VENDORS
    sr_file: str = s_file_prefix + "aab_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Import previous vendors...")
    so_curs.execute("CREATE TABLE " + sr_file + "(VENDOR_ID_PREV TEXT,VENDOR_MAIL_PREV TEXT)")
    co = open(ed_path + "201_vendor_new.csv", "r")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "VENDOR_ID":
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the imported data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "201_vendor_new.csv (" + sr_file + ")")

    # EXPORT THE PREVIOUS VENDORS AS BACKUP - NEW VENDORS
    if l_record:
        print("Export previous vendor details...")
        sr_filet: str = s_file_prefix + "aab_" + s_file_name
        sx_path = ed_path
        sx_file = "201_vendor_new_prev"
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # EXPORT THE CURRENT VENDORS - NEW VENDORS
    if l_record:
        print("Export current vendor details...")
        sr_filet: str = s_file_prefix + "aaa_" + s_file_name
        sx_path = ed_path
        sx_file = "201_vendor_new"
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # COMBINE CURRENT AND PREVIOUS VENDORS  - NEW VENDORS WITH NWU.AC.ZA MAIL
    print("Combine current and previous vendors...")
    sr_file: str = s_file_prefix + "aac_" + s_file_name
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NEW VENDOR' As VENDOR_CATEGORY,
        NEW.VENDOR_ID,
        Lower(NEW.VENDOR_MAIL) As VENDOR_MAIL
    From
        X100baaa_employee_vendor_share_email NEW Left Join
        X100baab_employee_vendor_share_email OLD On OLD.VENDOR_ID_PREV = NEW.VENDOR_ID
    Where
        Lower(NEW.VENDOR_MAIL) Like ('%nwu.ac.za%') And
        OLD.VENDOR_ID_PREV Is Null
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COMBINE CURRENT AND PREVIOUS VENDORS  - CURRENT VENDOR CHANGED TO NWU.AC.ZA
    print("Combine current and previous vendors...")
    sr_file: str = s_file_prefix + "ab_" + s_file_name
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'CHANGED VENDOR' As VENDOR_CATEGORY,
        OLD.VENDOR_ID_PREV As VENDOR_ID,
        OLD.VENDOR_MAIL_PREV As VENDOR_MAIL_OLD,
        NEW.VENDOR_MAIL As VENDOR_MAIL_NEW,
        Case
            When OLD.VENDOR_MAIL_PREV Not Like('%nwu.ac.za%') And
                NEW.VENDOR_MAIL Not Like('%nwu.ac.za%')
                Then '1 NO NWU TO NO NWU'
            When OLD.VENDOR_MAIL_PREV Not Like('%nwu.ac.za%') And
                NEW.VENDOR_MAIL Like('%nwu.ac.za%')
                Then '2 NO NWU TO NWU'
            When OLD.VENDOR_MAIL_PREV Like('%nwu.ac.za%') And
                NEW.VENDOR_MAIL Not Like('%nwu.ac.za%')
                Then '3 NWU TO NO NWU'
            When Substr(OLD.VENDOR_ID_PREV, 1, 8) = Substr(NEW.VENDOR_MAIL, 1, 8) And
                NEW.VENDOR_MAIL Like('%nwu.ac.za%')
                Then '4 NWU TO NWU'     
            Else '0'
        End As TEST_TYPE 
    From
        X100baab_employee_vendor_share_email OLD Left Join
        X100baaa_employee_vendor_share_email NEW On NEW.VENDOR_ID = OLD.VENDOR_ID_PREV
    Where
        OLD.VENDOR_MAIL_PREV != NEW.VENDOR_MAIL
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # OBTAIN TEST DATA FOR CURRENT ACTIVE VENDORS - CATEGORY CURRENT ACTIVE VENDOR
    print("Obtain test data...")
    sr_file: str = s_file_prefix + "ac_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'CURRENT VENDOR' As VENDOR_CATEGORY,
        VEND.VENDOR_ID,
        VEND.VNDR_NM As VENDOR_NAME,
        VEND.VNDR_TYP_CD As VENDOR_TYPE,
        SUMM.LAST_PMT_DT As LAST_PAYMENT_DATE,
        Cast(SUMM.NET_PMT_AMT As REAL) As PAYMENT_VALUE,
        Cast(SUMM.TRAN_COUNT As INT) As PAYMENT_TRANSACTION_COUNT,
        Lower(VEND.VEND_MAIL) As VENDOR_MAIL,
        Lower(VEND.EMAIL) As VENDOR_MAIL2,
        Lower(VEND.EMAIL_CONTACT) As VENDOR_MAIL_CONTACT
    From
        KFS.X000_Vendor VEND Left Join
        KFSCURR.X002aa_Report_payments_summary SUMM On SUMM.VENDOR_ID = VEND.VENDOR_ID Left Join
        PEOPLE.X002_PEOPLE_CURR_YEAR PEOP On PEOP.EMPLOYEE_NUMBER = Substr(VEND.VENDOR_ID,1,8) Left Join
        PEOPLE.X002_PEOPLE_PREV_YEAR PREP On PREP.EMPLOYEE_NUMBER = Substr(VEND.VENDOR_ID,1,8) Left Join
        VSSCURR.X001_STUDENT STUD On Cast(STUD.KSTUDBUSENTID As TEXT) = Substr(VEND.VENDOR_ID,1,8)
    Where
        Lower(VEND.VEND_MAIL) Like ('%nwu.ac.za%') And
        VEND.DOBJ_MAINT_CD_ACTV_IND = 'Y' And
        SUMM.VENDOR_ID Is Not Null And
        Cast(SUMM.TRAN_COUNT As INT) > 1 And
        PEOP.EMPLOYEE_NUMBER Is Null And
        PREP.EMPLOYEE_NUMBER Is Null And
        STUD.KSTUDBUSENTID Is Null And
        Substr(VEND.VENDOR_ID, 1, 8) != Substr(VEND.VEND_MAIL, 1, 8)
    ;"""
    # Substr(VEND.VENDOR_ID, 1, 8) != Substr(VEND.VEND_MAIL, 1, 8) And
    # PEOP.EMPLOYEE_NUMBER Is Null
    # PREP.EMPLOYEE_NUMBER Is Null
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COMBINE VENDOR FINDINGS
    print("Create combined table...")
    sr_file: str = s_file_prefix + "ad_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(
        "CREATE TABLE " + sr_file + "(VENDOR_CATEGORY TEXT, VENDOR_ID TEXT, VENDOR_MAIL TEXT, VENDOR_MAIL_NEW TEXT)")
    # NEW VENDOR
    print("Add new vendors...")
    data_file: str = s_file_prefix + "aac_" + s_file_name
    s_sql = "INSERT INTO " + sr_file + \
            "(VENDOR_CATEGORY, VENDOR_ID, VENDOR_MAIL)" \
            " SELECT VENDOR_CATEGORY, VENDOR_ID, VENDOR_MAIL FROM " + \
            data_file + \
            ";"
    so_curs.execute(s_sql)
    # CHANGED VENDOR
    print("Add changed vendors...")
    data_file: str = s_file_prefix + "ab_" + s_file_name
    s_sql = "INSERT INTO " + sr_file + \
            "(VENDOR_CATEGORY, VENDOR_ID, VENDOR_MAIL, VENDOR_MAIL_NEW)" \
            " SELECT VENDOR_CATEGORY, VENDOR_ID, VENDOR_MAIL_OLD, VENDOR_MAIL_NEW FROM " + \
            data_file + \
            " WHERE TEST_TYPE Like('0%')" + \
            ";"
    so_curs.execute(s_sql)
    # CURRENT VENDOR VENDOR
    print("Add current vendors...")
    data_file: str = s_file_prefix + "ac_" + s_file_name
    s_sql = "INSERT INTO " + sr_file + \
            "(VENDOR_CATEGORY, VENDOR_ID, VENDOR_MAIL)" \
            " SELECT VENDOR_CATEGORY, VENDOR_ID, VENDOR_MAIL FROM " + \
            data_file + \
            ";"
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t COMBINE TABLE: " + sr_file)

    # SELECT TEST DATA
    print("Identify findings...")
    sr_file: str = s_file_prefix + "ae_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        COMB.*,
        VEND.*,
        CASE
            WHEN PEOP.EMPLOYEE_NUMBER Is Not Null THEN 'EMPLOYEE'
            WHEN STUD.KSTUDBUSENTID Is Not Null THEN 'STUDENT'
            ELSE 'VENDOR'
        END As VENDOR_CLASS    
    From
        %FILEP%%FILEN% COMB Left Join
        KFS.X000_Vendor VEND On VEND.VENDOR_ID = COMB.VENDOR_ID Left Join
        PEOPLE.X002_PEOPLE_CURR_YEAR PEOP On PEOP.EMPLOYEE_NUMBER = Substr(COMB.VENDOR_ID,1,8) Left Join
        VSSCURR.X001_STUDENT STUD On Cast(STUD.KSTUDBUSENTID As TEXT) = Substr(COMB.VENDOR_ID,1,8)        
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", "ad_" + s_file_name)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # SELECT TEST DATA
    print("Identify findings...")
    sr_file = s_file_prefix + "b_finding"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.VENDOR_ID,
        FIND.VENDOR_MAIL,
        FIND.VENDOR_MAIL_NEW,        
        FIND.VNDR_TYP_CD As VENDOR_TYPE,
        FIND.VENDOR_CATEGORY,
        FIND.VENDOR_CLASS
    From
        %FILEP%%FILEN% FIND
    Where
        FIND.VENDOR_CLASS Like('VENDOR%')        
    Order by
        VENDOR_ID
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", "ae_" + s_file_name)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")

    # GET PREVIOUS FINDINGS
    if i_finding_before > 0:
        functest.get_previous_finding(so_curs, ed_path, s_report_file, s_finding, "TTTTT")
        so_conn.commit()

    # SET PREVIOUS FINDINGS
    if i_finding_before > 0:
        functest.set_previous_finding(so_curs)
        so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = s_file_prefix + "d_addprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            Lower('%FINDING%') AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%b_finding FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = FIND.VENDOR_ID And
                PREV.FIELD2 = FIND.VENDOR_MAIL
        ;"""
        s_sql = s_sql.replace("%FINDING%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = s_file_prefix + "e_newprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.VENDOR_ID AS FIELD1,
            PREV.VENDOR_MAIL AS FIELD2,
            PREV.VENDOR_CATEGORY AS FIELD3,
            PREV.VENDOR_CLASS AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%d_addprev PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""        
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = s_report_file[:-4]
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_finding_before) + '/' + str(
                    i_finding_after) + '</b> ' + s_description)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        functest.get_officer(so_curs, "HR", "TEST " + s_finding + " OFFICER")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        functest.get_supervisor(so_curs, "HR", "TEST " + s_finding + " SUPERVISOR")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = s_file_prefix + "h_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.VENDOR_ID,
            FIND.VNDR_NM As VENDOR_NAME,
            PREV.VENDOR_TYPE,
            PREV.VENDOR_MAIL,
            PREV.VENDOR_MAIL_NEW,
            FIND.EMAIL As VENDOR_MAIL_ALT,
            FIND.EMAIL_CONTACT As VENDOR_MAIL_CON,
            FIND.VENDOR_CATEGORY,
            PREV.VENDOR_CLASS,
            SUMM.LAST_PMT_DT As PAY_DATE,
            SUMM.NET_PMT_AMT As PAY_AMOUNT,
            SUMM.TRAN_COUNT As COUNT_TRAN,
            CAMP_OFF.EMPLOYEE_NUMBER AS CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR AS CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END AS CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS AS CAMP_OFF_MAIL2,        
            CAMP_SUP.EMPLOYEE_NUMBER AS CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR AS CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER != '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END AS CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS AS CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER AS ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR AS ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER != '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END AS ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS AS ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER AS ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR AS ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER != '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END AS ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS AS ORG_SUP_MAIL2,
            AUD_OFF.EMPLOYEE_NUMBER As AUD_OFF_NUMB,
            AUD_OFF.NAME_ADDR As AUD_OFF_NAME,
            AUD_OFF.EMAIL_ADDRESS As AUD_OFF_MAIL,
            AUD_SUP.EMPLOYEE_NUMBER As AUD_SUP_NUMB,
            AUD_SUP.NAME_ADDR As AUD_SUP_NAME,
            AUD_SUP.EMAIL_ADDRESS As AUD_SUP_MAIL
        From
            %FILEP%d_addprev PREV Left Join
            %FILEP%%FILEN% FIND On FIND.VENDOR_ID = PREV.VENDOR_ID,
            Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.VENDOR_TYPE Left Join
            Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD' Left Join
            Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.VENDOR_TYPE Left Join
            Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD' Left Join
            KFSCURR.X002aa_Report_payments_summary SUMM On SUMM.VENDOR_ID = PREV.VENDOR_ID
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%FILEN%", "ae_" + s_file_name)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = s_file_prefix + "x_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            '%FIND%' As Audit_finding,
            FIND.VENDOR_CATEGORY As Vendor_category,
            FIND.VENDOR_CLASS As Vendor_class,
            FIND.VENDOR_TYPE As Vendor_type,
            FIND.VENDOR_ID As Vendor_id,
            FIND.VENDOR_NAME As Vendor_name,
            FIND.VENDOR_MAIL As Mail_address,
            FIND.VENDOR_MAIL_NEW As Mail_new,
            FIND.VENDOR_MAIL_ALT As Mail_alternate,
            FIND.VENDOR_MAIL_CON As Mail_contact,
            FIND.PAY_DATE As Last_transaction_date,
            FIND.PAY_AMOUNT As Total_amount,
            FIND.COUNT_TRAN As Transaction_count,
            FIND.ORG As Organization,
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.AUD_OFF_NAME AS Audit_Officer,
            FIND.AUD_OFF_NUMB AS Audit_Officer_Numb,
            FIND.AUD_OFF_MAIL AS Audit_Officer_Mail,
            FIND.AUD_SUP_NAME AS Audit_Supervisor,
            FIND.AUD_SUP_NUMB AS Audit_Supervisor_Numb,
            FIND.AUD_SUP_MAIL AS Audit_Supervisor_Mail
        From
            %FILEP%h_detail FIND
        Order By
            FIND.VENDOR_CATEGORY,
            FIND.VENDOR_CLASS,
            FIND.VENDOR_MAIL,
            FIND.VENDOR_NAME            
        ;"""
        s_sql = s_sql.replace("%FIND%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path
            sx_file = s_file_prefix + "_" + s_finding.lower() + "_"
            sx_file_dated = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    TEST EMPLOYEE NO DECLARATION
    *****************************************************************************"""

    """
    Test if employees declared conflict of interest.
        Request remediation from employee.
        Notify employee line manager.
    Test exclude:
        Person type:
        AD HOC APPOINTMENT
        COUNCIL MEMBER
        ADVISORY BOARD MEMBER
        If employed less than 31 days.                
    Created: 21 May 2021 (Albert J v Rensburg NWU:21162395)
    """

    # TABLES NEEDED
    # X003_dashboard_curr
    # PEOPLE.X002_PEOPLE_CURR

    # TEST WILL ONLY RUN FROM MAY TO NOVEMBER
    if funcdate.cur_month() in ("05", "06", "07", "08", "09", "10", "11"):

        # DECLARE TEST VARIABLES
        i_finding_after: int = 0
        s_description = "Employee did not declare interest"
        s_file_name: str = "employee_no_declaration"
        s_file_prefix: str = "X101a"
        s_finding: str = "EMPLOYEE NO DECLARATION"
        s_report_file: str = "002_reported.txt"

        # OPEN LOG
        if l_debug:
            print("TEST " + s_finding)
        funcfile.writelog("TEST " + s_finding)

        # OBTAIN TEST DATA FOR EMPLOYEES
        if l_debug:
            print("Obtain test data...")
        sr_file: str = s_file_prefix + "a_" + s_file_name
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'NWU' As ORG,
            Substr(a.LOCATION,1,3) As LOC,
            a.EMPLOYEE,
            a.NAME,
            a.CATEGORY,
            a.PERSON_TYPE,
            a.SUPERVISOR,
            a.DECLARED,
            a.EMP_START,
            Cast(Julianday('%TODAY%') - Julianday(a.EMP_START) As Int) As DAYS_IN_SERVICE
        From
            X003_dashboard_curr a
        ;"""
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # SELECT TEST DATA
        if l_debug:
            print("Identify findings...")
        sr_file = s_file_prefix + "b_finding"
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            FIND.ORG,
            FIND.LOC,
            FIND.SUPERVISOR,
            FIND.EMPLOYEE,
            FIND.CATEGORY
        From
            %FILEP%%FILEN% FIND
        Where
            FIND.PERSON_TYPE Not In (
                'AD HOC APPOINTMENT',
                'COUNCIL MEMBER',
                'ADVISORY BOARD MEMBER'
                ) And
            FIND.DECLARED = 'NO DECLARATION' And
            FIND.SUPERVISOR Is Not Null And
            FIND.EMP_START < '%YEAR_BEGIN%' And
            FIND.DAYS_IN_SERVICE > 30
        Order By
            FIND.SUPERVISOR,
            FIND.EMPLOYEE    
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
        s_sql = s_sql.replace("%YEAR_BEGIN%", funcdate.cur_yearbegin())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # COUNT THE NUMBER OF FINDINGS
        i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
        if l_debug:
            print("*** Found " + str(i_finding_before) + " exceptions ***")
        funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")

        # GET PREVIOUS FINDINGS
        if i_finding_before > 0:
            functest.get_previous_finding(so_curs, ed_path, s_report_file, s_finding, "TTTTT")
            so_conn.commit()

        # SET PREVIOUS FINDINGS
        if i_finding_before > 0:
            functest.set_previous_finding(so_curs)
            so_conn.commit()

        # ADD PREVIOUS FINDINGS
        sr_file = s_file_prefix + "d_addprev"
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        if i_finding_before > 0:
            if l_debug:
                print("Join previously reported to current findings...")
            s_sql = "CREATE TABLE " + sr_file + " AS" + """
            Select
                FIND.*,
                Lower('%FINDING%') AS PROCESS,
                '%TODAY%' AS DATE_REPORTED,
                '%DAYS%' AS DATE_RETEST,
                PREV.PROCESS AS PREV_PROCESS,
                PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
                PREV.DATE_RETEST AS PREV_DATE_RETEST,
                PREV.REMARK
            From
                %FILEP%b_finding FIND Left Join
                Z001ab_setprev PREV ON
                 PREV.FIELD1 = FIND.SUPERVISOR And
                 PREV.FIELD2 = FIND.EMPLOYEE
            ;"""
            s_sql = s_sql.replace("%FINDING%", s_finding)
            s_sql = s_sql.replace("%FILEP%", s_file_prefix)
            s_sql = s_sql.replace("%TODAY%", funcdate.today())
            s_sql = s_sql.replace("%DAYS%", funcdate.cur_monthendnext())
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # BUILD LIST TO UPDATE FINDINGS
        sr_file = s_file_prefix + "e_newprev"
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        if i_finding_before > 0:
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                PREV.PROCESS,
                PREV.SUPERVISOR AS FIELD1,
                PREV.EMPLOYEE AS FIELD2,
                '' AS FIELD3,
                '' AS FIELD4,
                '' AS FIELD5,
                PREV.DATE_REPORTED,
                PREV.DATE_RETEST,
                PREV.REMARK
            From
                %FILEP%d_addprev PREV
            Where
                PREV.PREV_PROCESS Is Null Or
                PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""        
            ;"""
            s_sql = s_sql.replace("%FILEP%", s_file_prefix)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)
            # Export findings to previous reported file
            i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
            if i_finding_after > 0:
                if l_debug:
                    print("*** " + str(i_finding_after) + " Finding(s) to report ***")
                sx_path = ed_path
                sx_file = s_report_file[:-4]
                # Read the header data
                s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
                # Write the data
                if l_record:
                    funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                    funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                    funcfile.writelog("%t EXPORT DATA: " + sr_file)
                if l_mess:
                    funcsms.send_telegram('', 'administrator', '<b>' + str(i_finding_before) + '/' + str(
                        i_finding_after) + '</b> ' + s_description)
            else:
                if l_debug:
                    print("*** No new findings to report ***")
                funcfile.writelog("%t FINDING: No new findings to export")

        # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
        if i_finding_before > 0 and i_finding_after > 0:
            functest.get_officer(so_curs, "HR", "TEST " + s_finding + " OFFICER")
            so_conn.commit()

        # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
        if i_finding_before > 0 and i_finding_after > 0:
            functest.get_supervisor(so_curs, "HR", "TEST " + s_finding + " SUPERVISOR")
            so_conn.commit()

        # ADD CONTACT DETAILS TO FINDINGS
        sr_file = s_file_prefix + "h_detail"
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        if i_finding_before > 0 and i_finding_after > 0:
            if l_debug:
                print("Add contact details to findings...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                PREV.ORG,
                PREV.LOC,
                PREV.CATEGORY,
                PREV.EMPLOYEE,
                EMPL.NAME_ADDR As EMP_NAME,
                EMPL.PERSON_TYPE As EMP_PERSON_TYPE,        
                Upper(EMPL.POSITION_FULL) As EMP_POSITION,
                EMPL.EMAIL_ADDRESS As EMP_MAIL1,
                PREV.EMPLOYEE || '@nwu.ac.za' As EMP_MAIL2,
                PREV.SUPERVISOR,
                SUPE.NAME_ADDR As SUP_NAME,
                SUPE.EMAIL_ADDRESS As SUP_MAIL1,
                PREV.SUPERVISOR || '@nwu.ac.za' As SUP_MAIL2,
                CAMP_OFF.EMPLOYEE_NUMBER AS CAMP_OFF_NUMB,
                CAMP_OFF.NAME_ADDR AS CAMP_OFF_NAME,
                CAMP_OFF.EMAIL_ADDRESS AS CAMP_OFF_MAIL1,        
                CASE
                    WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                    ELSE CAMP_OFF.EMAIL_ADDRESS
                END AS CAMP_OFF_MAIL2,
                CAMP_SUP.EMPLOYEE_NUMBER AS CAMP_SUP_NUMB,
                CAMP_SUP.NAME_ADDR AS CAMP_SUP_NAME,
                CAMP_SUP.EMAIL_ADDRESS AS CAMP_SUP_MAIL1,
                CASE
                    WHEN CAMP_SUP.EMPLOYEE_NUMBER != '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                    ELSE CAMP_SUP.EMAIL_ADDRESS
                END AS CAMP_SUP_MAIL2,
                ORG_OFF.EMPLOYEE_NUMBER AS ORG_OFF_NUMB,
                ORG_OFF.NAME_ADDR AS ORG_OFF_NAME,
                ORG_OFF.EMAIL_ADDRESS AS ORG_OFF_MAIL1,
                CASE
                    WHEN ORG_OFF.EMPLOYEE_NUMBER != '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                    ELSE ORG_OFF.EMAIL_ADDRESS
                END AS ORG_OFF_MAIL2,
                ORG_SUP.EMPLOYEE_NUMBER AS ORG_SUP_NUMB,
                ORG_SUP.NAME_ADDR AS ORG_SUP_NAME,
                ORG_SUP.EMAIL_ADDRESS AS ORG_SUP_MAIL1,
                CASE
                    WHEN ORG_SUP.EMPLOYEE_NUMBER != '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                    ELSE ORG_SUP.EMAIL_ADDRESS
                END AS ORG_SUP_MAIL2,
                AUD_OFF.EMPLOYEE_NUMBER As AUD_OFF_NUMB,
                AUD_OFF.NAME_ADDR As AUD_OFF_NAME,
                AUD_OFF.EMAIL_ADDRESS As AUD_OFF_MAIL,
                AUD_SUP.EMPLOYEE_NUMBER As AUD_SUP_NUMB,
                AUD_SUP.NAME_ADDR As AUD_SUP_NAME,
                AUD_SUP.EMAIL_ADDRESS As AUD_SUP_MAIL
            From
                %FILEP%d_addprev PREV Left Join
                PEOPLE.X002_PEOPLE_CURR EMPL On EMPL.EMPLOYEE_NUMBER = PREV.EMPLOYEE Left Join
                PEOPLE.X002_PEOPLE_CURR SUPE On SUPE.EMPLOYEE_NUMBER = PREV.SUPERVISOR Left Join
                Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.CATEGORY Left Join
                Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
                Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD' Left Join
                Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.CATEGORY Left Join
                Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
                Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD'
            Where
                PREV.PREV_PROCESS Is Null Or
                PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
            ;"""
            """
                EMPL.NAME_ADDR,
                EMPL.PERSON_TYPE,
                Upper(EMPL.POSITION_FULL) As POSITION,
                PREV.EMPLOYEE_NUMBER || '@nwu.ac.za' As EMAIL2,
                EMPL.EMAIL_ADDRESS As EMAIL1,
            """
            s_sql = s_sql.replace("%FILEP%", s_file_prefix)
            s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
        sr_file = s_file_prefix + "x_" + s_file_name
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        if l_debug:
            print("Build the final report")
        if i_finding_before > 0 and i_finding_after > 0:
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                '%FIND%' As Audit_finding,
                FIND.CATEGORY As Employee_category,
                FIND.EMP_PERSON_TYPE As Employee_type,
                FIND.EMPLOYEE As Employee_number,
                FIND.EMP_NAME As Employee_name,
                FIND.EMP_POSITION As Employee_position,
                FIND.EMP_MAIL1 As Employee_mail_address,
                FIND.EMP_MAIL2 As Employee_mail_alternate,
                FIND.SUPERVISOR As Supervisor_number,
                FIND.SUP_NAME As Supervisor_name,
                FIND.SUP_MAIL1 As Supervisor_mail_address,
                FIND.SUP_MAIL2 As Supervisor_mail_alternate,
                FIND.ORG As Organization,
                FIND.LOC As Campus,
                FIND.CAMP_OFF_NAME AS Responsible_officer,
                FIND.CAMP_OFF_NUMB AS Responsible_officer_numb,
                FIND.CAMP_OFF_MAIL1 AS Responsible_officer_mail,
                FIND.CAMP_SUP_NAME AS Resp_supervisor,
                FIND.CAMP_SUP_NUMB AS Resp_supervisor_numb,
                FIND.CAMP_SUP_MAIL1 AS Resp_supervisor_mail,
                FIND.ORG_OFF_NAME AS Org_officer,
                FIND.ORG_OFF_NUMB AS Org_officer_numb,
                FIND.ORG_OFF_MAIL1 AS Org_officer_mail,
                FIND.ORG_SUP_NAME AS Org_supervisor,
                FIND.ORG_SUP_NUMB AS Org_supervisor_numb,
                FIND.ORG_SUP_MAIL1 AS Org_Supervisor_mail,
                FIND.AUD_OFF_NAME AS Audit_officer,
                FIND.AUD_OFF_NUMB AS Audit_officer_numb,
                FIND.AUD_OFF_MAIL AS Audit_officer_mail,
                FIND.AUD_SUP_NAME AS Audit_supervisor,
                FIND.AUD_SUP_NUMB AS Audit_supervisor_numb,
                FIND.AUD_SUP_MAIL AS Audit_supervisor_mail
            From
                %FILEP%h_detail FIND
            ;"""
            s_sql = s_sql.replace("%FIND%", s_finding)
            s_sql = s_sql.replace("%FILEP%", s_file_prefix)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)
            # Export findings
            if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
                print("Export findings...")
                sx_path = re_path
                sx_file = s_file_prefix + "_" + s_finding.lower() + "_"
                sx_file_dated = sx_file + funcdate.today_file()
                s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
                funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
        else:
            s_sql = "CREATE TABLE " + sr_file + " (" + """
            BLANK TEXT
            );"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("----------------------------------------")
    funcfile.writelog("COMPLETED: C002_PEOPLE_TEST_CONFLICT_DEV")

    return


if __name__ == '__main__':
    try:
        people_test_conflict()
    except Exception as e:
        funcsys.ErrMessage(e)
