"""
Script to test PEOPLE conflict of interest
Created on: 8 Apr 2019
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3
import sys

# IMPORT OWN MODULES
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcsys

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
BUILD CONFLICT MASTER TABLES
BUILD ANNUAL TABLES
BUILD DASHBOARD TABLES
BANK NUMBER MASTER FILES
TEST EMPLOYEE VENDOR SHARE BANK ACCOUNT
END OF SCRIPT
*****************************************************************************"""


def people_test_conflict():
    """
    Cript to test PEOPLE conflict of interest
    :return:
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # OPEN THE SCRIPT LOG FILE
    print("-----------------------------")    
    print("C002_PEOPLE_TEST_CONFLICT_DEV")
    print("-----------------------------")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C002_PEOPLE_TEST_CONFLICT_DEV")
    funcfile.writelog("-------------------------------------")
    ilog_severity = 1

    # DECLARE VARIABLES
    so_path = "W:/People_conflict/" #Source database path
    re_path = "R:/People/" # Results path
    ed_path = "S:/_external_data/" #external data path
    so_file = "People_conflict.sqlite" # Source database
    s_sql = "" # SQL statements
    l_export: bool = False
    l_mail: bool = False
    l_record: bool = True

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
        X000_declarations_all
        Left Join PEOPLE.X002_PEOPLE_CURR_YEAR EMPLOYEE ON EMPLOYEE.EMPLOYEE_NUMBER = X000_declarations_all.EMPLOYEE_NUMBER
        Left Join PEOPLE.X002_PEOPLE_CURR_YEAR MANAGER ON MANAGER.EMPLOYEE_NUMBER = X000_declarations_all.LINE_MANAGER
        Left Join PEOPLE.X002_PEOPLE_CURR_YEAR SUPERVISOR ON SUPERVISOR.EMPLOYEE_NUMBER = EMPLOYEE.SUPERVISOR
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
    s_sql = s_sql.replace("%CYEARB%",funcdate.cur_yearbegin())
    s_sql = s_sql.replace("%CYEARE%",funcdate.cur_yearend())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # BUILD PREVIOUS YEAR DECLARATIONS
    print("Build previous declarations...")
    sr_file = "X001_declarations_prev"
    s_sql = s_sqlp
    s_sql = s_sql.replace("X001_declarations_curr",sr_file)
    s_sql = s_sql.replace("X002_PEOPLE_CURR_YEAR","X002_PEOPLE_PREV_YEAR")
    s_sql = s_sql.replace("%CYEARB%",funcdate.prev_yearbegin())
    s_sql = s_sql.replace("%CYEARE%",funcdate.prev_yearend())
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
    s_sql = s_sql.replace("X002_interests_curr",sr_file)
    s_sql = s_sql.replace("X001_declarations_curr","X001_declarations_prev")
    s_sql = s_sql.replace("X002_PEOPLE_CURR_YEAR","X002_PEOPLE_PREV_YEAR")
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
        Case
            When PERSON.EMPLOYMENT_CATEGORY = 'T' Then 'TEMPORARY'
            When PERSON.EMPLOYMENT_CATEGORY = 'P' Then 'PERMANENT'
            Else 'OTHER'
        End As CATEGORY,
        Upper(PERSON.GRADE_CALC) As POS_GRADE,
        Upper(PERSON.JOB_NAME) As JOB_NAME,
        Upper(PERSON.PERSON_TYPE) As PERSON_TYPE,
        PERSON.AGE
    From
        PEOPLE.X002_PEOPLE_CURR PERSON 
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # BUILD CURRENT DECLARATION DASHBOARD DECLARATION DATA
    print("Build current declaration dashboard unique declarions...")
    sr_file = "X003ab_declarations_curr"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        DECLARE.DECLARATION_ID,
        DECLARE.EMPLOYEE,
        DECLARE.DECLARATION_DATE,
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
        Upper(STATUS) AS DECLARE_STATUS
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
        Case
            When PERSON.EMPLOYMENT_CATEGORY = 'T' Then 'TEMPORARY'
            When PERSON.EMPLOYMENT_CATEGORY = 'P' Then 'PERMANENT'
            Else 'OTHER'
        End As CATEGORY,
        Upper(PERSON.GRADE_CALC) As POS_GRADE,
        Upper(PERSON.JOB_NAME) As JOB_NAME,
        Upper(PERSON.PERSON_TYPE) As PERSON_TYPE,
        PERSON.AGE
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
    print("Build previous declaration dashboard unique declarions...")
    sr_file = "X003ab_declarations_prev"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        DECLARE.DECLARATION_ID,
        DECLARE.EMPLOYEE,
        DECLARE.DECLARATION_DATE,
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
        Upper(STATUS) AS DECLARE_STATUS
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
    i_find = 0 # Number of findings before previous reported findings
    i_coun = 0 # Number of new findings to report

    # BUILD TABLE WITH VENDOR BANK ACCOUNT NUMBERS
    print("Merge employees and vendors on bank account...")
    sr_file = "X100aa_bank_empven"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        BANK.*,
        VEND.*   
    From
        X100_bank_emp BANK Inner Join
        X100_bank_ven VEND On VEND.VENDOR_BANK = BANK.EMP_BANK And
            Instr(VEND.VENDOR_ID, BANK.EMP) = 0
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD TABLE WITH VENDOR BANK ACCOUNT NUMBERS
    print("Compile list of shared bank accounts...")
    sr_file = "X100ab_bank_empven"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
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
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find = funcsys.tablerowcount(so_curs,sr_file)
    print("*** Found "+str(i_find)+" exceptions ***")
    funcfile.writelog("%t FINDING: "+str(i_find)+" EMPL BANK conflict finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X100ac_bank_getprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,REMARK TEXT)")
        s_cols = ""
        co = open(ed_path + "002_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "bank_share_emp_ven":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the impoted data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "002_reported.txt (" + sr_file + ")")

    # SET PREVIOUS FINDINGS
    sr_file = "X100ac_bank_setprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        print("Obtain the latest previous finding...")
        s_sql = "Create Table " + sr_file + " As" + """
        Select
            GET.PROCESS,
            GET.FIELD1,
            GET.FIELD2,
            GET.FIELD3,
            GET.FIELD4,
            GET.FIELD5,
            Max(GET.DATE_REPORTED) As DATE_REPORTED,
            GET.DATE_RETEST,
            GET.REMARK
        From
            X100ac_bank_getprev GET
        Group By
            GET.FIELD1        
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD PREVIOUS FINDINGS
    sr_file = "X100ad_bank_addprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        SELECT
            FINDING.*,
            'bank_share_emp_ven' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%TODAYPLUS%' AS DATE_RETEST,
            PREVIOUS.PROCESS AS PREV_PROCESS,
            PREVIOUS.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREVIOUS.DATE_RETEST AS PREV_DATE_RETEST,
            PREVIOUS.REMARK
        FROM
            X100ab_bank_empven FINDING Left Join
            X100ac_bank_setprev PREVIOUS ON PREVIOUS.FIELD1 = FINDING.EMP AND
                PREVIOUS.FIELD2 = FINDING.EMP_BANK
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%",funcdate.cur_monthend())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X100ae_bank_newprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE "+sr_file+" AS " + """
        SELECT
            PREVIOUS.PROCESS,
            PREVIOUS.EMP AS FIELD1,
            PREVIOUS.EMP_BANK AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREVIOUS.DATE_REPORTED,
            PREVIOUS.DATE_RETEST,
            PREVIOUS.REMARK
        FROM
            X100ad_bank_addprev PREVIOUS
        WHERE
            PREVIOUS.PREV_PROCESS IS NULL Or
            PREVIOUS.DATE_REPORTED > PREVIOUS.PREV_DATE_RETEST And PREVIOUS.REMARK = ""
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: "+sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(so_curs,sr_file)
        if i_coun > 0:
            print("*** " +str(i_coun)+ " Finding(s) to report ***")    
            sr_filet = sr_file
            sx_path = ed_path
            sx_file = "002_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
                funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
                funcfile.writelog("%t EXPORT DATA: "+sr_file)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X100af_bank_offi"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          LOOKUP.LOOKUP,
          LOOKUP.LOOKUP_CODE AS CAMPUS,
          LOOKUP.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.NAME_ADDR,
          PEOPLE.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS LOOKUP
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR PEOPLE ON PEOPLE.EMPLOYEE_NUMBER = LOOKUP.LOOKUP_DESCRIPTION
        WHERE
          LOOKUP.LOOKUP = 'TEST_BANKACC_CONFLICT_VENDOR_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X100ag_bank_supe"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          LOOKUP.LOOKUP,
          LOOKUP.LOOKUP_CODE AS CAMPUS,
          LOOKUP.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.NAME_ADDR,
          PEOPLE.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS LOOKUP
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR PEOPLE ON PEOPLE.EMPLOYEE_NUMBER = LOOKUP.LOOKUP_DESCRIPTION
        WHERE
          LOOKUP.LOOKUP = 'TEST_BANKACC_CONFLICT_VENDOR_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X100ah_bank_addempven"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            FINDING.*,
            PERSON.NAME_ADDR AS EMP_NAME,
            PERSON.ACC_TYPE AS BANKACC_TYPE,
            PERSON.ACC_BRANCH AS BANKACC_BRANCH,
            PERSON.ACC_RELATION AS BANKACC_RELATION,
            PERSON.PERSON_TYPE,
            VENDOR.VNDR_NM AS VENDOR_NAME,
            DECLARE.DECLARATION_DATE AS DECLARE_DATE,
            DECLARE.STATUS AS DECLARE_STATUS,
            DECLARE.INTEREST_TO_DECLARE_FLAG AS DECLARE_INTEREST,
            PAYMENTS.Max_PMT_DT AS PAY_DATE_LAST,
            PAYMENTS.Count_TRAN AS PAY_NO_TRAN,
            PAYMENTS.Sum_NET_PMT_AMT AS PAY_TOTAL_AMOUNT,
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
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X100ad_bank_addprev FINDING
            Left Join PEOPLE.X002_PEOPLE_CURR PERSON On PERSON.EMPLOYEE_NUMBER = FINDING.EMP
            Left Join KFS.X000_Vendor VENDOR On VENDOR.VENDOR_ID = FINDING.VENDOR_ID
            Left Join KFSCURR.X002aa_Report_payments_summary PAYMENTS On PAYMENTS.VENDOR_ID = FINDING.VENDOR_ID
            Left Join X001_declarations_curr DECLARE On DECLARE.EMPLOYEE = FINDING.EMP
            Left Join X100af_bank_offi CAMP_OFF On CAMP_OFF.CAMPUS = FINDING.LOC
            Left Join X100af_bank_offi ORG_OFF On ORG_OFF.CAMPUS = FINDING.ORG
            Left Join X100ag_bank_supe CAMP_SUP On CAMP_SUP.CAMPUS = FINDING.LOC
            Left Join X100ag_bank_supe ORG_SUP On ORG_SUP.CAMPUS = FINDING.ORG
        WHERE
          FINDING.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X100ax_bank_emp_vend"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    print("Build the final report")
    if i_find > 0 and i_coun > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'EMPL VENDOR SAME BANK ACC' As AUDIT_FINDING,
            FINDING.ORG AS ORGANIZATION,
            FINDING.LOC AS LOCATION,
            FINDING.EMP AS EMPLOYEE_NUMBER,
            FINDING.EMP_NAME AS EMPLOYEE_NAME,
            FINDING.PERSON_TYPE,
            FINDING.EMP_BANK AS EMPLOYEE_BANK,
            FINDING.BANKACC_TYPE,
            FINDING.BANKACC_BRANCH,
            FINDING.BANKACC_RELATION,
            FINDING.DECLARE_DATE,
            FINDING.DECLARE_STATUS,
            FINDING.DECLARE_INTEREST,
            FINDING.VENDOR_ID,
            FINDING.VENDOR_NAME,
            FINDING.VENDOR_BANK,
            FINDING.PAY_DATE_LAST,
            FINDING.PAY_NO_TRAN,
            FINDING.PAY_TOTAL_AMOUNT,
            FINDING.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
            FINDING.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
            FINDING.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
            FINDING.CAMP_SUP_NAME AS SUPERVISOR,
            FINDING.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
            FINDING.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
            FINDING.ORG_OFF_NAME AS ORG_OFFICER,
            FINDING.ORG_OFF_NUMB AS ORG_OFFICER_NUMB,
            FINDING.ORG_OFF_MAIL AS ORG_OFFICER_MAIL,
            FINDING.ORG_SUP_NAME AS ORG_SUPERVISOR,
            FINDING.ORG_SUP_NUMB AS ORG_SUPERVISOR_NUMB,
            FINDING.ORG_SUP_MAIL AS ORG_SUPERVISOR_MAIL
        From
            X100ah_bank_addempven FINDING
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
            print("Export findings...")
            sr_filet = sr_file
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "People_test_100ax_bank_emp_vend_"
            sx_filet = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
            funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
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
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("----------------------------------------")
    funcfile.writelog("COMPLETED: C002_PEOPLE_TEST_CONFLICT_DEV")

    return
