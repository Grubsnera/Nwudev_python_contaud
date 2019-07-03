"""
Script to test GL TRANSACTIONS
Created on: 16 Apr 2019
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcsys

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# OPEN THE SCRIPT LOG FILE
print("----------------------------")
print("C202_GL_TEST_TRANSACTION_DEV")
print("----------------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C202_GL_TEST_TRANSACTION_DEV")
funcfile.writelog("------------------------------------")
ilog_severity = 1

# DECLARE VARIABLES
so_path = "W:/Kfs/"  # Source database path
so_file = "Kfs_test_gl_transaction.sqlite"  # Source database
re_path = "R:/Kfs/"  # Results path
ed_path = "S:/_external_data/"  # External data path
l_export = False
l_mail = False
l_record = True

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

""" ****************************************************************************
PROFESSIONAL FEES GL MASTER FILE
*****************************************************************************"""
print("PROFESSIONAL FEES GL MASTER FILE")
funcfile.writelog("PROFESSIONAL FEES GL MASTER FILE")

# OBTAIN GL PROFESSIONAL FEE TRANSACTIONS
print("Obtain gl professional (2056) fee transactions...")
sr_file: str = "X001_gl_professional_fee"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    GL.*,
    ACC.ACCT_MGR_UNVL_ID As ACC_MGR,
    ACC.ACCT_SPVSR_UNVL_ID As ACC_SUP,
    ACC.ACCT_FSC_OFC_UID As ACC_FIS,
    CASE
        WHEN ACC.ACCT_PHYS_CMP_CD = 'P' THEN 'POTCHEFSTROOM'
        WHEN ACC.ACCT_PHYS_CMP_CD = 'V' THEN 'VAAL TRIANGLE'
        WHEN ACC.ACCT_PHYS_CMP_CD = 'M' THEN 'MAFIKENG'
        ELSE 'NWU'
    END As ACC_CAMPUS 
From
    KFS.X000_GL_trans_curr GL Left Join
    KFS.X000_Account ACC On ACC.ACCOUNT_NBR = GL.ACCOUNT_NBR
Where
    GL.FS_DATABASE_DESC = 'KFS' And
    Instr(GL.CALC_COST_STRING, '.2056') > 0
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# OBTAIN GL PROFESSIONAL FEE TRANSACTIONS WITH CURRENT YEAR PAYMENTS
print("Obtain gl professional (2056) fee transactions...")
sr_file: str = "X001_gl_professional_fee_pay"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    GL.*,
    PAY.VENDOR_ID,
    PAY.PAYEE_NAME As STUDENT_NAME,
    PAY.INV_NBR,
    PAY.PAYEE_TYP_DESC,
    PAY.COMPLETE_EMP_NO As EMP_INI,
    PAY.APPROVE_EMP_NO As EMP_APP
From
    X001_gl_professional_fee GL Inner Join
    KFS.X001aa_Report_payments_curr PAY On PAY.CUST_PMT_DOC_NBR = GL.FDOC_NBR And
        PAY.NET_PMT_AMT = GL.CALC_AMOUNT      
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

""" ****************************************************************************
TEST PROFESSIONAL FEES PAID TO STUDENTS
*****************************************************************************"""
print("PROFESSIONAL FEES PAID TO STUDENTS")
funcfile.writelog("PROFESSIONAL FEES PAID TO STUDENTS")

# DECLARE VARIABLES
i_finding_after: int = 0

# OBTAIN TEST DATA
print("Obtain test data...")
sr_file: str = "X001aa_professional_fee_student"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    GL.*,
    STUD.KSTUDBUSENTID As STUDENT,
    CASE
        WHEN STUD.FSITEORGUNITNUMBER = -1 THEN 'POT'
        WHEN STUD.FSITEORGUNITNUMBER = -2 THEN 'VAA'
        WHEN STUD.FSITEORGUNITNUMBER = -9 THEN 'MAF'
        ELSE 'OTH'
    END As LOC 
From
    X001_gl_professional_fee_pay GL Inner Join
    VSS.X001_student_curr STUD On Substr(GL.VENDOR_ID,1,8) = STUD.KSTUDBUSENTID And
        STUD.ISMAINQUALLEVEL = '1'
Order By
    GL.TIMESTAMP    
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IDENTIFY FINDINGS
print("Identify findings...")
sr_file = "X001ab_findings"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    'NWU' As ORG,
    CURR.LOC,
    CURR.STUDENT,
    CURR.FDOC_NBR,
    CURR.CALC_COST_STRING,
    EMP_INI,
    ACC_MGR
From
    X001aa_professional_fee_student CURR
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# COUNT THE NUMBER OF FINDINGS
i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
print("*** Found " + str(i_finding_before) + " exceptions ***")
funcfile.writelog("%t FINDING: " + str(i_finding_before) + " PROF FEE PAID TO STUDENT finding(s)")

# GET PREVIOUS FINDINGS
sr_file = "X001ac_get_previous"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    print("Import previously reported findings...")
    so_curs.execute(
        "CREATE TABLE " + sr_file + """
        (PROCESS TEXT,
        FIELD1 INT,
        FIELD2 TEXT,
        FIELD3 TEXT,
        FIELD4 TEXT,
        FIELD5 TEXT,
        DATE_REPORTED TEXT,
        DATE_RETEST TEXT,
        DATE_MAILED TEXT)
        """)
    s_cols = ""
    co = open(ed_path + "202_reported.txt", "r")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "PROCESS":
            continue
        elif row[0] != "prof fee paid to student":
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + \
                     row[
                         3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[
                         8] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the imported data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

# ADD PREVIOUS FINDINGS
sr_file = "X001ad_add_previous"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    print("Join previously reported to current findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        FIND.*,
        'prof fee paid to student' AS PROCESS,
        '%TODAY%' AS DATE_REPORTED,
        '%DAYS%' AS DATE_RETEST,
        PREV.PROCESS AS PREV_PROCESS,
        PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
        PREV.DATE_RETEST AS PREV_DATE_RETEST,
        PREV.DATE_MAILED
    From
        X001ab_findings FIND Left Join
        X001ac_get_previous PREV ON PREV.FIELD1 = FIND.STUDENT AND
            PREV.FIELD2 = FIND.FDOC_NBR And
            PREV.FIELD3 = FIND.CALC_COST_STRING And
            PREV.DATE_RETEST >= Date('%TODAY%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%", funcdate.today())
    s_sql = s_sql.replace("%DAYS%", funcdate.today_plusdays(366))
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD LIST TO UPDATE FINDINGS
# NOTE ADD CODE
sr_file = "X001ae_new_previous"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.PROCESS,
        PREV.STUDENT AS FIELD1,
        PREV.FDOC_NBR AS FIELD2,
        PREV.CALC_COST_STRING AS FIELD3,
        '' AS FIELD4,
        '' AS FIELD5,
        PREV.DATE_REPORTED,
        PREV.DATE_RETEST,
        PREV.DATE_MAILED
    From
        X001ad_add_previous PREV
    Where
        PREV.PREV_PROCESS Is Null
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export findings to previous reported file
    i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
    if i_finding_after > 0:
        print("*** " + str(i_finding_after) + " Finding(s) to report ***")
        sx_path = ed_path
        sx_file = "202_reported"
        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        # Write the data
        if l_record:
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
            funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
            funcfile.writelog("%t EXPORT DATA: " + sr_file)
    else:
        print("*** No new findings to report ***")
        funcfile.writelog("%t FINDING: No new findings to export")

# IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
sr_file = "X001af_officer"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    if i_finding_after > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            OFFICER.LOOKUP,
            OFFICER.LOOKUP_CODE AS CAMPUS,
            OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            PEOPLE.X000_OWN_HR_LOOKUPS OFFICER Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON
                PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
        Where
            OFFICER.LOOKUP = 'TEST_GL_OBJECT_PROF_FEE_PAID_TO_STUDENT_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
sr_file = "X001ag_supervisor"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0 and i_finding_after > 0:
    print("Import reporting supervisors for mail purposes...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        SUPERVISOR.LOOKUP,
        SUPERVISOR.LOOKUP_CODE AS CAMPUS,
        SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
        PEOP.NAME_ADDR As NAME,
        PEOP.EMAIL_ADDRESS
    From
        PEOPLE.X000_OWN_HR_LOOKUPS SUPERVISOR Left Join
        PEOPLE.X002_PEOPLE_CURR PEOP ON 
            PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
    Where
        SUPERVISOR.LOOKUP = 'TEST_GL_OBJECT_PROF_FEE_PAID_TO_STUDENT_SUPERVISOR'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# ADD CONTACT DETAILS TO FINDINGS
sr_file = "X001ah_detail"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0 and i_finding_after > 0:
    print("Add contact details to findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.ORG,
        PREV.LOC,
        PREV.STUDENT,
        MASTER.STUDENT_NAME,
        PREV.FDOC_NBR,
        MASTER.TRANSACTION_DT,
        MASTER.CALC_AMOUNT,
        MASTER.TRN_LDGR_ENTR_DESC,
        MASTER.PAYEE_TYP_DESC,
        MASTER.INV_NBR,
        PREV.CALC_COST_STRING,
        MASTER.ORG_NM,
        MASTER.ACCOUNT_NM,
        CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
        CAMP_OFF.NAME As CAMP_OFF_NAME,
        CASE
            WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
            ELSE CAMP_OFF.EMAIL_ADDRESS
        END As CAMP_OFF_MAIL,
        CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL2,
        CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
        CAMP_SUP.NAME As CAMP_SUP_NAME,
        CASE
            WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
            ELSE CAMP_SUP.EMAIL_ADDRESS
        END As CAMP_SUP_MAIL,
        CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL2,
        ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
        ORG_OFF.NAME As ORG_OFF_NAME,
        CASE
            WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
            ELSE ORG_OFF.EMAIL_ADDRESS
        END As ORG_OFF_MAIL,
        ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL2,
        ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
        ORG_SUP.NAME As ORG_SUP_NAME,
        CASE
            WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
            ELSE ORG_SUP.EMAIL_ADDRESS
        END As ORG_SUP_MAIL,
        ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2,
        PREV.EMP_INI,
        INI.NAME_ADDR As INAME,
        PREV.EMP_INI||'@nwu.ac.za' As IMAIL,
        INI.EMAIL_ADDRESS As IMAIL2,
        PREV.ACC_MGR,
        ACCM.NAME_ADDR As ANAME,
        PREV.ACC_MGR||'@nwu.ac.za' As AMAIL,
        ACCM.EMAIL_ADDRESS As AMAIL2
    From
        X001ad_add_previous PREV
        Left Join X001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC
        Left Join X001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
        Left Join X001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC
        Left Join X001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Left Join X001aa_professional_fee_student MASTER On MASTER.STUDENT = PREV.STUDENT And
            MASTER.FDOC_NBR = PREV.FDOC_NBR And
            MASTER.CALC_COST_STRING = PREV.CALC_COST_STRING
        Left Join PEOPLE.X002_PEOPLE_CURR INI On INI.EMPLOYEE_NUMBER = PREV.EMP_INI 
        Left Join PEOPLE.X002_PEOPLE_CURR ACCM On ACCM.EMPLOYEE_NUMBER = PREV.ACC_MGR 
    Where
      PREV.PREV_PROCESS IS NULL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD THE FINAL TABLE FOR EXPORT AND REPORT
sr_file = "X001ax_professional_fee_student"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
print("Build the final report")
if i_finding_before > 0 and i_finding_after > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'PROFESSIONAL FEE PAID TO STUDENT' As Audit_finding,
        FIND.STUDENT As Student,
        FIND.STUDENT_NAME As Name,
        FIND.FDOC_NBR As Edoc,
        FIND.TRANSACTION_DT As Date,
        FIND.INV_NBR As Invoice,
        FIND.CALC_AMOUNT As Amount,
        FIND.PAYEE_TYP_DESC As Vendor_type,
        CASE
            WHEN Instr(FIND.TRN_LDGR_ENTR_DESC,'<VATI-0>') > 0 THEN Substr(FIND.TRN_LDGR_ENTR_DESC,9) 
            ELSE FIND.TRN_LDGR_ENTR_DESC
        END As Description,
        FIND.CALC_COST_STRING As Account,
        FIND.ORG_NM As Organization,
        FIND.ACCOUNT_NM As Account_name,
        FIND.EMP_INI As Initiator,
        FIND.INAME As Initiator_name,
        FIND.IMAIL As Initiator_mail,
        FIND.ACC_MGR As Acc_manager,
        FIND.ANAME As Acc_manager_name,
        FIND.AMAIL As Acc_manager_mail,
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
        FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail            
    From
        X001ah_detail FIND
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export findings
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        print("Export findings...")
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "Gltran_test_001ax_professional_fee_student_"
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

""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""

print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("---------------------------------------")
funcfile.writelog("COMPLETED: C202_GL_TEST_TRANSACTION_DEV")
