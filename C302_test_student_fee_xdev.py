"""
Script to test STUDENT FEES
Created on: 3 Dec 2019
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3
import csv

# IMPORT OWN MODULES
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcstat
from _my_modules import funcsys
from _my_modules import functest

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
END OF SCRIPT
*****************************************************************************"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# SCRIPT LOG FILE
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C302_TEST_STUDENT_FEE")
funcfile.writelog("-----------------------------")
print("---------------------")
print("C302_TEST_STUDENT_FEE")
print("---------------------")

s_period = "curr"
s_year = "0"

# DECLARE VARIABLES
if s_year == '0':
    if s_period == "prev":
        s_year = funcdate.prev_year()
    else:
        s_year = funcdate.cur_year()

ed_path = "S:/_external_data/"  # External data path
so_path = "W:/Vss_fee/"  # Source database path
re_path = "R:/Vss/" + s_year

if s_period == "prev":
    f_reg_fee = 1830.00
    d_sem1_con = "2019-03-05"
    d_sem1_dis = "2019-03-05"
    d_sem2_con = "2019-08-09"
    d_sem2_dis = "2019-08-09"
    so_file = "Vss_test_fee_prev.sqlite"  # Source database
    s_reg_trancode: str = "095"
    s_qual_trancode: str = "004"
    s_modu_trancode: str = "004"
    s_burs_trancode: str = "042z052z381z500"
    # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded
    s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
    s_mpa: str = "665566z618161z618167z618169"  # Exclude these FQUALLEVELAPID
    s_aud: str = "71839z71840z71841z71842z71820z71821z71822z1085390"  # Exclude these FQUALLEVELAPID
    l_record: bool = False
    l_export: bool = True
else:
    f_reg_fee = 1930.00
    d_sem1_con = "2020-02-21"
    d_sem1_dis = "2020-03-09"
    d_sem2_con = "2020-07-31"
    d_sem2_dis = "2020-08-15"
    so_file = "Vss_test_fee.sqlite"  # Source database
    s_reg_trancode: str = "095"
    s_qual_trancode: str = "004"
    s_modu_trancode: str = "004"
    s_burs_trancode: str = "042z052z381z500"
    # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded
    s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
    s_mpa: str = "665566z618161z618167z618169"  # Exclude these FQUALLEVELAPID
    s_aud: str = "71839z71840z71841z71842z71820z71821z71822z1085390"  # Exclude these FQUALLEVELAPID
    l_record: bool = True
    l_export: bool = True

l_vacuum: bool = False
l_mail: bool = False
i_calc = f_reg_fee
l_reg = True

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN SQLITE SOURCE table
print("Open sqlite database...")
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# ATTACH VSS DATABASE
print("Attach vss database...")
so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")
so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss_curr.sqlite' AS 'VSSCURR'")
funcfile.writelog("%t ATTACH DATABASE: Vss_curr.sqlite")
so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss_prev.sqlite' AS 'VSSPREV'")
funcfile.writelog("%t ATTACH DATABASE: Vss_prev.sqlite")
so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: People.sqlite")

"""*****************************************************************************
TEMPORARY AREA
*****************************************************************************"""
print("TEMPORARY AREA")
funcfile.writelog("TEMPORARY AREA")

"""*****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

"""*****************************************************************************
QUALIFICATION FEE TEST ABNORMAL TRANSACTION CONTACT
*****************************************************************************"""
print("QUALIFICATION FEE TEST ABNORMAL TRANSACTION CONTACT")
funcfile.writelog("QUALIFICATION FEE TEST ABNORMAL TRANSACTION CONTACT")

# FILES NEEDED
# X020ba_Student_master

# DECLARE VARIABLES
i_finding_after: int = 0

# ISOLATE QUALIFICATIONS WITH ABNORMAL TRANSACTIONS - CONTACT STUDENTS ONLY
print("Isolate qualifications with abnormal value transactions...")
sr_file = "X021fa_Qual_abnormalfee_transaction"
s_sql = "Create table " + sr_file + " AS" + """
Select
    STUD.*
From
    X020bx_Student_master_sort STUD
Where
    STUD.VALID = 0 And
    STUD.FEE_LEVIED_TYPE Like ('6%') And
    STUD.FEE_SHOULD_BE Like ('% C%')        
Order By
    STUD.CAMPUS,
    STUD.FEE_SHOULD_BE,
    STUD.KSTUDBUSENTID
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
so_conn.commit()
if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
    print("Export findings...")
    sx_path = re_path + "/"
    sx_file = "Student_fee_test_021fx_qual_fee_abnormal_transaction_studentlist_"
    sx_file_dated = sx_file + funcdate.today_file()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
    funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
    # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
    funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# IDENTIFY FINDINGS
print("Identify findings...")
sr_file = "X021fb_findings"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    'NWU' As ORG,
    FIND.CAMPUS As LOC,
    FIND.KSTUDBUSENTID As ID,
    FIND.QUALIFICATION,
    FIND.FEE_LEVIED,
    FIND.FUSERBUSINESSENTITYID As USER,
    FIND.NAME_ADDR As USER_NAME,
    FIND.SYSTEM_DESC       
From
    X021fa_Qual_abnormalfee_transaction FIND
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# COUNT THE NUMBER OF FINDINGS
i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
print("*** Found " + str(i_finding_before) + " exceptions ***")
funcfile.writelog("%t FINDING: " + str(i_finding_before) + " QUALIFICATION ABNORMAL FEE TRAN finding(s)")

# GET PREVIOUS FINDINGS
sr_file = "X021fc_get_previous"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    print("Import previously reported findings...")
    so_curs.execute(
        "CREATE TABLE " + sr_file + """
        (PROCESS TEXT,
        FIELD1 INT,
        FIELD2 TEXT,
        FIELD3 REAL,
        FIELD4 TEXT,
        FIELD5 TEXT,
        DATE_REPORTED TEXT,
        DATE_RETEST TEXT,
        REMARK TEXT)
        """)
    co = open(ed_path + "302_reported.txt", "r")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "PROCESS":
            continue
        elif row[0] != "qualification abnormal transaction":
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
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "302_reported.txt (" + sr_file + ")")

# SET PREVIOUS FINDINGS
sr_file = "X021fc_set_previous"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
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
        X021fc_get_previous GET
    Group By
        GET.FIELD1,
        GET.FIELD2,
        GET.FIELD3        
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# ADD PREVIOUS FINDINGS
sr_file = "X021fd_add_previous"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    print("Join previously reported to current findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        FIND.*,
        'qualification abnormal transaction' AS PROCESS,
        '%TODAY%' AS DATE_REPORTED,
        '%DAYS%' AS DATE_RETEST,
        PREV.PROCESS AS PREV_PROCESS,
        PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
        PREV.DATE_RETEST AS PREV_DATE_RETEST,
        PREV.REMARK
    From
        X021fb_findings FIND Left Join
        X021fc_set_previous PREV ON PREV.FIELD1 = FIND.ID And
            PREV.FIELD2 = FIND.QUALIFICATION And
            PREV.FIELD3 = FIND.FEE_LEVIED
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%", funcdate.today())
    s_sql = s_sql.replace("%DAYS%", funcdate.cur_monthend())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD LIST TO UPDATE FINDINGS
sr_file = "X021fe_new_previous"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.PROCESS,
        PREV.ID AS FIELD1,
        PREV.QUALIFICATION AS FIELD2,
        PREV.FEE_LEVIED AS FIELD3,
        '' AS FIELD4,
        '' AS FIELD5,
        PREV.DATE_REPORTED,
        PREV.DATE_RETEST,
        PREV.REMARK
    From
        X021fd_add_previous PREV
    Where
        PREV.PREV_PROCESS Is Null Or
        PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
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
        sx_file = "302_reported"
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
sr_file = "X021ff_officer"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    if i_finding_after > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            OFFICER.LOOKUP,
            Upper(OFFICER.LOOKUP_CODE) AS CAMPUS,
            OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            VSS.X000_OWN_LOOKUPS OFFICER Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
        Where
            OFFICER.LOOKUP = 'stud_fee_test_qual_abnormal_fee_transaction_officer'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
sr_file = "X021fg_supervisor"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0 and i_finding_after > 0:
    print("Import reporting supervisors for mail purposes...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        SUPERVISOR.LOOKUP,
        Upper(SUPERVISOR.LOOKUP_CODE) AS CAMPUS,
        SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
        PEOP.NAME_ADDR As NAME,
        PEOP.EMAIL_ADDRESS
    From
        VSS.X000_OWN_LOOKUPS SUPERVISOR Left Join
        PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
    Where
        SUPERVISOR.LOOKUP = 'stud_fee_test_qual_abnormal_fee_transaction_supervisor'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# ADD CONTACT DETAILS TO FINDINGS
sr_file = "X021fh_detail"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0 and i_finding_after > 0:
    print("Add contact details to findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.ORG,
        PREV.LOC,
        PREV.ID,
        PREV.QUALIFICATION,
        MAST.QUALIFICATION_NAME,
        MAST.FEE_LEVIED,
        MAST.FEE_SHOULD_BE,
        MAST.FEE_MODE,
        MAST.DATEENROL,
        MAST.RESULTPASSDATE,
        MAST.DISCONTINUEDATE,
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
        Case
            When PREV.USER_NAME != '' Then PREV.USER
            Else CAMP_OFF.EMPLOYEE_NUMBER
        End As U_NUMB,
        Case
            When PREV.USER_NAME != '' Then PREV.USER_NAME
            Else CAMP_OFF.NAME
        End As U_NAME, 
        Case
            When PREV.USER_NAME != '' Then PREV.USER||'@nwu.ac.za'
            Else CAMP_OFF.EMAIL_ADDRESS
        End As U_MAIL, 
        PREV.SYSTEM_DESC          
    From
        X021fd_add_previous PREV Left Join
        X021fa_Qual_abnormalfee_transaction MAST On MAST.KSTUDBUSENTID = PREV.ID And
            MAST.QUALIFICATION = PREV.QUALIFICATION Left Join
        X021ff_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
        X021ff_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
        X021fg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
        X021fg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
    Where
        PREV.PREV_PROCESS Is Null Or
        PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD THE FINAL TABLE FOR EXPORT AND REPORT
sr_file = "X021fx_Qual_abnormalfee_transaction"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
print("Build the final report")
if i_finding_before > 0 and i_finding_after > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'QUALIFICATION FEE ABNORMAL VALUE TRANSACTION' As Audit_finding,
        FIND.ORG As 'Organization',
        FIND.LOC As 'Campus',
        FIND.ID As 'Student',
        FIND.QUALIFICATION As 'Qualification',
        FIND.QUALIFICATION_NAME As 'Qualification_name',
        FIND.FEE_LEVIED As 'Fee_levied',
        FIND.FEE_SHOULD_BE As 'Fee_should_be',
        FIND.FEE_MODE As 'Qual_fee',
        FIND.DATEENROL As 'Date_enrol',
        FIND.RESULTPASSDATE As 'Date_pass',
        FIND.DISCONTINUEDATE As 'Date_discontinue',
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
        FIND.U_NAME As User,
        FIND.U_NUMB As User_Numb,
        FIND.U_MAIL As User_Mail        
    From
        X021fh_detail FIND
    Order by
        FIND.LOC,
        FIND.QUALIFICATION,
        FIND.FEE_SHOULD_BE,
        FIND.ID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export findings
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        print("Export findings...")
        sx_path = re_path + "/"
        sx_file = "Student_fee_test_021fx_qual_fee_abnormal_transaction_"
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
if l_vacuum:
    print("Vacuum the database...")
    so_conn.commit()
    so_conn.execute('VACUUM')
    funcfile.writelog("%t VACUUM DATABASE: " + so_file)
so_conn.commit()
so_conn.close()

# CLOSE THE LOG WRITER *********************************************************
funcfile.writelog("--------------------------------")
funcfile.writelog("COMPLETED: C302_TEST_STUDENT_FEE")

"""
# COMPLETE HEADING
print("Complete message...")
sr_file = "X_"
s_sql = "Create Table " + sr_file + " As" + """
"""
# s_sql = s_sql.replace("%PERIOD%", s_period)
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
"""
