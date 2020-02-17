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
    d_sem1_con = "2020-02-17"
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

"""*****************************************************************************
QUALIFICATION FEE TEST FEE LOADED INCORRECTLY
*****************************************************************************"""
print("QUALIFICATION FEE TEST FEE LOADED INCORRECTLY")
funcfile.writelog("QUALIFICATION FEE TEST FEE LOADED INCORRECTLY")

# FILES NEEDED
# X022ae_Qual_present_final

# DECLARE TEST VARIABLES
s_fprefix: str = "X022b"
s_finding: str = "QUALIFICATION FEE LOADED INCORRECTLY"
s_xfile: str = "302_reported.txt"
i_finding_before: int = 0
i_finding_after: int = 0

# OBTAIN TEST DATA
print("Obtain test data...")
sr_file: str = s_fprefix + "a_fee_loaded_incorrectly"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    TEST.FQUALLEVELAPID,
    TEST.CAMPUS,
    TEST.PRESENT_ID,
    TEST.PRESENT_CAT,
    TEST.ENROL_ID,
    TEST.ENROL_CAT,
    TEST.QUALIFICATION,
    TEST.QUALIFICATION_NAME,
    TEST.COUNT_STUD,
    TEST.AMOUNT,
    TEST.COUNT_OCC
From
    X022ae_Qual_present_final TEST
;"""
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# SELECT TEST DATA
print("Identify findings...")
sr_file = s_fprefix + "b_finding"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    'NWU' As ORG,
    FIND.CAMPUS As LOC,    
    FIND.QUALIFICATION,
    FIND.QUALIFICATION_NAME,
    FIND.FQUALLEVELAPID,
    FIND.PRESENT_ID,
    FIND.PRESENT_CAT,
    FIND.ENROL_ID,
    FIND.ENROL_CAT,
    FIND.COUNT_STUD,
    FIND.AMOUNT
From
    %FILEP%a_fee_loaded_incorrectly FIND
Where
    FIND.COUNT_OCC = 1
;"""
s_sql = s_sql.replace("%FILEP%", s_fprefix)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# COUNT THE NUMBER OF FINDINGS
i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
print("*** Found " + str(i_finding_before) + " exceptions ***")
funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")

# GET PREVIOUS FINDINGS
if i_finding_before > 0:
    i = functest.get_previous_finding(so_curs, ed_path, s_xfile, s_finding, "ITIIT")
    so_conn.commit()

# SET PREVIOUS FINDINGS
if i_finding_before > 0:
    i = functest.set_previous_finding(so_curs)
    so_conn.commit()

# ADD PREVIOUS FINDINGS
sr_file = s_fprefix + "d_addprev"
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
        Z001ab_setprev PREV ON PREV.FIELD1 = FIND.FQUALLEVELAPID And
            PREV.FIELD2 = FIND.LOC And
            PREV.FIELD3 = FIND.PRESENT_ID And
            PREV.FIELD4 = FIND.ENROL_ID And
            PREV.FIELD5 = FIND.QUALIFICATION
    ;"""
    s_sql = s_sql.replace("%FINDING%", s_finding)
    s_sql = s_sql.replace("%FILEP%", s_fprefix)
    s_sql = s_sql.replace("%TODAY%", funcdate.today())
    s_sql = s_sql.replace("%DAYS%", funcdate.cur_monthend())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD LIST TO UPDATE FINDINGS
sr_file = s_fprefix + "e_newprev"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.PROCESS,
        PREV.FQUALLEVELAPID AS FIELD1,
        PREV.LOC AS FIELD2,
        PREV.PRESENT_ID AS FIELD3,
        PREV.ENROL_ID AS FIELD4,
        PREV.QUALIFICATION AS FIELD5,
        PREV.DATE_REPORTED,
        PREV.DATE_RETEST,
        PREV.REMARK
    From
        %FILEP%d_addprev PREV
    Where
        PREV.PREV_PROCESS Is Null Or
        PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""        
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_fprefix)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export findings to previous reported file
    i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
    if i_finding_after > 0:
        print("*** " + str(i_finding_after) + " Finding(s) to report ***")
        sx_path = ed_path
        sx_file = s_xfile[:-4]
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
if i_finding_before > 0 and i_finding_after > 0:
    i = functest.get_officer(so_curs, "VSS", "TEST " + s_finding + " OFFICER")
    so_conn.commit()

# IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
if i_finding_before > 0 and i_finding_after > 0:
    i = functest.get_supervisor(so_curs, "VSS", "TEST " + s_finding + " SUPERVISOR")
    so_conn.commit()

# ADD CONTACT DETAILS TO FINDINGS
sr_file = s_fprefix + "h_detail"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0 and i_finding_after > 0:
    print("Add contact details to findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.ORG,
        PREV.LOC,
        PREV.QUALIFICATION,
        PREV.QUALIFICATION_NAME,
        PREV.FQUALLEVELAPID,
        PREV.PRESENT_ID,
        PREV.PRESENT_CAT,
        PREV.ENROL_ID,
        PREV.ENROL_CAT,
        PREV.COUNT_STUD,
        PREV.AMOUNT,    
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
        %FILEP%d_addprev PREV
        Left Join Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC
        Left Join Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
        Left Join Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC
        Left Join Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
    Where
        PREV.PREV_PROCESS Is Null Or
        PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_fprefix)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD THE FINAL TABLE FOR EXPORT AND REPORT
sr_file = s_fprefix + "x_work_permit_expire"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
print("Build the final report")
if i_finding_before > 0 and i_finding_after > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        '%FIND%' As Audit_finding,
        FIND.ORG As Organization,
        FIND.LOC As Campus,
        FIND.FQUALLEVELAPID As Qualification_id,
        FIND.QUALIFICATION As Qualification,
        FIND.QUALIFICATION_NAME As Qualification_name,
        FIND.PRESENT_ID As Present_id,
        FIND.PRESENT_CAT As Present_category,
        FIND.ENROL_ID As Enrol_id,
        FIND.ENROL_CAT As Enrol_category,
        FIND.COUNT_STUD As Student_count,
        FIND.AMOUNT As Fee_amount,    
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
        %FILEP%h_detail FIND
    ;"""
    s_sql = s_sql.replace("%FIND%", s_finding)
    s_sql = s_sql.replace("%FILEP%", s_fprefix)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export findings
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        print("Export findings...")
        sx_path = re_path + "/"
        sx_file = "Qualification_test_" + s_fprefix + "_" + s_finding.lower() + "_"
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
