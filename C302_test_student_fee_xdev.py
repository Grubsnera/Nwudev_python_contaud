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
    l_record: bool = False
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
print("Open " + so_file + " database...")
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
QUALIFICATION FEE TEST OVERCHARGE CONTACT
*****************************************************************************"""
print("QUALIFICATION FEE TEST OVERCHARGE CONTACT")
funcfile.writelog("QUALIFICATION FEE TEST OVERCHARGE CONTACT")

# FILES NEEDED
# X020bx_Student_master_sort

# DECLARE TEST VARIABLES
s_fprefix: str = "X021g"
s_finding: str = "QUALIFICATION FEE OVERCHARGE"
s_xfile: str = "302_reported.txt"
i_finding_before: int = 0
i_finding_after: int = 0

# OBTAIN TEST DATA
print("Obtain test data...")
sr_file: str = s_fprefix + "a_qual_fee_overcharge"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        STUD.*
    From
        X020bx_Student_master_sort STUD
    Where
        STUD.VALID = 2 And
        STUD.FEE_SHOULD_BE Like ('% C%')        
    Order By
        STUD.CAMPUS,
        STUD.FEE_SHOULD_BE,
        STUD.KSTUDBUSENTID
;"""
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)
if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
    print("Export findings...")
    sx_path = re_path + "/"
    sx_file = "Student_fee_test_" + s_fprefix + "_" + s_finding + "_studentlist_"
    sx_file_dated = sx_file + funcdate.today_file()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
    funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
    # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
    funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# SELECT TEST DATA
print("Identify findings...")
sr_file = s_fprefix + "b_finding"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    'NWU' As ORG,
    FIND.CAMPUS As LOC,
    FIND.KSTUDBUSENTID As ID,
    FIND.QUALIFICATION,
    FIND.FEE_LEVIED
From
    %FILEP%a_qual_fee_overcharge FIND
Order by
    LOC,
    QUALIFICATION,
    ID   
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
    i = functest.get_previous_finding(so_curs, ed_path, s_xfile, s_finding, "ITTRT")
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
        Z001ab_setprev PREV ON PREV.FIELD1 = FIND.ID And
            PREV.FIELD2 = FIND.LOC And
            PREV.FIELD3 = FIND.QUALIFICATION And
            PREV.FIELD4 = FIND.FEE_LEVIED
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
        PREV.ID AS FIELD1,
        PREV.LOC AS FIELD2,
        PREV.QUALIFICATION AS FIELD3,
        PREV.FEE_LEVIED AS FIELD4,
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
        PREV.ID,
        PREV.FEE_LEVIED,
        MAST.FEE_LEVIED_TYPE,
        MAST.FEE_SHOULD_BE,
        MAST.FEE_MODE,
        MAST.FEE_MODE_HALF,
        MAST.FEE_BURS,
        PREV.QUALIFICATION,
        MAST.QUALIFICATION_NAME,
        MAST.PRESENT_CAT,
        MAST.ENROL_CAT,
        MAST.SEM1,
        MAST.SEM2,
        MAST.SEM7,        
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
        CASE
            WHEN MAST.NAME_ADDR != '' THEN MAST.FUSERBUSINESSENTITYID
        END AS U_NUMB,
        CASE
            WHEN MAST.NAME_ADDR != '' THEN MAST.NAME_ADDR
            ELSE CAMP_OFF.NAME_ADDR
        END AS U_NAME, 
        CASE
            WHEN MAST.NAME_ADDR != '' THEN MAST.FUSERBUSINESSENTITYID||'@nwu.ac.za'
            WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
            ELSE CAMP_OFF.EMAIL_ADDRESS
        END AS U_MAIL, 
        MAST.SYSTEM_DESC        
    From
        %FILEP%d_addprev PREV Left Join
        %FILEP%a_qual_fee_overcharge MAST On MAST.KSTUDBUSENTID = PREV.ID And
            MAST.CAMPUS = PREV.LOC And
            MAST.QUALIFICATION = PREV.QUALIFICATION And
            MAST.FEE_LEVIED = PREV.FEE_LEVIED Left Join
        Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
        Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
        Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
        Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
    Where
        PREV.PREV_PROCESS Is Null Or
        PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_fprefix)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD THE FINAL TABLE FOR EXPORT AND REPORT
sr_file = s_fprefix + "x_qual_fee_overcharge"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
print("Build the final report")
if i_finding_before > 0 and i_finding_after > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        '%FIND%' As Audit_finding,
        FIND.ORG As Organization,
        FIND.LOC As Campus,
        FIND.ID As Student,
        FIND.FEE_LEVIED As Fee_levied,
        FIND.FEE_LEVIED_TYPE As Fee_levied_type,
        FIND.FEE_SHOULD_BE As Fee_should_be_type,
        FIND.FEE_MODE As Fee_normal,
        FIND.FEE_MODE_HALF As Fee_half,
        FIND.FEE_BURS As Bursary_received,
        FIND.QUALIFICATION As Qualification,
        FIND.QUALIFICATION_NAME As Qualification_name,
        FIND.PRESENT_CAT As Present_category,
        FIND.ENROL_CAT As Enrol_category,
        FIND.SEM1 As Sem1_mod,
        FIND.SEM2 As Sem2_mod,
        FIND.SEM7 As Year_mod,
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
        FIND.U_NUMB As Tran_owner_numb,
        FIND.U_NAME As Tran_owner,
        FIND.U_MAIL As Tran_owner_mail,
        FIND.SYSTEM_DESC As System_description            
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
