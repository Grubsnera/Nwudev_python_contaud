"""
Script to test STUDENT FEES
Created on: 28 Aug 2019
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

# DECLARE VARIABLES
ed_path = "S:/_external_data/"  # External data path
so_path = "W:/Vss_fee/"  # Source database path
so_file = "Vss_test_fee.sqlite"  # Source database
re_path = "R:/Vss/"
l_export: bool = False
l_mail: bool = False
l_record: bool = False
l_vacuum: bool = False
s_period: str = "curr"

# DECLARE VARIABLES
s_reg_trancode: str = "095"
s_qual_trancode: str = "004"
s_modu_trancode: str = "004"
s_burs_trancode: str = "042z052z381z500"
s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
s_mpa: str = "665566"  # Exclude these FQUALLEVELAPID

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN SQLITE SOURCE table
print("Open sqlite database...")
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# ATTACH VSS DATABASE
print("Attach vss database...")
so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")
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
TEST MODULE FEE NOT LOADED
*****************************************************************************"""
print("MODULE FEE NOT LOADED")
funcfile.writelog("MODULE FEE NOT LOADED")

# DECLARE VARIABLES
i_finding_after: int = 0

# JOIN MODULES PRESENTED AND LEVY LIST
print("Join modules presented and levy list...")
sr_file = "X031aa_Modu_nofee_loaded"
s_sql = "Create Table " + sr_file + " As" + """
Select
    PRES.*,
    LIST.AMOUNT As LIST_AMOUNT,
    MODU.MODULE,
    MODU.MODULE_NAME
From
    X030ad_Stud_modu_present PRES Left Join
    X030aa_Fiabd007_summ LIST On LIST.FMODAPID = PRES.FMODULEAPID And LIST.ENROL_ID = PRES.ENROL_ID Left Join
    VSS.X000_Modules MODU On MODU.KENROLMENTPRESENTATIONID = PRES.KENROLMENTPRESENTATIONID
Where
    LIST.AMOUNT Is Null    
;"""
# s_sql = s_sql.replace("%PERIOD%", s_period)
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# JOIN MODULES PRESENTED AND STUDENT LIST
print("Join modules presented and student list...")
sr_file = "X031aa_Modu_nofee_loaded_stud"
s_sql = "Create Table " + sr_file + " As" + """
Select
    STUD.*,
    LIST.*
From
    VSS.X001_Student_module_curr STUD Inner Join
    X031aa_Modu_nofee_loaded LIST On LIST.KENROLMENTPRESENTATIONID = STUD.KENROLMENTPRESENTATIONID And
         LIST.FMODULEAPID = STUD.FMODULEAPID And
         LIST.ENROL_ID = STUD.FENROLMENTCATEGORYCODEID
;"""
# s_sql = s_sql.replace("%PERIOD%", s_period)
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# CREATE SUMMARY OF MODULES
# NOTE EXCLUDE SHORT COURSE
print("Build summary of modules...")
sr_file = "X031aa_Modu_nofee_loaded_modu"
s_sql = "Create Table " + sr_file + " As" + """
Select
    MODU.FMODULEAPID,
    MODU.FENROLMENTCATEGORYCODEID As ENROL_ID,
    MODU.ENROL_CATEGORY As ENROL_CAT,
    MODU.FPRESENTATIONCATEGORYCODEID As PRESENT_ID,
    MODU.PRESENT_CATEGORY As PRESENT_CAT,
    MODU.MODULE,
    MODU.MODULE_NAME,
    MODU.CAMPUS,
    Cast(Count(KENROLSTUDID) As INT) As COUNT_STUD
From
    X031aa_Modu_nofee_loaded_stud MODU
Where
    MODU.ENROL_CATEGORY Not Like ('SHORT COURSE')
Group By
    MODU.FMODULEAPID,
    MODU.FENROLMENTCATEGORYCODEID,
    MODU.FPRESENTATIONCATEGORYCODEID,
    MODU.CAMPUS
;"""
# s_sql = s_sql.replace("%PERIOD%", s_period)
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IDENTIFY FINDINGS
# NOTE EXCLUDE DISTANCE STUDENTS
print("Identify findings...")
sr_file = "X031ab_findings"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    'NWU' As ORG,
    FIND.CAMPUS As LOC,
    FIND.FMODULEAPID As ID,
    FIND.ENROL_ID,
    FIND.ENROL_CAT,
    FIND.MODULE,
    FIND.MODULE_NAME,
    FIND.COUNT_STUD
From
    X031aa_Modu_nofee_loaded_modu FIND
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# COUNT THE NUMBER OF FINDINGS
i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
print("*** Found " + str(i_finding_before) + " exceptions ***")
funcfile.writelog("%t FINDING: " + str(i_finding_before) + " MODULE NO FEE LOADED finding(s)")

# GET PREVIOUS FINDINGS
sr_file = "X031ac_get_previous"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    print("Import previously reported findings...")
    so_curs.execute(
        "CREATE TABLE " + sr_file + """
        (PROCESS TEXT,
        FIELD1 INT,
        FIELD2 TEXT,
        FIELD3 INT,
        FIELD4 TEXT,
        FIELD5 TEXT,
        DATE_REPORTED TEXT,
        DATE_RETEST TEXT,
        DATE_MAILED TEXT)
        """)
    co = open(ed_path + "302_reported.txt", "r")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "PROCESS":
            continue
        elif row[0] != "module no fee loaded":
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

# ADD PREVIOUS FINDINGS
sr_file = "X031ad_add_previous"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    print("Join previously reported to current findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        FIND.*,
        'module no fee loaded' AS PROCESS,
        '%TODAY%' AS DATE_REPORTED,
        '%DAYS%' AS DATE_RETEST,
        PREV.PROCESS AS PREV_PROCESS,
        PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
        PREV.DATE_RETEST AS PREV_DATE_RETEST,
        PREV.DATE_MAILED
    From
        X031ab_findings FIND Left Join
        X031ac_get_previous PREV ON PREV.FIELD1 = FIND.ID And
            PREV.FIELD3 = FIND.ENROL_ID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%", funcdate.today())
    s_sql = s_sql.replace("%DAYS%", funcdate.cur_yearend())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD LIST TO UPDATE FINDINGS
sr_file = "X031ae_new_previous"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.PROCESS,
        PREV.ID AS FIELD1,
        PREV.MODULE AS FIELD2,
        PREV.ENROL_ID AS FIELD3,
        PREV.ENROL_CAT AS FIELD4,
        '' AS FIELD5,
        PREV.DATE_REPORTED,
        PREV.DATE_RETEST,
        PREV.DATE_MAILED
    From
        X031ad_add_previous PREV
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
sr_file = "X031af_officer"
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
            OFFICER.LOOKUP = 'stud_fee_test_modu_no_fee_loaded_officer'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
sr_file = "X031ag_supervisor"
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
        SUPERVISOR.LOOKUP = 'stud_fee_test_modu_no_fee_loaded_supervisor'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# ADD CONTACT DETAILS TO FINDINGS
sr_file = "X031ah_detail"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0 and i_finding_after > 0:
    print("Add contact details to findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.ORG,
        PREV.LOC,
        PREV.ID,
        PREV.MODULE,
        PREV.MODULE_NAME,
        PREV.ENROL_CAT,
        PREV.COUNT_STUD,
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
        ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2
    From
        X031ad_add_previous PREV Left Join
        X031af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
        X031af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
        X031ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
        X031ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
    Where
      PREV.PREV_PROCESS IS NULL
    ;"""
    """
    WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD THE FINAL TABLE FOR EXPORT AND REPORT
sr_file = "X031ax_Modu_nofee_loaded"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
print("Build the final report")
if i_finding_before > 0 and i_finding_after > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'MODULE NO FEE LOADED' As Audit_finding,
        FIND.ORG As 'Organization',
        FIND.LOC As 'Campus',
        FIND.ID As 'Moduleid',
        FIND.MODULE As 'Module',
        FIND.MODULE_NAME As 'Module_name',
        FIND.ENROL_CAT As 'Enrol',
        FIND.COUNT_STUD As 'Student_count',
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
        X031ah_detail FIND
    Order by
        FIND.LOC,
        FIND.MODULE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export findings
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        print("Export findings...")
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "Student_fee_test_031ax_modu_fee_not_loaded_"
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
