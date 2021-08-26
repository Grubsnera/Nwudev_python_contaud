"""
Script to test PEOPLE conflict of interest
Created on: 8 Apr 2019
Modified on: 18 May 2021
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
from _my_modules import funcsms
from _my_modules import funcsys
from _my_modules import functest

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
END OF SCRIPT
"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# DECLARE VARIABLES
so_path: str = "W:/People_conflict/"  # Source database path
re_path: str = "R:/People/" + funcdate.cur_year() + "/"  # Results path
ed_path: str = "S:/_external_data/"  # external data path
so_file: str = "People_conflict.sqlite"  # Source database
s_sql: str = ""  # SQL statements
l_debug: bool = True
l_export: bool = False
l_mess: bool = False
l_mail: bool = False
l_record: bool = False

# OPEN THE SCRIPT LOG FILE
if l_debug:
    print("-----------------------------")
    print("C002_PEOPLE_TEST_CONFLICT_DEV")
    print("-----------------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C002_PEOPLE_TEST_CONFLICT_DEV")
funcfile.writelog("-------------------------------------")

if l_mess:
    funcsms.send_telegram('', 'administrator', 'Testing employee <b>conflict of interest</b>.')

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
if l_debug:
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

"""*****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
if l_debug:
    print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

"""*****************************************************************************
TEST DECLARATION PENDING
*****************************************************************************"""

"""
Test if employee declaration is pending.
    Request remediation from line manager.
Test exclude:
    If pending less than 31 days.                
Created: 26 May 2021 (Albert J v Rensburg NWU:21162395)
"""

# TABLES NEEDED
# X003_dashboard_curr
# PEOPLE.X002_PEOPLE_CURR

# TEST WILL ONLY RUN FROM MAY TO NOVEMBER
if funcdate.cur_month() in ("05", "06", "07", "08", "09", "10", "11"):

    # DECLARE TEST VARIABLES
    i_finding_after: int = 0
    s_description = "Employee declaration pending"
    s_file_name: str = "employee_declaration_pending"
    s_file_prefix: str = "X101b"
    s_finding: str = "EMPLOYEE DECLARATION PENDING"
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
        a.CATEGORY,
        a.PERSON_TYPE,
        a.SUPERVISOR,
        a.DECLARED,
        Cast(Julianday('%TODAY%') - Julianday(a.DECLARATION_DATE) As Int) As DAYS_DECLARED
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
    FIND.DECLARED = 'PENDING' And
    FIND.SUPERVISOR Is Not Null And
    FIND.DAYS_DECLARED > 30
Order By
    FIND.SUPERVISOR,
    FIND.EMPLOYEE    
;"""
s_sql = s_sql.replace("%FILEP%", s_file_prefix)
s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
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
if l_debug:
    print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C002_PEOPLE_TEST_CONFLICT_DEV")
