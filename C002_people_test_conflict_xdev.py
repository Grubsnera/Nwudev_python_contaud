"""
Script to test PEOPLE conflict of interest
Created on: 8 Apr 2019
Modified on: 18 May 2021
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
so_curs.execute("ATTACH DATABASE 'W:/People_payroll/People_payroll.sqlite' AS 'PAYROLL'")
funcfile.writelog("%t ATTACH DATABASE: PAYROLL.SQLITE")
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

""" ****************************************************************************
BUILD DASHBOARD TABLES
*****************************************************************************"""
print("BUILD DASHBOARD TABLES")
funcfile.writelog("BUILD DASHBOARD TABLES")

# BUILD TABLE WITH PAYROLL FOREIGN PAYMENTS FOR THE PREVIOUS MONTH
print("Obtain master list of foreign employee payments...")
sr_file = "X003_foreign_payments"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    pfp.EMPLOYEE_NUMBER,
    Max(pfp.EFFECTIVE_DATE) As LAST_PAYMENT_DATE,
    Count(pfp.RUN_RESULT_ID) As COUNT_PAYMENTS
From
    PAYROLL.X000aa_payroll_history_%PERIOD% pfp
Where
    pfp.ELEMENT_NAME Like 'NWU Foreign Payment%' And
    pfp.EFFECTIVE_DATE Like '%PREVMONTH%%'
Group By
    pfp.EMPLOYEE_NUMBER    
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = s_sql.replace("%PREVMONTH%", funcdate.prev_monthend()[0:7])
if funcdate.prev_monthend()[0:4] == funcdate.prev_year:
    s_sql = s_sql.replace("%PERIOD%", 'prev')
else:
    s_sql = s_sql.replace("%PERIOD%", 'curr')
if l_debug:
    print(s_sql)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
if l_debug:
    so_conn.commit()

# BUILD TABLE WITH UIF PAYMENTS FOR THE PREVIOUS MONTH
print("Obtain master list of uif employee payments...")
sr_file = "X003_uif_payments"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    pfp.EMPLOYEE_NUMBER,
    Max(pfp.EFFECTIVE_DATE) As LAST_PAYMENT_DATE,
    Count(pfp.RUN_RESULT_ID) As COUNT_PAYMENTS
From
    PAYROLL.X000aa_payroll_history_%PERIOD% pfp
Where
    pfp.ELEMENT_NAME Like 'ZA_UIF_Employer_Contribution%' And
    pfp.EFFECTIVE_DATE Like '%PREVMONTH%%'
Group By
    pfp.EMPLOYEE_NUMBER    
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = s_sql.replace("%PREVMONTH%", funcdate.prev_monthend()[0:7])
if funcdate.prev_monthend()[0:4] == funcdate.prev_year:
    s_sql = s_sql.replace("%PERIOD%", 'prev')
else:
    s_sql = s_sql.replace("%PERIOD%", 'curr')
if l_debug:
    print(s_sql)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
if l_debug:
    so_conn.commit()

# BUILD CURRENT DECLARATION DASHBOARD PEOPLE
print("Build current declaration dashboard people data...")
sr_file = "X003aa_people_curr"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    PEOPLE.employee_number AS EMPLOYEE,
    PEOPLE.name_address AS NAME,
    PEOPLE.gender AS GENDER,
    Upper(PEOPLE.race) As RACE,
    Upper(PEOPLE.corr_language) As LANGUAGE,
    Upper(PEOPLE.location) As LOCATION,
    Upper(PEOPLE.employee_category) As ACAD_SUPP,
    Upper(PEOPLE.faculty) As FACULTY,
    Upper(PEOPLE.division) As DIVISION,
    Upper(PEOPLE.assignment_category) As CATEGORY,
    Upper(PEOPLE.grade_calc) As POS_GRADE,
    Upper(PEOPLE.job_name) As JOB_NAME,
    Upper(PEOPLE.user_person_type) As PERSON_TYPE,
    PEOPLE.employee_age As AGE,
    PEOPLE.supervisor_number AS SUPERVISOR,
    CASE
        When PEOPLE.date_started Is Null And PEOPLE.service_start_date Is Null Then PEOPLE.people_start_date
        When PEOPLE.date_started Is Null Then PEOPLE.service_start_date
        Else PEOPLE.date_started
    END As EMP_START
From
    PEOPLE.X000_PEOPLE PEOPLE 
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD CURRENT DECLARATION DASHBOARD DECLARATION DATA
print("Build current declaration dashboard unique declarations...")
sr_file = "X003ab_declarations_curr"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
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
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD CURRENT DECLARATION DASHBOARD DATA
print("Build current declaration dashboard data...")
sr_file = "X003_dashboard_curr"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    p.EMPLOYEE,
    p.NAME,
    p.GENDER,
    p.RACE,
    p.LANGUAGE,
    p.LOCATION,
    p.ACAD_SUPP,
    p.FACULTY,
    p.DIVISION,
    p.CATEGORY,
    p.POS_GRADE,
    p.JOB_NAME,
    p.PERSON_TYPE,
    p.AGE,
    p.SUPERVISOR,
    p.EMP_START,
    d.DECLARATION_ID,
    d.DECLARATION_DATE,
    d.CREATION_DATE,
    d.UNDERSTAND_POLICY,
    d.INTERESTS_TO_DECLARE,
    d.FULL_DISCLOSURE,
    d.DECLARE_STATUS,
    d.LINE_MANAGER,
    d.REJECTION_REASON,
    u.LAST_PAYMENT_DATE As UIF_PAY_DATE,
    u.COUNT_PAYMENTS As UIF_PAY_COUNT,
    f.LAST_PAYMENT_DATE As FOREIGN_PAY_DATE,
    f.COUNT_PAYMENTS As FOREIGN_PAY_COUNT,
    Case
        When d.DECLARE_STATUS IS NOT NULL Then d.DECLARE_STATUS
        Else 'NO DECLARATION'
    End As DECLARED,
    Cast(Julianday('%TODAY%') - Julianday(p.EMP_START) As Int) As DAYS_IN_SERVICE    
From
    X003aa_people_curr p Left Join
    X003ab_declarations_curr d On d.EMPLOYEE = p.EMPLOYEE Left Join
    X003_uif_payments u On p.EMPLOYEE = u.EMPLOYEE_NUMBER Left Join
    X003_foreign_payments f On p.EMPLOYEE = f.EMPLOYEE_NUMBER
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = s_sql.replace("%TODAY%", funcdate.today())
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

"""*****************************************************************************
TEST EMPLOYEE NO DECLARATION
*****************************************************************************"""

"""
Test if employees declared conflict of interest.
    Request remediation from employee supervisor as per declaration.
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

    # OBTAIN TEST RUN FLAG
    if functest.get_test_flag(so_curs, "HR", "TEST " + s_finding, "RUN") == "FALSE":

        if l_debug:
            print('TEST DISABLED')
        funcfile.writelog("TEST " + s_finding + " DISABLED")

    else:

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
            d.EMPLOYEE,
            d.NAME,
            d.GENDER,
            d.RACE,
            d.LANGUAGE,
            d.LOCATION,
            d.ACAD_SUPP,
            d.FACULTY,
            d.DIVISION,
            d.CATEGORY,
            d.POS_GRADE,
            d.JOB_NAME,
            d.PERSON_TYPE,
            d.AGE,
            d.SUPERVISOR,
            d.EMP_START,
            d.UIF_PAY_DATE,
            d.FOREIGN_PAY_DATE,
            d.DECLARED,
            d.DAYS_IN_SERVICE
        From
            X003_dashboard_curr d
        Where
            d.DECLARED = 'NO DECLARATION' And
            d.PERSON_TYPE Not In ('COUNCIL MEMBER', 'ADVISORY BOARD MEMBER', 'AD HOC APPOINTMENT', 'TEMPORARY APPOINTMENT') And
            d.SUPERVISOR Is Not Null And
            d.DAYS_IN_SERVICE > 30
        ;"""
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
            'NWU' As ORG,
            FIND.LOCATION,
            FIND.SUPERVISOR,
            FIND.EMPLOYEE,
            FIND.CATEGORY,
            FIND.PERSON_TYPE
        From
            %FILEP%%FILEN% FIND
        Where
            (((FIND.CATEGORY Like 'PERM%')) Or
            ((FIND.PERSON_TYPE Like 'EX%') And
            (FIND.FOREIGN_PAY_DATE Like '%PREVMONTH%%')) Or
            ((FIND.PERSON_TYPE Like 'EX%') And
            (FIND.UIF_PAY_DATE Like '%PREVMONTH%%')) Or
            ((FIND.CATEGORY Like 'TEMP%') And
            (FIND.PERSON_TYPE Not Like 'EX%')))
        Order By
            FIND.SUPERVISOR,
            FIND.EMPLOYEE    
        ;"""
        """
        Where
            (((FIND.CATEGORY Like 'PERM%')) Or
            ((FIND.PERSON_TYPE Like 'EX%') And
            (FIND.FOREIGN_PAY_DATE Like '%PREVMONTH%%'))
            ((FIND.CATEGORY Like 'TEMP%') And
            (FIND.UIF_PAY_DATE Like '%PREVMONTH%%')))
        """
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
        s_sql = s_sql.replace("%PREVMONTH%", funcdate.prev_monthend()[0:7])
        if l_debug:
            print(s_sql)
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
                PREV.CATEGORY AS FIELD3,
                PREV.PERSON_TYPE AS FIELD4,
                PREV.LOCATION AS FIELD5,
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
                PREV.LOCATION,
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
                FIND.SUPERVISOR As Line_manager_number,
                FIND.SUP_NAME As Line_manager_name,
                FIND.SUP_MAIL1 As Line_manager_mail_address,
                FIND.SUP_MAIL2 As Line_manager_mail_alternate,
                FIND.ORG As Organization,
                FIND.LOCATION As Campus,
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
