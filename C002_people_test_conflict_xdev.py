"""
Script to test PEOPLE conflict of interest
Created on: 8 Apr 2019
Modified on: 18 May 2021
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3
# from fuzzywuzzy import fuzz

# IMPORT OWN MODULES
from _my_modules import funccsv
from _my_modules import funcdatn
from _my_modules import funcfile
from _my_modules import funcsms
from _my_modules import funcsys
from _my_modules import functest
from _my_modules import funcstr
from _my_modules import funcstat

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
source_database_path: str = "W:/People_conflict/"  # Source database path
source_database_name: str = "People_conflict.sqlite"  # Source database
source_database: str = source_database_path + source_database_name
external_data_path: str = "S:/_external_data/"  # external data path
results_path: str = "R:/Kfs/" + funcdatn.get_current_year() + "/"  # Results path
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
with sqlite3.connect(source_database) as sqlite_connection:
    sqlite_cursor = sqlite_connection.cursor()
funcfile.writelog("OPEN DATABASE: " + source_database)

# ATTACH DATA SOURCES
sqlite_cursor.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
sqlite_cursor.execute("ATTACH DATABASE 'W:/People_payroll/People_payroll.sqlite' AS 'PAYROLL'")
funcfile.writelog("%t ATTACH DATABASE: PAYROLL.SQLITE")
sqlite_cursor.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
sqlite_cursor.execute("ATTACH DATABASE 'W:/Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
sqlite_cursor.execute("ATTACH DATABASE 'W:/Vss/Vss_curr.sqlite' AS 'VSSCURR'")
funcfile.writelog("%t ATTACH DATABASE: VSS_CURR.SQLITE")

"""*****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
if l_debug:
    print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

"""*****************************************************************************
TEST ACTIVE CIPC DIRECTOR LIST
*****************************************************************************"""

""" DESCRIPTION
A test to distribute a list of all active CIPC directorships to employees FYI to take into account when
declaring interests.
"""

""" INDEX
"""

"""" TABLES USED IN TEST
"""

# DECLARE TEST VARIABLES
count_findings_after: int = 0
test_description = "Active CIPC director list"
test_file_name: str = "active_cipc_director_list"
test_file_prefix: str = "X200a"
test_finding: str = "ACTIVE CIPC DIRECTOR LIST"
test_report_file: str = "002_reported.txt"

# OBTAIN TEST RUN FLAG
if not functest.get_test_flag(sqlite_cursor, "HR", f"TEST {test_finding}", "RUN"):

    if l_debug:
        print('TEST DISABLED')
    funcfile.writelog("TEST " + test_finding + " DISABLED")

else:

    # OPEN LOG
    if l_debug:
        print("TEST " + test_finding)
    funcfile.writelog("TEST " + test_finding)

    # Fetch initial data from the master table
    if l_debug:
        print("Fetch initial data from the master table...")
    table_name = test_file_prefix + f"a_{test_file_name}"
    s_sql = f"CREATE TABLE {table_name} As " + """
    Select
        d.nwu_number,
        d.employee_name,
        d.national_identifier,
        d.user_person_type,
        d.position_name,
        d.date_submitted,
        d.import_date,
        d.registration_number,
        d.company_name,
        d.enterprise_type,
        d.company_status,
        d.history_date,
        d.business_start_date,
        d.directorship_status,
        d.directorship_start_date,
        d.nwu_number || '-' || d.registration_number As exclude_combination
    From
        X004x_searchworks_directors d    
    ;"""
    if l_debug:
        # print(s_sql)
        pass
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    # Read the list employees and companies to exclude from the test
    exclude_employee_company = funcstat.stat_tuple(sqlite_cursor,
                                         "KFS.X000_Own_kfs_lookups",
                                         "LOOKUP_CODE",
                                         "LOOKUP='EXCLUDE EMPLOYEE COMPANY LIST'")
    if l_debug:
        print('List of employees and companies to exclude:')
        print(exclude_employee_company)
        pass

    # Select the test data
    # Directorship data match with current active vendor but no declaration
    # Match types 0 = No match in declaration
    #             1 = Match on company registration number
    #             2 = Match on company name
    if l_debug:
        print("Identify findings...")
    table_name = test_file_prefix + "b_finding"
    s_sql = f"CREATE TABLE {table_name} As " + f"""
    Select
        'NWU' As org,
        f.nwu_number,
        f.registration_number,
        f.employee_name,
        f.company_name
    From
        {test_file_prefix}a_{test_file_name} f
    Where
        f.exclude_combination Not In {exclude_employee_company}
    ;"""
    if l_debug:
        # print(s_sql)
        pass
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    sqlite_cursor.execute(s_sql)
    sqlite_connection.commit()
    funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    # Count the number of findings
    count_findings_before: int = funcsys.tablerowcount(sqlite_cursor, table_name)
    if l_debug:
        print("*** Found " + str(count_findings_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(count_findings_before) + " " + test_finding + " finding(s)")

    # Get previous findings
    if count_findings_before > 0:
        functest.get_previous_finding(sqlite_cursor, external_data_path, test_report_file, test_finding, "TTTTT")
        sqlite_connection.commit()

    # Set previous findings
    if count_findings_before > 0:
        functest.set_previous_finding(sqlite_cursor)
        sqlite_connection.commit()

    # Add previous findings
    table_name = test_file_prefix + "d_addprev"
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    if count_findings_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        today = funcdatn.get_today_date()
        next_test_date = funcdatn.get_current_year_end()
        s_sql = f"CREATE TABLE {table_name} As" + f"""
        Select
            f.*,
            Lower('{test_finding}') AS PROCESS,
            '{today}' AS DATE_REPORTED,
            '{next_test_date}' AS DATE_RETEST,
            p.PROCESS AS PREV_PROCESS,
            p.DATE_REPORTED AS PREV_DATE_REPORTED,
            p.DATE_RETEST AS PREV_DATE_RETEST,
            p.REMARK
        From
            {test_file_prefix}b_finding f Left Join
            Z001ab_setprev p On
            p.FIELD1 = f.nwu_number And
            p.FIELD2 = f.registration_number
        ;"""
        if l_debug:
            # print(s_sql)
            pass
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    # Build table to update findings
    table_name = test_file_prefix + "e_newprev"
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    if count_findings_before > 0:
        s_sql = f"CREATE TABLE {table_name} As " + f"""
        Select
            p.PROCESS,
            p.nwu_number AS FIELD1,
            p.registration_number AS FIELD2,
            p.employee_name AS FIELD3,
            p.company_name AS FIELD4,
            '' AS FIELD5,
            p.DATE_REPORTED,
            p.DATE_RETEST,
            p.REMARK
        From
            {test_file_prefix}d_addprev p
        Where
            p.PREV_PROCESS Is Null Or
            p.DATE_REPORTED > p.PREV_DATE_RETEST And p.REMARK = ""        
        ;"""
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog(f"%t BUILD TABLE: {table_name}")
        # Export findings to previous reported file
        count_findings_after = funcsys.tablerowcount(sqlite_cursor, table_name)
        if count_findings_after > 0:
            if l_debug:
                print("*** " + str(count_findings_after) + " Finding(s) to report ***")
            sx_path = external_data_path
            sx_file = test_report_file[:-4]
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(sqlite_connection, table_name)
            # Write the data
            l_record_temporary: bool = True
            if l_record and l_record_temporary:
                funccsv.write_data(sqlite_connection, "main", table_name, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(count_findings_after) + " new finding(s) to export")
                funcfile.writelog(f"%t EXPORT DATA: {table_name}")
            if l_mess:
                funcsms.send_telegram('', 'administrator', '<b>' + str(count_findings_before) + '/' + str(
                    count_findings_after) + '</b> ' + test_description)
        else:
            if l_debug:
                print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # Import officers for reporting purposes
    if count_findings_before > 0 and count_findings_after > 0:
        functest.get_officer(sqlite_cursor, "HR", f"TEST {test_finding} OFFICER")
        sqlite_connection.commit()

    # Import supervisors for reporting purposes
    if count_findings_before > 0 and count_findings_after > 0:
        functest.get_supervisor(sqlite_cursor, "HR", f"TEST {test_finding} SUPERVISOR")
        sqlite_connection.commit()

    # Add contact and other details needed to findings
    table_name = test_file_prefix + "h_detail"
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    if count_findings_before > 0 and count_findings_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = f"CREATE TABLE {table_name} As " + f"""
        Select
            p.org,
            'OTHER' As vendor_type,
            p.nwu_number,
            p.employee_name,
            e.user_person_type As emp_person_type,
            e.position_name As emp_position,
            p.registration_number,
            p.company_name,
            d.company_status,
            Lower(e.email_address) As emp_mail1,
            p.nwu_number || '@nwu.ac.za' As emp_mail2,
            -- Supervisor
            e.supervisor_number As sup_number,
            s.name_address As sup_name,
            Lower(s.email_address) As sup_mail1,
            e.supervisor_number || '@nwu.ac.za' As sup_mail2,
            -- Next level supervisor
            s.supervisor_number As sup2_number,
            n.name_address As sup2_name,
            Lower(n.email_address) As sup2_mail1,
            s.supervisor_number || '@nwu.ac.za' As sup2_mail2,
            -- Campus officer / responsible officer
            oc.EMPLOYEE_NUMBER As campus_officer_number,
            oc.NAME_ADDR As campus_officer_name,
            oc.EMAIL_ADDRESS As campus_officer_mail1,        
            Case
                When  oc.EMPLOYEE_NUMBER != '' Then oc.EMPLOYEE_NUMBER||'@nwu.ac.za'
                Else oc.EMAIL_ADDRESS
            End As campus_officer_mail2,
            -- Campus supervisor
            sc.EMPLOYEE_NUMBER As campus_supervisor_number,
            sc.NAME_ADDR As campus_supervisor_name,
            sc.EMAIL_ADDRESS As campus_supervisor_mail1,        
            Case
                When sc.EMPLOYEE_NUMBER != '' Then sc.EMPLOYEE_NUMBER||'@nwu.ac.za'
                Else sc.EMAIL_ADDRESS
            End As campus_supervisor_mail2,
            -- Organization officer
            oo.EMPLOYEE_NUMBER As organization_officer_number,
            oo.NAME_ADDR As organization_officer_name,
            oo.EMAIL_ADDRESS As organization_officer_mail1,        
            Case
                When  oo.EMPLOYEE_NUMBER != '' Then oo.EMPLOYEE_NUMBER||'@nwu.ac.za'
                Else oo.EMAIL_ADDRESS
            End As organization_officer_mail2,
            -- Campus supervisor
            so.EMPLOYEE_NUMBER As organization_supervisor_number,
            so.NAME_ADDR As organization_supervisor_name,
            so.EMAIL_ADDRESS As organization_supervisor_mail1,        
            Case
                When so.EMPLOYEE_NUMBER != '' Then so.EMPLOYEE_NUMBER||'@nwu.ac.za'
                Else so.EMAIL_ADDRESS
            End As organization_supervisor_mail2,
            -- Auditor
            oa.EMPLOYEE_NUMBER As audit_officer_number,
            oa.NAME_ADDR As audit_officer_name,
            oa.EMAIL_ADDRESS As audit_officer_mail1,        
            Case
                When  oa.EMPLOYEE_NUMBER != '' Then oa.EMPLOYEE_NUMBER||'@nwu.ac.za'
                Else oa.EMAIL_ADDRESS
            End As audit_officer_mail2,
            -- Audit supervisor
            sa.EMPLOYEE_NUMBER As audit_supervisor_number,
            sa.NAME_ADDR As audit_supervisor_name,
            sa.EMAIL_ADDRESS As audit_supervisor_mail1,        
            Case
                When sa.EMPLOYEE_NUMBER != '' Then sa.EMPLOYEE_NUMBER||'@nwu.ac.za'
                Else sa.EMAIL_ADDRESS
            End As audit_supervisor_mail2
        From
            {test_file_prefix}d_addprev p Left Join
            X004x_searchworks_directors d On d.nwu_number = p.nwu_number And d.registration_number = p.registration_number Left Join
            PEOPLE.X000_PEOPLE e On e.employee_number = p.nwu_number Left Join
            PEOPLE.X000_PEOPLE s On s.employee_number = e.supervisor_number Left Join
            PEOPLE.X000_PEOPLE n On n.employee_number = s.supervisor_number Left Join
            Z001af_officer oc On oc.CAMPUS = 'OTHER' Left Join
            Z001af_officer oo On oo.CAMPUS = p.org Left Join
            Z001af_officer oa On oa.CAMPUS = 'AUD' Left Join
            Z001ag_supervisor sc On sc.CAMPUS = 'OTHER' Left Join
            Z001ag_supervisor so On so.CAMPUS = p.org Left Join
            Z001ag_supervisor sa On sa.CAMPUS = 'AUD'
        Where
            p.PREV_PROCESS Is Null Or
            p.DATE_REPORTED > p.PREV_DATE_RETEST And p.REMARK = ""
        ;"""
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog(f"%t BUILD TABLE: {table_name}")

    # Build the final table for export and reporting
    table_name = test_file_prefix + "x_" + test_file_name
    sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    if l_debug:
        print("Build the final report")
    if count_findings_before > 0 and count_findings_after > 0:
        s_sql = f"CREATE TABLE {table_name} As " + f"""
        Select
            '{test_finding}' As Audit_finding,
            f.nwu_number || '-' || f.registration_number As Unique_id,
            f.nwu_number As Employee_nwu,
            f.employee_name As Employee,
            f.emp_person_type As Employee_person_type,
            f.emp_position As Employee_position,
            f.emp_mail1 As Employee_mail1,
            f.emp_mail2 As Employee_mail2,
            'Active' As CIPC_director,
            f.registration_number As CIPC_regno,
            f.company_name As CIPC_company,
            f.company_status As CIPC_company_status,
            f.org As Organization,
            f.sup_name As Supervisor,
            f.sup2_name As Supervisor_next,
            f.campus_officer_name As Responsible_officer,
            f.campus_supervisor_name As Responsible_supervisor,
            f.organization_officer_name As Organization_officer,
            f.organization_supervisor_name As Organization_supervisor,
            f.audit_officer_name As Audit_officer,
            f.audit_supervisor_name As Audit_supervisor,
            f.sup_number As Supervisor_nwu,
            f.sup_mail1 As Supervisor_mail1,
            f.sup_mail2 As Supervisor_mail2,
            f.sup2_number As Supervisor_next_nwu,
            f.sup2_mail1 As Supervisor_next_mail1,
            f.sup2_mail2 As Supervisor_next_mail2,
            f.campus_officer_number As Responsible_officer_nwu,
            f.campus_officer_mail1 As Responsible_officer_mail1,
            f.campus_officer_mail2 As Responsible_officer_mail2,
            f.campus_supervisor_number As Responsible_supervisor_nwu,
            f.campus_supervisor_mail1 As Responsible_supervisor_mail1,
            f.campus_supervisor_mail2 As Responsible_supervisor_mail2,
            f.organization_officer_number As Organization_officer_nwu,
            f.organization_officer_mail1 As Organization_officer_mail1,
            f.organization_officer_mail2 As Organization_officer_mail2,
            f.organization_supervisor_number As Organization_supervisor_nwu,
            f.organization_supervisor_mail1 As Organization_supervisor_mail1,
            f.organization_supervisor_mail2 As Organization_supervisor_mail2,
            f.audit_officer_number As Audit_officer_nwu,
            f.audit_officer_mail1 As Audit_officer_mail1,
            f.audit_officer_mail2 As Audit_officer_mail2,
            f.audit_supervisor_number As Audit_supervisor_nwu,
            f.audit_supervisor_mail1 As Audit_supervisor_mail1,
            f.audit_supervisor_mail2 As Audit_supervisor_mail2                
        From
            {test_file_prefix}h_detail f
        ;"""
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog(f"%t BUILD TABLE: {table_name}")
        # Export findings
        if l_export and funcsys.tablerowcount(sqlite_cursor, table_name) > 0:
            print("Export findings...")
            sx_path = results_path
            sx_file = test_file_prefix + "_" + test_finding.lower() + "_"
            sx_file_dated = sx_file + funcdatn.get_today_date_file()
            s_head = funccsv.get_colnames_sqlite(sqlite_connection, table_name)
            funccsv.write_data(sqlite_connection, "main", table_name, sx_path, sx_file, s_head)
            funccsv.write_data(sqlite_connection, "main", table_name, sx_path, sx_file_dated, s_head)
            funcfile.writelog(f"%t EXPORT DATA: {sx_path}{sx_file}")

    else:

        s_sql = f"CREATE TABLE {table_name} (" + """
        BLANK TEXT
        );"""
        sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        sqlite_cursor.execute(s_sql)
        sqlite_connection.commit()
        funcfile.writelog(f"%t BUILD TABLE: {table_name}")

"""*****************************************************************************
END OF SCRIPT
*****************************************************************************"""
if l_debug:
    print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
sqlite_connection.close()

# CLOSE THE LOG WRITER
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C002_PEOPLE_TEST_CONFLICT_DEV")
