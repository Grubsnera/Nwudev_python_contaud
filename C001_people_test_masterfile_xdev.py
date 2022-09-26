"""
Script to test PEOPLE master file data
Created on: 20 Apr 2021
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcmail
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

# SCRIPT WIDE VARIABLES
s_function: str = "C001_people_test_masterfile_xdev"


def people_test_masterfile_xdev():
    """
    Script to test multiple PEOPLE MASTER FILE items
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # DECLARE VARIABLES
    so_path = "W:/People/"  # Source database path
    re_path = "R:/People/"  # Results path
    ed_path = "S:/_external_data/"  # external data path
    so_file = "People_test_masterfile.sqlite"  # Source database
    s_sql = ""  # SQL statements
    l_debug: bool = True  # Display statements on screen
    l_export: bool = False  # Export findings to text file
    l_mail: bool = funcconf.l_mail_project
    l_mail: bool = True  # Send email messages
    l_mess: bool = funcconf.l_mess_project
    l_mess: bool = False  # Send communicator messages
    l_record: bool = False  # Record findings for future use
    i_finding_before: int = 0
    i_finding_after: int = 0

    # LOG
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: " + s_function.upper())
    funcfile.writelog("-" * len("script: "+s_function))
    if l_debug:
        print(s_function.upper())

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>" + s_function.upper() + "</b>")

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN THE WORKING DATABASE
    with sqlite3.connect(so_path + so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH DATA SOURCES
    so_curs.execute("ATTACH DATABASE '" + so_path + "People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/People_payroll/People_payroll.sqlite' AS 'PAYROLL'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

    """ ****************************************************************************
    TEMPORARY SCRIPT
    *****************************************************************************"""

    # TODO Delete after first run

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """ ****************************************************************************
    MASTER FILE LISTS
    *****************************************************************************"""

    """ ****************************************************************************
    SPOUSE INSURANCE MASTER FILES
    *****************************************************************************"""

    # IMPORT SPOUSE BENCHMARK
    sr_file = "X009_spouse_matrix"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Import spouse matrix...")
    so_curs.execute(
        "CREATE TABLE " + sr_file + "(PERSON_TYPE, MARITAL_STATUS, TEST1)")
    s_cols = ""
    co = open(ed_path + "001_employee_marital_status.csv", "r")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "PERSON_TYPE":
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the impoted data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_employee_marital_status.csv (" + sr_file + ")")

    # OBTAIN MASTER DATA
    # if test <> '1' then employee does not form part of the test
    # if married = '1' then married for all married person types
    if l_debug:
        print("Obtain employee and spouse data...")
    sr_file: str = 'X009_people_spouse_all'
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        p.employee_number,
        p.person_id,
        p.name_address,
        p.user_person_type,
        cast(t.TEST1 As Int) As test,
        p.marital_status,
        cast(m.TEST1 As Int) As married,
        p.date_started,
        cast(i.ELEMENT_VALUE As Int) As spouse_insurance_status,
        cast(s.spouse_age As Int) As spouse_age,
        s.person_extra_info_id,
        s.spouse_number,
        s.spouse_active,
        s.spouse_address,
        s.spouse_date_of_birth,
        s.spouse_national_identifier,
        s.spouse_passport,
        s.spouse_start_date,
        s.spouse_end_date,
        s.spouse_create_date,
        s.spouse_created_by,
        s.spouse_update_date,
        s.spouse_updated_by,
        s.spouse_update_login
    From
        PEOPLE.X000_PEOPLE p Left Join
        PEOPLE.X000_GROUPINSURANCE_SPOUSE i On i.EMPLOYEE_NUMBER = p.employee_number Left Join
        PEOPLE.X002_SPOUSE_CURR s On s.employee_number = p.employee_number Left Join
        X009_spouse_matrix t On t.PERSON_TYPE = p.user_person_type Left Join
        X009_spouse_matrix m On m.MARITAL_STATUS = p.marital_status
    Group By
        p.employee_number    
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # Notes: Tests that can be performed on spouse insurance
    # 1a. SPOUSE INSURANCE AFTER 65
    #     test = 1 and married = 1 and insurance status is 1 or 2 and age > 65
    #     Spouse insurance must be stopped in December in the year they turn 65.
    # 1b. NO ACTIVE SPOUSE ON RECORD
    #     test = 1 and married = 1 and insurance status is 1 or 2 and person_extra_info_id is null (no spouse)
    #     Employee must provide spouse details.
    # 1c. COMPULSORY SPOUSE INSURANCE IF EMPLOYED AFTER 1 JAN 2022
    #     test = 1 and married = 1 and date_started >= 2022-01-01 and insurance status = 0 or null (must have)
    #     Must have spouse insurance. Transfers etc should be excluded.
    # 2a. NOT MARRIED BUT ACTIVE SPOUSE INSURANCE
    #     married is null and insurance status is 1 or 2
    #     Cancel insurance.
    # 2b. NOT MARRIED BUT ACTIVE SPOUSE RECORD
    #     married is null and person_extra_info_id is not null
    #     End date spouse record.

    """*****************************************************************************
    TEST NOT MARRIED ACTIVE SPOUSE RECORD
    *****************************************************************************"""

    # DEFAULT TRANSACTION OWNER PEOPLE
    # 21022402 MS AC COERTZEN for all employees
    # Exclude 12795631 MR R VAN DEN BERG
    # Exclude 13277294 MRS MC STRYDOM

    # DECLARE TEST VARIABLES
    i_finding_before = 0
    i_finding_after = 0
    s_description = "Not married active spouse record"
    s_file_prefix: str = "X009e"
    s_file_name: str = "not_married_active_spouse_record"
    s_finding: str = "NOT MARRIED ACTIVE SPOUSE RECORD"
    s_report_file: str = "001_reported.txt"

    # OBTAIN TEST RUN FLAG
    if functest.get_test_flag(so_curs, "HR", "TEST " + s_finding, "RUN") == "FALSE":

        if l_debug:
            print('TEST DISABLED')
        funcfile.writelog("TEST " + s_finding + " DISABLED")

    else:

        # LOG
        funcfile.writelog("TEST " + s_finding)
        if l_debug:
            print("TEST " + s_finding)

        # OBTAIN MASTER DATA 1
        if l_debug:
            print("Obtain master data...")
        sr_file: str = s_file_prefix + "aa_" + s_file_name
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = "Create Table " + sr_file + " As " + """
        Select
            'NWU' As ORG,
            Substr(p.location,1,3) As LOC,
            s.employee_number As EMPLOYEE_NUMBER,
            s.name_address As EMPLOYEE,
            s.user_person_type As PERSON_TYPE,
            s.marital_status As MARITAL_STATUS,
            s.spouse_address As SPOUSE
        From
            X009_people_spouse_all s Left Join
            PEOPLE.X000_people p On p.employee_number = s.employee_number
        Where
            s.married Is Null
        ;"""
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()

        # IDENTIFY FINDINGS
        if l_debug:
            print("Identify findings...")
        sr_file = s_file_prefix + "b_finding"
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            FIND.ORG,
            FIND.LOC,
            FIND.EMPLOYEE_NUMBER,
            FIND.EMPLOYEE,
            FIND.PERSON_TYPE,
            FIND.MARITAL_STATUS,
            FIND.SPOUSE
        From
            %FILEP%%FILEN% FIND
        Where
            FIND.SPOUSE Is Not Null
        Order By
            FIND.EMPLOYEE_NUMBER
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%FILEN%", "aa_" + s_file_name)
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()

        # COUNT THE NUMBER OF FINDINGS
        if l_debug:
            print("Count the number of findings...")
        i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
        funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")
        if l_debug:
            print("*** Found " + str(i_finding_before) + " exceptions ***")

        # GET PREVIOUS FINDINGS
        if i_finding_before > 0:
            functest.get_previous_finding(so_curs, ed_path, s_report_file, s_finding, "TTTTT")
            if l_debug:
                so_conn.commit()

        # SET PREVIOUS FINDINGS
        if i_finding_before > 0:
            functest.set_previous_finding(so_curs)
            if l_debug:
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
                '%DATETEST%' AS DATE_RETEST,
                PREV.PROCESS AS PREV_PROCESS,
                PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
                PREV.DATE_RETEST AS PREV_DATE_RETEST,
                PREV.REMARK
            From
                %FILEP%b_finding FIND Left Join
                Z001ab_setprev PREV ON PREV.FIELD1 = FIND.EMPLOYEE_NUMBER
            ;"""
            s_sql = s_sql.replace("%FINDING%", s_finding)
            s_sql = s_sql.replace("%FILEP%", s_file_prefix)
            s_sql = s_sql.replace("%TODAY%", funcdate.today())
            s_sql = s_sql.replace("%DATETEST%", funcdate.cur_monthendnext())
            so_curs.execute(s_sql)
            funcfile.writelog("%t BUILD TABLE: " + sr_file)
            if l_debug:
                so_conn.commit()

        # BUILD LIST TO UPDATE FINDINGS
        sr_file = s_file_prefix + "e_newprev"
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        if i_finding_before > 0:
            if l_debug:
                print("Build list to update findings...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                PREV.PROCESS,
                PREV.EMPLOYEE_NUMBER AS FIELD1,
                '' AS FIELD2,
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
            funcfile.writelog("%t BUILD TABLE: " + sr_file)
            if l_debug:
                so_conn.commit()
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
                funcfile.writelog("%t FINDING: No new findings to export")
                if l_debug:
                    print("*** No new findings to report ***")

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
                PREV.EMPLOYEE_NUMBER,
                PREV.EMPLOYEE,
                PREV.PERSON_TYPE,
                PREV.MARITAL_STATUS,
                PREV.SPOUSE,
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
                Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = 'ALL' Left Join
                Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
                Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD' Left Join
                Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = 'ALL' Left Join
                Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
                Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD'                    
            Where
                PREV.PREV_PROCESS Is Null Or
                PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
            ;"""
            s_sql = s_sql.replace("%FILEP%", s_file_prefix)
            s_sql = s_sql.replace("%FILEN%", s_file_name)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
        sr_file = s_file_prefix + "x_" + s_file_name
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        if i_finding_before > 0 and i_finding_after > 0:
            if l_debug:
                print("Build the final report")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                '%FIND%' As Audit_finding,
                FIND.EMPLOYEE_NUMBER As Employee,
                FIND.EMPLOYEE As Name,
                FIND.PERSON_TYPE As Type,
                FIND.MARITAL_STATUS As Marital_status,
                FIND.SPOUSE As Spouse,
                FIND.LOC As Campus,
                FIND.CAMP_OFF_NAME AS Officer,
                FIND.CAMP_OFF_NUMB AS Officer_Numb,
                FIND.CAMP_OFF_MAIL1 AS Officer_Mail,
                FIND.CAMP_SUP_NAME AS Supervisor,
                FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
                FIND.CAMP_SUP_MAIL1 AS Supervisor_Mail,
                FIND.ORG_OFF_NAME AS Org_Officer,
                FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
                FIND.ORG_OFF_MAIL1 AS Org_Officer_Mail,
                FIND.ORG_SUP_NAME AS Org_Supervisor,
                FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
                FIND.ORG_SUP_MAIL1 AS Org_Supervisor_Mail,
                FIND.AUD_OFF_NAME AS Audit_Officer,
                FIND.AUD_OFF_NUMB AS Audit_Officer_Numb,
                FIND.AUD_OFF_MAIL AS Audit_Officer_Mail,
                FIND.AUD_SUP_NAME AS Audit_Supervisor,
                FIND.AUD_SUP_NUMB AS Audit_Supervisor_Numb,
                FIND.AUD_SUP_MAIL AS Audit_Supervisor_Mail
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
                if l_debug:
                    print("Export findings...")
                sx_path = re_path + funcdate.cur_year() + "/"
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

    """ ****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    funcfile.writelog("END OF SCRIPT")
    if l_debug:
        print("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    so_conn.commit()
    so_conn.close()

    # CLOSE THE LOG
    funcfile.writelog("-" * len("completed: "+s_function))
    funcfile.writelog("COMPLETED: " + s_function.upper())

    return


if __name__ == '__main__':
    try:
        people_test_masterfile_xdev()
    except Exception as e:
        funcsys.ErrMessage(e,
                           funcconf.l_mess_project,
                           "C001_people_test_masterfile_xdev",
                           "C001_people_test_masterfile_xdev")
