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
    l_export: bool = True  # Export findings to text file
    # l_mail: bool = funcconf.l_mail_project
    l_mail: bool = False  # Send email messages
    # l_mess: bool = funcconf.l_mess_project
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

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """ ****************************************************************************
    MASTER FILE LISTS
    *****************************************************************************"""

    """*****************************************************************************
    TEST ACADEMIC SUPPORT INVALID
    *****************************************************************************"""

    # FILES NEEDED
    # X007_grade_leave_master

    # DECLARE TEST VARIABLES
    i_finding_before = 0
    i_finding_after = 0
    s_description = "Academic support invalid"
    s_file_prefix: str = "X007b"
    s_file_name: str = "academic_support_invalid"
    s_finding: str = "ACADEMIC SUPPORT INVALID"
    s_report_file: str = "001_reported.txt"

    # LOG
    funcfile.writelog("TEST " + s_finding)
    if l_debug:
        print("TEST " + s_finding)

    # TODO Delete after first run
    sr_file: str = s_file_prefix + "a_acadsupp"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file: str = s_file_prefix + "b_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file: str = s_file_prefix + "x_acadsupp_invalid"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)

    # OBTAIN MASTER DATA

    sr_file: str = s_file_prefix + "a_a_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)

    if l_debug:
        print("Obtain master data...")
    sr_file: str = s_file_prefix + "a_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        a.ORG,
        a.LOC,
        a.EMPLOYEE_NUMBER,
        a.EMPLOYMENT_CATEGORY,
        a.ACAD_SUPP
    From
        X007_grade_leave_master a
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
        FIND.*
    From
        %FILEP%%FILEN% FIND
    Where
        FIND.ACAD_SUPP = 'OTHER' or
        FIND.ACAD_SUPP Is Null   
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
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
        functest.get_previous_finding(so_curs, ed_path, s_report_file, s_finding, "TITTT")
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
            Z001ab_setprev PREV ON PREV.FIELD1 = EMPLOYEE_NUMBER
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
            PEOP.NAME_LIST,
            PREV.EMPLOYMENT_CATEGORY,
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
            AUD_OFF.EMPLOYEE_NUMBER As AUD_OFF_NUMB,
            AUD_OFF.NAME_ADDR As AUD_OFF_NAME,
            AUD_OFF.EMAIL_ADDRESS As AUD_OFF_MAIL,
            AUD_SUP.EMPLOYEE_NUMBER As AUD_SUP_NUMB,
            AUD_SUP.NAME_ADDR As AUD_SUP_NAME,
            AUD_SUP.EMAIL_ADDRESS As AUD_SUP_MAIL
        From
            %FILEP%d_addprev PREV Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = PREV.EMPLOYEE_NUMBER Left Join
            Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.EMPLOYMENT_CATEGORY Left Join
            Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD' Left Join
            Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
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
            FIND.NAME_LIST As Name,
            FIND.EMPLOYMENT_CATEGORY As Category,
            FIND.ORG As Organization,
            FIND.LOC As Campus,
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

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "Finished <b>" + s_function.upper() + "</b> tests.")

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
