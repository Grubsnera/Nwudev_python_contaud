"""
Script to test STUDENT BURSARIES
Created on: 29 Jan 2021
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3
import csv

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcstat
from _my_modules import funcsys
from _my_modules import funcsms
from _my_modules import functest

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
END OF SCRIPT
"""

# SCRIPT WIDE VARIABLES
s_function: str = "C303_test_student_bursary_xdev"


def student_bursary(s_period: str = "curr"):
    """
    Script to test STUDENT BURSARIES
    :param s_period: str: The financial period
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # FUNCTION WIDE VARIABLES
    """
    if s_period == "prev":
        s_year = funcdate.prev_year()
    else:
        s_year = funcdate.cur_year()
    """
    ed_path: str = "S:/_external_data/"  # External data path
    re_path: str = "R:/Vss/"
    so_path: str = "W:/Vss_fee/"  # Source database path
    so_file: str = "Vss_test_bursary.sqlite"
    l_debug: bool = True
    # l_mail: bool = funcconf.l_mail_project
    l_mail: bool = False
    # l_mess: bool = funcconf.l_mess_project
    l_mess: bool = False
    l_record: bool = False
    l_export: bool = True
    s_burs_code: str = "('042', '052', '381', '500')"  # Current bursary transaction codes
    s_staff_code = "('021')"  # Staff discount transaction code

    # LOG
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: " + s_function.upper())
    funcfile.writelog("-" * len("script: "+s_function))
    if l_debug:
        print(s_function.upper())

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>" + s_function + "</b>")

    """************************************************************************
    OPEN THE DATABASES
    ************************************************************************"""
    funcfile.writelog("OPEN THE DATABASES")
    if l_debug:
        print("OPEN THE DATABASES")

    # OPEN THE SQLITE DATABASE
    # Create the connection
    so_conn = sqlite3.connect(so_path + so_file)
    # Create the cursor
    so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH VSS DATABASE
    if l_debug:
        print("Attach vss database...")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
    funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss_curr.sqlite' AS 'VSSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: Vss_curr.sqlite")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss_prev.sqlite' AS 'VSSPREV'")
    funcfile.writelog("%t ATTACH DATABASE: Vss_prev.sqlite")
    so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: People.sqlite")

    """************************************************************************
    TEMPORARY AREA
    ************************************************************************"""
    funcfile.writelog("TEMPORARY AREA")
    if l_debug:
        print("TEMPORARY AREA")

    """************************************************************************
    BEGIN OF SCRIPT
    ************************************************************************"""
    funcfile.writelog("BEGIN OF SCRIPT")
    if l_debug:
        print("BEGIN OF SCRIPT")

    """*****************************************************************************
    TEST BURSARY NOT CANCELLED
    *****************************************************************************"""

    # DECLARE TEST VARIABLES
    i_finding_before = 0
    i_finding_after = 0
    s_description = "Bursary not cancelled"
    s_file_prefix: str = "X005a"
    s_file_name: str = "bursary_not_cancelled"
    s_finding: str = "BURSARY NOT CANCELLED"
    s_report_file: str = "303_reported.txt"
    s_exclude_list: str = "('PASS CERTIFICATE'," \
                          "'PASS CERTIFICATE POSTHUMOUSLY'," \
                          "'PASS CERTIFICATE WITH DISTINCTION'," \
                          "'PASS DEGREE'," \
                          "'PASS DEGREE POSTHUMOUSLY'," \
                          "'PASS DEGREE WITH DISTINCTION'," \
                          "'PASS DEGREE WITH DISTINCTION POSTHUMOUSLY'," \
                          "'PASS DIPLOMA'," \
                          "'PASS DIPLOMA WITH DISTINCTION'," \
                          "'COURSE CONVERTED')"

    # OBTAIN TEST RUN FLAG
    if functest.get_test_flag(so_curs, "VSS", "TEST " + s_finding, "RUN") == "FALSE":

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
            burs.student,
            burs.campus,
            burs.total_burs,
            burs.total_loan,
            burs.total_external,
            burs.total_internal,
            burs.total_research,
            burs.total_trust,
            burs.total_other,
            burs.staff_discount,
            burs.active,
            burs.levy_category,
            burs.enrol_category,
            burs.qualification,
            burs.qualification_type,
            burs.discontinue_date,
            burs.discontinue_result,
            burs.discontinue_reason
        From
            X001_Bursary_summary_student burs
        Where
            (burs.total_burs <> 0 And
                burs.active = 'INACTIVE' And
                burs.total_loan = 0 And
                burs.enrol_category Not In ('POST DOC')) Or
            (burs.total_burs <> 0 And
                burs.discontinue_date Is Not Null And
                burs.total_loan = 0 And
                burs.enrol_category Not In ('POST DOC') And
                burs.discontinue_result Not In %RESULT_EXCLUDE%)
        ;"""
        s_sql = s_sql.replace("%RESULT_EXCLUDE%", s_exclude_list)
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()

        # IDENTIFY FINDINGS
        if l_debug:
            print("Identify findings...")
        sr_file: str = s_file_prefix + "b_finding"
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = "Create Table " + sr_file + " As " + """
        Select
            'NWU' As ORG,
            find.campus As LOC,
            find.student As STUDENT,
            Cast(find.total_burs As Int) As TOTAL_BURS
        From
            %FILEP%%FILEB% find
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%FILEB%", 'aa_' + s_file_name)
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
            functest.get_previous_finding(so_curs, ed_path, s_report_file, s_finding, "IITTT")
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
                Z001ab_setprev PREV ON PREV.FIELD1 = FIND.STUDENT
                    And PREV.FIELD2 = FIND.TOTAL_BURS
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
                PREV.STUDENT AS FIELD1,
                PREV.TOTAL_BURS AS FIELD2,
                PREV.LOC AS FIELD3,
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
            functest.get_officer(so_curs, "VSS", "TEST " + s_finding + " OFFICER")
            so_conn.commit()

        # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
        if i_finding_before > 0 and i_finding_after > 0:
            functest.get_supervisor(so_curs, "VSS", "TEST " + s_finding + " SUPERVISOR")
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
                Cast(PREV.STUDENT As TEXT) As STUDENT,
                Round(Cast(BURS.total_burs As REAL),2) As BURSARY_VALUE,
                BURS.active As ACTIVE,
                BURS.levy_category As LEVY_CATEGORY,
                BURS.enrol_category As ENROL_CATEGORY,
                BURS.qualification_type As QUALIFICATION_TYPE,
                BURS.qualification As QUALIFICATION,
                BURS.discontinue_date As DISCONTINUE_DATE,
                BURS.discontinue_result As DISCONTINUE_RESULT,
                BURS.discontinue_reason As DISCONTINUE_REASON,
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
                %FILEP%%FILEB% BURS On BURS.student = PREV.STUDENT Left Join
                Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
                Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
                Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD' Left Join
                Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
                Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
                Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD'                    
            Where
                PREV.PREV_PROCESS Is Null Or
                PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
            Order by
                BURS.ACTIVE,
                PREV.LOC,
                PREV.STUDENT                
            ;"""
            s_sql = s_sql.replace("%FILEP%", s_file_prefix)
            s_sql = s_sql.replace("%FILEN%", s_file_name)
            s_sql = s_sql.replace("%FILEB%", 'aa_' + s_file_name)
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
                FIND.ORG As Organization,
                FIND.LOC As Campus,
                FIND.STUDENT As Student,
                FIND.BURSARY_VALUE As Bursary_value,
                FIND.ACTIVE As Active,
                FIND.LEVY_CATEGORY As Levy_category,
                FIND.ENROL_CATEGORY As Enrol_category,
                FIND.QUALIFICATION_TYPE As Qualification_type,
                FIND.QUALIFICATION As Qualification,
                FIND.DISCONTINUE_DATE As Discontinue_date,
                FIND.DISCONTINUE_RESULT As Discontinue_result,
                FIND.DISCONTINUE_REASON As Discontinue_reason,
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

    """*****************************************************************************
    TEST BURSARY STAFF DISCOUNT AND NSFAS
    *****************************************************************************"""

    # DECLARE TEST VARIABLES
    i_finding_before = 0
    i_finding_after = 0
    s_description = "Bursary staff discount and nsfas"
    s_file_prefix: str = "X005b"
    s_file_name: str = "bursary_staff_discount_and_nsfas"
    s_finding: str = "BURSARY STAFF DISCOUNT AND NSFAS"
    s_report_file: str = "303_reported.txt"

    # OBTAIN TEST RUN FLAG
    if functest.get_test_flag(so_curs, "VSS", "TEST " + s_finding, "RUN") == "FALSE":

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
            burs.student,
            burs.total_burs,
            burs.total_loan,
            burs.total_external,
            burs.total_internal,
            burs.total_research,
            burs.total_trust,
            burs.total_other,
            burs.staff_discount,
            burs.active,
            burs.levy_category,
            burs.enrol_category,
            burs.qualification,
            burs.qualification_type,
            burs.discontinue_date,
            burs.discontinue_result,
            burs.discontinue_reason
        From
            X001_Bursary_summary_student burs
        Where
            burs.staff_discount <> 0
        ;"""
        s_sql = s_sql.replace("%RESULT_EXCLUDE%", s_exclude_list)
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()

    """************************************************************************
    END OF SCRIPT
    ************************************************************************"""
    funcfile.writelog("END OF SCRIPT")
    if l_debug:
        print("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    so_conn.commit()
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("-" * len("completed: "+s_function))
    funcfile.writelog("COMPLETED: " + s_function.upper())

    return


if __name__ == '__main__':
    try:
        student_bursary("curr")
    except Exception as e:
        funcsys.ErrMessage(e)
