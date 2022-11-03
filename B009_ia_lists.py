"""
Script to build internal audit lists
Created on 24 Oct 2022
Author: Albert J v Rensburg (NWU:21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3
import csv

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcstat
from _my_modules import funcsys
from _my_modules import funcsms

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
OBTAIN LIST OF ASSIGNMENTS
IDENTIFY PRIORITY INCONSISTENCY
IDENTIFY STATUS INCONSISTENCY
IDENTIFY YEAR INCONSISTENCY
END OF SCRIPT
"""

# SCRIPT WIDE VARIABLES
s_function: str = "B009_ia_lists"


def ia_lists(s_period: str = "curr"):
    """
    Script to build INTERNAL AUDIT lists
    :param s_period: str: The audit season
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # FUNCTION WIDE VARIABLES
    if s_period == "prev" and funcdate.cur_month() >= "10":
        s_year: str = str(int(funcdate.prev_year()) + 1)
    elif s_period == "prev":
        s_year: str = funcdate.prev_year()
    elif s_period == "curr" and funcdate.cur_month() >= "10":
        s_year: str = str(int(funcdate.cur_year())+1)
    elif s_period == "curr":
        s_year: str = funcdate.cur_year()
    else:
        s_year: str = s_period
    s_from: str = str(int(s_year) - 1) + '-10-01'
    s_to: str = s_year + '-09-30'
    # ed_path: str = "S:/_external_data/"  # External data path
    # re_path: str = "R:/Internal_audit/" + s_year
    so_path: str = "W:/Internal_audit/"  # Source database path
    so_file: str = "Web_ia_nwu.sqlite"
    l_debug: bool = False
    # l_mail: bool = funcconf.l_mail_project
    # l_mail: bool = True
    l_mess: bool = funcconf.l_mess_project
    # l_mess: bool = True
    # l_record: bool = False
    # l_export: bool = False
    s_madelein: str = "11987774@nwu.ac.za"
    s_shahed: str = "10933107@nwu.ac.za"
    s_nicolene: str = "12119180@nwu.ac.za"

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

    # OPEN SQLITE SOURCE table
    if l_debug:
        print("Open sqlite database...")
    with sqlite3.connect(so_path + so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    """
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
    """

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

    """************************************************************************
    OBTAIN LIST OF ASSIGNMENTS
    ************************************************************************"""
    funcfile.writelog("OBTAIN ASSIGNMENTS")
    if l_debug:
        print("OBTAIN ASSIGNMENTS")

    # OBTAIN THE LIST
    if l_debug:
        print("Obtain the list of assignments...")
    sr_file = "X000_Assignment_" + s_period
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        assi.ia_assi_auto As File,
        user.ia_user_name As Auditor,
        assi.ia_assi_year As Year,
        Cast(Case
            When Date(assi.ia_assi_startdate) >= '%from%' And Date(assi.ia_assi_startdate) <= '%to%' 
            Then %year%
            Else '0' 
        End As Integer) As Year_calc,     
        Case
            When assi.ia_assi_year = %year%
            Then 'Current'
            When assi.ia_assi_priority < 9
            Then 'Previous'
            Else 'Period'
        End As Year_indicator,
        cate.ia_assicate_name As Category,
        type.ia_assitype_name As Type,
        Case
            When cate.ia_assicate_name = 'Development'
            Then 'Administration (audit)'     
            When cate.ia_assicate_name = 'Administration (university process)'
            Then 'Administration (process)'        
            When cate.ia_assicate_name = 'Election'
            Then 'Verification audit'
            When cate.ia_assicate_name = 'Reporting'
            Then cate.ia_assicate_name
            When cate.ia_assicate_name = 'Consultation'
            Then cate.ia_assicate_name
            When cate.ia_assicate_name = 'Continuous audit'
            Then cate.ia_assicate_name
            When cate.ia_assicate_name = 'Special investigation'
            Then cate.ia_assicate_name
            Else type.ia_assitype_name
        End As Type_calc,
        assi.ia_assi_priority As Priority_number,
        Case
            When assi.ia_assi_priority = 9
            Then '9-Closed'
            When assi.ia_assi_priority = 8
            Then '8-Continuous'
            When assi.ia_assi_priority = 7
            Then '7-Follow-up'
            When assi.ia_assi_priority = 3
            Then '3-High'
            When assi.ia_assi_priority = 2
            Then '2-Medium'
            When assi.ia_assi_priority = 1
            Then '1-Low'
            Else '0-Inactive'
        End As Priority_word,
        asta.ia_assistat_name As Assignment_status,
        Case
            When SubStr(asta.ia_assistat_name, 1, 2) = '00'
            Then '1-NotStarted'
            When assi.ia_assi_priority = 8
            Then '8-Continuous'
            When assi.ia_assi_priority = 7
            Then '7-Follow-up'
            When Upper(SubStr(asta.ia_assistat_name, 1, 2)) = 'CO'
            Then '9-Completed'
            When Cast(SubStr(asta.ia_assistat_name, 1, 2) As Integer) >= 1 And Cast(SubStr(asta.ia_assistat_name, 1,
                2) As Integer) <= 10
            Then '2-Planning'
            When Cast(SubStr(asta.ia_assistat_name, 1, 2) As Integer) >= 11 And Cast(SubStr(asta.ia_assistat_name,
                1, 2) As Integer) <= 50
            Then '3-FieldworkInitial'
            When Cast(SubStr(asta.ia_assistat_name, 1, 2) As Integer) >= 51 And Cast(SubStr(asta.ia_assistat_name,
                1, 2) As Integer) <= 79
            Then '4-FieldworkFinal'
            When Cast(SubStr(asta.ia_assistat_name, 1, 2) As Integer) >= 80 And Cast(SubStr(asta.ia_assistat_name,
                1, 2) As Integer) <= 89
            Then '5-DraftReport'
            When Cast(SubStr(asta.ia_assistat_name, 1, 2) As Integer) >= 90 And Cast(SubStr(asta.ia_assistat_name,
                1, 2) As Integer) <= 99
            Then '6-FinalReport'
            Else 'Unknown'
        End As Assignment_status_calc,
        assi.ia_assi_name || ' (' || assi.ia_assi_auto || ')' As Assignment,
        assi.ia_assi_startdate As Date_opened,
        assi.ia_assi_si_reportdate As Date_case_reported,
        Case
            When Date(assi.ia_assi_startdate) < '%from%'
            Then StrfTime('%Y', assi.ia_assi_startdate)
            When Date(assi.ia_assi_startdate) < '%to%'
            Then StrfTime('%Y-%m', assi.ia_assi_startdate)
            When Date(assi.ia_assi_startdate) >= '%from%'
            Then StrfTime('%Y', assi.ia_assi_startdate)
            Else StrfTime('%Y-%m', 'now')
        End As Date_opened_month,
        Case
            When Cast((StrfTime("%s", 'now') - StrfTime("%s", assi.ia_assi_startdate)) / 86400.0 As Integer) > 0
            Then Cast((StrfTime("%s", 'now') - StrfTime("%s", assi.ia_assi_startdate)) / 86400.0 As Integer)
            Else 0
        End As Date_opened_days,
        assi.ia_assi_completedate As Date_due,
        assi.ia_assi_si_report2date As Date_due_si,
        assi.ia_assi_proofdate As Date_reported,
        assi.ia_assi_finishdate As Date_closed,
        Case
            When assi.ia_assi_priority = 7
            Then assi.ia_assi_proofdate
            When assi.ia_assi_priority = 8
            Then Date('now')
            Else assi.ia_assi_finishdate
        End As Date_closed_calc,
        Case
            When assi.ia_assi_priority = 7 And Date(assi.ia_assi_proofdate) >= '%from%' And Date(assi.ia_assi_proofdate) <= '%to%'
            Then StrfTime('%Y-%m', assi.ia_assi_proofdate)
            When assi.ia_assi_priority = 7
            Then StrfTime('%Y', assi.ia_assi_proofdate)
            When Date(assi.ia_assi_finishdate) >= '%from%' And Date(assi.ia_assi_finishdate) <= '%to%'
            Then StrfTime('%Y-%m', assi.ia_assi_finishdate)
            Else '00 Unclosed'
        End As Date_closed_month,
        Case
            When Cast((StrfTime("%s", assi.ia_assi_finishdate) - StrfTime("%s", assi.ia_assi_startdate)) / 86400.0
                As Integer) > 0
            Then Cast((StrfTime("%s", assi.ia_assi_finishdate) - StrfTime("%s", assi.ia_assi_startdate)) / 86400.0
                As Integer)
            Else 0
        End As Days_to_close,
        Case
            When assi.ia_assi_priority < 7 And StrfTime("%s", 'now') > StrfTime("%s", assi.ia_assi_completedate)
            Then Cast((StrfTime("%s", 'now') - StrfTime("%s", assi.ia_assi_completedate)) / 86400.0 As Integer)
            Else 0
        End As Days_due,
        Case
            When assi.ia_assi_priority < 7 And StrfTime("%s", 'now') > StrfTime("%s", assi.ia_assi_si_report2date)
            Then Cast((StrfTime("%s", 'now') - StrfTime("%s", assi.ia_assi_si_report2date)) / 86400.0 As Integer)
            Else 0
        End As Days_due_si,
        user.ia_user_mail,
        Case
            When user.ia_user_mail == '%madelein%'
            Then ''
            When user.ia_user_mail == '%shahed%'
            Then ''
            When user.ia_user_mail == '%nicolene%'
            Then ''
            When cate.ia_assicate_name = 'Assignment'
            Then '%nicolene%'
            When cate.ia_assicate_name = 'Special investigation'
            Then '%shahed%'
            Else ''
        End As Email_manager1,
        Case
            When user.ia_user_mail == '%madelein%'
            Then ''
            Else '%madelein%'  
        End As Email_manager2
    From
        ia_assignment assi Inner Join
        ia_user user On user.ia_user_sysid = assi.ia_user_sysid Inner Join
        ia_assignment_status asta On asta.ia_assistat_auto = assi.ia_assistat_auto Inner Join
        ia_assignment_category cate On cate.ia_assicate_auto = assi.ia_assicate_auto Inner Join
        ia_assignment_type type On type.ia_assitype_auto = assi.ia_assitype_auto
    Where
        (assi.ia_assi_year = %year% And
            user.ia_user_active = '1' And
            cate.ia_assicate_private = '0') Or
        (assi.ia_assi_year < %year% And
            assi.ia_assi_priority < 9 And
            user.ia_user_active = '1' And
            cate.ia_assicate_private = '0') Or
        (assi.ia_assi_finishdate >= '%from%' And
            assi.ia_assi_finishdate <= '%to%' And
            user.ia_user_active = '1' And
            cate.ia_assicate_private = '0')
    ;"""
    s_sql = s_sql.replace("%year%", s_year)
    s_sql = s_sql.replace("%from%", s_from)
    s_sql = s_sql.replace("%to%", s_to)
    s_sql = s_sql.replace("%madelein%", s_madelein)
    s_sql = s_sql.replace("%nicolene%", s_nicolene)
    s_sql = s_sql.replace("%shahed%", s_shahed)
    if l_debug:
        print(s_sql)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_curs.execute("SELECT File FROM " + sr_file)
    ri_count: int = len(so_curs.fetchall())
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if l_mess:
        funcsms.send_telegram('', 'administrator', '<b>IA Assignments</b> ' + s_year + ' records ' + str(ri_count))

    """************************************************************************
    IDENTIFY PRIORITY INCONSISTENCY
    ************************************************************************"""
    funcfile.writelog("IDENTIFY PRIORITY INCONSISTENCY")
    if l_debug:
        print("IDENTIFY PRIORITY INCONSISTENCY")

    # OBTAIN THE LIST
    if l_debug:
        print("Identify priority inconsistency...")
    sr_file = "X001_Test_priority_inconsistent_" + s_period
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        'Assignment priority inconsistent' As Test,
        assc.File,
        assc.Auditor,
        assc.Year,
        assc.Category,
        assc.Type,
        assc.Priority_word As AssPriority,
        assc.Assignment_status_calc As AssStatus,
        assc.Date_closed_calc As Date_closed,
        assc.Assignment,
        assc.ia_user_mail,
        assc.Email_manager1,
        assc.Email_manager2
    From
        X000_Assignment_%period% assc
    Where
        assc.Priority_word Not Like ('9%') And
        assc.Assignment_status_calc Like ('9%')
    Order By
        assc.Auditor,
        assc.Category,
        assc.Type,
        assc.Year,
        assc.Assignment
    ;"""
    s_sql = s_sql.replace("%period%", s_period)
    if l_debug:
        print(s_sql)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_curs.execute("SELECT File FROM " + sr_file)
    ri_count: int = len(so_curs.fetchall())
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if l_mess and ri_count > 0:
        funcsms.send_telegram('', 'administrator', '<b>Priority inconsistencies</b> ' + str(ri_count))

    """************************************************************************
    IDENTIFY STATUS INCONSISTENCY
    ************************************************************************"""
    funcfile.writelog("IDENTIFY STATUS INCONSISTENCY")
    if l_debug:
        print("IDENTIFY STATUS INCONSISTENCY")

    # OBTAIN THE LIST
    if l_debug:
        print("Identify status inconsistency...")
    sr_file = "X001_Test_status_inconsistent_" + s_period
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        'Assignment status inconsistent' As Test,
        assc.File,
        assc.Auditor,
        assc.Year,
        assc.Category,
        assc.Type,
        assc.Priority_word As AssPriority,
        assc.Assignment_status_calc As AssStatus,
        assc.Date_closed_calc As Date_closed,
        assc.Assignment,
        assc.ia_user_mail,
        assc.Email_manager1,
        assc.Email_manager2
    From
        X000_Assignment_%period% assc
    Where
        assc.Priority_word Like ('9%') And
        assc.Assignment_status_calc Not Like ('9%')
    Order By
        assc.Auditor,
        assc.Category,
        assc.Type,
        assc.Year,
        assc.Assignment
    ;"""
    s_sql = s_sql.replace("%period%", s_period)
    if l_debug:
        print(s_sql)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_curs.execute("SELECT File FROM " + sr_file)
    ri_count: int = len(so_curs.fetchall())
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if l_mess and ri_count > 0:
        funcsms.send_telegram('', 'administrator', '<b>Status inconsistencies</b> ' + str(ri_count))

    """************************************************************************
    IDENTIFY YEAR INCONSISTENCY
    ************************************************************************"""
    funcfile.writelog("IDENTIFY YEAR INCONSISTENCY")
    if l_debug:
        print("IDENTIFY YEAR INCONSISTENCY")

    # OBTAIN THE LIST
    if l_debug:
        print("Identify year inconsistency...")
    sr_file = "X001_Test_year_inconsistent_" + s_period
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        'Assignment year inconsistent' As Test,
        assc.File,
        assc.Auditor,
        assc.Year,
        assc.Year_calc As "Year should be",
        assc.Date_opened,
        assc.Assignment,
        assc.ia_user_mail,
        assc.Email_manager1,
        assc.Email_manager2
    From
        X000_Assignment_%period% assc
    Where
        assc.Year != assc.Year_calc And
        assc.Year_calc > 0
    Order By
        assc.Auditor,
        assc.Year,
        assc.Assignment
    ;"""
    s_sql = s_sql.replace("%period%", s_period)
    if l_debug:
        print(s_sql)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_curs.execute("SELECT File FROM " + sr_file)
    ri_count: int = len(so_curs.fetchall())
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if l_mess and ri_count > 0:
        funcsms.send_telegram('', 'administrator', '<b>Year inconsistencies</b> ' + str(ri_count))

    """************************************************************************
    ASSIGNMENT OVERDUE
    ************************************************************************"""
    funcfile.writelog("ASSIGNMENT OVERDUE")
    if l_debug:
        print("ASSIGNMENT OVERDUE")

    # OBTAIN THE LIST
    if l_debug:
        print("Assignment overdue...")
    sr_file = "X001_Assignment_overdue_" + s_period
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file = "X001_Test_assignment_overdue_" + s_period
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        'Assignment overdue' As Test,
        assc.File,
        assc.Auditor,
        assc.Year,
        assc.Category,
        assc.Type,
        assc.Priority_word As AssPriority,
        assc.Assignment_status_calc As AssStatus,
        Case
            When assc.Date_case_reported <> ''
            Then assc.Date_case_reported
            Else assc.Date_opened
        End As Date_opened,
        Case
            When assc.Date_due_si <> ''
            Then assc.Date_due_si
            Else assc.Date_due
        End As Date_due,
        Case
            When assc.Days_due_si > 0
            Then assc.Days_due_si
            Else assc.Days_due
        End As Days_overdue,
        assc.Assignment,
        assc.ia_user_mail,
        assc.Email_manager1,
        assc.Email_manager2
    From
        X000_Assignment_%period% assc
    Where
        Case
            When assc.Days_due_si > 0
            Then assc.Days_due_si
            When assc.Days_due > 0 And assc.Date_due_si = ''
            Then assc.Days_due
            Else 0
        End > 0
    Order By
        assc.Auditor,
        Days_overdue Desc,
        assc.Assignment
    ;"""
    s_sql = s_sql.replace("%period%", s_period)
    if l_debug:
        print(s_sql)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_curs.execute("SELECT File FROM " + sr_file)
    ri_count: int = len(so_curs.fetchall())
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    if l_mess and ri_count > 0:
        funcsms.send_telegram('', 'administrator', '<b>Assignment overdue</b> ' + str(ri_count))

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
        ia_lists('curr')
    except Exception as e:
        funcsys.ErrMessage(e)
