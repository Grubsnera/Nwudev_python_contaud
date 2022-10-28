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
    sr_file = "X000_Assignment" + s_period
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file = "X000_Assignment_" + s_period
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        assi.ia_assi_auto As File,
        user.ia_user_name As Auditor,
        assi.ia_assi_year As Year,
        Case
            When assi.ia_assi_year = %year%
            Then 1
            When assi.ia_assi_priority < 9
            Then 2
            Else 3
        End As Year_indicator,
        cate.ia_assicate_name As Category,
        type.ia_assitype_name As Type,
        Case
            When cate.ia_assicate_name = 'Election'
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
        Case
            When Date(assi.ia_assi_startdate) < '%from%'
            Then StrfTime('%Y', assi.ia_assi_startdate) || '-00'
            When Date(assi.ia_assi_startdate) < '%to%'
            Then StrfTime('%Y-%m', assi.ia_assi_startdate)
            When Date(assi.ia_assi_startdate) >= '%from%'
            Then StrfTime('%Y', assi.ia_assi_startdate) || '-00'
            Else StrfTime('%Y-%m', 'now')
        End As Date_opened_month,
        Case
            When Cast((StrfTime("%s", 'now') - StrfTime("%s", assi.ia_assi_startdate)) / 86400.0 As Integer) > 0
            Then Cast((StrfTime("%s", 'now') - StrfTime("%s", assi.ia_assi_startdate)) / 86400.0 As Integer)
            Else 0
        End As Date_opened_days,
        assi.ia_assi_completedate As Date_due,
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
            When assi.ia_assi_priority = 7
            Then StrfTime('%Y-%m', assi.ia_assi_proofdate)
            When assi.ia_assi_priority = 8
            Then StrfTime('%Y-%m', 'now')
            When Date(assi.ia_assi_finishdate) >= '%from%' And Date(assi.ia_assi_finishdate) <= '%to%'
            Then StrfTime('%Y-%m', assi.ia_assi_finishdate)
            Else '00'
        End As Date_closed_month,
        Case
            When Cast((StrfTime("%s", assi.ia_assi_finishdate) - StrfTime("%s", assi.ia_assi_startdate)) / 86400.0
                As Integer) > 0
            Then Cast((StrfTime("%s", assi.ia_assi_finishdate) - StrfTime("%s", assi.ia_assi_startdate)) / 86400.0
                As Integer)
            Else 0
        End As Days_to_close,
        Case
            When Cast((StrfTime("%s", 'now') - StrfTime("%s", assi.ia_assi_proofdate)) / 86400.0 As Integer) > 0
            Then Cast((StrfTime("%s", 'now') - StrfTime("%s", assi.ia_assi_proofdate)) / 86400.0 As Integer)
            Else 0
        End As Days_due,
        user.ia_user_mail,
        Case
            When cate.ia_assicate_name = 'Assignment'
            Then '%nicolene%'
            When cate.ia_assicate_name = 'Special investigation'
            Then '%shahed%'
            Else user.ia_user_mail
        End As Email_manager1,
        '%madelein%' As Email_manager2
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
        ia_lists("curr")
    except Exception as e:
        funcsys.ErrMessage(e)
