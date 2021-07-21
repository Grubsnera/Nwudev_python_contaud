"""
PREPARE AND SEND AUDIT REPORTS VIA EMAIL FROM THE NWUIA WEB
WORK IN PROGRESS REPORT
Script: D003_robot_report_audit_wip.py
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 20 Nov 2020
"""

# IMPORT PYTHON MODULES
# import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcmail
from _my_modules import funcmysql
from _my_modules import funcsms
from _my_modules import funcsys

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
BUILD THE REPORT
END OF SCRIPT
"""

# VARIABLES
s_function: str = "D003_report_audit_wip"


def robot_report_audit_wip(s_year: str = "", s_type: str = "", s_name: str = "", s_mail: str = ""):
    """
    SEARCH VSS.PARTY FOR NAMES, NUMBERS AND ID'S
    :param s_year: Working year
    :param s_type: Report type
    :param s_name: The name of the requester / recipient
    :param s_mail: The requester mail address
    :return: str: Info in message format
    """

    from datetime import datetime

    # DECLARE VARIABLES
    l_debug: bool = False

    """*************************************************************************
    ENVIRONMENT
    *************************************************************************"""
    if l_debug:
        print("ENVIRONMENT")

    # DECLARE VARIABLES
    re_path: str = "R:/Audit/"  # Results

    l_mess: bool = funcconf.l_mess_project
    # l_mess: bool = False
    s_file: str = "Report_assignment_wip.csv"
    s_message: str = "Audit assignment work in progress (wip) report for year " + s_year + " "

    # LOG
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: " + s_function.upper())
    funcfile.writelog("-" * len("script: "+s_function))
    if l_debug:
        print(s_function.upper())

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>" + s_function.upper() + "</b>")

    """****************************************************************************
    OPEN THE DATABASES
    ****************************************************************************"""

    if l_debug:
        print("OPEN THE MYSQL DATABASE")
    funcfile.writelog("OPEN THE MYSQL DATABASE")

    # VARIABLES
    s_source_database: str = "Web_ia_nwu"

    # OPEN THE SOURCE FILE
    ms_from_connection = funcmysql.mysql_open(s_source_database)
    ms_from_cursor = ms_from_connection.cursor()
    funcfile.writelog("%t OPEN MYSQL DATABASE: " + s_source_database)

    """*****************************************************************************
    BUILD THE REPORT
    *****************************************************************************"""
    funcfile.writelog("BUILD THE REPORT")
    if l_debug:
        print("BUILD THE REPORT")

    # BUILD THE HEADINGS
    s_line: str = "AUDITOR,"
    s_line += "YEAR,"
    s_line += "ASSIGNMENT,"
    s_line += "TYPE,"
    s_line += "PRIORITY,"
    s_line += "STATUS,"
    s_line += "START_DATE,"
    s_line += "DUE_DATE,"
    s_line += "COMPLETE_DATE,"
    s_line += "NOTES"
    funcfile.writelog(s_line, re_path, s_file)

    # BUILD THE WHERE CLAUSE
    if s_year != "":
        s_where = "ia_assi_year = " + s_year
        s_where += " or "
        s_where += "ia_assi_year < " + s_year + " and ia_assi_priority < 8"
    else:
        s_where = "ia_assi_year = " + funcdate.cur_year()
        s_where += " or "
        s_where += "ia_assi_year < " + funcdate.cur_year() + " and ia_assi_priority < 8"

    # BUILD THE SQL QUERY
    s_sql = """
    SELECT
    ia_user.ia_user_name,
    ia_assignment.ia_assi_year,
    ia_assignment.ia_assi_name,
    ia_assignment_type.ia_assitype_name, 
    ia_assignment.ia_assi_priority,
    ia_assignment_status.ia_assistat_name,
    ia_assignment.ia_assi_startdate,
    ia_assignment.ia_assi_completedate,
    ia_assignment.ia_assi_finishdate,
    ia_assignment.ia_assi_desc,
    ia_assignment.ia_assi_auto
    FROM
    ia_assignment Left Join
    ia_user On ia_user.ia_user_sysid = ia_assignment.ia_user_sysid Left Join
    ia_assignment_type On ia_assignment_type.ia_assitype_auto = ia_assignment.ia_assitype_auto Left Join
    ia_assignment_status On ia_assignment_status.ia_assistat_auto = ia_assignment.ia_assistat_auto
    WHERE %WHERE%
    ORDER BY
    ia_user_name,
    ia_assi_year,
    ia_assi_completedate,
    ia_assitype_name,
    ia_assi_priority desc,
    ia_assi_name
    ;
    """
    s_sql = s_sql.replace("%WHERE%", s_where)

    if l_debug:
        print(s_sql)

    # BUILD THE ASSIGNMENT RECORD
    for row in ms_from_cursor.execute(s_sql).fetchall():

        # USER NAME
        s_line = '"' + row[0] + '",'

        # YEAR
        s_line += str(row[1]) + ","

        # NAME
        s_line += '"' + row[2].replace(",", "") + ' (' + str(row[10]) + ')",'

        # TYPE
        s_line += '"' + row[3] + '",'

        # PRIORITY
        s_priority: str = '"Inactive"'
        if row[4] == "9":
            s_priority = '"Closed"'
        elif row[4] == "8":
            s_priority = '"Continuous"'
        elif row[4] == "3":
            s_priority = '"High"'
        elif row[4] == "2":
            s_priority = '"Medium"'
        elif row[4] == "1":
            s_priority = '"Low"'
        s_line += s_priority + ","

        # STATUS
        s_line += '"' + row[5] + '",'

        # START DATE
        if row[6]:
            s_line += datetime.strftime(row[6], "%Y-%m-%d") + ","
        else:
            s_line += ","

        # DUE DATE
        if row[7]:
            s_line += datetime.strftime(row[7], "%Y-%m-%d") + ","
        else:
            s_line += ","

        # FINISH DATE
        if row[8]:
            s_line += datetime.strftime(row[8], "%Y-%m-%d") + ","
        else:
            s_line += ","

        # NOTES
        s_line += '"' + row[9].replace(",", "") + '"'

        # WRITE TO FILE
        funcfile.writelog(s_line, re_path, s_file)

    if l_debug:
        print(s_line)
    funcfile.writelog("%t Audit assignment wip report requested by " + s_name)
    s_report = "Include all assignments for the year mentioned and "
    s_report += "all previous assignments with an unclosed priority."

    # MAIL THE AUDIT REPORT
    if s_name != "" and s_mail != "":
        funcfile.writelog("%t Audit assignment wip report mailed to " + s_mail)
        if l_debug:
            print("Send the report...")
        s_body: str = "Attached please find audit assignment work in progress (wip) report as requested for " + \
                      s_year + "."
        s_body += "\n\r"
        s_body += s_report
        funcmail.send(s_name,
                      s_mail,
                      "E",
                      "Report audit assignment work in progress (wip) " + s_year,
                      s_body,
                      re_path,
                      s_file)
        s_message += " was mailed to " + s_mail

    # POPULATE THE RETURN MESSAGE
    s_return_message = s_message

    # DELETE THE MAILED FILE
    if funcfile.file_delete(re_path, s_file):
        funcfile.writelog("%t Audit assignment wip report deleted")
        if l_debug:
            print("Delete the report...")

    """*****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    funcfile.writelog("END OF SCRIPT")
    if l_debug:
        print("END OF SCRIPT")

    # CLOSE THE LOG WRITER
    funcfile.writelog("-" * len("completed: "+s_function))
    funcfile.writelog("COMPLETED: " + s_function.upper())

    return s_return_message[0:4096]


if __name__ == '__main__':
    try:
        s_return = robot_report_audit_wip("2021", "0", "Albert", "21162395@nwu.ac.za")
        if funcconf.l_mess_project:
            print("RETURN: " + s_return)
            print("LENGTH: " + str(len(s_return)))
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, s_function, s_function)
