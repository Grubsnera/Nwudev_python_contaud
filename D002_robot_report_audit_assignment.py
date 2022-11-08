"""
PREPARE AND SEND AUDIT REPORTS VIA EMAIL FROM THE NWUIA WEB
Script: D002_robot_report_audit_assignment.py
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 31 Oct 2020
"""

# IMPORT PYTHON MODULES
# import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
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
s_function: str = "D002_report_audit_assignment"


def robot_report_audit_assignment(s_number: str = "", s_name: str = "", s_mail: str = ""):
    """
    SEARCH VSS.PARTY FOR NAMES, NUMBERS AND ID'S
    :param s_number: Assignment number
    :param s_name: The name of the requester / recipient
    :param s_mail: The requester mail address
    :return: str: Info in message format
    """

    # VARIABLES
    # s_function: str = "D002_report_audit_assignment"
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
    s_file: str = "Report_assignment_" + s_number + ".html"
    s_report: str = ""
    s_message: str = "Audit assignment report (" + s_number + ") "
    s_report_footer: str = ""
    s_report_signature: str = ""

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

    # BUILD THE ASSIGNMENT RECORD
    s_sql: str = """
    Select
        assi.ia_assi_name,
        assi.ia_assi_header_text,
        assi.ia_assi_header,
        assi.ia_assi_report,
        assi.ia_assi_report_text1,
        assi.ia_assi_report_text2,
        assi.ia_assi_footer,
        assi.ia_assi_signature
    From
        ia_assignment assi
    Where
        assi.ia_assi_auto = %idn%    
    ;"""
    s_sql = s_sql.replace("%idn%", s_number)
    for row in ms_from_cursor.execute(s_sql).fetchall():

        # Previous select statement for reference
        # "SELECT "
        # "ia_assi_name, "
        # "ia_assi_report "
        # "FROM ia_assignment "
        # "WHERE ia_assi_auto = " +

        if l_debug:
            print(row[0])
        funcfile.writelog("%t Audit assignment report " + s_number + " requested by " + s_name)
        s_report = row[0]
        s_message += s_report + " was mailed to " + s_mail

        # EXPORT THE ASSIGNMENT RECORD
        # REPORT HEADER LINE
        if not row[1]:
            funcfile.writelog("<h1>Audit report</h1>", re_path, s_file)
        else:
            funcfile.writelog(row[1], re_path, s_file)

        # REPORT HEADER
        if row[2]:
            funcfile.writelog(row[2], re_path, s_file)

        # REPORT BODY
        if row[3]:
            funcfile.writelog(row[3], re_path, s_file)

        # REPORT FINDINGS HEADER
        if not row[5]:
            funcfile.writelog("<h1>Audit finding(s)</h1>", re_path, s_file)
        else:
            funcfile.writelog(row[5], re_path, s_file)

        # SAVE THE REPORT FOOTER FOR LATER USE
        if row[6]:
            s_report_footer = row[6]
        if row[7]:
            s_report_signature = row[7]

    # BUILD THE FINDING RECORD
    s_sql = """
    Select
        find.ia_find_name,
        find.ia_find_desc,
        find.ia_find_criteria,
        find.ia_find_procedure,
        find.ia_find_condition,
        find.ia_find_effect,
        find.ia_find_cause,
        find.ia_find_risk,
        find.ia_find_recommend,
        find.ia_find_comment,
        find.ia_find_frequency,
        find.ia_find_definition,
        find.ia_find_reference
    From
        ia_finding find
    Where
        find.ia_assi_auto = %idn% And
        find.ia_find_private = 0
    Order By
        find.ia_find_name    
    ;"""
    s_sql = s_sql.replace("%idn%", s_number)
    for row in ms_from_cursor.execute(s_sql).fetchall():

        if l_debug:
            print(row[0])

        # if row[0] != "":
        #     funcfile.writelog(row[0], re_path, s_file)

        # DESCRIPTION
        if row[1]:
            funcfile.writelog(row[1], re_path, s_file)

        # CRITERIA
        if row[2]:
            funcfile.writelog(row[2], re_path, s_file)

        # PROCEDURE
        if row[3]:
            funcfile.writelog(row[3], re_path, s_file)

        # CONDITION
        if row[4]:
            funcfile.writelog(row[4], re_path, s_file)

        # EFFECT
        if row[5]:
            funcfile.writelog(row[5], re_path, s_file)

        # CAUSE
        if row[6]:
            funcfile.writelog(row[6], re_path, s_file)

        # RISK
        if row[7]:
            funcfile.writelog(row[7], re_path, s_file)

        # RECOMMEND
        if row[8]:
            funcfile.writelog(row[8], re_path, s_file)

        # COMMENT
        if row[9]:
            funcfile.writelog(row[9], re_path, s_file)

        # FREQUENCY
        if row[10]:
            funcfile.writelog(row[10], re_path, s_file)

        # DEFINITION
        if row[11]:
            funcfile.writelog(row[11], re_path, s_file)

        # REFERENCE
        if row[12]:
            funcfile.writelog(row[12], re_path, s_file)

    # ADD THE REPORT FOOTER AND SIGNATURE
    if s_report_footer != "":
        funcfile.writelog(s_report_footer, re_path, s_file)
    if s_report_signature != "":
        funcfile.writelog(s_report_signature, re_path, s_file)

    # MAIL THE AUDIT REPORT
    if s_name != "" and s_mail != "":
        funcfile.writelog("%t Audit assignment report " + s_number + " mailed to " + s_mail)
        if l_debug:
            print("Send the report...")
        s_body: str = "Attached please find audit assignment report as requested:\n\r"
        s_body += "\n\r"
        s_body += s_report
        funcmail.send(s_name,
                      s_mail,
                      "E",
                      "Report audit assignment number (" + s_number + ")",
                      s_body,
                      re_path,
                      s_file)

    # POPULATE THE RETURN MESSAGE
    s_return_message = s_message

    # DELETE THE MAILED FILE
    if funcfile.file_delete(re_path, s_file):
        funcfile.writelog("%t Audit assignment report " + s_number + " deleted")
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
        s_return = robot_report_audit_assignment("397", "Albert", "21162395@nwu.ac.za")
        if funcconf.l_mess_project:
            print("RETURN: " + s_return)
            print("LENGTH: " + str(len(s_return)))
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, s_function, s_function)
