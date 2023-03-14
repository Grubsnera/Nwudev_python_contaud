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
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE_PAYROLL.SQLITE")

    """ ****************************************************************************
    TEMPORARY SCRIPT
    *****************************************************************************"""

    # TODO

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    # BUILD TABLE WITH PAYROLL FOREIGN PAYMENTS FOR THE PREVIOUS MONTH
    print("Obtain master list of foreign employee payments...")
    sr_file = "X003_foreign_payments"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
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
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    s_sql = s_sql.replace("%PREVMONTH%", funcdate.prev_monthend()[0:7])
    if funcdate.prev_monthend()[0:4] == funcdate.prev_year:
        s_sql = s_sql.replace("%PERIOD%", 'prev')
    else:
        s_sql = s_sql.replace("%PERIOD%", 'curr')
    if l_debug:
        print(s_sql)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
    if l_debug:
        so_conn.commit()

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
