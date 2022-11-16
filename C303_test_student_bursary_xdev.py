"""
Script to test STUDENT BURSARIES
Created on: 29 Jan 2021
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3
# import csv

# IMPORT OWN MODULES
from _my_modules import funcconf
# from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcstat
from _my_modules import funcsys
from _my_modules import funcsms
# from _my_modules import functest

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
    # ed_path: str = "S:/_external_data/"  # External data path
    # re_path: str = "R:/Vss/" + s_year
    so_path: str = "W:/Vss_fee/"  # Source database path
    so_file: str = "Vss_test_bursary.sqlite"
    l_debug: bool = True
    # l_mail: bool = funcconf.l_mail_project
    l_mail: bool = True
    # l_mess: bool = funcconf.l_mess_project
    l_mess: bool = False
    # l_record: bool = True
    # l_export: bool = True
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

    # OPEN SQLITE SOURCE table
    if l_debug:
        print("Open sqlite database...")
    with sqlite3.connect(so_path + so_file) as so_conn:
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
