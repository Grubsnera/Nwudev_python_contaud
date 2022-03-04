"""
SCRIPT TO TEST GL TRANSACTIONS
Created: 2 Jul 2019
Author: Albert J v Rensburg (NWU:21162395)
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcfile
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcmail
from _my_modules import funcsms
from _my_modules import funcsys
from _my_modules import functest

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT

PROFESSIONAL FEES GL MASTER FILE
TEST PROFESSIONAL FEES PAID TO STUDENTS

END OF SCRIPT
*****************************************************************************"""


def gl_test_transactions():
    """
    Script to test GL transactions
    :return: int
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # DECLARE VARIABLES
    so_path = "W:/Kfs/"  # Source database path
    so_file = "Kfs_test_gl_transaction.sqlite"  # Source database
    ed_path = "S:/_external_data/"  # External data path
    re_path = "R:/Kfs/"  # Results path
    l_debug: bool = True
    l_export = False
    # l_mess: bool = funcconf.l_mess_project
    l_mess: bool = False
    l_record = False

    # OPEN THE SCRIPT LOG FILE
    if l_debug:
        print("-------------------------")
        print("C202_GL_TEST_TRANSACTIONS")
        print("-------------------------")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C202_GL_TEST_TRANSACTIONS")
    funcfile.writelog("---------------------------------")

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>C202 Kfs gl transaction tests</b>")

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    if l_debug:
        print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN THE WORKING DATABASE
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH DATA SOURCES
    so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
    funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss_curr.sqlite' AS 'VSSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: VSS_CURR.SQLITE")

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """ ****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    so_conn.commit()
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("------------------------------------")
    funcfile.writelog("COMPLETED: C202_GL_TEST_TRANSACTIONS")

    return


if __name__ == '__main__':
    try:
        gl_test_transactions()
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, "C202_gl_test_transactions", "C202_gl_test_transactions")
