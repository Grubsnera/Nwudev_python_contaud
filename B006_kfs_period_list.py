"""
Script to build standard KFS transaction lists
Created on: 27 Aug 2019
Copyright: Albert J v Rensburg (NWU:21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcdate
from _my_modules import funcfile

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
END OF SCRIPT
*****************************************************************************"""


def kfs_period_list(s_period="curr", s_yyyy=""):
    """
    Script to build standard KFS lists
    :type s_period: str: The financial period (curr, prev or year)
    :type s_yyyy: str: The financial year
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # OPEN THE LOG WRITER
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: B006_KFS_PERIOD_LIST")
    funcfile.writelog("----------------------------")
    print("--------------------")
    print("B006_KFS_PERIOD_LIST")
    print("--------------------")

    # DECLARE VARIABLES
    s_year: str = s_yyyy
    so_file: str = ""
    so_path = "W:/Kfs/"  # Source database path
    if s_period == "curr":
        s_year = funcdate.cur_year()
        so_file = "Kfs_curr.sqlite"  # Source database
    elif s_period == "prev":
        s_year = funcdate.prev_year()
        so_file = "Kfs_prev.sqlite"  # Source database
    else:
        so_file = "Kfs" + s_year + "sqlite"  # Source database
    l_vacuum = False  # Vacuum database

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN THE WORKING DATABASE
    with sqlite3.connect(so_path + so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("%t OPEN DATABASE: " + so_file)

    # ATTACH DATA SOURCES
    so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
    funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """ ****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    if l_vacuum:
        print("Vacuum the database...")
        so_conn.commit()
        so_conn.execute('VACUUM')
        funcfile.writelog("%t DATABASE: Vacuum kfs")
    so_conn.commit()
    so_conn.close()

    # Close the log writer *********************************************************
    funcfile.writelog("-------------------------------")
    funcfile.writelog("COMPLETED: B006_KFS_PERIOD_LIST")

    return
