"""
Script to test GL TRANSACTIONS
Created: 2 Jul 2019
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import csv
import datetime
import sqlite3
import sys

# IMPORT OWN MODULES
import funcfile
import funccsv
import funcdate
import funcsys

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
END OF SCRIPT
*****************************************************************************"""

def gl_test_transactions():
    """
    Script to test GL transactions
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # OPEN THE SCRIPT LOG FILE
    print("-------------------------")
    print("C202_GL_TEST_TRANSACTIONS")
    print("-------------------------")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C202_GL_TEST_TRANSACTIONS")
    funcfile.writelog("---------------------------------")

    # DECLARE VARIABLES
    so_path = "W:/Kfs/" #Source database path
    so_file = "Kfs_test_gl_tran.sqlite" # Source database
    ed_path = "S:/_external_data/" #external data path
    re_path = "R:/Kfs/" # Results path
    l_export = True
    l_mail = True
    l_record = True
    l_vacuum = False

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN THE WORKING DATABASE
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("%t OPEN DATABASE: " + so_file)

    # ATTACH DATA SOURCES
    so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
    funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
    funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")

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
    if l_vacuum == True:
        print("Vacuum the database...")
        so_conn.commit()
        so_conn.execute('VACUUM')
        funcfile.writelog("%t DATABASE: Vacuum kfs")    
    so_conn.commit()
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("------------------------------------")
    funcfile.writelog("COMPLETED: C202_GL_TEST_TRANSACTIONS")

    return
