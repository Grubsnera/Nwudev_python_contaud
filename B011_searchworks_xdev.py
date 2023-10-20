"""
Script to prepare, export and mail Searchworks requests
Created on: 21 September 2023
Author: Yolandie Koekemoer (NWU:12788074)
Author: Albert J van Rensburg (NWU:21162395)
"""

# IMPORT PYTHON MODULES
import datetime
import sys
import sqlite3
import csv
import shutil
import os
import pandas as pd

# IMPORT OWN MODULES
# sys.path.append('_my_modules')
from _my_modules import funcconf
from _my_modules import funcdate
from _my_modules import funcsys
from _my_modules import funcfile
from _my_modules import funcmail
from _my_modules import funcsms
from _my_modules import funcsqlite

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
BUILD TABLE WITH PREVIOUS SUBMITTED EMPLOYEES
READ THE WATCHLIST DATA
BUILD NEW SUBMISSIONS IN SQLITE
BUILD THE EMAIL
IMPORT SEARCHWORKS CIPC RESULTS IF A FILE WITH DATA EXISTS
NO SEARCHWORKS DIRECTORS IMPORTED
END OF SCRIPT
"""

# LOGIC
"""

"""

# SCRIPT WIDE VARIABLES
s_function: str = "B011_searchworks"


def searchworks_submit(l_override_date: bool = False):

    """****************************************************************************
    ENVIRONMENT
    Format can be different and not exact.
    <Tab> to shift in
    <Shift><Tab> to shift out
    ****************************************************************************"""

    # DECLARE VARIABLES
    l_debug: bool = True  # Enable the display of on-screen debug messages.
    l_mess: bool = funcconf.l_mess_project  # Enable / disable the robot communicator message function.
    # l_mess: bool = False  # Enable / disable the robot communicator message function.
    so_path: str = "W:/People_conflict/"  # Source database path
    so_file: str = "People_conflict.sqlite"  # Source database
    sw_path: str = "S:/searchworks/"
    csv_master_name: str = "01_master_submit.csv"
    csv_send_name: str = "02_nwu_submit_" + funcdate.today_file() + ".csv"
    csv_watchlist_name: str = "03_searchworks_watchlist.csv"
    csv_watchlist_new: str = "03_searchworks_watchlist_" + funcdate.today_file() + ".csv"
    xls_results: str = "NWU CIPC Results.xlsx"
    csv_results: str = "04_cipc_results_" + funcdate.today_file() + ".csv"
    csv_history: str = "05_cipc_history_" + funcdate.today_file() + ".csv"

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
    if l_debug:
        print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN THE WORKING DATABASE
    funcfile.writelog("OPEN DATABASE: " + so_file)
    with sqlite3.connect(so_path + so_file) as so_conn:
        so_curs = so_conn.cursor()

    # ATTACH DATA SOURCES
    funcfile.writelog("%t ATTACH DATABASE: People.sqlite")
    so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")

    """************************************************************************
    TEMPORARY AREA
    ************************************************************************"""
    if l_debug:
        print("TEMPORARY AREA")
    funcfile.writelog("TEMPORARY AREA")

    """************************************************************************
    BEGIN OF SCRIPT
    ************************************************************************"""
    if l_debug:
        print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    # Create SQLite table to join with people
    sr_file = "X004f_searchworks_results_people"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        i.nwu_number,
        i.employee_name,
        i.national_identifier,
        i.ni_type,
        i.date_submitted,
        i.registration_number,
        i.company_name,
        i.enterprise_type,
        i.company_status,
        i.history_date,
        i.business_start_date,
        i.directorship_status,
        Case
            When i.directorship_start_date = '' Then i.business_start_date  
            Else i.directorship_start_date
        End As directorship_start_date,
        i.directorship_end_date,
        i.directorship_type,
        i.directorship_interest,
        p.employee_number,
        p.user_person_type,
        p.position_name,
        Date('now') As import_date
        --'2023-09-26' As import_date
    From
        X004e_searchworks_results_import i Left Join
        PEOPLE.X000_PEOPLE As p On p.employee_number = i.nwu_number
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Create or Update the history table with the new results
    sr_file = "X004g_searchworks_results_history"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    # Create SQLite history file if it does not exist
    s_sql = "CREATE TABLE IF NOT EXISTS " + sr_file + " AS " + """
    Select
        p.*
    From
        X004f_searchworks_results_people p
    """
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Write all the history to a csv file
    funcsqlite.sqlite_to_csv(so_curs, sr_file, sw_path + csv_history)

    # Build the final SQLite table containing all the current directorship data.
    sr_file = "X004x_searchworks_directors"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        r.nwu_number,
        r.employee_name,
        r.national_identifier,
        r.user_person_type,
        r.position_name,
        Max(r.date_submitted) As date_submitted,
        r.import_date,
        r.registration_number,
        r.company_name,
        r.enterprise_type,
        r.company_status,
        r.history_date,
        r.business_start_date,
        r.directorship_status,
        r.directorship_start_date,
        r.directorship_end_date,
        r.directorship_type,
        r.directorship_interest,
        '1' As customer
    From
        X004g_searchworks_results_history r
    Where
        r.employee_number Is Not Null
    Group By
        r.national_identifier,
        r.registration_number
    Order By
        r.nwu_number,
        r.directorship_start_date        
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    line_count_director: int = funcsqlite.table_row_count(so_curs, sr_file)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """************************************************************************
    END OF SCRIPT
    ************************************************************************"""
    if l_debug:
        print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # Commit the changes and close the connection
    so_conn.commit()
    so_conn.close()

    return


if __name__ == '__main__':
    try:
        searchworks_submit(True)
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, s_function, s_function)
