"""
Script to prepare, export and mail searchworks requests
Created on: 21 September 2023
Author: Yolandie Koekemoer (NWU:12788074)
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

    # SCRIPT MUST ONLY RUN ON 2ND OF EACH MONTH

    # Get the current date
    today = datetime.date.today()
    if l_debug:
        print("Today: ")
        print(today)

    # Check if it is the 2nd or 3rd of the month
    if today.day == 2 or today.day == 3:
        # Check if it is a Saturday
        if today.day == 2 and today.weekday() == 5:
            # Add 1 day to run the script on the 3rd
            run_date = today + datetime.timedelta(days=1)
        # Check if it is a Sunday
        elif today.day == 3 and today.weekday() == 6:
            # Run the script on the 3rd (Sunday)
            run_date = today
        else:
            # Run the script on the current day (2nd or 3rd)
            run_date = today
    else:
        # Get the next 2nd of the month
        next_month = today.replace(day=28) + datetime.timedelta(days=4)
        run_date = next_month.replace(day=2)

    # Send message to indicate run date
    if l_debug:
        if l_mess:
            s_desc = "Run date"
            funcsms.send_telegram('', 'administrator', '<b>' + str(run_date) + '</b> ' + s_desc)

    # Override the run date
    if l_override_date:
        run_date = datetime.date.today()

    # Debug
    if l_debug:
        print("Run date: ")
        print(run_date)

    # Check if it is the scheduled run date
    if today == run_date:

        # IDENTIFY NEW EMPLOYEES WHO SHOULD BE SUBMITTED

        # Create SQLite table to receive previously submitted employees.
        sr_file = "X004a_submitted_to_searchworks"
        s_sql = "CREATE TABLE " + sr_file + """
        (
        nwu_number TEXT,
        employee_name TEXT,
        national_identifier TEXT,
        ni_type TEXT,
        date_submitted TEXT
        )
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # Read csv data previously submitted and populate SQLite table.
        with open(sw_path + csv_master_name, "r") as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                so_curs.execute(
                    "INSERT INTO " + sr_file + " VALUES (:nwu_number,"
                                               " :employee_name,"
                                               " :national_identifier,"
                                               " :ni_type,"
                                               " :date_submitted)",
                    row)
        so_conn.commit()
        funcfile.writelog("%t IMPORT CSV: " + sw_path + csv_master_name)

        # Build a new SQLite table with all current employees, and join the previously submitted employees.
        sr_file = "X004b_employee_submitted"
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            p.employee_number,
            p.name_address,
            Case
                When p.national_identifier == '' And p.passport = '' Then p.permit
                When p.national_identifier == '' Then p.passport
                Else p.national_identifier
            End As national_identifier,
            Case
                When p.national_identifier == '' And p.passport = '' Then 'WP'
                When p.national_identifier == '' Then 'PP'
                Else 'ID'
            End As ni_type,
            s.date_submitted
        From
            PEOPLE.X000_PEOPLE p Left Join
            X004a_submitted_to_searchworks s On s.nwu_number = p.employee_number
        Where
            (p.assignment_category Like('PERMANENT%')) Or
            (p.user_person_type Like('COUNCIL%'))     
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # IMPORT WATCHLIST ID NUMBERS RECEIVED FROM SEARCHWORKS TO FORM PART OF THE NEW SUBMISSION

        # Create SQLite table to receive watchlist id numbers.
        sr_file = "X004d_searchworks_watchlist"
        s_sql = "CREATE TABLE " + sr_file + """
        (
        national_identifier TEXT
        )
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # Read watchlist csv data and populate SQLite table.
        with open(sw_path + csv_watchlist_name, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                so_curs.execute(
                    "INSERT INTO " + sr_file + " VALUES (:national_identifier)",
                    row)
        so_conn.commit()
        funcfile.writelog("%t IMPORT CSV: " + sw_path + csv_watchlist_name)

        # Count the number of lines in the watchlist.
        line_count_watchlist: int = 0
        with open(sw_path + csv_watchlist_name, 'r') as file:
            csv_reader = csv.reader(file)
            line_count_watchlist = sum(1 for row in csv_reader) - 1
            if l_debug:
                print('Watchlist items:')
                print(line_count_watchlist)

        # Add the list of watchlist id's to the employee SQLite table above containing all employees. Join the
        # watchlist data on national_identifier.
        sr_file = "X004b_employee_submitted_watchlist"
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            s.employee_number,
            s.name_address,
            s.national_identifier,
            s.ni_type,
            s.date_submitted,
            w.national_identifier As national_identifier_watchlist
        From
            X004b_employee_submitted s Left Join
            X004d_searchworks_watchlist w On w.national_identifier = s.national_identifier
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # Make a copy of the raw watchlist data submitted for future reference.
        send_file_size: int = os.path.getsize(sw_path + csv_watchlist_name)
        # Debug
        if l_debug:
            print("Watchlist file size: ")
            print(send_file_size)

        # Only create a copy of the watchlist file if not empty
        if send_file_size > 24:

            # Make a copy of the temporary watchlist file.
            shutil.copy2(sw_path + csv_watchlist_name, sw_path + csv_watchlist_new)

            if not l_debug:
                # Delete the watchlist file.
                os.remove(sw_path + csv_watchlist_name)
                # Create a blank watchlist file size = 24.
                header = ['national_identifier']
                data = ['0']
                with open(sw_path + csv_watchlist_name, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(header)
                    writer.writerow(data)

        # BUILD NEW SUBMISSIONS IN SQLITE

        # Build the final SQLite table containing all the new submissions.
        sr_file = "X004c_new_submissions"
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            s.employee_number,
            s.name_address,
            s.national_identifier,
            s.ni_type,
            CURRENT_DATE As date_submitted
        From
            X004b_employee_submitted_watchlist s
        Where
            (s.date_submitted Is Null) Or
            (s.national_identifier_watchlist Is Not Null)
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        line_count_total: int = funcsys.tablerowcount(so_curs, sr_file)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # Only build new files if records found
        if line_count_total > 0:

            # Calculate the number of new submissions.
            line_count_new: int = line_count_total - line_count_watchlist

            # Communicate the new submission count
            if l_mess:
                s_desc = "New submissions"
                funcsms.send_telegram('', 'administrator', '<b>' + str(line_count_new) + '</b> ' + s_desc)

            # Open the new submissions SQLite table.
            sr_file = "X004c_new_submissions"
            s_sql = """SELECT
            employee_number,
            name_address,
            national_identifier,
            ni_type,
            date_submitted
            """ + " FROM " + sr_file
            so_curs.execute(s_sql)
            data = so_curs.fetchall()
            funcfile.writelog("%t READ DATA: " + sr_file)

            # Create and export the new submissions to a csv file to send via email.
            funcfile.file_delete(sw_path, csv_send_name)
            with open(sw_path + csv_send_name, 'w', newline='') as csv_file:
                writer = csv.writer(csv_file)
                header = [('nwu_number', 'employee_name', 'national_identifier', 'ni_type', 'date_submitted')]
                # Write data to the CSV file
                writer.writerows(header)
                writer.writerows(data)
            funcfile.writelog("%t BUILD CSV: " + csv_send_name + " (" + str(line_count_total) + " new records)")

            # Communicate the new submission count
            if l_mess:
                s_desc = "Watchlist submissions"
                funcsms.send_telegram('', 'administrator', '<b>' + str(line_count_watchlist) + '</b> ' + s_desc)

            # Update the master submission csv file with the new submissions.
            if not l_debug:
                with open(sw_path + csv_master_name, 'a', newline='') as csv_file:
                    writer = csv.writer(csv_file)
                    # Write data to the CSV file
                    writer.writerows(data)
                funcfile.writelog("%t UPDATE CSV: " + csv_master_name)

            # Communicate the total submission count
            if l_mess:
                s_desc = "Total submission"
                funcsms.send_telegram('', 'administrator', '<b>' + str(line_count_total) + '</b> ' + s_desc)

            # BUILD THE EMAIL

            # The body
            mail_subject: str = "NWU Employee data for company search and ID watchlist."
            mail_body: str = """
            Attached please find a list of updated employees for which we require a company search and upload to the ID watchlist.
            Please prepare and forward the quotation to us please.
            """

            # Send the mail
            if not l_debug:
                funcmail.send_mail('b011_searchworks_submit', mail_subject, mail_body)

        else:

            # No new records found
            funcfile.writelog("%t NO NEW EMPLOYEES FOUND TO SUBMIT TO SEARCHWORKS")

            # Communicate no submissions
            if l_mess:
                s_desc = "No submissions"
                funcsms.send_telegram('', 'administrator', s_desc)
                
    # IMPORT SEARCHWORKS CIPC RESULTS IF A FILE WITH DATA EXISTS
    # Test for file name NWU CIPC Results.xlsx.
    # File should contain sheet named Directorships.

    if os.path.isfile(sw_path + xls_results):

        # Read the Excel file.
        excel_file = pd.read_excel(sw_path + xls_results, sheet_name='Directorships')
        funcfile.writelog("%t IMPORT EXCEL: " + sw_path + xls_results)

        # Write to csv.
        excel_file.to_csv(sw_path + csv_results, index=False)
        funcfile.writelog("%t BUILD CSV: " + sw_path + csv_results)

        # Read the csv file.
        df = pd.read_csv(sw_path + csv_results)

        # Actions to clean the data.
        # Rename the columns by replacing spaces with underscores and removing ':'.
        df.columns = df.columns.str.replace(' ', '_').str.replace(':', '').str.lower()
        # Replace illegal characters with normal characters.
        df.replace('É', 'E', regex=True, inplace=True)

        # Write the new dataframe.
        df.to_csv(sw_path + csv_results, index=False)
        funcfile.writelog("%t CLEAN CSV: " + sw_path + csv_results)

        # Count the number of lines in the watchlist.
        line_count_results: int = 0
        with open(sw_path + csv_results, 'r') as file:
            csv_reader = csv.reader(file)
            line_count_results = sum(1 for row in csv_reader) - 1
            if l_debug:
                print('Results imported:')
                print(line_count_results)

        # Communicate the total submission count
        if l_mess:
            s_desc = "Results imported"
            funcsms.send_telegram('', 'administrator', '<b>' + str(line_count_results) + '</b> ' + s_desc)

        # Create SQLite table to receive searchworks results.
        sr_file = "X004e_searchworks_results_import"
        # Remove before production, otherwise import history would be deleted.
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = "CREATE TABLE IF NOT EXISTS " + sr_file + """
        (
        nwu_number TEXT,
        employee_name TEXT,
        national_identifier TEXT,
        ni_type TEXT,
        date_submitted TEXT,
        id_number TEXT,
        surname TEXT,
        full_name TEXT,
        date_of_birth TEXT,
        registration_number TEXT,
        company_name TEXT,
        enterprise_type TEXT,
        company_status TEXT,
        history_date TEXT,
        business_start_date TEXT,
        directorship_status TEXT,
        directorship_start_date TEXT,
        directorship_end_date TEXT,
        directorship_type TEXT,
        directorship_interest TEXT,
        nationality TEXT,
        unnamed TEXT
        )
        """
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # Read csv searchworks results into SQLite.
        with open(sw_path + csv_results, "r") as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                so_curs.execute(
                    "INSERT INTO " + sr_file + " VALUES (:nwu_number,"
                                               " :employee_name,"
                                               " :national_identifier,"
                                               " :ni_type,"
                                               " :date_submitted,"
                                               " :idnum,"
                                               " :surname,"
                                               " :full_name,"
                                               " :dob,"
                                               " :regnum,"
                                               " :companyname,"
                                               " :enterprisetype,"
                                               " :companystatus,"
                                               " :historydate,"
                                               " :businessstartdate,"
                                               " :directorship_status,"
                                               " :directorship_startdate,"
                                               " :directorship_enddate,"
                                               " :directorship_type,"
                                               " :directorship_interest,"
                                               " :nationality,"
                                               " :unnamed_21)", row)

        so_conn.commit()
        funcfile.writelog("%t IMPORT CSV: " + sw_path + csv_results)

        # Build the final SQLite table containing all the new submissions.
        sr_file = "X004x_searchworks_directors"
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            r.nwu_number,
            r.employee_name,
            r.national_identifier,
            Max(r.date_submitted) As Date_submitted,
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
            r.nationality
        From
            X004e_searchworks_results_import r
        Group By
            r.nwu_number,
            r.national_identifier,
            r.registration_number
        Order By
            r.nwu_number,
            r.business_start_date        
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        line_count_director: int = funcsys.tablerowcount(so_curs, sr_file)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # Communicate the total director count
        if l_mess:
            s_desc = "Directors"
            funcsms.send_telegram('', 'administrator', '<b>' + str(line_count_director) + '</b> ' + s_desc)

        # Delete the imported Excel file.
        if not l_debug:
            os.remove(sw_path + xls_results)

    else:

        # NO SEARCHWORKS DIRECTORS IMPORTED

        # No new records found
        funcfile.writelog("%t NO SEARCHWORKS DIRECTORS IMPORTED")

        # Communicate the total director count
        sr_file = "X004x_searchworks_directors"
        line_count_director: int = funcsys.tablerowcount(so_curs, sr_file)
        if l_mess:
            s_desc = "Directors"
            funcsms.send_telegram('', 'administrator', '<b>' + str(line_count_director) + '</b> ' + s_desc)

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
