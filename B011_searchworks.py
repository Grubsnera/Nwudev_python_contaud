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

# IMPORT OWN MODULES
sys.path.append('_my_modules')
import funcconf
import funcdate
import funcsys
import funcfile
import funcmail
import funcsms

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
END OF SCRIPT
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
    csv_master_path: str = "S:/searchworks/"
    csv_master_name: str = "01_master_submit.csv"
    csv_send_path: str = "S:/searchworks/"
    csv_send_name: str = "02_nwu_submit_" + funcdate.today_file() + ".csv"

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
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
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
        # tomorrow = today = datetime.date.today() + datetime.timedelta(days=1)
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

    # Override the run date
    if l_override_date:
        run_date = datetime.date.today()

    if l_debug:
        print("Run date: ")
        print(run_date)

    # Check if it is the scheduled run date
    if today == run_date:

        # BUILD TABLE WITH PREVIOUS SUBMITTED EMPLOYEES

        # Create table in SQLite database
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

        # Read data from CSV file and insert into the SQLite table
        with open(csv_master_path + csv_master_name, "r") as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                so_curs.execute(
                    "INSERT INTO " + sr_file + " VALUES (:nwu_number, :employee_name, :national_identifier, :ni_type, :date_submitted)",
                    row)
        so_conn.commit()
        funcfile.writelog("%t IMPORT CSV: " + csv_master_path + csv_master_name)

        # BUILD CURRENT AND ADD PREVIOUS SUBMITTED EMPLOYEES
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
            p.assignment_category Like('PERM%')     
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # BUILD NEW SUBMISSIONS IN SQLITE
        sr_file = "X004c_new_submissions"
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            s.employee_number,
            s.name_address,
            s.national_identifier,
            s.ni_type,
            CURRENT_DATE As date_submitted
        From
            X004b_employee_submitted s
        Where
            s.date_submitted Is Null
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        i_record_count: int = funcsys.tablerowcount(so_curs, sr_file)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

        # Only build new files if records found
        if i_record_count > 0:

            if l_mess:
                s_desc = "New employees"
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_record_count) + '</b> ' + s_desc)

            # EXPORT THE NEW SUBMISSIONS TO A CSV FILE
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

            # Create the send.csv file
            funcfile.file_delete(csv_send_path, csv_send_name)
            with open(csv_send_path + csv_send_name, 'a', newline='') as csv_file:
                writer = csv.writer(csv_file)
                header = [('nwu_number', 'employee_name', 'national_identifier', 'ni_type', 'date_submitted')]
                # Write data to the CSV file
                writer.writerows(header)
                writer.writerows(data)
            funcfile.writelog("%t BUILD CSV: " + csv_send_name + " (" + str(i_record_count) + " new records)")

            # Update the master file
            # '''
            with open(csv_master_path + csv_master_name, 'a', newline='') as csv_file:
                writer = csv.writer(csv_file)
                # Write data to the CSV file
                writer.writerows(data)
            funcfile.writelog("%t UPDATE CSV: " + csv_master_name)
            # '''

            # BUILD THE EMAIL

            # The body
            mail_body: str = """
            Attached please find a list of updated employees for which we require a company search and upload to the ID watchlist.
            Please prepare and forward the quotation to us please.
            """

            # Send the mail
            mail_to_name: str = "Albert"
            mail_to_address: str = "21162395@nwu.ac.za"
            funcmail.send(mail_to_name, mail_to_address, 'E', 'NWU Employee data for company search and ID watchlist.', mail_body, csv_send_path, csv_send_name)
            funcfile.writelog("%t EMAIL TO: " + mail_to_name + " - " + mail_to_address)
            # funcmail.send('Cindy', 'cbrummer@ searchworks360.co.za', 'E', 'NWU Employee data for company search and ID watchlist.', mail_body, csv_send_path, csv_send_name)

        else:

            # No new records found
            funcfile.writelog("%t NO NEW EMPLOYEES FOUND TO SUBMIT TO SEARCHWORKS")

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
