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
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

    """ ****************************************************************************
    TEMPORARY SCRIPT
    *****************************************************************************"""

    # TODO Delete after first run

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """ ****************************************************************************
    MASTER FILE LISTS
    *****************************************************************************"""

    """ ****************************************************************************
    SPOUSE INSURANCE MASTER FILES
    *****************************************************************************"""

    # IMPORT SPOUSE BENCHMARK
    sr_file = "X009_spouse_matrix"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Import spouse matrix...")
    so_curs.execute(
        "CREATE TABLE " + sr_file + "(PERSON_TYPE, MARITAL_STATUS, TEST1)")
    s_cols = ""
    co = open(ed_path + "001_employee_marital_status.csv", "r")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "PERSON_TYPE":
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the impoted data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_employee_marital_status.csv (" + sr_file + ")")

    # OBTAIN MASTER DATA
    # if test <> '1' then employee does not form part of the test
    # if married = '1' then married for all married person types
    if l_debug:
        print("Obtain employee and spouse data...")
    sr_file: str = 'X009_people_spouse_all'
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        p.employee_number,
        p.person_id,
        p.name_address,
        p.user_person_type,
        cast(t.TEST1 As Int) As test,
        p.marital_status,
        cast(m.TEST1 As Int) As married,
        p.date_started,
        cast(i.ELEMENT_VALUE As Int) As spouse_insurance_status,
        cast(s.spouse_age As Int) As spouse_age,
        s.person_extra_info_id,
        s.spouse_number,
        s.spouse_address,
        s.spouse_date_of_birth,
        s.spouse_national_identifier,
        s.spouse_passport,
        s.spouse_start_date,
        s.spouse_end_date,
        s.spouse_create_date,
        s.spouse_created_by,
        s.spouse_update_date,
        s.spouse_updated_by,
        s.spouse_update_login
    From
        PEOPLE.X000_PEOPLE p Left Join
        PEOPLE.X000_GROUPINSURANCE_SPOUSE i On i.EMPLOYEE_NUMBER = p.employee_number Left Join
        PEOPLE.X002_SPOUSE_CURR s On s.employee_number = p.employee_number Left Join
        X009_spouse_matrix t On t.PERSON_TYPE = p.user_person_type Left Join
        X009_spouse_matrix m On m.MARITAL_STATUS = p.marital_status
    Group By
        p.employee_number    
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # Notes: Tests that can be performed on spouse insurance
    # 1a. SPOUSE INSURANCE AFTER 65
    #     test = 1 and married = 1 and insurance status is 1 or 2 and age > 65
    #     Spouse insurance must be stopped in December in the year they turn 65.
    # 1b. NO ACTIVE SPOUSE ON RECORD
    #     test = 1 and married = 1 and insurance status is 1 or 2 and person_extra_info_id is null (no spouse)
    #     Employee must provide spouse details.
    # 1c. COMPULSORY SPOUSE INSURANCE IF EMPLOYED AFTER 1 JAN 2022
    #     test = 1 and married = 1 and date_started >= 2022-01-01 and insurance status = 0 or null (must have)
    #     Must have spouse insurance. Transfers etc should be excluded.
    # 2a. NOT MARRIED BUT ACTIVE SPOUSE INSURANCE
    #     married is null and insurance status is 1 or 2
    #     Cancel insurance.
    # 2b. NOT MARRIED BUT ACTIVE SPOUSE RECORD
    #     married is null and person_extra_info_id is not null
    #     End date spouse record.

    """*****************************************************************************
    TEST SPOUSE INSURANCE AFTER 65
    *****************************************************************************"""

    # DEFAULT TRANSACTION OWNER PEOPLE
    # 21022402 MS AC COERTZEN for permanent employees
    # 20742010 MRS N BOTHA for temporary employees
    # Exclude 12795631 MR R VAN DEN BERG
    # Exclude 13277294 MRS MC STRYDOM

    # DECLARE TEST VARIABLES
    i_finding_before = 0
    i_finding_after = 0
    s_description = "Spouse insurance after 65"
    s_file_prefix: str = "X009a"
    s_file_name: str = "spouse_insurance_after_65"
    s_finding: str = "SPOUSE INSURANCE AFTER 65"
    s_report_file: str = "001_reported.txt"

    # OBTAIN TEST RUN FLAG
    if functest.get_test_flag(so_curs, "HR", "TEST " + s_finding, "RUN") == "FALSE":

        if l_debug:
            print('TEST DISABLED')
        funcfile.writelog("TEST " + s_finding + " DISABLED")

    else:

        # LOG
        funcfile.writelog("TEST " + s_finding)
        if l_debug:
            print("TEST " + s_finding)

    """*****************************************************************************
    TEST NO ACTIVE SPOUSE ON RECORD
    *****************************************************************************"""

    # DEFAULT TRANSACTION OWNER PEOPLE
    # 21022402 MS AC COERTZEN for permanent employees
    # 20742010 MRS N BOTHA for temporary employees
    # Exclude 12795631 MR R VAN DEN BERG
    # Exclude 13277294 MRS MC STRYDOM

    # DECLARE TEST VARIABLES
    i_finding_before = 0
    i_finding_after = 0
    s_description = "No active spouse on record"
    s_file_prefix: str = "X009b"
    s_file_name: str = "no_active_spouse_on_record"
    s_finding: str = "NO ACTIVE SPOUSE ON RECORD"
    s_report_file: str = "001_reported.txt"

    # OBTAIN TEST RUN FLAG
    if functest.get_test_flag(so_curs, "HR", "TEST " + s_finding, "RUN") == "FALSE":

        if l_debug:
            print('TEST DISABLED')
        funcfile.writelog("TEST " + s_finding + " DISABLED")

    else:

        # LOG
        funcfile.writelog("TEST " + s_finding)
        if l_debug:
            print("TEST " + s_finding)

    """*****************************************************************************
    TEST COMPULSORY SPOUSE INSURANCE
    *****************************************************************************"""

    # DEFAULT TRANSACTION OWNER PEOPLE
    # 21022402 MS AC COERTZEN for permanent employees
    # 20742010 MRS N BOTHA for temporary employees
    # Exclude 12795631 MR R VAN DEN BERG
    # Exclude 13277294 MRS MC STRYDOM

    # DECLARE TEST VARIABLES
    i_finding_before = 0
    i_finding_after = 0
    s_description = "Compulsory spouse insurance"
    s_file_prefix: str = "X009c"
    s_file_name: str = "compulsory_spouse_insurance"
    s_finding: str = "COMPULSORY SPOUSE INSURANCE"
    s_report_file: str = "001_reported.txt"

    # OBTAIN TEST RUN FLAG
    if functest.get_test_flag(so_curs, "HR", "TEST " + s_finding, "RUN") == "FALSE":

        if l_debug:
            print('TEST DISABLED')
        funcfile.writelog("TEST " + s_finding + " DISABLED")

    else:

        # LOG
        funcfile.writelog("TEST " + s_finding)
        if l_debug:
            print("TEST " + s_finding)

    """*****************************************************************************
    TEST NOT MARRIED ACTIVE SPOUSE RECORD
    *****************************************************************************"""

    # DEFAULT TRANSACTION OWNER PEOPLE
    # 21022402 MS AC COERTZEN for permanent employees
    # 20742010 MRS N BOTHA for temporary employees
    # Exclude 12795631 MR R VAN DEN BERG
    # Exclude 13277294 MRS MC STRYDOM

    # DECLARE TEST VARIABLES
    i_finding_before = 0
    i_finding_after = 0
    s_description = "Not married active spouse record"
    s_file_prefix: str = "X009e"
    s_file_name: str = "not_married_active_spouse_record"
    s_finding: str = "NOT MARRIED ACTIVE SPOUSE RECORD"
    s_report_file: str = "001_reported.txt"

    # OBTAIN TEST RUN FLAG
    if functest.get_test_flag(so_curs, "HR", "TEST " + s_finding, "RUN") == "FALSE":

        if l_debug:
            print('TEST DISABLED')
        funcfile.writelog("TEST " + s_finding + " DISABLED")

    else:

        # LOG
        funcfile.writelog("TEST " + s_finding)
        if l_debug:
            print("TEST " + s_finding)

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
