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

    # CURRENT EMPLOYEES
    if l_debug:
        print("CURRENT EMPLOYEES")

    # BUILD CURRENT EMPLOYEES
    # SQL Test query is Sqlite_people->List_active_employee_today
    sr_file = "X000_People_current"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        pap.EMPLOYEE_NUMBER As employee_number,
        pap.PERSON_ID As person_id,
        paa.ASSIGNMENT_ID As assignment_id,
        pap.FULL_NAME As name_full,
        Upper(hrl.LOCATION_CODE) As location,
        Max(pps.ACTUAL_TERMINATION_DATE) As service_end_date,
        Upper(ppt.USER_PERSON_TYPE) As user_person_type,
        Upper(cat.MEANING) As assignment_category,
        Case
            When paa.POSITION_ID = 0
            Then Upper(paa.EMPLOYEE_CATEGORY)
            Else Upper(hrp.ACAD_SUPP)
        End As employee_category,
        Upper(hrp.POSITION_NAME) position_name,
        Upper(nat.meaning) nationality,
        Upper(pan.meaning) nationality_passport,      
        pap.PER_INFORMATION9 As is_foreign,
        pap.NATIONAL_IDENTIFIER As national_identifier,
        Upper(pap.PER_INFORMATION2) passport,
        Upper(pap.PER_INFORMATION3) permit,
        Replace(Substr(pap.PER_INFORMATION8,1,10),'/','-') permit_expire,
        acc.ORG_PAYMENT_METHOD_NAME As account_pay_method    
    From
        PER_ALL_PEOPLE_F pap Left Join
        PER_ALL_ASSIGNMENTS_F paa On paa.PERSON_ID = pap.PERSON_ID
                And Date() Between paa.EFFECTIVE_START_DATE And paa.EFFECTIVE_END_DATE
                And paa.EFFECTIVE_END_DATE Between pap.EFFECTIVE_START_DATE And pap.EFFECTIVE_END_DATE
                And paa.ASSIGNMENT_STATUS_TYPE_ID = 1 Left Join
        PER_PERIODS_OF_SERVICE pps On pps.PERSON_ID = pap.PERSON_ID Left Join
        PER_PERSON_TYPE_USAGES_F ptu On pap.PERSON_ID = ptu.PERSON_ID
                And paa.EFFECTIVE_END_DATE Between ptu.EFFECTIVE_START_DATE And ptu.EFFECTIVE_END_DATE Left Join
        PER_PERSON_TYPES ppt On ptu.PERSON_TYPE_ID = ppt.PERSON_TYPE_ID Left Join
        HR_LOCATIONS_ALL hrl On hrl.LOCATION_ID = paa.LOCATION_ID Left Join
        HR_LOOKUPS cat On cat.LOOKUP_CODE = paa.EMPLOYMENT_CATEGORY
                And cat.LOOKUP_TYPE = 'EMP_CAT' Left Join
        X000_POSITIONS hrp On hrp.POSITION_ID = paa.POSITION_ID
                And paa.EFFECTIVE_END_DATE Between hrp.EFFECTIVE_START_DATE And hrp.EFFECTIVE_END_DATE Left join
        HR_LOOKUPS nat on nat.lookup_type = 'NATIONALITY' and nat.lookup_code = pap.nationality Left join
        HR_LOOKUPS pan on pan.lookup_type = 'NATIONALITY' and pan.lookup_code = pap.per_information10 Left join
        X000_pay_accounts_latest acc on acc.assignment_id = paa.assignment_id
    Where
        (paa.EFFECTIVE_END_DATE Between pps.DATE_START And pps.ACTUAL_TERMINATION_DATE And
            ppt.USER_PERSON_TYPE != 'Retiree') Or
        (ppt.USER_PERSON_TYPE != 'Retiree' And
            pps.ACTUAL_TERMINATION_DATE == pps.DATE_START)
    Group By
        pap.EMPLOYEE_NUMBER
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print(s_sql)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

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
