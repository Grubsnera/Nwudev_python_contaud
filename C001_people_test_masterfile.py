"""
Script to test PEOPLE master file data
Created on: 1 Mar 2019
Modified on: 20 Apr 2021
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

MASTER FILE LISTS
PEOPLE BIRTHDAYS

ID NUMBER MASTER FILE
TEST ID NUMBER BLANK (V1.0.8)
TEST ID NUMBER INVALID (V1.0.8)
TEST ZA DATE OF BIRTH INVALID (V2.0.0)
TEST ZA GENDER INVALID (V1.0.8)
TEST ID NUMBER DUPLICATE (V1.0.8)
TEST ADDRESS DUPLICATE (In development)

PASSPORT NUMBER MASTER FILE
TEST PASSPORT NUMBER BLANK (V1.0.8)
TEST PASSPORT NUMBER DUPLICATE (V1.0.8)
TEST WORK PERMIT EXPIRED (V2.0.0)

BANK NUMBER MASTER FILE
TEST BANK NUMBER DUPLICATE (V1.0.8)
TEST BANK SARS VERIFICATION

BANK CHANGE MASTER FILE
BANK CHANGE VERIFICATION *

TAX NUMBER MASTER FILE
TEST TAX NUMBER BLANK *
TEST TAX NUMBER INVALID *
TEST TAX NUMBER DUPLICATE

NAME MASTER FILE
TEST NAME DUPLICATE *

GRADE LEAVE MASTER FILE
TEST ASSIGNMENT CATEGORY INVALID (PERMANENT:TEMPORARY) (X007ax)(V2.0.4)
TEST EMPLOYEE CATEGORY INVALID (ACADEMIC:SUPPORT) (X007bx)(V2.0.4)
TEST GRADE INVALID
TEST LEAVE CODE INVALID

BIO MASTER FILE
TEST PHONE WORK INVALID
TEST ADDRESS PRIMARY INVALID

END OF SCRIPT
"""

# SCRIPT WIDE VARIABLES
s_function: str = "C001_people_test_masterfile"


def people_test_masterfile():
    """
    Script to test multiple PEOPLE MASTER FILE items
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # DECLARE VARIABLES
    so_path = "W:/People/" #Source database path
    re_path = "R:/People/" # Results path
    ed_path = "S:/_external_data/" #external data path
    so_file = "People_test_masterfile.sqlite" # Source database
    s_sql = "" # SQL statements
    l_debug: bool = False  # Display statements on screen
    l_export: bool = False  # Export findings to text file
    l_mail: bool = funcconf.l_mail_project
    # l_mail: bool = False  # Send email messages
    l_mess: bool = funcconf.l_mess_project
    # l_mess: bool = False  # Send communicator messages
    l_record: bool = True  # Record findings for future use
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
        funcsms.send_telegram("", "administrator", "<b>" + s_function + "</b>")

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN THE WORKING DATABASE
    with sqlite3.connect(so_path+so_file) as so_conn:
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
    s_file_prefix: str = "X007a"
    sr_file: str = s_file_prefix + "a_category"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file: str = s_file_prefix + "b_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file: str = s_file_prefix + "x_category_invalid"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_file_prefix: str = "X007b"
    sr_file: str = s_file_prefix + "a_acadsupp"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file: str = s_file_prefix + "b_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file: str = s_file_prefix + "x_acadsupp_invalid"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_file_prefix: str = "X007b"
    sr_file: str = s_file_prefix + "a_academic_support_invalid"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file: str = s_file_prefix + "x_academic_support_invalid"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """ ****************************************************************************
    MASTER FILE LISTS
    *****************************************************************************"""

    """*****************************************************************************
    PEOPLE BIRTHDAYS
    *****************************************************************************"""
    print("PEOPLE BIRTHDAYS")
    funcfile.writelog("PEOPLE BIRTHDAYS")

    # BUILD PEOPLE BIRTHDAYS
    so_curs.execute("DROP TABLE IF EXISTS X001_People_birthdays")
    sr_file = "X001_People_birthday"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
        PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER,
        PEOPLE.X002_PEOPLE_CURR.NAME_LIST,
        PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
        PEOPLE.X002_PEOPLE_CURR.DATE_OF_BIRTH,
        PEOPLE.X002_PEOPLE_CURR.AGE,
        PEOPLE.X002_PEOPLE_CURR.POSITION_FULL,
        PEOPLE.X002_PEOPLE_CURR.OE_CODE
    FROM
        PEOPLE.X002_PEOPLE_CURR
    WHERE
        %WHERE%
    ORDER BY
        StrfTime('%m-%d', PEOPLE.X002_PEOPLE_CURR.DATE_OF_BIRTH),
        PEOPLE.X002_PEOPLE_CURR.AGE DESC,
        PEOPLE.X002_PEOPLE_CURR.NAME_LIST
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if funcdate.today_dayname() == "Fri":
        s_sql = s_sql.replace("%WHERE%","StrfTime('%m-%d',PEOPLE.X002_PEOPLE_CURR.DATE_OF_BIRTH)>=StrfTime('%m-%d','now') AND StrfTime('%m-%d',PEOPLE.X002_PEOPLE_CURR.DATE_OF_BIRTH)<=StrfTime('%m-%d','now','+2 day')")
    else:
        s_sql = s_sql.replace("%WHERE%","StrfTime('%m-%d',PEOPLE.X002_PEOPLE_CURR.DATE_OF_BIRTH)>=StrfTime('%m-%d','now') AND StrfTime('%m-%d',PEOPLE.X002_PEOPLE_CURR.DATE_OF_BIRTH)<=StrfTime('%m-%d','now')")
    # print(s_sql) # DEBUG
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export the birthdays
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "People_003_birthday_"
    sx_filet = sx_file + funcdate.cur_month()
    print("Export people birthday..." + sx_path + sx_filet)
    # Read the header data
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    # Write the data
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    # Mail the birthdays
    if l_mail:
        funcmail.Mail("hr_people_birthday")

    """ ****************************************************************************
    ID NUMBER MASTER FILE
    *****************************************************************************"""

    # BUILD TABLE WITH EMPLOYEE TAX NUMBERS
    print("Obtain master list of all employees...")
    sr_file = "X002_id_master"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        'NWU' AS ORG,
        CASE LOCATION_DESCRIPTION
            WHEN 'MAFIKENG CAMPUS' THEN 'MAF'
            WHEN 'POTCHEFSTROOM CAMPUS' THEN 'POT'
            WHEN 'VAAL TRIANGLE CAMPUS' THEN 'VAA'
            ELSE 'NWU'
        END AS LOC,
        PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER AS EMP,
        PEOPLE.X002_PEOPLE_CURR.IDNO AS NUMB,
        PEOPLE.X002_PEOPLE_CURR.DATE_OF_BIRTH AS DOB,
        PEOPLE.X002_PEOPLE_CURR.SEX,
        PEOPLE.X002_PEOPLE_CURR.NATIONALITY AS NAT
        
    From
        PEOPLE.X002_PEOPLE_CURR
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    """*****************************************************************************
    TEST ID NUMBER BLANK
    *****************************************************************************"""
    print("TEST ID NUMBER BLANK")
    funcfile.writelog("TEST ID NUMBER BLANK")

    # TODO:
    #   Add nationality in the Highbond notification email.
    #   Remove supervisor and org officer names in the highbond notification email.

    # DECLARE TEST VARIABLES
    i_find = 0 # Number of findings before previous reported findings
    i_coun = 0 # Number of new findings to report

    # SELECT ALL EMPLOYEES WITHOUT AN ID NUMBER
    print("Select all employees without an ID number...")
    sr_file = "X002aa_id_blank"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X002_id_master.ORG,
        X002_id_master.LOC,
        X002_id_master.EMP
    From
        X002_id_master
    Where
        X002_id_master.NUMB = '' AND
        X002_id_master.NAT = 'SAF'  
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
            
    # SELECT ALL EMPLOYEES WITHOUT AN ID NUMBER
    print("Select all employees without an ID number...")
    sr_file = "X002ab_id_blank"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X002aa_id_blank.*
    From
        X002aa_id_blank
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find = funcsys.tablerowcount(so_curs,sr_file)
    print("*** Found "+str(i_find)+" exceptions ***")
    funcfile.writelog("%t FINDING: "+str(i_find)+" EMPL ID blank finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X002ac_id_getprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,REMARK TEXT)")
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "id_blank":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the impoted data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # SET PREVIOUS FINDINGS
    sr_file = "X002ac_id_setprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        print("Obtain the latest previous finding...")
        s_sql = "Create Table " + sr_file + " As" + """
        Select
            GET.PROCESS,
            GET.FIELD1,
            GET.FIELD2,
            GET.FIELD3,
            GET.FIELD4,
            GET.FIELD5,
            Max(GET.DATE_REPORTED) As DATE_REPORTED,
            GET.DATE_RETEST,
            GET.REMARK
        From
            X002ac_id_getprev GET
        Group By
            GET.FIELD1        
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X002ad_id_addprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        SELECT
            X002ab_id_blank.*,
            'id_blank' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%TODAYPLUS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        FROM
            X002ab_id_blank Left Join
            X002ac_id_setprev PREV ON PREV.FIELD1 = X002ab_id_blank.EMP
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X002ae_id_newprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE "+sr_file+" AS " + """
        SELECT
            PREV.PROCESS,
            PREV.EMP AS FIELD1,
            '' AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        FROM
            X002ad_id_addprev PREV
        WHERE
            PREV.PREV_PROCESS IS NULL Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""          
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: "+sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(so_curs,sr_file)
        if i_coun > 0:
            print("*** " +str(i_coun)+ " Finding(s) to report ***")    
            sr_filet = sr_file
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
                funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
                funcfile.writelog("%t EXPORT DATA: "+sr_file)
            if l_mess:
                s_desc = "ID number blank"
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X002af_offi"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_ID_BLANK_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X002ag_supe"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_ID_BLANK_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X002ah_id_cont"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            X002ad_id_addprev.ORG,
            X002ad_id_addprev.LOC,
            X002ad_id_addprev.EMP,
            PEOPLE.X002_PEOPLE_CURR.NAME_LIST AS NAME,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X002ad_id_addprev
            Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X002ad_id_addprev.EMP
            Left Join X002af_offi CAMP_OFF On CAMP_OFF.CAMPUS = X002ad_id_addprev.LOC
            Left Join X002af_offi ORG_OFF On ORG_OFF.CAMPUS = X002ad_id_addprev.ORG
            Left Join X002ag_supe CAMP_SUP On CAMP_SUP.CAMPUS = X002ad_id_addprev.LOC
            Left Join X002ag_supe ORG_SUP On ORG_SUP.CAMPUS = X002ad_id_addprev.ORG
        WHERE
          X002ad_id_addprev.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)    

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X002ax_id_blank"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    print("Build the final report")
    if i_find > 0 and i_coun > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'EMPLOYEE ID NUMBER BLANK' As AUDIT_FINDING,
            X002ah_id_cont.ORG AS ORGANIZATION,
            X002ah_id_cont.LOC AS LOCATION,
            X002ah_id_cont.EMP AS EMPLOYEE_NUMBER,
            X002ah_id_cont.NAME,
            X002ah_id_cont.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
            X002ah_id_cont.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
            X002ah_id_cont.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
            X002ah_id_cont.CAMP_SUP_NAME AS SUPERVISOR,
            X002ah_id_cont.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
            X002ah_id_cont.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
            X002ah_id_cont.ORG_OFF_NAME AS ORGANIZATION_OFFICER,
            X002ah_id_cont.ORG_OFF_NUMB AS ORGANIZATION_OFFICER_NUMB,
            X002ah_id_cont.ORG_OFF_MAIL AS ORGANIZATION_OFFICER_MAIL,
            X002ah_id_cont.ORG_SUP_NAME AS ORGANIZATION_SUPERVISOR,
            X002ah_id_cont.ORG_SUP_NUMB AS ORGANIZATION_SUPERVISOR_NUMB,
            X002ah_id_cont.ORG_SUP_MAIL AS ORGANIZATION_SUPERVISOR_MAIL
        From
            X002ah_id_cont
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
            print("Export findings...")
            sr_filet = sr_file
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "People_test_002ax_id_blank_"
            sx_filet = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
            funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
   
    """ ****************************************************************************
    TEST ID NUMBER INVALID
        NOTE 01: SELECT ALL CURRENT EMPLOYEES WITH ID NUMBER
    *****************************************************************************"""
    print("TEST ID NUMBER INVALID")
    funcfile.writelog("TEST ID NUMBER INVALID")

    # DECLARE TEST VARIABLES
    i_find = 0 # Number of findings before previous reported findings
    i_coun = 0 # Number of new findings to report

    # SELECT ALL EMPLOYEES WITH AN ID NUMBER
    print("Select all employees with id number...")
    sr_file = "X002ba_id_calc"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X002_id_master.ORG,
        X002_id_master.LOC,
        X002_id_master.EMP,
        X002_id_master.NUMB,
        SUBSTR(NUMB,1,1)+SUBSTR(NUMB,3,1)+SUBSTR(NUMB,5,1)+SUBSTR(NUMB,7,1)+SUBSTR(NUMB,9,1)+SUBSTR(NUMB,11,1) AS ODDT,
        (SUBSTR(NUMB,2,1)||SUBSTR(NUMB,4,1)||SUBSTR(NUMB,6,1)||SUBSTR(NUMB,8,1)||SUBSTR(NUMB,10,1)||SUBSTR(NUMB,12,1))*2 AS EVEC,
        0 AS EVET,    
        '' AS CONT,
        '' AS VAL
    From
        X002_id_master
    Where
        X002_id_master.NUMB <> ''
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
    print("Update calculation columns...")
    # Update columns
    so_curs.execute("UPDATE X002ba_id_calc SET EVET = SUBSTR(EVEC,1,1)+SUBSTR(EVEC,2,1)+SUBSTR(EVEC,3,1)+SUBSTR(EVEC,4,1)+SUBSTR(EVEC,5,1)+SUBSTR(EVEC,6,1)+SUBSTR(EVEC,7,1);")
    so_conn.commit()
    so_curs.execute("UPDATE X002ba_id_calc SET CONT = SUBSTR(10-SUBSTR(ODDT+EVET,-1,1),-1,1);")
    so_conn.commit()
    so_curs.execute("UPDATE X002ba_id_calc " + """
                     SET VAL =
                     CASE
                         WHEN SUBSTR(NUMB,13) = CONT THEN 'T'
                         ELSE 'F'
                     END;""")
    so_conn.commit()

    # SELECT ALL EMPLOYEES WITH AN INVALID ID NUMBER
    print("Select all employees with an invalid id number...")
    sr_file = "X002bb_id_inva"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X002ba_id_calc.ORG,
        X002ba_id_calc.LOC,
        X002ba_id_calc.EMP,
        X002ba_id_calc.NUMB
    From
        X002ba_id_calc
    Where
        X002ba_id_calc.VAL = 'F'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find = funcsys.tablerowcount(so_curs,sr_file)
    print("*** Found "+str(i_find)+" exceptions ***")
    funcfile.writelog("%t FINDING: "+str(i_find)+" EMPL ID invalid finding(s)")

    # GET PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X002bc_id_getprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,REMARK TEXT)")
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "id_invalid":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the impoted data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # SET PREVIOUS FINDINGS
    sr_file = "X002bc_id_setprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        print("Obtain the latest previous finding...")
        s_sql = "Create Table " + sr_file + " As" + """
        Select
            GET.PROCESS,
            GET.FIELD1,
            GET.FIELD2,
            GET.FIELD3,
            GET.FIELD4,
            GET.FIELD5,
            Max(GET.DATE_REPORTED) As DATE_REPORTED,
            GET.DATE_RETEST,
            GET.REMARK
        From
            X003ec_get_previous GET
        Group By
            GET.FIELD1        
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD PREVIOUS FINDINGS
    sr_file = "X002bd_id_addprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        SELECT
            X002bb_id_inva.*,
            'id_invalid' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%TODAYPLUS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        FROM
            X002bb_id_inva Left Join
            X002bc_id_setprev PREV ON PREV.FIELD1 = X002bb_id_inva.EMP
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%",  funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X002be_id_newprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE "+sr_file+" AS " + """
        SELECT
            PREV.PROCESS,
            PREV.EMP AS FIELD1,
            '' AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        FROM
            X002bd_id_addprev PREV
        WHERE
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: "+sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(so_curs,sr_file)
        if i_coun > 0:
            print("*** " +str(i_coun)+ " Finding(s) to report ***")    
            sr_filet = sr_file
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
                funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
                funcfile.writelog("%t EXPORT DATA: "+sr_file)
            if l_mess:
                s_desc = "ID number invalid"
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X002bf_offi"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_ID_INVALID_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X002bg_supe"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_ID_INVALID_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X002bh_id_cont"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            X002bd_id_addprev.ORG,
            X002bd_id_addprev.LOC,
            X002bd_id_addprev.EMP,
            PEOPLE.X002_PEOPLE_CURR.NAME_LIST AS NAME,
            X002bd_id_addprev.NUMB,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X002bd_id_addprev
            Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X002bd_id_addprev.EMP
            Left Join X002bf_offi CAMP_OFF On CAMP_OFF.CAMPUS = X002bd_id_addprev.LOC
            Left Join X002bf_offi ORG_OFF On ORG_OFF.CAMPUS = X002bd_id_addprev.ORG
            Left Join X002bg_supe CAMP_SUP On CAMP_SUP.CAMPUS = X002bd_id_addprev.LOC
            Left Join X002bg_supe ORG_SUP On ORG_SUP.CAMPUS = X002bd_id_addprev.ORG
        WHERE
          X002bd_id_addprev.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    print("Build the final report")
    sr_file = "X002bx_id_invalid"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'ID NUMBER INVALID' As AUDIT_FINDING,
            X002bh_id_cont.ORG AS ORGANIZATION,
            X002bh_id_cont.LOC AS LOCATION,
            X002bh_id_cont.EMP AS EMPLOYEE_NUMBER,
            X002bh_id_cont.NAME,
            X002bh_id_cont.NUMB AS ID_NUMBER,
            X002bh_id_cont.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
            X002bh_id_cont.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
            X002bh_id_cont.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
            X002bh_id_cont.CAMP_SUP_NAME AS SUPERVISOR,
            X002bh_id_cont.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
            X002bh_id_cont.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
            X002bh_id_cont.ORG_OFF_NAME AS ORG_OFFICER,
            X002bh_id_cont.ORG_OFF_NUMB AS ORG_OFFICER_NUMB,
            X002bh_id_cont.ORG_OFF_MAIL AS ORG_OFFICER_MAIL,
            X002bh_id_cont.ORG_SUP_NAME AS ORG_SUPERVISOR,
            X002bh_id_cont.ORG_SUP_NUMB AS ORG_SUPERVISOR_NUMB,
            X002bh_id_cont.ORG_SUP_MAIL AS ORG_SUPERVISOR_MAIL
        From
            X002bh_id_cont
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
            print("Export findings...")
            sr_filet = sr_file
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "People_test_002bx_id_invalid_"
            sx_filet = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
            funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    TEST ZA DATE OF BIRTH INVALID
    *****************************************************************************"""
    print("TEST ZA DATE OF BIRTH INVALID")
    funcfile.writelog("TEST ZA DATE OF BIRTH INVALID")

    # VARIABLES
    s_fprefix: str = 'X002c'
    s_finding: str = 'ID DOB INVALID'
    s_xfile: str = "001_reported.txt"

    # BUILD TABLE WITH NOT EMPTY ID NUMBERS
    print("Build not empty ID number table...")
    sr_file = s_fprefix + "a_iddob_invalid"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        MAST.ORG,
        MAST.LOC,
        MAST.EMP,
        MAST.NUMB,
        MAST.DOB,
        SUBSTR(NUMB,1,2)||'-'||SUBSTR(NUMB,3,2)||'-'||SUBSTR(NUMB,5,2) AS DOBC,
        '' AS VAL
    From
        X002_id_master MAST
    Where
        MAST.NUMB <> ''
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # UPDATE COLUMNS
    print("Update column valid...")
    s_sql = "UPDATE " + s_fprefix + "a_iddob_invalid " + """
        SET VAL =
        CASE
            WHEN SUBSTR(DOB,3,8) = DOBC THEN 'T'
            ELSE 'F'
        END;"""
    so_curs.execute(s_sql)
    so_conn.commit()

    # SELECT ALL EMPLOYEES WITH AN INVALID ID NUMBER
    print("Select all employees with an invalid date of birth...")
    sr_file = s_fprefix + "b_finding"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        FIND.ORG,
        FIND.LOC,
        FIND.EMP,
        FIND.NUMB,
        FIND.DOB,
        FIND.DOBC
    From
        %FILEP%a_iddob_invalid FIND
    Where
        FIND.VAL = 'F'
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_fprefix)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " invalid finding(s)")

    # GET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.get_previous_finding(so_curs, ed_path, s_xfile, s_finding, "ITTTT")
        so_conn.commit()

    # SET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.set_previous_finding(so_curs)
        so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = s_fprefix + "d_addprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        SELECT
            FIND.*,
            Lower('%FINDING%') AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%TODAYPLUS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        FROM
            %FILEP%b_finding FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = FIND.EMP
        ;"""
        s_sql = s_sql.replace("%FINDING%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = s_fprefix + "e_newprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
            PREV.PROCESS,
            PREV.EMP AS FIELD1,
            '' AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        FROM
            %FILEP%d_addprev PREV
        WHERE
            PREV.PREV_PROCESS IS NULL Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sr_filet = sr_file
            sx_path = ed_path
            sx_file = s_xfile[:-4]
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                s_desc = "Date of birth invalid"
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_officer(so_curs, "HR", "TEST " + s_finding + " OFFICER")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_supervisor(so_curs, "HR", "TEST " + s_finding + " SUPERVISOR")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = s_fprefix + "h_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.EMP,
            PEOPLE.X002_PEOPLE_CURR.NAME_LIST AS NAME,
            PREV.NUMB,
            SUBSTR(PREV.DOB,3,8) AS DOB,
            PREV.DOBC,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            %FILEP%d_addprev PREV
            Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PREV.EMP
            Left Join Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC
            Left Join Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
            Left Join Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC
            Left Join Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        WHERE
          PREV.PREV_PROCESS IS NULL
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = s_fprefix + "x_iddob_invalid"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'ID DOB INVALID' As AUDIT_FINDING,
            FIND.ORG AS ORGANIZATION,
            FIND.LOC AS LOCATION,
            FIND.EMP AS EMPLOYEE_NUMBER,
            FIND.NAME,
            FIND.NUMB AS ID_NUMBER,
            FIND.DOBC AS ID_DATE_OF_BIRTH,
            FIND.DOB AS SYSTEM_DATE_OF_BIRTH,
            FIND.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
            FIND.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
            FIND.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
            FIND.CAMP_SUP_NAME AS SUPERVISOR,
            FIND.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
            FIND.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
            FIND.ORG_OFF_NAME AS ORG_OFFICER,
            FIND.ORG_OFF_NUMB AS ORG_OFFICER_NUMB,
            FIND.ORG_OFF_MAIL AS ORG_OFFICER_MAIL,
            FIND.ORG_SUP_NAME AS ORG_SUPERVISOR,
            FIND.ORG_SUP_NUMB AS ORG_SUPERVISOR_NUMB,
            FIND.ORG_SUP_MAIL AS ORG_SUPERVISOR_MAIL
        From
            %FILEP%h_detail FIND
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export == True and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sr_filet = sr_file
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "Peoplemaster_test_" + s_fprefix + "_" + s_finding.lower() + "_"
            sx_filet = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    TEST ZA GENDER INVALID
    *****************************************************************************"""
    print("TEST ZA SEX INVALID")
    funcfile.writelog("TEST ZA SEX INVALID")

    # BUILD TABLE WITH NOT EMPTY ID NUMBERS
    print("Build not empty ID number table...")
    sr_file = "X002da_sex_calc"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X002_id_master.ORG,
        X002_id_master.LOC,
        X002_id_master.EMP,
        X002_id_master.NUMB,
        X002_id_master.SEX,
        CASE
            WHEN CAST(SUBSTR(NUMB,7,1) AS INT) >= 5 THEN 'MALE'
            WHEN CAST(SUBSTR(NUMB,7,1) AS INT) >= 0 THEN 'FEMALE'
            ELSE 'OTHER'
        END AS GEND,    
        '' AS VAL
    From
        X002_id_master
    Where
        X002_id_master.NUMB <> ''
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
    # UPDATE COLUMNS
    print("Update column valid...")
    so_curs.execute("UPDATE X002da_sex_calc " + """
                     SET VAL =
                     CASE
                         WHEN SEX = GEND THEN 'T'
                         ELSE 'F'                 
                     END;""")
    so_conn.commit()

    # SELECT ALL EMPLOYEES WITH AN INVALID SEX
    print("Select all employees with an invalid sex...")
    sr_file = "X002db_sex_inva"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X002da_sex_calc.ORG,
        X002da_sex_calc.LOC,
        X002da_sex_calc.EMP,
        X002da_sex_calc.NUMB,
        X002da_sex_calc.SEX,
        X002da_sex_calc.GEND
    From
        X002da_sex_calc
    Where
        X002da_sex_calc.VAL = 'F'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find = funcsys.tablerowcount(so_curs,sr_file)
    print("*** Found "+str(i_find)+" exceptions ***")
    funcfile.writelog("%t FINDING: "+str(i_find)+" EMPL ID GENDER invalid finding(s)")

    # GET PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X002dc_sex_getprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,REMARK TEXT)")
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "sex_invalid":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the impoted data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # SET PREVIOUS FINDINGS
    sr_file = "X002dc_sex_setprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        print("Obtain the latest previous finding...")
        s_sql = "Create Table " + sr_file + " As" + """
        Select
            GET.PROCESS,
            GET.FIELD1,
            GET.FIELD2,
            GET.FIELD3,
            GET.FIELD4,
            GET.FIELD5,
            Max(GET.DATE_REPORTED) As DATE_REPORTED,
            GET.DATE_RETEST,
            GET.REMARK
        From
            X003ec_get_previous GET
        Group By
            GET.FIELD1        
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD PREVIOUS FINDINGS
    sr_file = "X002dd_sex_addprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        SELECT
            X002db_sex_inva.*,
            'sex_invalid' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%TODAYPLUS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        FROM
            X002db_sex_inva Left Join
            X002dc_sex_setprev PREV ON PREV.FIELD1 = X002db_sex_inva.EMP
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X002de_sex_newprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE "+sr_file+" AS " + """
        SELECT
            PREV.PROCESS,
            PREV.EMP AS FIELD1,
            '' AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        FROM
            X002dd_sex_addprev PREV
        WHERE
            PREV.PREV_PROCESS IS NULL And
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: "+sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(so_curs,sr_file)
        if i_coun > 0:
            print("*** " +str(i_coun)+ " Finding(s) to report ***")    
            sr_filet = sr_file
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
                funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
                funcfile.writelog("%t EXPORT DATA: "+sr_file)
            if l_mess:
                s_desc = "Gender invalid"
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X002df_offi"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_SEX_INVALID_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X002dg_supe"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_SEX_INVALID_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X002dh_sex_cont"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            X002dd_sex_addprev.ORG,
            X002dd_sex_addprev.LOC,
            X002dd_sex_addprev.EMP,
            PEOPLE.X002_PEOPLE_CURR.NAME_LIST AS NAME,
            X002dd_sex_addprev.NUMB,
            X002dd_sex_addprev.SEX,
            X002dd_sex_addprev.GEND,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X002dd_sex_addprev
            Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X002dd_sex_addprev.EMP
            Left Join X002df_offi CAMP_OFF On CAMP_OFF.CAMPUS = X002dd_sex_addprev.LOC
            Left Join X002df_offi ORG_OFF On ORG_OFF.CAMPUS = X002dd_sex_addprev.ORG
            Left Join X002dg_supe CAMP_SUP On CAMP_SUP.CAMPUS = X002dd_sex_addprev.LOC
            Left Join X002dg_supe ORG_SUP On ORG_SUP.CAMPUS = X002dd_sex_addprev.ORG
        WHERE
          X002dd_sex_addprev.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X002dx_sex_invalid"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'ID GENDER INVALID' As AUDIT_FINDING,
            X002dh_sex_cont.ORG AS ORGANIZATION,
            X002dh_sex_cont.LOC AS LOCATION,
            X002dh_sex_cont.EMP AS EMPLOYEE_NUMBER,
            X002dh_sex_cont.NAME,
            X002dh_sex_cont.NUMB AS ID_NUMBER,
            X002dh_sex_cont.GEND AS ID_GENDER,
            X002dh_sex_cont.SEX AS SYSTEM_GENDER,
            X002dh_sex_cont.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
            X002dh_sex_cont.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
            X002dh_sex_cont.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
            X002dh_sex_cont.CAMP_SUP_NAME AS SUPERVISOR,
            X002dh_sex_cont.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
            X002dh_sex_cont.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
            X002dh_sex_cont.ORG_OFF_NAME AS ORG_OFFICER,
            X002dh_sex_cont.ORG_OFF_NUMB AS ORG_OFFICER_NUMB,
            X002dh_sex_cont.ORG_OFF_MAIL AS ORG_OFFICER_MAIL,
            X002dh_sex_cont.ORG_SUP_NAME AS ORG_SUPERVISOR,
            X002dh_sex_cont.ORG_SUP_NUMB AS ORG_SUPERVISOR_NUMB,
            X002dh_sex_cont.ORG_SUP_MAIL AS ORG_SUPERVISOR_MAIL
        From
            X002dh_sex_cont
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
            print("Export findings...")
            sr_filet = sr_file
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "People_test_002cx_sex_invalid_"
            sx_filet = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
            funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    TEST ID NUMBER DUPLICATE
        NOTE 01: SELECT ALL CURRENT EMPLOYEES WITH ID NUMBERS
    *****************************************************************************"""
    print("TEST ID NUMBER DUPLICATE")
    funcfile.writelog("TEST ID NUMBER DUPLICATE")

    # DECLARE TEST VARIABLES
    i_find = 0 # Number of findings before previous reported findings
    i_coun = 0 # Number of new findings to report

    # COUNT ALL EMPLOYEES WITH AN ID NUMBER
    print("Count all employees with id number...")
    sr_file = "X002ea_id_coun"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X002_id_master.NUMB,
        Count(X002_id_master.EMP) As COUNT
    From
        X002_id_master
    Where
        X002_id_master.NUMB <> ''
    Group By
        X002_id_master.NUMB
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # IDENTIFY DUPLICATE ACCOUNTS
    print("Build list of duplicate accounts...")
    sr_file = "X002eb_id_dupl"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X002_id_master.ORG,
        X002_id_master.LOC,
        X002_id_master.EMP,
        X002_id_master.NUMB,
        X002ea_id_coun.COUNT
    From
        X002_id_master Left Join
        X002ea_id_coun On X002ea_id_coun.NUMB = X002_id_master.NUMB
    Where
        X002ea_id_coun.COUNT > 1
    Order by
        X002ea_id_coun.NUMB, EMP
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find = funcsys.tablerowcount(so_curs,sr_file)
    print("*** Found "+str(i_find)+" exceptions ***")
    funcfile.writelog("%t FINDING: "+str(i_find)+" EMPL ID duplicate finding(s)")

    # GET PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X002ec_id_getprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,REMARK TEXT)")
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "id_duplicate":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the impoted data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # SET PREVIOUS FINDINGS
    sr_file = "X002ec_id_setprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        print("Obtain the latest previous finding...")
        s_sql = "Create Table " + sr_file + " As" + """
        Select
            GET.PROCESS,
            GET.FIELD1,
            GET.FIELD2,
            GET.FIELD3,
            GET.FIELD4,
            GET.FIELD5,
            Max(GET.DATE_REPORTED) As DATE_REPORTED,
            GET.DATE_RETEST,
            GET.REMARK
        From
            X002ec_id_getprev GET
        Group By
            GET.FIELD1        
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD PREVIOUS FINDINGS
    sr_file = "X002ed_id_addprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        SELECT
            X002eb_id_dupl.*,
            'id_duplicate' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%TODAYPLUS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        FROM
            X002eb_id_dupl Left Join
            X002ec_id_setprev PREV ON PREV.FIELD1 = X002eb_id_dupl.EMP
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X002ee_id_newprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE "+sr_file+" AS " + """
        SELECT
            PREV.PROCESS,
            PREV.EMP AS FIELD1,
            '' AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        FROM
            X002ed_id_addprev PREV
        WHERE
            PREV.PREV_PROCESS IS NULL And
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: "+sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(so_curs,sr_file)
        if i_coun > 0:
            print("*** " +str(i_coun)+ " Finding(s) to report ***")    
            sr_filet = sr_file
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
                funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
                funcfile.writelog("%t EXPORT DATA: "+sr_file)
            if l_mess:
                s_desc = "ID number duplicate"
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X002ef_offi"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_ID_DUPL_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X002eg_supe"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_ID_DUPL_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X002eh_id_cont"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            X002ed_id_addprev.ORG,
            X002ed_id_addprev.LOC,
            X002ed_id_addprev.EMP,
            X002ed_id_addprev.NUMB,
            X002ed_id_addprev.COUNT,
            PEOPLE.X002_PEOPLE_CURR.NAME_LIST AS NAME,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X002ed_id_addprev
            Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X002ed_id_addprev.EMP
            Left Join X002ef_offi CAMP_OFF On CAMP_OFF.CAMPUS = X002ed_id_addprev.LOC
            Left Join X002ef_offi ORG_OFF On ORG_OFF.CAMPUS = X002ed_id_addprev.ORG
            Left Join X002eg_supe CAMP_SUP On CAMP_SUP.CAMPUS = X002ed_id_addprev.LOC
            Left Join X002eg_supe ORG_SUP On ORG_SUP.CAMPUS = X002ed_id_addprev.ORG
        WHERE
          X002ed_id_addprev.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X002ex_id_duplicate"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'ID NUMBER DUPLICATE' As AUDIT_FINDING,
            X002eh_id_cont.ORG AS ORGANIZATION,
            X002eh_id_cont.LOC AS LOCATION,
            X002eh_id_cont.EMP AS EMPLOYEE_NUMBER,
            X002eh_id_cont.NAME,
            X002eh_id_cont.NUMB AS ID_DUPLICATE,
            X002eh_id_cont.COUNT,
            X002eh_id_cont.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
            X002eh_id_cont.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
            X002eh_id_cont.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
            X002eh_id_cont.CAMP_SUP_NAME AS SUPERVISOR,
            X002eh_id_cont.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
            X002eh_id_cont.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
            X002eh_id_cont.ORG_OFF_NAME AS ORG_OFFICER,
            X002eh_id_cont.ORG_OFF_NUMB AS ORG_OFFICER_NUMB,
            X002eh_id_cont.ORG_OFF_MAIL AS ORG_OFFICER_MAIL,
            X002eh_id_cont.ORG_SUP_NAME AS ORG_SUPERVISOR,
            X002eh_id_cont.ORG_SUP_NUMB AS ORG_SUPERVISOR_NUMB,
            X002eh_id_cont.ORG_SUP_MAIL AS ORG_SUPERVISOR_MAIL
        From
            X002eh_id_cont
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
            print("Export findings...")
            sr_filet = sr_file
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "People_test_002ex_id_duplicate_"
            sx_filet = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
            funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    PASSPORT NUMBER MASTER FILE
    *****************************************************************************"""

    # BUILD TABLE WITH EMPLOYEE PASSPORT
    print("Obtain master list of all employees...")
    sr_file = "X003_pass_master"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        'NWU' AS ORG,
        CASE MASTER.LOCATION_DESCRIPTION
            WHEN 'MAFIKENG CAMPUS' THEN 'MAF'
            WHEN 'POTCHEFSTROOM CAMPUS' THEN 'POT'
            WHEN 'VAAL TRIANGLE CAMPUS' THEN 'VAA'
            ELSE 'NWU'
        END AS LOC,
        MASTER.EMPLOYEE_NUMBER AS EMP,
        MASTER.IDNO,
        MASTER.PASSPORT AS NUMB,
        MASTER.PERMIT,
        MASTER.PERMIT_EXPIRE,
        MASTER.NATIONALITY AS NAT,
        MASTER.POSITION_NAME As POSITION,
        MASTER.ADDRESS_SARS        
    From
        PEOPLE.X002_PEOPLE_CURR MASTER
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    """ ****************************************************************************
    TEST PASSPORT NUMBER BLANK
    *****************************************************************************"""
    print("TEST PASSPORT NUMBER BLANK")
    funcfile.writelog("TEST PASSPORT NUMBER BLANK")

    # DECLARE TEST VARIABLES
    i_find = 0 # Number of findings before previous reported findings
    i_coun = 0 # Number of new findings to report

    # SELECT ALL FOREIGN EMPLOYEES WITHOUT A PASSPORT NUMBER
    print("Select all foreign without a passport...")
    sr_file = "X003aa_pass_blank"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X003_pass_master.ORG,
        X003_pass_master.LOC,
        X003_pass_master.EMP
    From
        X003_pass_master
    Where
        X003_pass_master.NUMB = '' AND
        X003_pass_master.IDNO = '' AND
        X003_pass_master.NAT <> 'SAF'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # SELECT ALL FOREIGN EMPLOYEES WITHOUT A PASSPORT NUMBER
    print("Select all foreign employees without a passport number...")
    sr_file = "X003ab_pass_blank"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X003aa_pass_blank.*
    From
        X003aa_pass_blank
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find = funcsys.tablerowcount(so_curs,sr_file)
    print("*** Found "+str(i_find)+" exceptions ***")
    funcfile.writelog("%t FINDING: "+str(i_find)+" EMPL PASSPORT blank finding(s)")

    # GET PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X003ac_pass_getprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,REMARK TEXT)")
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "passport_blank":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the impoted data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # SET PREVIOUS FINDINGS
    sr_file = "X003ac_setprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        print("Obtain the latest previous finding...")
        s_sql = "Create Table " + sr_file + " As" + """
        Select
            GET.PROCESS,
            GET.FIELD1,
            GET.FIELD2,
            GET.FIELD3,
            GET.FIELD4,
            GET.FIELD5,
            Max(GET.DATE_REPORTED) As DATE_REPORTED,
            GET.DATE_RETEST,
            GET.REMARK
        From
            X003ac_pass_getprev GET
        Group By
            GET.FIELD1        
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD PREVIOUS FINDINGS
    sr_file = "X003ad_pass_addprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        SELECT
            X003ab_pass_blank.*,
            'passport_blank' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%TODAYPLUS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        FROM
            X003ab_pass_blank Left Join
            X003ac_setprev PREV ON PREV.FIELD1 = X003ab_pass_blank.EMP
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X003ae_pass_newprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE "+sr_file+" AS " + """
        SELECT
            PREV.PROCESS,
            PREV.EMP AS FIELD1,
            '' AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        FROM
            X003ad_pass_addprev PREV
        WHERE
            PREV.PREV_PROCESS IS NULL Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: "+sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(so_curs,sr_file)
        if i_coun > 0:
            print("*** " +str(i_coun)+ " Finding(s) to report ***")    
            sr_filet = sr_file
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
                funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
                funcfile.writelog("%t EXPORT DATA: "+sr_file)
            if l_mess:
                s_desc = "Passport number blank"
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X003af_offi"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_PASSPORT_BLANK_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X003ag_supe"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_PASSPORT_BLANK_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X003ah_pass_cont"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            X003ad_pass_addprev.ORG,
            X003ad_pass_addprev.LOC,
            X003ad_pass_addprev.EMP,
            PEOPLE.X002_PEOPLE_CURR.NAME_LIST AS NAME,
            UPPER(PEOPLE.X002_PEOPLE_CURR.NATIONALITY_NAME) AS NATIONALITY,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X003ad_pass_addprev
            Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X003ad_pass_addprev.EMP
            Left Join X003af_offi CAMP_OFF On CAMP_OFF.CAMPUS = X003ad_pass_addprev.LOC
            Left Join X003af_offi ORG_OFF On ORG_OFF.CAMPUS = X003ad_pass_addprev.ORG
            Left Join X003ag_supe CAMP_SUP On CAMP_SUP.CAMPUS = X003ad_pass_addprev.LOC
            Left Join X003ag_supe ORG_SUP On ORG_SUP.CAMPUS = X003ad_pass_addprev.ORG
        WHERE
          X003ad_pass_addprev.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X003ax_pass_blank"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    print("Build the final report")
    if i_find > 0 and i_coun > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'PASSPORT BLANK' As AUDIT_FINDING,
            X003ah_pass_cont.ORG AS ORGANIZATION,
            X003ah_pass_cont.LOC AS LOCATION,
            X003ah_pass_cont.EMP AS EMPLOYEE_NUMBER,
            X003ah_pass_cont.NAME,
            X003ah_pass_cont.NATIONALITY,
            X003ah_pass_cont.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
            X003ah_pass_cont.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
            X003ah_pass_cont.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
            X003ah_pass_cont.CAMP_SUP_NAME AS SUPERVISOR,
            X003ah_pass_cont.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
            X003ah_pass_cont.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
            X003ah_pass_cont.ORG_OFF_NAME AS ORGANIZATION_OFFICER,
            X003ah_pass_cont.ORG_OFF_NUMB AS ORGANIZATION_OFFICER_NUMB,
            X003ah_pass_cont.ORG_OFF_MAIL AS ORGANIZATION_OFFICER_MAIL,
            X003ah_pass_cont.ORG_SUP_NAME AS ORGANIZATION_SUPERVISOR,
            X003ah_pass_cont.ORG_SUP_NUMB AS ORGANIZATION_SUPERVISOR_NUMB,
            X003ah_pass_cont.ORG_SUP_MAIL AS ORGANIZATION_SUPERVISOR_MAIL
        From
            X003ah_pass_cont
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
            print("Export findings...")
            sr_filet = sr_file
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "People_test_005ax_pass_blank_"
            sx_filet = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
            funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    TEST PASSPORT NUMBER DUPLICATE
    *****************************************************************************"""
    print("TEST PASSPORT NUMBER DUPLICATE")
    funcfile.writelog("TEST PASSPORT NUMBER DUPLICATE")

    # DECLARE TEST VARIABLES
    i_find = 0 # Number of findings before previous reported findings
    i_coun = 0 # Number of new findings to report

    # COUNT ALL EMPLOYEES WITH A BANK ACCOUNT NUMBER
    print("Count all employees with passport number...")
    sr_file = "X003ba_pass_coun"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X003_pass_master.NUMB,
        Count(X003_pass_master.EMP) As COUNT
    From
        X003_pass_master
    Where
        X003_pass_master.NUMB <> ''
    Group By
        X003_pass_master.NUMB
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # IDENTIFY DUPLICATE ACCOUNTS
    print("Build list of duplicate accounts...")
    sr_file = "X003bb_pass_dupl"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X003_pass_master.*,
        X003ba_pass_coun.COUNT
    From
        X003_pass_master Left Join
        X003ba_pass_coun On X003ba_pass_coun.NUMB = X003_pass_master.NUMB
    Where
        X003ba_pass_coun.COUNT > 1
    Order by
        X003ba_pass_coun.NUMB, EMP
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find = funcsys.tablerowcount(so_curs,sr_file)
    print("*** Found "+str(i_find)+" exceptions ***")
    funcfile.writelog("%t FINDING: "+str(i_find)+" EMPL PASSPORT duplicate finding(s)")

    # GET PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X003bc_pass_getprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,REMARK TEXT)")
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "passport_duplicate":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the impoted data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # SET PREVIOUS FINDINGS
    sr_file = "X003bc_setprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        print("Obtain the latest previous finding...")
        s_sql = "Create Table " + sr_file + " As" + """
        Select
            GET.PROCESS,
            GET.FIELD1,
            GET.FIELD2,
            GET.FIELD3,
            GET.FIELD4,
            GET.FIELD5,
            Max(GET.DATE_REPORTED) As DATE_REPORTED,
            GET.DATE_RETEST,
            GET.REMARK
        From
            X003bc_pass_getprev GET
        Group By
            GET.FIELD1        
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X003bd_pass_addprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        SELECT
            X003bb_pass_dupl.*,
            'passport_duplicate' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%TODAYPLUS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        FROM
            X003bb_pass_dupl Left Join
            X003bc_setprev PREV ON PREV.FIELD1 = X003bb_pass_dupl.EMP
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X003be_pass_newprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE "+sr_file+" AS " + """
        SELECT
            PREV.PROCESS,
            PREV.EMP AS FIELD1,
            '' AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        FROM
            X003bd_pass_addprev PREV
        WHERE
            PREV.PREV_PROCESS IS NULL Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: "+sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(so_curs,sr_file)
        if i_coun > 0:
            print("*** " +str(i_coun)+ " Finding(s) to report ***")    
            sr_filet = sr_file
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
                funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
                funcfile.writelog("%t EXPORT DATA: "+sr_file)
            if l_mess:
                s_desc = "Passport number duplicate"
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X003bf_offi"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_PASSPORT_DUPLICATE_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X003bg_supe"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_PASSPORT_DUPLICATE_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X003bh_pass_cont"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            X003bd_pass_addprev.ORG,
            X003bd_pass_addprev.LOC,
            X003bd_pass_addprev.EMP,
            X003bd_pass_addprev.NUMB,
            X003bd_pass_addprev.COUNT,
            PEOPLE.X002_PEOPLE_CURR.NAME_ADDR AS NAME,
            PEOPLE.X002_PEOPLE_CURR.NATIONALITY_NAME AS NAT,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X003bd_pass_addprev
            Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X003bd_pass_addprev.EMP
            Left Join X003bf_offi CAMP_OFF On CAMP_OFF.CAMPUS = X003bd_pass_addprev.LOC
            Left Join X003bf_offi ORG_OFF On ORG_OFF.CAMPUS = X003bd_pass_addprev.ORG
            Left Join X003bg_supe CAMP_SUP On CAMP_SUP.CAMPUS = X003bd_pass_addprev.LOC
            Left Join X003bg_supe ORG_SUP On ORG_SUP.CAMPUS = X003bd_pass_addprev.ORG
        WHERE
          X003bd_pass_addprev.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X003bx_pass_duplicate"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    print("Build the final report")
    if i_find > 0 and i_coun > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'PASSPORT DUPLICATE' As AUDIT_FINDING,
            X003bh_pass_cont.ORG AS ORGANIZATION,
            X003bh_pass_cont.LOC AS LOCATION,
            X003bh_pass_cont.EMP AS EMPLOYEE_NUMBER,
            X003bh_pass_cont.NAME,
            X003bh_pass_cont.NAT AS NATIONALITY,
            X003bh_pass_cont.NUMB AS PASSPORT_NUMBER,
            X003bh_pass_cont.COUNT AS OCCURANCES,
            X003bh_pass_cont.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
            X003bh_pass_cont.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
            X003bh_pass_cont.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
            X003bh_pass_cont.CAMP_SUP_NAME AS SUPERVISOR,
            X003bh_pass_cont.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
            X003bh_pass_cont.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
            X003bh_pass_cont.ORG_OFF_NAME AS ORG_OFFICER,
            X003bh_pass_cont.ORG_OFF_NUMB AS ORG_OFFICER_NUMB,
            X003bh_pass_cont.ORG_OFF_MAIL AS ORG_OFFICER_MAIL,
            X003bh_pass_cont.ORG_SUP_NAME AS ORG_SUPERVISOR,
            X003bh_pass_cont.ORG_SUP_NUMB AS ORG_SUPERVISOR_NUMB,
            X003bh_pass_cont.ORG_SUP_MAIL AS ORG_SUPERVISOR_MAIL
        From
            X003bh_pass_cont
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
            print("Export findings...")
            sr_filet = sr_file
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "People_test_003bx_pass_duplicate_"
            sx_filet = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
            funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    TEST WORK PERMIT EXPIRED
    *****************************************************************************"""
    print("WORK PERMIT EXPIRED")
    funcfile.writelog("WORK PERMIT EXPIRED")

    # DECLARE TEST VARIABLES
    s_fprefix: str = "X003e"
    s_finding: str = "EMPLOYEE WORK PERMIT EXPIRED"
    s_xfile:str = "001_reported.txt"
    i_finding_after: int = 0

    # OBTAIN TEST DATA
    print("Obtain test data and add employee details...")
    sr_file: str = s_fprefix + "a_work_permit_expire"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        MASTER.ORG,
        MASTER.LOC,
        MASTER.EMP,
        MASTER.IDNO,
        MASTER.NUMB,
        MASTER.PERMIT,
        Substr(Replace(MASTER.PERMIT_EXPIRE,'/','-'),1,10) As EXPIRE_DATE,
        MASTER.POSITION,
        MASTER.ADDRESS_SARS,
        '' As VALID
    From
        X003_pass_master MASTER
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # UPDATE SELECT FIELD WITH POSSIBLE FINDINGS
    print("Update column valid...")
    s_sql = "Update " + sr_file + """
        Set VALID =
        Case
            When PERMIT Like('PRP%') Then '0 PRP Permit'
            When NUMB <> '' And IDNO <> '' Then '0 RSA ID number'
            When
                NUMB <> '' And
                POSITION Like('EXTRA%')
            Then '0 Extraordinary position'
            When
                NUMB <> '' And
                EXPIRE_DATE >= Date('1900-01-01') And
                EXPIRE_DATE < Date('%TODAY%')
            Then '1 Select with date'
            When
                NUMB <> '' And
                EXPIRE_DATE = ''
            Then '1 Select no date'        
        End;"""
    s_sql = s_sql.replace("%TODAY%", funcdate.cur_monthendnext())
    so_curs.execute(s_sql)
    so_conn.commit()

    # IDENTIFY FINDINGS
    # TOTO Delete after first run
    sr_file = s_fprefix + "b_findings"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Identify findings...")
    sr_file = s_fprefix + "b_finding"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        CURR.ORG,
        CURR.LOC,
        CURR.EMP,
        CURR.NUMB,
        CURR.PERMIT,
        CURR.EXPIRE_DATE
    From
        %FILEP%a_work_permit_expire CURR
    Where
        CURR.VALID Like('1%')
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_fprefix)
    s_sql = s_sql.replace("%TODAY%", funcdate.today())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")

    # GET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.get_previous_finding(so_curs, ed_path, s_xfile, s_finding, "ITTTT")
        so_conn.commit()

    # SET PREVIOUS FINDINGS
    if i_finding_before > 0:
        i = functest.set_previous_finding(so_curs)
        so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = s_fprefix + "d_addprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            Lower('%FINDING%') AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%b_finding FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = FIND.EMP
        ;"""
        s_sql = s_sql.replace("%FINDING%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%",  funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = s_fprefix + "e_newprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.EMP AS FIELD1,
            '' AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%d_addprev PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""        
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = s_xfile[:-4]
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                s_desc = "Work permit expired"
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_officer(so_curs, "HR", "TEST " + s_finding + " OFFICER")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        i = functest.get_supervisor(so_curs, "HR", "TEST " + s_finding + " SUPERVISOR")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = s_fprefix + "h_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.EMP,
            PEOP.NAME_LIST,
            PREV.NUMB,
            PEOP.NATIONALITY_NAME,
            PREV.PERMIT,
            PREV.EXPIRE_DATE,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            %FILEP%d_addprev PREV
            Left Join PEOPLE.X002_PEOPLE_CURR PEOP On PEOP.EMPLOYEE_NUMBER = PREV.EMP
            Left Join Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC
            Left Join Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
            Left Join Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC
            Left Join Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = s_fprefix + "x_work_permit_expire"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'EMPLOYEE WORK PERMIT EXPIRED' As Audit_finding,
            FIND.EMP AS Employee,
            FIND.NAME_LIST As Name,
            FIND.NUMB As Passport,
            FIND.NATIONALITY_NAME As Nationality,
            FIND.PERMIT As Work_permit,
            FIND.EXPIRE_DATE As Permit_expire_date,
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail            
        From
            %FILEP%h_detail FIND
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_fprefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "Peoplemaster_test_" + s_fprefix + "_" + s_finding.lower() + "_"
            sx_file_dated = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    BANK NUMBER MASTER FILE
    *****************************************************************************"""

    # BUILD TABLE WITH EMPLOYEE BANK ACCOUNT NUMBERS
    print("Obtain master list of all employees...")
    sr_file = "X004_bank_master"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' AS ORG,
        CASE LOCATION_DESCRIPTION
            WHEN 'MAFIKENG CAMPUS' THEN 'MAF'
            WHEN 'POTCHEFSTROOM CAMPUS' THEN 'POT'
            WHEN 'VAAL TRIANGLE CAMPUS' THEN 'VAA'
            ELSE 'NWU'
        END AS LOC,
        PEOP.EMPLOYEE_NUMBER AS EMP,
        PEOP.ACC_TYPE,
        PEOP.ACC_BRANCH,
        PEOP.ACC_NUMBER,
        PEOP.ACC_RELATION,
        PEOP.ACC_SARS
    From
        PEOPLE.X002_PEOPLE_CURR PEOP
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    TEST BANK NUMBER DUPLICATE
        NOTE 01: SELECT ALL CURRENT EMPLOYEES WITH BANK NUMBERS
    *****************************************************************************"""
    print("TEST BANK NUMBER DUPLICATE")
    funcfile.writelog("TEST BANK NUMBER DUPLICATE")

    # DECLARE TEST VARIABLES
    i_find = 0 # Number of findings before previous reported findings
    i_coun = 0 # Number of new findings to report

    # COUNT ALL EMPLOYEES WITH A BANK ACCOUNT NUMBER
    print("Count all employees with bank number...")
    sr_file = "X004aa_bank_coun"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X004_bank_master.ACC_NUMBER,
        Count(X004_bank_master.EMP) As COUNT
    From
        X004_bank_master
    Where
        X004_bank_master.ACC_NUMBER <> ''
    Group By
        X004_bank_master.ACC_NUMBER
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # IDENTIFY DUPLICATE ACCOUNTS
    print("Build list of duplicate accounts...")
    sr_file = "X004ab_bank_dupl"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X004_bank_master.*,
        X004aa_bank_coun.COUNT
    From
        X004_bank_master Left Join
        X004aa_bank_coun On X004aa_bank_coun.ACC_NUMBER = X004_bank_master.ACC_NUMBER
    Where
        X004aa_bank_coun.COUNT > 1
    Order by
        X004aa_bank_coun.ACC_NUMBER, EMP
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find = funcsys.tablerowcount(so_curs,sr_file)
    print("*** Found "+str(i_find)+" exceptions ***")
    funcfile.writelog("%t FINDING: "+str(i_find)+" EMPL BANK duplicate finding(s)")

    # GET PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X004ac_bank_getprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,REMARK TEXT)")
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "bank_duplicate":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the impoted data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # SET PREVIOUS FINDINGS
    sr_file = "X004ac_setprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        print("Obtain the latest previous finding...")
        s_sql = "Create Table " + sr_file + " As" + """
        Select
            GET.PROCESS,
            GET.FIELD1,
            GET.FIELD2,
            GET.FIELD3,
            GET.FIELD4,
            GET.FIELD5,
            Max(GET.DATE_REPORTED) As DATE_REPORTED,
            GET.DATE_RETEST,
            GET.REMARK
        From
            X004ac_bank_getprev GET
        Group By
            GET.FIELD1        
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD PREVIOUS FINDINGS
    sr_file = "X004ad_bank_addprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        SELECT
            X004ab_bank_dupl.*,
            'bank_duplicate' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%TODAYPLUS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        FROM
            X004ab_bank_dupl Left Join
            X004ac_setprev PREV ON PREV.FIELD1 = X004ab_bank_dupl.EMP
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X004ae_bank_newprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE "+sr_file+" AS " + """
        SELECT
            PREV.PROCESS,
            PREV.EMP AS FIELD1,
            '' AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        FROM
            X004ad_bank_addprev PREV
        WHERE
            PREV.PREV_PROCESS IS NULL Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: "+sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(so_curs,sr_file)
        if i_coun > 0:
            print("*** " +str(i_coun)+ " Finding(s) to report ***")    
            sr_filet = sr_file
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
                funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
                funcfile.writelog("%t EXPORT DATA: "+sr_file)
            if l_mess:
                s_desc = "Bank acc number duplicate"
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")    

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X004af_offi"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_BANKACC_DUPL_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X004ag_supe"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_BANKACC_DUPL_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X004ah_bank_cont"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            X004ad_bank_addprev.ORG,
            X004ad_bank_addprev.LOC,
            X004ad_bank_addprev.EMP,
            X004ad_bank_addprev.ACC_TYPE,
            X004ad_bank_addprev.ACC_BRANCH,
            X004ad_bank_addprev.ACC_NUMBER,
            X004ad_bank_addprev.ACC_RELATION,
            X004ad_bank_addprev.COUNT,
            PEOPLE.X002_PEOPLE_CURR.NAME_LIST AS NAME,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X004ad_bank_addprev
            Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X004ad_bank_addprev.EMP
            Left Join X004af_offi CAMP_OFF On CAMP_OFF.CAMPUS = X004ad_bank_addprev.LOC
            Left Join X004af_offi ORG_OFF On ORG_OFF.CAMPUS = X004ad_bank_addprev.ORG
            Left Join X004ag_supe CAMP_SUP On CAMP_SUP.CAMPUS = X004ad_bank_addprev.LOC
            Left Join X004ag_supe ORG_SUP On ORG_SUP.CAMPUS = X004ad_bank_addprev.ORG
        WHERE
          X004ad_bank_addprev.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X004ax_bank_duplicate"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    print("Build the final report")
    if i_find > 0 and i_coun > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'BANK ACC DUPLICATE' As AUDIT_FINDING,
            X004ah_bank_cont.ORG AS ORGANIZATION,
            X004ah_bank_cont.LOC AS LOCATION,
            X004ah_bank_cont.EMP AS EMPLOYEE_NUMBER,
            X004ah_bank_cont.NAME,
            X004ah_bank_cont.ACC_TYPE,
            X004ah_bank_cont.ACC_BRANCH,
            X004ah_bank_cont.ACC_NUMBER,
            X004ah_bank_cont.ACC_RELATION,
            X004ah_bank_cont.COUNT AS OCCURANCES,
            X004ah_bank_cont.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
            X004ah_bank_cont.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
            X004ah_bank_cont.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
            X004ah_bank_cont.CAMP_SUP_NAME AS SUPERVISOR,
            X004ah_bank_cont.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
            X004ah_bank_cont.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
            X004ah_bank_cont.ORG_OFF_NAME AS ORG_OFFICER,
            X004ah_bank_cont.ORG_OFF_NUMB AS ORG_OFFICER_NUMB,
            X004ah_bank_cont.ORG_OFF_MAIL AS ORG_OFFICER_MAIL,
            X004ah_bank_cont.ORG_SUP_NAME AS ORG_SUPERVISOR,
            X004ah_bank_cont.ORG_SUP_NUMB AS ORG_SUPERVISOR_NUMB,
            X004ah_bank_cont.ORG_SUP_MAIL AS ORG_SUPERVISOR_MAIL
        From
            X004ah_bank_cont
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
            print("Export findings...")
            sr_filet = sr_file
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "People_test_004ax_bank_duplicate_"
            sx_filet = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
            funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    TEST BANK SARS VERIFICATION
    *****************************************************************************"""
    print("BANK SARS VERIFICATION")
    funcfile.writelog("BANK SARS VERIFICATION")

    # DECLARE TEST VARIABLES
    i_finding_after: int = 0

    # OBTAIN TEST DATA
    print("Obtain test data and add employee details...")
    sr_file: str = "X004ca_bank_sars_verify"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' AS ORG,
        CASE PEOP.LOCATION_DESCRIPTION
            WHEN 'MAFIKENG CAMPUS' THEN 'MAF'
            WHEN 'POTCHEFSTROOM CAMPUS' THEN 'POT'
            WHEN 'VAAL TRIANGLE CAMPUS' THEN 'VAA'
            ELSE 'NWU'
        END AS LOC,
        BANK.EMP,
        BANK.ACC_NUMBER,
        BANK.ACC_SARS
    From
        X004_bank_master BANK Left Join
        PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = BANK.EMP
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X004cb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        CURR.ORG,
        CURR.LOC,
        CURR.EMP,
        CURR.ACC_NUMBER,
        CURR.ACC_SARS
    From
        X004ca_bank_sars_verify CURR
    Where
        CURR.ACC_NUMBER <> '' And
        CURR.ACC_SARS <> 'Y'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " EMPL BANK SARS verify finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X004cc_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 INT,
            FIELD2 TEXT,
            FIELD3 TEXT,
            FIELD4 TEXT,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            REMARK TEXT)
            """)
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "bank_sars_verify":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + \
                         row[
                             3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[
                             8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the imported data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    sr_file = "X004cd_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'bank_sars_verify' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            X004cb_findings FIND
            LEFT JOIN X004cc_get_previous PREV ON PREV.FIELD1 = FIND.EMP AND
                PREV.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X004ce_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.EMP AS FIELD1,
            '' AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            X004cd_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                s_desc = "Bank acc SARS verification"
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X004cf_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if i_finding_after > 0:
            print("Import reporting officers for mail purposes...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                OFFICER.LOOKUP,
                OFFICER.LOOKUP_CODE AS CAMPUS,
                OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
                PEOP.NAME_ADDR As NAME,
                PEOP.EMAIL_ADDRESS
            From
                PEOPLE.X000_OWN_HR_LOOKUPS OFFICER Left Join
                PEOPLE.X002_PEOPLE_CURR PEOP ON
                    PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'TEST_BANKACC_SARS_VERIFY_OFFICER'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X004cg_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            SUPERVISOR.LOOKUP,
            SUPERVISOR.LOOKUP_CODE AS CAMPUS,
            SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            PEOPLE.X000_OWN_HR_LOOKUPS SUPERVISOR Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON 
                PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'TEST_BANKACC_SARS_VERIFY_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X004ch_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.EMP,
            PEOP.NAME_LIST,
            PEOP.ACC_TYPE,
            PEOP.ACC_BRANCH,
            PREV.ACC_NUMBER,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X004cd_add_previous PREV
            Left Join PEOPLE.X002_PEOPLE_CURR PEOP On PEOP.EMPLOYEE_NUMBER = PREV.EMP
            Left Join X004cf_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC
            Left Join X004cf_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
            Left Join X004cg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC
            Left Join X004cg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X004cx_bank_sars_verify"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'EMPLOYEE BANKACC SARS FLAG INVALID' As Audit_finding,
            FIND.EMP AS Employee,
            FIND.NAME_LIST As Name,
            FIND.ACC_TYPE As Bank_type,
            FIND.ACC_BRANCH As Bank_branch,
            FIND.ACC_NUMBER As Bank_number,  
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail            
            
        From
            X004ch_detail FIND
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "People_test_004cx_bank_sars_verify_"
            sx_file_dated = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    BANK CHANGE MASTER FILE
    *****************************************************************************"""

    # BUILD TABLE WITH BANK ACCOUNT CHANGES FOR YESTERDAY (FRIDAY IF MONDAY)
    print("Obtain master list of all bank changes...")
    sr_file = "X004_bank_change"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
        PEOP.ASSIGNMENT_ID,
        PEOPLE.X001_ASSIGNMENT_CURR.EMPLOYEE_NUMBER,
        PEOP.EFFECTIVE_START_DATE,
        PEOP.EFFECTIVE_END_DATE,
        PEOP.PERSONAL_PAYMENT_METHOD_ID,
        PEOP.BUSINESS_GROUP_ID,
        PEOP.ORG_PAYMENT_METHOD_ID,
        PEOP.PPM_INFORMATION_CATEGORY,
        PEOP.PPM_INFORMATION1,
        PEOP.CREATION_DATE,
        PEOP.CREATED_BY,
        PEOP.LAST_UPDATE_DATE,
        PEOP.LAST_UPDATED_BY,
        PEOP.EXTERNAL_ACCOUNT_ID,
        PEOP.TERRITORY_CODE,
        PEOP.ACC_BRANCH,
        PEOP.ACC_TYPE_CODE,
        PEOP.ACC_TYPE,
        PEOP.ACC_NUMBER,
        PEOP.ACC_HOLDER,
        PEOP.ACC_UNKNOWN,
        PEOP.ACC_RELATION_CODE,
        PEOP.ACC_RELATION    
    FROM
        PEOPLE.X000_PAY_ACCOUNTS PEOP LEFT JOIN
        PEOPLE.X001_ASSIGNMENT_CURR ON PEOPLE.X001_ASSIGNMENT_CURR.ASS_ID = PEOP.ASSIGNMENT_ID AND
            StrfTime('%Y-%m-%d',PEOPLE.X001_ASSIGNMENT_CURR.ASS_START) <= StrfTime('%Y-%m-%d',PEOP.LAST_UPDATE_DATE) AND
            StrfTime('%Y-%m-%d',PEOPLE.X001_ASSIGNMENT_CURR.ASS_END) >= StrfTime('%Y-%m-%d',PEOP.LAST_UPDATE_DATE)
    WHERE
        %WHERE%
    ORDER BY
        ASSIGNMENT_ID,
        LAST_UPDATE_DATE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if funcdate.today_dayname() == "Mon":
        s_sql = s_sql.replace("%WHERE%",
                              "StrfTime('%Y-%m-%d',PEOP.LAST_UPDATE_DATE)>=StrfTime('%Y-%m-%d','now','-3 day') AND StrfTime('%Y-%m-%d',PEOP.CREATION_DATE)<StrfTime('%Y-%m-%d','now','-3 day')")
    else:
        s_sql = s_sql.replace("%WHERE%",
                              "StrfTime('%Y-%m-%d',PEOP.LAST_UPDATE_DATE)>=StrfTime('%Y-%m-%d','now','-1 day') AND StrfTime('%Y-%m-%d',PEOP.CREATION_DATE)<StrfTime('%Y-%m-%d','now','-1 day')")
    # print(s_sql) # DEBUG
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # GET PREVIOUS BANK ACCOUNTS
    sr_file = "X004_bank_master_prev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    print("Import previous employee banks...")
    so_curs.execute("CREATE TABLE " + sr_file + "(ORG TEXT, LOC TEXT, EMP TEXT, ACC_TYPE TEXT, ACC_BRANCH TEXT, ACC_NUMBER TEXT, ACC_RELATION TEXT)")
    s_cols = ""
    co = open(ed_path + "001_employee_bank.csv", "r")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "ORG":
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the impoted data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_employee_bank.csv (" + sr_file + ")")

    # EXPORT THE PREVIOUS BANK DETAILS
    print("Export previous bank details...")
    sr_filet = "X004_bank_master_prev"
    sx_path = ed_path
    sx_file = "001_employee_bank_prev"
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    # EXPORT THE CURRENT BANK DETAILS
    print("Export current bank details...")
    sr_filet = "X004_bank_master"
    sx_path = ed_path
    sx_file = "001_employee_bank"
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    """ ****************************************************************************
    BANK CHANGE VERIFICATION
    *****************************************************************************"""
    print("BANK CHANGE VERIFICATION")
    funcfile.writelog("BANK CHANGE VERIFICATION")

    # ADD EMPLOYEE DETAILS
    print("Add employee details...")
    sr_file: str = "X004ba_bank_verify"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' AS ORG,
        CASE PEOP.LOCATION_DESCRIPTION
            WHEN 'MAFIKENG CAMPUS' THEN 'MAF'
            WHEN 'POTCHEFSTROOM CAMPUS' THEN 'POT'
            WHEN 'VAAL TRIANGLE CAMPUS' THEN 'VAA'
            ELSE 'NWU'
        END AS LOC,
        BANK.EMPLOYEE_NUMBER AS EMP,
        PEOP.NAME_ADDR AS NAME,
        BANK.EFFECTIVE_START_DATE AS START_DATE,
        BANK.EFFECTIVE_END_DATE AS END_DATE,
        BANK.ACC_TYPE,
        BANK.ACC_BRANCH,
        BANK.ACC_NUMBER,
        BANK.LAST_UPDATE_DATE AS UPDATE_DATE,
        BANK.LAST_UPDATED_BY AS UPDATE_BY
    From
        X004_bank_change BANK Left Join
        PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = BANK.EMPLOYEE_NUMBER
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # FILTER RECORDS TO REPORT - ONLY EMPLOYEES ACTIVE TODAY
    print("Filter employee records...")
    sr_file = "X004bb_bank_verify"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        CURR.ORG,
        CURR.LOC,
        CURR.EMP,
        CURR.ACC_TYPE,
        CURR.ACC_BRANCH,
        CURR.ACC_NUMBER,
        CURR.UPDATE_DATE,
        CURR.UPDATE_BY,
        OLD.ACC_BRANCH As OLD_ACC_BRANCH,
        OLD.ACC_NUMBER As OLD_ACC_NUMBER
    From
        X004ba_bank_verify CURR Inner Join
        X004_bank_master_prev OLD On OLD.EMP = CURR.EMP
    Where
        CURR.NAME <> '' And
        StrfTime('%Y-%m-%d',CURR.END_DATE) >= StrfTime('%Y-%m-%d','now') And
        CURR.ACC_NUMBER <> OLD.ACC_NUMBER
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " EMPL BANK change finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X004bc_bank_getprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 INT,
            FIELD2 TEXT,
            FIELD3 TEXT,
            FIELD4 TEXT,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            REMARK TEXT)
            """)
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "bank_change":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[
                    3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the imported data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    sr_file = "X004bd_bank_addprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        SELECT
          X004bb_bank_verify.*,
          'bank_change' AS PROCESS,
          '%TODAY%' AS DATE_REPORTED,
          '%TODAYPLUS%' AS DATE_RETEST,
          X004bc_bank_getprev.PROCESS AS PREV_PROCESS,
          X004bc_bank_getprev.DATE_REPORTED AS PREV_DATE_REPORTED,
          X004bc_bank_getprev.DATE_RETEST AS PREV_DATE_RETEST,
          X004bc_bank_getprev.REMARK
        FROM
          X004bb_bank_verify
          LEFT JOIN X004bc_bank_getprev ON X004bc_bank_getprev.FIELD1 = X004bb_bank_verify.EMP AND
              X004bc_bank_getprev.FIELD2 = X004bb_bank_verify.ACC_NUMBER AND
              X004bc_bank_getprev.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X004be_bank_newprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          X004bd_bank_addprev.PROCESS,
          X004bd_bank_addprev.EMP AS FIELD1,
          X004bd_bank_addprev.ACC_NUMBER AS FIELD2,
          '' AS FIELD3,
          '' AS FIELD4,
          '' AS FIELD5,
          X004bd_bank_addprev.DATE_REPORTED,
          X004bd_bank_addprev.DATE_RETEST,
          X004bd_bank_addprev.REMARK
        FROM
          X004bd_bank_addprev
        WHERE
          X004bd_bank_addprev.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after: int = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                s_desc = "Bank acc change verification"
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

        # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X004bf_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
            PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
            PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
            PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
            PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
            PEOPLE.X000_OWN_HR_LOOKUPS
            LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON
            PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
            PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_BANKACC_VERIFY_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X004bg_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
            PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
            PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
            PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
            PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
            PEOPLE.X000_OWN_HR_LOOKUPS
            LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON 
            PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_BANKACC_VERIFY_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X004bh_bank_cont"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            X004bd_bank_addprev.ORG,
            X004bd_bank_addprev.LOC,
            X004bd_bank_addprev.EMP,
            PEOPLE.X002_PEOPLE_CURR.NAME_ADDR AS NAME,
            X004bd_bank_addprev.ACC_TYPE,
            X004bd_bank_addprev.ACC_BRANCH,
            X004bd_bank_addprev.ACC_NUMBER,
            X004bd_bank_addprev.OLD_ACC_BRANCH,
            X004bd_bank_addprev.OLD_ACC_NUMBER,
            Trim(X004bd_bank_addprev.EMP)||'@nwu.ac.za' As MAIL,
            Case
                When Trim(X004bd_bank_addprev.EMP)||'@nwu.ac.za' <> Trim(PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS) Then Trim(PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS)
                Else ''
            End As MAIL2,
            PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS AS MAIL,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL,
            X004bd_bank_addprev.UPDATE_DATE,
            X004bd_bank_addprev.UPDATE_BY,
            PEOPLE.X000_USER_CURR.EMPLOYEE_NUMBER AS UPDATE_EMP,
            PEOPLE.X000_USER_CURR.KNOWN_NAME AS UPDATE_NAME,
            PEOPLE.X000_USER_CURR.EMAIL_ADDRESS AS UPDATE_MAIL
        From
            X004bd_bank_addprev
            Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X004bd_bank_addprev.EMP
            Left Join PEOPLE.X000_USER_CURR On PEOPLE.X000_USER_CURR.USER_ID = X004bd_bank_addprev.UPDATE_BY
            Left Join X004bf_officer CAMP_OFF On CAMP_OFF.CAMPUS = X004bd_bank_addprev.LOC
            Left Join X004bf_officer ORG_OFF On ORG_OFF.CAMPUS = X004bd_bank_addprev.ORG
            Left Join X004bg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = X004bd_bank_addprev.LOC
            Left Join X004bg_supervisor ORG_SUP On ORG_SUP.CAMPUS = X004bd_bank_addprev.ORG
        WHERE
          X004bd_bank_addprev.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X004bx_bank_verify"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'EMPLOYEE BANK CHANGE VERIFICATION' As FINDING,
            X004bh_bank_cont.EMP AS EMPLOYEE_NUMBER,
            X004bh_bank_cont.NAME,
            X004bh_bank_cont.ACC_TYPE,
            X004bh_bank_cont.ACC_BRANCH,
            X004bh_bank_cont.ACC_NUMBER,
            X004bh_bank_cont.OLD_ACC_BRANCH,
            X004bh_bank_cont.OLD_ACC_NUMBER,
            X004bh_bank_cont.MAIL,
            X004bh_bank_cont.MAIL2,        
            X004bh_bank_cont.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
            X004bh_bank_cont.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
            X004bh_bank_cont.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
            X004bh_bank_cont.CAMP_SUP_NAME AS SUPERVISOR,
            X004bh_bank_cont.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
            X004bh_bank_cont.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
            X004bh_bank_cont.ORG_OFF_NAME AS ORGANIZATION_OFFICER,
            X004bh_bank_cont.ORG_OFF_NUMB AS ORGANIZATION_OFFICER_NUMB,
            X004bh_bank_cont.ORG_OFF_MAIL AS ORGANIZATION_OFFICER_MAIL,
            X004bh_bank_cont.ORG_SUP_NAME AS ORGANIZATION_SUPERVISOR,
            X004bh_bank_cont.ORG_SUP_NUMB AS ORGANIZATION_SUPERVISOR_NUMB,
            X004bh_bank_cont.ORG_SUP_MAIL AS ORGANIZATION_SUPERVISOR_MAIL,
            X004bh_bank_cont.UPDATE_DATE,
            X004bh_bank_cont.UPDATE_BY,
            X004bh_bank_cont.UPDATE_EMP,
            X004bh_bank_cont.UPDATE_NAME,
            X004bh_bank_cont.UPDATE_MAIL
        From
            X004bh_bank_cont
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "People_test_004bx_bank_verify_"
            sx_file_dated = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    TAX NUMBER MASTER FILE
    *****************************************************************************"""

    # BUILD TABLE WITH EMPLOYEE TAX NUMBERS
    print("Obtain master list of all employees...")
    sr_file = "X005_paye_master"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        'NWU' AS ORG,
        CASE LOCATION_DESCRIPTION
            WHEN 'MAFIKENG CAMPUS' THEN 'MAF'
            WHEN 'POTCHEFSTROOM CAMPUS' THEN 'POT'
            WHEN 'VAAL TRIANGLE CAMPUS' THEN 'VAA'
            ELSE 'NWU'
        END AS LOC,
        PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER AS EMP,
        PEOPLE.X002_PEOPLE_CURR.TAX_NUMBER AS NUMB,
        PEOPLE.X002_PEOPLE_CURR.NATIONALITY AS NAT,
        UPPER(PEOPLE.X002_PEOPLE_CURR.PERSON_TYPE) AS TYPE,
        PEOPLE.X002_PEOPLE_CURR.EMP_START AS DSTA
    From
        PEOPLE.X002_PEOPLE_CURR
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    """ ****************************************************************************
    TEST TAX NUMBER BLANK
        01 Test only include SAF (Nationality) and PERMANENT APPOINTMENT (Person_type)
        02 Test only include appointments older than a month
        03 Ignore   AD HOC APPOINTMENT
                    COUNCIL MEMBER
                    EXTRAORDINARY APPOINTMENT
                    FACILITATOR
                    STUDENT ASSISTANT
                    TEMP FIXED TERM CONTRACT
                    TEMPORARY APPOINTMENT
    *****************************************************************************"""
    print("TEST TAX NUMBER BLANK")
    funcfile.writelog("TEST TAX NUMBER BLANK")

    # DECLARE TEST VARIABLES
    i_find = 0 # Number of findings before previous reported findings
    i_coun = 0 # Number of new findings to report

    # SELECT ALL EMPLOYEES WITHOUT A TAX NUMBER
    print("Select all employees with tax number...")
    sr_file = "X005aa_paye_blank"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X005_paye_master.ORG,
        X005_paye_master.LOC,
        X005_paye_master.EMP
    From
        X005_paye_master
    Where
        X005_paye_master.NUMB = '' AND
        X005_paye_master.NAT = 'SAF' AND
        X005_paye_master.TYPE = 'PERMANENT APPOINTMENT' AND
        X005_paye_master.DSTA <= StrfTime('%Y-%m-%d','now','-1 month')    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # SELECT ALL EMPLOYEES WITHOUT A TAX NUMBER
    print("Select all employees with a blank tax number...")
    sr_file = "X005ab_paye_blank"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X005aa_paye_blank.*
    From
        X005aa_paye_blank
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find = funcsys.tablerowcount(so_curs,sr_file)
    print("*** Found "+str(i_find)+" exceptions ***")
    funcfile.writelog("%t FINDING: "+str(i_find)+" EMPL TAX NUMBER blank finding(s)")

    # GET PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X005ac_paye_getprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,REMARK TEXT)")
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "paye_blank":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the impoted data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X005ad_paye_addprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        SELECT
          X005ab_paye_blank.*,
          'paye_blank' AS PROCESS,
          '%TODAY%' AS DATE_REPORTED,
          '%TODAYPLUS%' AS DATE_RETEST,
          X005ac_paye_getprev.PROCESS AS PREV_PROCESS,
          X005ac_paye_getprev.DATE_REPORTED AS PREV_DATE_REPORTED,
          X005ac_paye_getprev.DATE_RETEST AS PREV_DATE_RETEST,
          X005ac_paye_getprev.REMARK
        FROM
          X005ab_paye_blank
          LEFT JOIN X005ac_paye_getprev ON X005ac_paye_getprev.FIELD1 = X005ab_paye_blank.EMP AND
              X005ac_paye_getprev.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X005ae_paye_newprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE "+sr_file+" AS " + """
        SELECT
          X005ad_paye_addprev.PROCESS,
          X005ad_paye_addprev.EMP AS FIELD1,
          '' AS FIELD2,
          '' AS FIELD3,
          '' AS FIELD4,
          '' AS FIELD5,
          X005ad_paye_addprev.DATE_REPORTED,
          X005ad_paye_addprev.DATE_RETEST,
          X005ad_paye_addprev.REMARK
        FROM
          X005ad_paye_addprev
        WHERE
          X005ad_paye_addprev.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: "+sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(so_curs,sr_file)
        if i_coun > 0:
            print("*** " +str(i_coun)+ " Finding(s) to report ***")    
            sr_filet = sr_file
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
                funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
                funcfile.writelog("%t EXPORT DATA: "+sr_file)
            if l_mess:
                s_desc = "Tax number blank"
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X005af_offi"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_TAXNUMBER_BLANK_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X005ag_supe"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_TAXNUMBER_BLANK_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X005ah_paye_cont"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            X005ad_paye_addprev.ORG,
            X005ad_paye_addprev.LOC,
            X005ad_paye_addprev.EMP,
            PEOPLE.X002_PEOPLE_CURR.NAME_LIST AS NAME,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X005ad_paye_addprev
            Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X005ad_paye_addprev.EMP
            Left Join X005af_offi CAMP_OFF On CAMP_OFF.CAMPUS = X005ad_paye_addprev.LOC
            Left Join X005af_offi ORG_OFF On ORG_OFF.CAMPUS = X005ad_paye_addprev.ORG
            Left Join X005ag_supe CAMP_SUP On CAMP_SUP.CAMPUS = X005ad_paye_addprev.LOC
            Left Join X005ag_supe ORG_SUP On ORG_SUP.CAMPUS = X005ad_paye_addprev.ORG
        WHERE
          X005ad_paye_addprev.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X005ax_paye_blank"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    print("Build the final report")
    if i_find > 0 and i_coun > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'TAX NUMBER BLANK' As AUDIT_FINDING,
            X005ah_paye_cont.ORG AS ORGANIZATION,
            X005ah_paye_cont.LOC AS LOCATION,
            X005ah_paye_cont.EMP AS EMPLOYEE_NUMBER,
            X005ah_paye_cont.NAME,
            X005ah_paye_cont.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
            X005ah_paye_cont.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
            X005ah_paye_cont.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
            X005ah_paye_cont.CAMP_SUP_NAME AS SUPERVISOR,
            X005ah_paye_cont.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
            X005ah_paye_cont.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
            X005ah_paye_cont.ORG_OFF_NAME AS ORGANIZATION_OFFICER,
            X005ah_paye_cont.ORG_OFF_NUMB AS ORGANIZATION_OFFICER_NUMB,
            X005ah_paye_cont.ORG_OFF_MAIL AS ORGANIZATION_OFFICER_MAIL,
            X005ah_paye_cont.ORG_SUP_NAME AS ORGANIZATION_SUPERVISOR,
            X005ah_paye_cont.ORG_SUP_NUMB AS ORGANIZATION_SUPERVISOR_NUMB,
            X005ah_paye_cont.ORG_SUP_MAIL AS ORGANIZATION_SUPERVISOR_MAIL
        From
            X005ah_paye_cont
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
            print("Export findings...")
            sr_filet = sr_file
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "People_test_005ax_paye_blank_"
            sx_filet = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
            funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    TEST TAX NUMBER INVALID
        NOTE 01: SELECT ALL CURRENT EMPLOYEES WITH TAX TAX NUMBER
    *****************************************************************************"""
    print("TEST TAX NUMBER INVALID")
    funcfile.writelog("TEST TAX NUMBER INVALID")

    # DECLARE TEST VARIABLES
    i_find = 0 # Number of findings before previous reported findings
    i_coun = 0 # Number of new findings to report

    # SELECT ALL EMPLOYEES WITH A TAX TAX NUMBER
    print("Select all employees with tax number...")
    sr_file = "X005ba_paye_calc"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X005_paye_master.ORG,
        X005_paye_master.LOC,
        X005_paye_master.EMP,
        X005_paye_master.NUMB,
        CASE
            WHEN SUBSTR(NUMB,1,1)*2 > 0 THEN SUBSTR(SUBSTR(NUMB,1,1)*2,1,1)+SUBSTR(SUBSTR(NUMB,1,1)*2,2,1)
            ELSE SUBSTR(NUMB,1,1)*2
        END AS CD1,
        CASE
            WHEN SUBSTR(NUMB,3,1)*2 > 0 THEN SUBSTR(SUBSTR(NUMB,3,1)*2,1,1)+SUBSTR(SUBSTR(NUMB,3,1)*2,2,1)
            ELSE SUBSTR(NUMB,3,1)*2
        END AS CD3,
        CASE
            WHEN SUBSTR(NUMB,5,1)*2 > 0 THEN SUBSTR(SUBSTR(NUMB,5,1)*2,1,1)+SUBSTR(SUBSTR(NUMB,5,1)*2,2,1)
            ELSE SUBSTR(NUMB,5,1)*2
        END AS CD5,
        CASE
            WHEN SUBSTR(NUMB,7,1)*2 > 0 THEN SUBSTR(SUBSTR(NUMB,7,1)*2,1,1)+SUBSTR(SUBSTR(NUMB,7,1)*2,2,1)
            ELSE SUBSTR(NUMB,7,1)*2
        END AS CD7,
        CASE
            WHEN SUBSTR(NUMB,9,1)*2 > 0 THEN SUBSTR(SUBSTR(NUMB,9,1)*2,1,1)+SUBSTR(SUBSTR(NUMB,9,1)*2,2,1)
            ELSE SUBSTR(NUMB,9,1)*2
        END AS CD9,
        0 AS TOT1,
        SUBSTR(NUMB,2,1)+SUBSTR(NUMB,4,1)+SUBSTR(NUMB,6,1)+SUBSTR(NUMB,8,1) AS TOT2,
        '' AS CONT,
        '' AS VAL
    From
        X005_paye_master
    Where
        X005_paye_master.NUMB <> ''
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
    print("Update calculation columns...")
    so_curs.execute("UPDATE X005ba_paye_calc SET TOT1 = CD1+CD3+CD5+CD7+CD9+TOT2;")
    so_conn.commit()
    so_curs.execute("UPDATE X005ba_paye_calc " + """
                     SET CONT =
                     CASE
                         WHEN ABS(SUBSTR(TOT1,-1,1)) = 0 THEN 0
                         WHEN ABS(SUBSTR(TOT1,-1,1)) > 0 THEN 10 - SUBSTR(TOT1,-1,1)
                         ELSE 0
                     END;""")
    so_conn.commit()
    so_curs.execute("UPDATE X005ba_paye_calc " + """
                     SET VAL =
                     CASE
                         WHEN LENGTH(TRIM(NUMB)) <> 10 THEN 'F'
                         WHEN ABS(SUBSTR(NUMB,10)) = ABS(CONT) THEN 'T'
                         ELSE 'F'
                     END;""")
    so_conn.commit()

    # SELECT ALL EMPLOYEES WITH AN INVALID TAX NUMBER
    print("Select all employees with an invalid tax number...")
    sr_file = "X005bb_paye_inva"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X005ba_paye_calc.ORG,
        X005ba_paye_calc.LOC,
        X005ba_paye_calc.EMP,
        X005ba_paye_calc.NUMB
    From
        X005ba_paye_calc
    Where
        X005ba_paye_calc.VAL = 'F'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS X005ba_numb_calc")
    so_curs.execute("DROP TABLE IF EXISTS X005ba_tax_invalid")
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find = funcsys.tablerowcount(so_curs,sr_file)
    print("*** Found "+str(i_find)+" exceptions ***")
    funcfile.writelog("%t FINDING: "+str(i_find)+" EMPL TAX NUMBER invalid finding(s)")

    # GET PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X005bc_paye_getprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,REMARK TEXT)")
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "paye_invalid":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the impoted data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X005bd_paye_addprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        SELECT
          X005bb_paye_inva.*,
          'paye_invalid' AS PROCESS,
          '%TODAY%' AS DATE_REPORTED,
          '%TODAYPLUS%' AS DATE_RETEST,
          X005bc_paye_getprev.PROCESS AS PREV_PROCESS,
          X005bc_paye_getprev.DATE_REPORTED AS PREV_DATE_REPORTED,
          X005bc_paye_getprev.DATE_RETEST AS PREV_DATE_RETEST,
          X005bc_paye_getprev.REMARK
        FROM
          X005bb_paye_inva
          LEFT JOIN X005bc_paye_getprev ON X005bc_paye_getprev.FIELD1 = X005bb_paye_inva.EMP AND
              X005bc_paye_getprev.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X005be_paye_newprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE "+sr_file+" AS " + """
        SELECT
          X005bd_paye_addprev.PROCESS,
          X005bd_paye_addprev.EMP AS FIELD1,
          '' AS FIELD2,
          '' AS FIELD3,
          '' AS FIELD4,
          '' AS FIELD5,
          X005bd_paye_addprev.DATE_REPORTED,
          X005bd_paye_addprev.DATE_RETEST,
          X005bd_paye_addprev.REMARK
        FROM
          X005bd_paye_addprev
        WHERE
          X005bd_paye_addprev.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: "+sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(so_curs,sr_file)
        if i_coun > 0:
            print("*** " +str(i_coun)+ " Finding(s) to report ***")    
            sr_filet = sr_file
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
                funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
                funcfile.writelog("%t EXPORT DATA: "+sr_file)
            if l_mess:
                s_desc = "Tax number invalid"
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X005bf_offi"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_TAXNUMBER_INVALID_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X005bg_supe"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_TAXNUMBER_INVALID_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X005bh_paye_cont"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            X005bd_paye_addprev.ORG,
            X005bd_paye_addprev.LOC,
            X005bd_paye_addprev.EMP,
            PEOPLE.X002_PEOPLE_CURR.NAME_LIST AS NAME,
            X005bd_paye_addprev.NUMB,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X005bd_paye_addprev
            Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X005bd_paye_addprev.EMP
            Left Join X005bf_offi CAMP_OFF On CAMP_OFF.CAMPUS = X005bd_paye_addprev.LOC
            Left Join X005bf_offi ORG_OFF On ORG_OFF.CAMPUS = X005bd_paye_addprev.ORG
            Left Join X005bg_supe CAMP_SUP On CAMP_SUP.CAMPUS = X005bd_paye_addprev.LOC
            Left Join X005bg_supe ORG_SUP On ORG_SUP.CAMPUS = X005bd_paye_addprev.ORG
        WHERE
          X005bd_paye_addprev.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X005bx_paye_invalid"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute("DROP TABLE IF EXISTS X005bx_paye_cont")
    if i_find > 0 and i_coun > 0:
        print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'TAX NUMBER INVALID' As AUDIT_FINDING,
            X005bh_paye_cont.ORG AS ORGANIZATION,
            X005bh_paye_cont.LOC AS LOCATION,
            X005bh_paye_cont.EMP AS EMPLOYEE_NUMBER,
            X005bh_paye_cont.NAME,
            X005bh_paye_cont.NUMB AS PAYE_NUMBER,
            X005bh_paye_cont.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
            X005bh_paye_cont.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
            X005bh_paye_cont.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
            X005bh_paye_cont.CAMP_SUP_NAME AS SUPERVISOR,
            X005bh_paye_cont.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
            X005bh_paye_cont.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
            X005bh_paye_cont.ORG_OFF_NAME AS ORG_OFFICER,
            X005bh_paye_cont.ORG_OFF_NUMB AS ORG_OFFICER_NUMB,
            X005bh_paye_cont.ORG_OFF_MAIL AS ORG_OFFICER_MAIL,
            X005bh_paye_cont.ORG_SUP_NAME AS ORG_SUPERVISOR,
            X005bh_paye_cont.ORG_SUP_NUMB AS ORG_SUPERVISOR_NUMB,
            X005bh_paye_cont.ORG_SUP_MAIL AS ORG_SUPERVISOR_MAIL
        From
            X005bh_paye_cont
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
            print("Export findings...")
            sr_filet = sr_file
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "People_test_005bx_paye_invalid_"
            sx_filet = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
            funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)        
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    TEST TAX NUMBER DUPLICATE
    *****************************************************************************"""
    print("TEST TAX NUMBER DUPLICATE")
    funcfile.writelog("TEST TAX NUMBER DUPLICATE")

    # DECLARE TEST VARIABLES
    i_find = 0 # Number of findings before previous reported findings
    i_coun = 0 # Number of new findings to report

    # COUNT ALL EMPLOYEES WITH A BANK ACCOUNT NUMBER
    print("Count all employees with tax number...")
    sr_file = "X005ca_paye_coun"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X005_paye_master.NUMB,
        Count(X005_paye_master.EMP) As COUNT
    From
        X005_paye_master
    Where
        X005_paye_master.NUMB <> ''
    Group By
        X005_paye_master.NUMB
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # IDENTIFY DUPLICATE ACCOUNTS
    print("Build list of duplicate accounts...")
    sr_file = "X005cb_paye_dupl"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X005_paye_master.*,
        X005ca_paye_coun.COUNT
    From
        X005_paye_master Left Join
        X005ca_paye_coun On X005ca_paye_coun.NUMB = X005_paye_master.NUMB
    Where
        X005ca_paye_coun.COUNT > 1
    Order by
        X005ca_paye_coun.NUMB, EMP
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find = funcsys.tablerowcount(so_curs,sr_file)
    print("*** Found "+str(i_find)+" exceptions ***")
    funcfile.writelog("%t FINDING: "+str(i_find)+" EMPL TAX NUMBER duplicate finding(s)")

    # GET PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X005cc_paye_getprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,REMARK TEXT)")
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "paye_duplicate":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the impoted data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X005cd_paye_addprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        SELECT
          X005cb_paye_dupl.*,
          'paye_duplicate' AS PROCESS,
          '%TODAY%' AS DATE_REPORTED,
          '%TODAYPLUS%' AS DATE_RETEST,
          X005cc_paye_getprev.PROCESS AS PREV_PROCESS,
          X005cc_paye_getprev.DATE_REPORTED AS PREV_DATE_REPORTED,
          X005cc_paye_getprev.DATE_RETEST AS PREV_DATE_RETEST,
          X005cc_paye_getprev.REMARK
        FROM
          X005cb_paye_dupl
          LEFT JOIN X005cc_paye_getprev ON X005cc_paye_getprev.FIELD1 = X005cb_paye_dupl.EMP AND
              X005cc_paye_getprev.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X005ce_paye_newprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE "+sr_file+" AS " + """
        SELECT
          X005cd_paye_addprev.PROCESS,
          X005cd_paye_addprev.EMP AS FIELD1,
          '' AS FIELD2,
          '' AS FIELD3,
          '' AS FIELD4,
          '' AS FIELD5,
          X005cd_paye_addprev.DATE_REPORTED,
          X005cd_paye_addprev.DATE_RETEST,
          X005cd_paye_addprev.REMARK
        FROM
          X005cd_paye_addprev
        WHERE
          X005cd_paye_addprev.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: "+sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(so_curs,sr_file)
        if i_coun > 0:
            print("*** " +str(i_coun)+ " Finding(s) to report ***")    
            sr_filet = sr_file
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
                funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
                funcfile.writelog("%t EXPORT DATA: "+sr_file)
            if l_mess:
                s_desc = "Tax number duplicate"
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X005cf_offi"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_TAXNUMBER_DUPLICATE_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X005cg_supe"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_TAXNUMBER_DUPLICATE_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X005ch_paye_cont"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            X005cd_paye_addprev.ORG,
            X005cd_paye_addprev.LOC,
            X005cd_paye_addprev.EMP,
            X005cd_paye_addprev.NUMB,
            X005cd_paye_addprev.COUNT,
            PEOPLE.X002_PEOPLE_CURR.NAME_ADDR AS NAME,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X005cd_paye_addprev
            Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X005cd_paye_addprev.EMP
            Left Join X005cf_offi CAMP_OFF On CAMP_OFF.CAMPUS = X005cd_paye_addprev.LOC
            Left Join X005cf_offi ORG_OFF On ORG_OFF.CAMPUS = X005cd_paye_addprev.ORG
            Left Join X005cg_supe CAMP_SUP On CAMP_SUP.CAMPUS = X005cd_paye_addprev.LOC
            Left Join X005cg_supe ORG_SUP On ORG_SUP.CAMPUS = X005cd_paye_addprev.ORG
        WHERE
          X005cd_paye_addprev.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X005cx_paye_duplicate"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    print("Build the final report")
    if i_find > 0 and i_coun > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'TAX NUMBER DUPLICATE' As AUDIT_FINDING,
            X005ch_paye_cont.ORG AS ORGANIZATION,
            X005ch_paye_cont.LOC AS LOCATION,
            X005ch_paye_cont.EMP AS EMPLOYEE_NUMBER,
            X005ch_paye_cont.NAME,
            X005ch_paye_cont.NUMB AS PAYE_NUMBER,
            X005ch_paye_cont.COUNT AS OCCURANCES,
            X005ch_paye_cont.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
            X005ch_paye_cont.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
            X005ch_paye_cont.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
            X005ch_paye_cont.CAMP_SUP_NAME AS SUPERVISOR,
            X005ch_paye_cont.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
            X005ch_paye_cont.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
            X005ch_paye_cont.ORG_OFF_NAME AS ORG_OFFICER,
            X005ch_paye_cont.ORG_OFF_NUMB AS ORG_OFFICER_NUMB,
            X005ch_paye_cont.ORG_OFF_MAIL AS ORG_OFFICER_MAIL,
            X005ch_paye_cont.ORG_SUP_NAME AS ORG_SUPERVISOR,
            X005ch_paye_cont.ORG_SUP_NUMB AS ORG_SUPERVISOR_NUMB,
            X005ch_paye_cont.ORG_SUP_MAIL AS ORG_SUPERVISOR_MAIL
        From
            X005ch_paye_cont
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
            print("Export findings...")
            sr_filet = sr_file
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "People_test_005cx_paye_duplicate_"
            sx_filet = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
            funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    NAME MASTER FILE
    *****************************************************************************"""

    # BUILD TABLE WITH EMPLOYEE NAMES
    print("Obtain master list of all employees...")
    sr_file = "X006_name_master"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        'NWU' AS ORG,
        CASE LOCATION_DESCRIPTION
            WHEN 'MAFIKENG CAMPUS' THEN 'MAF'
            WHEN 'POTCHEFSTROOM CAMPUS' THEN 'POT'
            WHEN 'VAAL TRIANGLE CAMPUS' THEN 'VAA'
            ELSE 'NWU'
        END AS LOC,
        PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER AS EMP,
        TRIM(PEOPLE.X002_PEOPLE_CURR.LAST_NAME) || ' ' || TRIM(PEOPLE.X002_PEOPLE_CURR.FIRST_NAME) AS NAME,
        PEOPLE.X002_PEOPLE_CURR.IDNO
    From
        PEOPLE.X002_PEOPLE_CURR
    Where
        PEOPLE.X002_PEOPLE_CURR.FIRST_NAME != ''        
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    """ ****************************************************************************
    TEST NAME DUPLICATE
    *****************************************************************************"""
    print("TEST NAME DUPLICATE")
    funcfile.writelog("TEST NAME DUPLICATE")

    # DECLARE TEST VARIABLES
    i_find = 0 # Number of findings before previous reported findings
    i_coun = 0 # Number of new findings to report

    # COUNT ALL EMPLOYEES WITH A NAME ;-)
    print("Count all employees with names...")
    sr_file = "X006aa_name_coun"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X006_name_master.NAME,
        Count(X006_name_master.EMP) As COUNT
    From
        X006_name_master
    Where
        X006_name_master.NAME <> ''
    Group By
        X006_name_master.NAME
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # IDENTIFY DUPLICATE ACCOUNTS
    print("Build list of duplicate accounts...")
    sr_file = "X006ab_name_dupl"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X006_name_master.*,
        X006aa_name_coun.COUNT
    From
        X006_name_master Left Join
        X006aa_name_coun On X006aa_name_coun.NAME = X006_name_master.NAME
    Where
        X006aa_name_coun.COUNT > 1
    Order by
        X006aa_name_coun.NAME, IDNO
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find = funcsys.tablerowcount(so_curs,sr_file)
    print("*** Found "+str(i_find)+" exceptions ***")
    funcfile.writelog("%t FINDING: "+str(i_find)+" EMPL NAME duplicate finding(s)")

    # GET PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X006ac_name_getprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,REMARK TEXT)")
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "name_duplicate":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the impoted data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X006ad_name_addprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        SELECT
          X006ab_name_dupl.*,
          'name_duplicate' AS PROCESS,
          '%TODAY%' AS DATE_REPORTED,
          '%TODAYPLUS%' AS DATE_RETEST,
          X006ac_name_getprev.PROCESS AS PREV_PROCESS,
          X006ac_name_getprev.DATE_REPORTED AS PREV_DATE_REPORTED,
          X006ac_name_getprev.DATE_RETEST AS PREV_DATE_RETEST,
          X006ac_name_getprev.REMARK
        FROM
          X006ab_name_dupl
          LEFT JOIN X006ac_name_getprev ON X006ac_name_getprev.FIELD1 = X006ab_name_dupl.EMP AND
              X006ac_name_getprev.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X006ae_name_newprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE "+sr_file+" AS " + """
        SELECT
          X006ad_name_addprev.PROCESS,
          X006ad_name_addprev.EMP AS FIELD1,
          '' AS FIELD2,
          '' AS FIELD3,
          '' AS FIELD4,
          '' AS FIELD5,
          X006ad_name_addprev.DATE_REPORTED,
          X006ad_name_addprev.DATE_RETEST,
          X006ad_name_addprev.REMARK
        FROM
          X006ad_name_addprev
        WHERE
          X006ad_name_addprev.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: "+sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(so_curs,sr_file)
        if i_coun > 0:
            print("*** " +str(i_coun)+ " Finding(s) to report ***")    
            sr_filet = sr_file
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
                funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
                funcfile.writelog("%t EXPORT DATA: "+sr_file)
            if l_mess:
                s_desc = "Name duplicate"
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")    

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X006af_offi"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_NAME_DUPL_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X006ag_supe"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
          PEOPLE.X002_PEOPLE_CURR.NAME_ADDR As NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_NAME_DUPL_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X006ah_name_cont"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            X006ad_name_addprev.ORG,
            X006ad_name_addprev.LOC,
            X006ad_name_addprev.EMP,
            X006ad_name_addprev.NAME,
            X006ad_name_addprev.IDNO,
            X006ad_name_addprev.COUNT,
            PEOPLE.X002_PEOPLE_CURR.FULL_NAME AS FULLNAME,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X006ad_name_addprev
            Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X006ad_name_addprev.EMP
            Left Join X006af_offi CAMP_OFF On CAMP_OFF.CAMPUS = X006ad_name_addprev.LOC
            Left Join X006af_offi ORG_OFF On ORG_OFF.CAMPUS = X006ad_name_addprev.ORG
            Left Join X006ag_supe CAMP_SUP On CAMP_SUP.CAMPUS = X006ad_name_addprev.LOC
            Left Join X006ag_supe ORG_SUP On ORG_SUP.CAMPUS = X006ad_name_addprev.ORG
        WHERE
          X006ad_name_addprev.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X006ax_name_duplicate"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    print("Build the final report")
    if i_find > 0 and i_coun > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'NAME DUPLICATE' As AUDIT_FINDING,
            X006ah_name_cont.ORG AS ORGANIZATION,
            X006ah_name_cont.LOC AS LOCATION,
            X006ah_name_cont.EMP AS EMPLOYEE_NUMBER,
            X006ah_name_cont.FULLNAME,
            X006ah_name_cont.IDNO,
            X006ah_name_cont.COUNT AS OCCURANCES,
            X006ah_name_cont.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
            X006ah_name_cont.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
            X006ah_name_cont.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
            X006ah_name_cont.CAMP_SUP_NAME AS SUPERVISOR,
            X006ah_name_cont.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
            X006ah_name_cont.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
            X006ah_name_cont.ORG_OFF_NAME AS ORG_OFFICER,
            X006ah_name_cont.ORG_OFF_NUMB AS ORG_OFFICER_NUMB,
            X006ah_name_cont.ORG_OFF_MAIL AS ORG_OFFICER_MAIL,
            X006ah_name_cont.ORG_SUP_NAME AS ORG_SUPERVISOR,
            X006ah_name_cont.ORG_SUP_NUMB AS ORG_SUPERVISOR_NUMB,
            X006ah_name_cont.ORG_SUP_MAIL AS ORG_SUPERVISOR_MAIL
        From
            X006ah_name_cont
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
            print("Export findings...")
            sr_filet = sr_file
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "People_test_006ax_name_duplicate_"
            sx_filet = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
            funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    GRADE LEAVE MASTER FILE
    *****************************************************************************"""
    print("GRADE LEAVE MASTER FILE")
    funcfile.writelog("GRADE LEAVE MASTER FILE")

    # OBTAIN LIST OF LONG SERVICE AWARD DATES
    # BUILD THE CURRENT ELEMENT LIST
    print("Obtain long service awards...")
    sr_file = "X007_long_service_date"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select Distinct
        per.employee_number,
        per.full_name,
        Upper(petf.element_name) As ELEMENT_NAME,
        Upper(pivf.name) As INPUT_VALUE,
        Date(Substr(peevf.screen_entry_value,1,4)||'-'||
            Substr(peevf.screen_entry_value,6,2)||'-'||
            Substr(peevf.screen_entry_value,9,2)) As DATE_LONG_SERVICE
    From
        PAYROLL.PAY_ELEMENT_ENTRIES_F_CURR peef,
        PEOPLE.PER_ALL_PEOPLE_F per,
        PEOPLE.PER_ALL_ASSIGNMENTS_F paaf,
        PAYROLL.PAY_ELEMENT_TYPES_F petf,
        PAYROLL.PAY_ELEMENT_ENTRY_VALUES_F_CURR peevf,
        PAYROLL.PAY_INPUT_VALUES_F pivf
    Where
        per.person_id = paaf.person_id And
        paaf.assignment_id = peef.assignment_id And
        peef.element_type_id = petf.element_type_id And
        peevf.element_entry_id = peef.element_entry_id And
        pivf.element_type_id = petf.element_type_id And
        pivf.input_value_id = peevf.input_value_id And
        Date('%TODAY%') Between peef.effective_start_date And peef.effective_end_date And
        Date('%TODAY%') Between per.effective_start_date And per.effective_end_date And
        Date('%TODAY%') Between paaf.effective_start_date And paaf.effective_end_date And
        Date('%TODAY%') Between petf.effective_start_date And petf.effective_end_date And
        paaf.primary_flag = 'Y' And
        peevf.screen_entry_value > 0 And
        Upper(petf.element_name) = 'NWU LONG SERVICE AWARD' And
        Upper(pivf.name) = 'LONG SERVICE DATE'
    Order By
        per.employee_number    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%", funcdate.today())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # LIST OF CURRENT SECONDARY ASSIGNMENTS (TEMPORARY ASSIGNMENT and TEMP FIXED TERM CONTRACT)
    print("Obtain a list of secondary assignments...")
    sr_file = "X007_leave01_secass_all"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PEOP.EMPLOYEE_NUMBER,
        PEOP.ASS_ID,
        PEOP.NAME_LIST,
        PEOP.PERSON_TYPE,
        SEC.SEC_HOURS_FORECAST,
        SEC.SEC_DATE_FROM,
        SEC.SEC_DATE_TO,
        SEC.SEC_TYPE,
        SEC.SEC_RATE,
        SEC.SEC_UNIT,
        SEC.SEC_FULLPART_FLAG,
        SEC.ASSIGNMENT_EXTRA_INFO_ID,
        CASE
            WHEN SEC.SEC_UNIT = 'MS' And SEC.SEC_TYPE = 'SALARY' And SEC.SEC_FULLPART_FLAG = 'F' THEN PEOP.PERSON_TYPE||'(F)'
            WHEN SEC.SEC_UNIT = 'MS' And SEC.SEC_TYPE = 'SALARY' And SEC.SEC_FULLPART_FLAG <> 'F' THEN 'C'
            ELSE PEOP.PERSON_TYPE||'(P)'    
        END As PERSON_TYPE_CALC,
        (JulianDay(SEC.SEC_DATE_TO) - JulianDay(SEC.SEC_DATE_FROM)) / 30.4167 As CALC_DUR_MONTH,
        SEC.SEC_HOURS_FORECAST / ((JulianDay(SEC.SEC_DATE_TO) - JulianDay(SEC.SEC_DATE_FROM)) / 30.4167) As CALC_HOUR_MONTH
    From
        PEOPLE.X002_PEOPLE_CURR PEOP Inner Join
        PEOPLE.X001_ASSIGNMENT_SEC_CURR_YEAR SEC On SEC.ASSIGNMENT_ID = PEOP.ASS_ID
    Where
        (PEOP.PERSON_TYPE = 'TEMPORARY APPOINTMENT' And
        SEC_DATE_FROM <= Date('%TODAY%') And
        SEC_DATE_TO >= Date('%TODAY%')) Or
        (PEOP.PERSON_TYPE = 'TEMP FIXED TERM CONTRACT' And
        SEC_DATE_FROM <= Date('%TODAY%') And
        SEC_DATE_TO >= Date('%TODAY%')) Or
        (PEOP.PERSON_TYPE = 'HOUSE PARENT' And
        SEC_DATE_FROM <= Date('%TODAY%') And
        SEC_DATE_TO >= Date('%TODAY%')) Or
        (PEOP.PERSON_TYPE = 'STUDENT ASSISTANT' And
        SEC_DATE_FROM <= Date('%TODAY%') And
        SEC_DATE_TO >= Date('%TODAY%'))
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%", funcdate.yesterday())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ISOLATE RECORDS WITHOUT FURTHER CALCULATION
    print("Isolate records without further calculations...")
    sr_file = "X007_leave02_ms_list"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        SEC.EMPLOYEE_NUMBER,
        SEC.PERSON_TYPE_CALC,
        Max(SEC.ASSIGNMENT_EXTRA_INFO_ID) AS ASSIGNMENT_EXTRA_INFO_ID, 
        Count(SEC.ASS_ID) As COUNT
    From
        X007_leave01_secass_all SEC
    Where
        SEC.PERSON_TYPE_CALC <> 'C'
    Group By
        SEC.EMPLOYEE_NUMBER,
        SEC.PERSON_TYPE_CALC
    Order By
        SEC.PERSON_TYPE_CALC
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD UNIQUE LIST OF RECORDS WITHOUT FURTHER CALCULATION
    print("Build unique list of records...")
    sr_file = "X007_leave03_ms_empl"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        SEC.EMPLOYEE_NUMBER,
        SEC.PERSON_TYPE_CALC
    From
        X007_leave02_ms_list SEC
    Group By
        SEC.EMPLOYEE_NUMBER
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD HOURS PER MONTH SUMMARY LIST
    print("Build hours per month list...")
    sr_file = "X007_leave04_hoursum"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        SEC.EMPLOYEE_NUMBER,
        Count(SEC.ASS_ID) As SEC_COUNT,
        Sum(SEC.CALC_HOUR_MONTH) As CALC_HOUR_MONTH_SUM,
        CASE
            WHEN Sum(SEC.CALC_HOUR_MONTH) < 24 THEN SEC.PERSON_TYPE||'(P)'
            ELSE SEC.PERSON_TYPE||'(F)'
        END As PERSON_TYPE_CALC
    From
        X007_leave01_secass_all SEC
    Group By
        SEC.EMPLOYEE_NUMBER
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD GRADE AND LEAVE MASTER TABLE
    print("Obtain master list of all grades and leave codes...")
    sr_file = "X007_grade_leave_master"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        CASE LOCATION_DESCRIPTION
            WHEN 'MAFIKENG CAMPUS' THEN 'MAF'
            WHEN 'POTCHEFSTROOM CAMPUS' THEN 'POT'
            WHEN 'VAAL TRIANGLE CAMPUS' THEN 'VAA'
            ELSE 'NWU'
        END AS LOC,
        PEOPLE.EMPLOYEE_NUMBER,
        PEOPLE.EMP_START,
        LONG.DATE_LONG_SERVICE As SERVICE_START,
        PEOPLE.ACAD_SUPP,
        PEOPLE.EMPLOYMENT_CATEGORY,
        PEOPLE.PERSON_TYPE,
        PEOPLE.ASS_WEEK_LEN,
        PEOPLE.LEAVE_CODE,
        PEOPLE.GRADE,
        PEOPLE.GRADE_CALC,
        CASE
            WHEN LIST2.PERSON_TYPE_CALC IS NOT NULL THEN LIST2.PERSON_TYPE_CALC
            WHEN LIST1.PERSON_TYPE_CALC IS NOT NULL THEN LIST1.PERSON_TYPE_CALC 
            ELSE PEOPLE.PERSON_TYPE
        END As PERSON_TYPE_LEAVE,
        CASE
            WHEN LONG.DATE_LONG_SERVICE Is Null And PEOPLE.EMP_START < Date('2017-05-01') THEN 'OLD'
            WHEN LONG.DATE_LONG_SERVICE < Date('2017-05-01') THEN 'OLD'
            ELSE '2017'
        END As PERIOD
    From
        PEOPLE.X002_PEOPLE_CURR PEOPLE Left Join
        X007_long_service_date LONG On LONG.EMPLOYEE_NUMBER = PEOPLE.EMPLOYEE_NUMBER Left Join
        X007_leave03_ms_empl LIST1 On LIST1.EMPLOYEE_NUMBER = PEOPLE.EMPLOYEE_NUMBER Left Join
        X007_leave04_hoursum LIST2 On LIST2.EMPLOYEE_NUMBER = PEOPLE.EMPLOYEE_NUMBER     
    Where
        Substr(PEOPLE.PERSON_TYPE,1,6) <> 'AD HOC'
    ;"""
    """
    20210420 Remove
    Where
        Substr(PEOPLE.PERSON_TYPE,1,6) <> 'AD HOC'
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    TEST ASSIGNMENT CATEGORY INVALID (PERMANENT:TEMPORARY)
    *****************************************************************************"""

    # FILES NEEDED
    # X000_PEOPLE

    # DEFAULT TRANSACTION OWNER PEOPLE
    # 21022402 MS AC COERTZEN for permanent employees
    # 20742010 MRS N BOTHA for temporary employees
    # Exclude 12795631 MR R VAN DEN BERG
    # Exclude 13277294 MRS MC STRYDOM

    # DECLARE TEST VARIABLES
    i_finding_before = 0
    i_finding_after = 0
    s_description = "Assignment category invalid"
    s_file_prefix: str = "X007a"
    s_file_name: str = "assignment_category_invalid"
    s_finding: str = "ASSIGNMENT CATEGORY INVALID"
    s_report_file: str = "001_reported.txt"

    # LOG
    funcfile.writelog("TEST " + s_finding)
    if l_debug:
        print("TEST " + s_finding)

    # OBTAIN MASTER DATA
    if l_debug:
        print("Obtain master data...")
    sr_file: str = s_file_prefix + "a_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        'NWU' As ORG,
        Substr(p.location,1,3) As LOC,
        p.employee_number As EMPLOYEE_NUMBER,
        p.assignment_category As ASSIGNMENT_CATEGORY,
        p.employee_category As EMPLOYEE_CATEGORY,
        Case
            When au.EMPLOYEE_NUMBER Is Not Null And
             au.EMPLOYEE_NUMBER Not In ('12795631','13277294') Then
             au.EMPLOYEE_NUMBER
            When p.service_start_date Is Null Then '21022402'
            Else '20742010'
        End As TRAN_OWNER,
        p.service_start_date As SERVICE_START_DATE,
        Case
            When p.service_start_date Is Null Then 'PERMANENT'
            Else 'TEMPORARY'
        End As CATEGORY,
        p.assignment_update_by As ASSIGN_USER_ID,
        au.EMPLOYEE_NUMBER As ASSIGN_UPDATE,
        au.NAME_ADDR As ASSIGN_UPDATE_NAME,
        p.people_update_by As PEOPLE_USER_ID,
        pu.EMPLOYEE_NUMBER As PEOPLE_UPDATE,
        pu.NAME_ADDR As PEOPLE_UPDATE_NAME
    From
        X000_PEOPLE p Left Join
        X000_USER_CURR au On au.USER_ID = p.assignment_update_by Left join
        X000_USER_CURR pu On pu.USER_ID = p.people_update_by
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # IDENTIFY FINDINGS
    if l_debug:
        print("Identify findings...")
    sr_file = s_file_prefix + "b_finding"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
     Select
        FIND.ORG,
        FIND.LOC,
        FIND.EMPLOYEE_NUMBER,
        FIND.TRAN_OWNER,
        FIND.CATEGORY
    From
        %FILEP%%FILEN% FIND
    Where
        FIND.ASSIGNMENT_CATEGORY Is Null   
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # COUNT THE NUMBER OF FINDINGS
    if l_debug:
        print("Count the number of findings...")
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")

    # GET PREVIOUS FINDINGS
    if i_finding_before > 0:
        functest.get_previous_finding(so_curs, ed_path, s_report_file, s_finding, "TITTT")
        if l_debug:
            so_conn.commit()

    # SET PREVIOUS FINDINGS
    if i_finding_before > 0:
        functest.set_previous_finding(so_curs)
        if l_debug:
            so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = s_file_prefix + "d_addprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            Lower('%FINDING%') AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DATETEST%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%b_finding FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = EMPLOYEE_NUMBER
        ;"""
        s_sql = s_sql.replace("%FINDING%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DATETEST%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = s_file_prefix + "e_newprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Build list to update findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.EMPLOYEE_NUMBER AS FIELD1,
            '' AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%d_addprev PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""        
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = s_report_file[:-4]
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_finding_before) + '/' + str(
                    i_finding_after) + '</b> ' + s_description)
        else:
            funcfile.writelog("%t FINDING: No new findings to export")
            if l_debug:
                print("*** No new findings to report ***")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        functest.get_officer(so_curs, "HR", "TEST " + s_finding + " OFFICER")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        functest.get_supervisor(so_curs, "HR", "TEST " + s_finding + " SUPERVISOR")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = s_file_prefix + "h_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.EMPLOYEE_NUMBER,
            PEOP.name_address As NAME_ADDRESS,
            PEOP.user_person_type As PERSON_TYPE,
            OWNR.EMPLOYEE_NUMBER AS TRAN_OWNER_NUMB,
            OWNR.name_address AS TRAN_OWNER_NAME,
            OWNR.EMAIL_ADDRESS AS TRAN_OWNER_MAIL1,        
            CASE
                WHEN  OWNR.EMPLOYEE_NUMBER != '' THEN OWNR.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE OWNR.EMAIL_ADDRESS
            END AS TRAN_OWNER_MAIL2,
            CAMP_OFF.EMPLOYEE_NUMBER AS CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR AS CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS AS CAMP_OFF_MAIL1,        
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END AS CAMP_OFF_MAIL2,
            CAMP_SUP.EMPLOYEE_NUMBER AS CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR AS CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS AS CAMP_SUP_MAIL1,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER != '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END AS CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER AS ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR AS ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS AS ORG_OFF_MAIL1,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER != '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END AS ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER AS ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR AS ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS AS ORG_SUP_MAIL1,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER != '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END AS ORG_SUP_MAIL2,
            AUD_OFF.EMPLOYEE_NUMBER As AUD_OFF_NUMB,
            AUD_OFF.NAME_ADDR As AUD_OFF_NAME,
            AUD_OFF.EMAIL_ADDRESS As AUD_OFF_MAIL,
            AUD_SUP.EMPLOYEE_NUMBER As AUD_SUP_NUMB,
            AUD_SUP.NAME_ADDR As AUD_SUP_NAME,
            AUD_SUP.EMAIL_ADDRESS As AUD_SUP_MAIL
        From
            %FILEP%d_addprev PREV Left Join
            PEOPLE.X000_PEOPLE PEOP ON PEOP.EMPLOYEE_NUMBER = PREV.EMPLOYEE_NUMBER Left Join
            PEOPLE.X000_PEOPLE OWNR ON OWNR.EMPLOYEE_NUMBER = PREV.TRAN_OWNER Left Join
            Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.CATEGORY Left Join
            Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD' Left Join
            Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD'                    
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%FILEN%", s_file_name)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = s_file_prefix + "x_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            '%FIND%' As Audit_finding,
            FIND.EMPLOYEE_NUMBER As Employee,
            FIND.NAME_ADDRESS As Name,
            FIND.PERSON_TYPE As Type,
            FIND.ORG As Organization,
            FIND.LOC As Campus,
            FIND.TRAN_OWNER_NAME AS Responsible_Officer,
            FIND.TRAN_OWNER_NUMB AS Responsible_Officer_Numb,
            FIND.TRAN_OWNER_MAIL1 AS Responsible_Officer_Mail,
            FIND.TRAN_OWNER_MAIL2 AS Responsible_Officer_Mail_Alternate,
            FIND.CAMP_OFF_NAME AS Officer,
            FIND.CAMP_OFF_NUMB AS Officer_Numb,
            FIND.CAMP_OFF_MAIL1 AS Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL1 AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL1 AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL1 AS Org_Supervisor_Mail,
            FIND.AUD_OFF_NAME AS Audit_Officer,
            FIND.AUD_OFF_NUMB AS Audit_Officer_Numb,
            FIND.AUD_OFF_MAIL AS Audit_Officer_Mail,
            FIND.AUD_SUP_NAME AS Audit_Supervisor,
            FIND.AUD_SUP_NUMB AS Audit_Supervisor_Numb,
            FIND.AUD_SUP_MAIL AS Audit_Supervisor_Mail
        From
            %FILEP%h_detail FIND
        ;"""
        s_sql = s_sql.replace("%FIND%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = s_file_prefix + "_" + s_finding.lower() + "_"
            sx_file_dated = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    TEST EMPLOYEE CATEGORY INVALID (ACADEMIC:SUPPORT)
    *****************************************************************************"""

    # FILES NEEDED
    # X000_PEOPLE

    # DEFAULT TRANSACTION OWNER PEOPLE
    # 21022402 MS AC COERTZEN for permanent employees
    # 20742010 MRS N BOTHA for temporary employees
    # Exclude 12795631 MR R VAN DEN BERG
    # Exclude 13277294 MRS MC STRYDOM

    # DECLARE TEST VARIABLES
    i_finding_before = 0
    i_finding_after = 0
    s_description = "Employee category invalid"
    s_file_prefix: str = "X007b"
    s_file_name: str = "employee_category_invalid"
    s_finding: str = "EMPLOYEE CATEGORY INVALID"
    s_report_file: str = "001_reported.txt"

    # LOG
    funcfile.writelog("TEST " + s_finding)
    if l_debug:
        print("TEST " + s_finding)

    # OBTAIN MASTER DATA
    if l_debug:
        print("Obtain master data...")
    sr_file: str = s_file_prefix + "a_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create Table " + sr_file + " As " + """
    Select
        'NWU' As ORG,
        Substr(p.location,1,3) As LOC,
        p.employee_number As EMPLOYEE_NUMBER,
        p.assignment_category As ASSIGNMENT_CATEGORY,
        p.employee_category As EMPLOYEE_CATEGORY,
        Case
            When au.EMPLOYEE_NUMBER Is Not Null And
             au.EMPLOYEE_NUMBER Not In ('12795631','13277294') Then
             au.EMPLOYEE_NUMBER
            When p.assignment_category = 'PERMANENT' Then '21022402'
            Else '20742010'
        End As TRAN_OWNER,
        p.assignment_update_by As ASSIGN_USER_ID,
        au.EMPLOYEE_NUMBER As ASSIGN_UPDATE,
        au.NAME_ADDR As ASSIGN_UPDATE_NAME,
        p.people_update_by As PEOPLE_USER_ID,
        pu.EMPLOYEE_NUMBER As PEOPLE_UPDATE,
        pu.NAME_ADDR As PEOPLE_UPDATE_NAME
    From
        X000_PEOPLE p Left Join
        X000_USER_CURR au On au.USER_ID = p.assignment_update_by Left join
        X000_USER_CURR pu On pu.USER_ID = p.people_update_by
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # IDENTIFY FINDINGS
    if l_debug:
        print("Identify findings...")
    sr_file = s_file_prefix + "b_finding"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
     Select
        FIND.ORG,
        FIND.LOC,
        FIND.EMPLOYEE_NUMBER,
        FIND.ASSIGNMENT_CATEGORY,
        FIND.TRAN_OWNER
    From
        %FILEP%%FILEN% FIND
    Where
        FIND.EMPLOYEE_CATEGORY Is Null And
        FIND.ASSIGNMENT_CATEGORY Is Not Null   
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

    # COUNT THE NUMBER OF FINDINGS
    if l_debug:
        print("Count the number of findings...")
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")
    if l_debug:
        print("*** Found " + str(i_finding_before) + " exceptions ***")

    # GET PREVIOUS FINDINGS
    if i_finding_before > 0:
        functest.get_previous_finding(so_curs, ed_path, s_report_file, s_finding, "TTTTT")
        if l_debug:
            so_conn.commit()

    # SET PREVIOUS FINDINGS
    if i_finding_before > 0:
        functest.set_previous_finding(so_curs)
        if l_debug:
            so_conn.commit()

    # ADD PREVIOUS FINDINGS
    sr_file = s_file_prefix + "d_addprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            Lower('%FINDING%') AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DATETEST%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%b_finding FIND Left Join
            Z001ab_setprev PREV ON PREV.FIELD1 = EMPLOYEE_NUMBER
        ;"""
        s_sql = s_sql.replace("%FINDING%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DATETEST%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = s_file_prefix + "e_newprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if l_debug:
            print("Build list to update findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.EMPLOYEE_NUMBER AS FIELD1,
            '' AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            %FILEP%d_addprev PREV
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""        
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        so_curs.execute(s_sql)
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        if l_debug:
            so_conn.commit()
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            if l_debug:
                print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = s_report_file[:-4]
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                funcsms.send_telegram('', 'administrator', '<b>' + str(i_finding_before) + '/' + str(
                    i_finding_after) + '</b> ' + s_description)
        else:
            funcfile.writelog("%t FINDING: No new findings to export")
            if l_debug:
                print("*** No new findings to report ***")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        functest.get_officer(so_curs, "HR", "TEST " + s_finding + " OFFICER")
        so_conn.commit()

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    if i_finding_before > 0 and i_finding_after > 0:
        functest.get_supervisor(so_curs, "HR", "TEST " + s_finding + " SUPERVISOR")
        so_conn.commit()

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = s_file_prefix + "h_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.EMPLOYEE_NUMBER,
            PEOP.name_address As NAME_ADDRESS,
            PEOP.assignment_category As ASSIGNMENT_CATEGORY,
            PEOP.user_person_type As PERSON_TYPE,
            OWNR.EMPLOYEE_NUMBER AS TRAN_OWNER_NUMB,
            OWNR.name_address AS TRAN_OWNER_NAME,
            OWNR.EMAIL_ADDRESS AS TRAN_OWNER_MAIL1,        
            CASE
                WHEN  OWNR.EMPLOYEE_NUMBER != '' THEN OWNR.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE OWNR.EMAIL_ADDRESS
            END AS TRAN_OWNER_MAIL2,
            CAMP_OFF.EMPLOYEE_NUMBER AS CAMP_OFF_NUMB,
            CAMP_OFF.NAME_ADDR AS CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS AS CAMP_OFF_MAIL1,        
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END AS CAMP_OFF_MAIL2,
            CAMP_SUP.EMPLOYEE_NUMBER AS CAMP_SUP_NUMB,
            CAMP_SUP.NAME_ADDR AS CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS AS CAMP_SUP_MAIL1,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER != '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END AS CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER AS ORG_OFF_NUMB,
            ORG_OFF.NAME_ADDR AS ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS AS ORG_OFF_MAIL1,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER != '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END AS ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER AS ORG_SUP_NUMB,
            ORG_SUP.NAME_ADDR AS ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS AS ORG_SUP_MAIL1,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER != '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END AS ORG_SUP_MAIL2,
            AUD_OFF.EMPLOYEE_NUMBER As AUD_OFF_NUMB,
            AUD_OFF.NAME_ADDR As AUD_OFF_NAME,
            AUD_OFF.EMAIL_ADDRESS As AUD_OFF_MAIL,
            AUD_SUP.EMPLOYEE_NUMBER As AUD_SUP_NUMB,
            AUD_SUP.NAME_ADDR As AUD_SUP_NAME,
            AUD_SUP.EMAIL_ADDRESS As AUD_SUP_MAIL
        From
            %FILEP%d_addprev PREV Left Join
            PEOPLE.X000_PEOPLE PEOP ON PEOP.EMPLOYEE_NUMBER = PREV.EMPLOYEE_NUMBER Left Join
            PEOPLE.X000_PEOPLE OWNR ON OWNR.EMPLOYEE_NUMBER = PREV.TRAN_OWNER Left Join
            Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.ASSIGNMENT_CATEGORY Left Join
            Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD' Left Join
            Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD'                    
        Where
            PREV.PREV_PROCESS Is Null Or
            PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
        ;"""
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        s_sql = s_sql.replace("%FILEN%", s_file_name)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = s_file_prefix + "x_" + s_file_name
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        if l_debug:
            print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            '%FIND%' As Audit_finding,
            FIND.EMPLOYEE_NUMBER As Employee,
            FIND.NAME_ADDRESS As Name,
            FIND.ASSIGNMENT_CATEGORY As Category,
            FIND.PERSON_TYPE As Type,
            FIND.ORG As Organization,
            FIND.LOC As Campus,
            FIND.TRAN_OWNER_NAME AS Responsible_Officer,
            FIND.TRAN_OWNER_NUMB AS Responsible_Officer_Numb,
            FIND.TRAN_OWNER_MAIL1 AS Responsible_Officer_Mail,
            FIND.TRAN_OWNER_MAIL2 AS Responsible_Officer_Mail_Alternate,
            FIND.CAMP_OFF_NAME AS Officer,
            FIND.CAMP_OFF_NUMB AS Officer_Numb,
            FIND.CAMP_OFF_MAIL1 AS Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL1 AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL1 AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL1 AS Org_Supervisor_Mail,
            FIND.AUD_OFF_NAME AS Audit_Officer,
            FIND.AUD_OFF_NUMB AS Audit_Officer_Numb,
            FIND.AUD_OFF_MAIL AS Audit_Officer_Mail,
            FIND.AUD_SUP_NAME AS Audit_Supervisor,
            FIND.AUD_SUP_NUMB AS Audit_Supervisor_Numb,
            FIND.AUD_SUP_MAIL AS Audit_Supervisor_Mail
        From
            %FILEP%h_detail FIND
        ;"""
        s_sql = s_sql.replace("%FIND%", s_finding)
        s_sql = s_sql.replace("%FILEP%", s_file_prefix)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            if l_debug:
                print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = s_file_prefix + "_" + s_finding.lower() + "_"
            sx_file_dated = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    TEST GRADE INVALID
    *****************************************************************************"""
    print("GRADE INVALID")
    funcfile.writelog("GRADE INVALID")

    # DECLARE TEST VARIABLES
    i_find = 0 # Number of findings before previous reported findings
    i_coun = 0 # Number of new findings to report

    # IMPORT GRADE BENCHMARK
    sr_file = "X007_grade_master"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    print("Import grade benchmark...")
    so_curs.execute("CREATE TABLE " + sr_file + "(CATEGORY TEXT,TYPE TEXT,ACADSUPP TEXT,GRADE TEXT)")
    s_cols = ""
    co = open(ed_path + "001_employee_grade.csv", "r")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "CATEGORY":
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the impoted data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_employee_grade.csv (" + sr_file + ")")

    # IDENTIFY FINDING 
    print("Identify incorrect data...")
    sr_file = "X007ca_grade"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        MASTER.ORG,
        MASTER.LOC,
        MASTER.EMPLOYEE_NUMBER,
        MASTER.GRADE,
        BENCH.GRADE As GRADE_BENCH,
        MASTER.EMPLOYMENT_CATEGORY,
        MASTER.ACAD_SUPP,
        MASTER.PERSON_TYPE,
        CASE
            WHEN Instr(BENCH.GRADE,'.'||Trim(MASTER.GRADE)||'~') > 0 Then 'TRUE'
            ELSE 'FALSE'
        END As VALID
    From
        X007_grade_leave_master MASTER Left Join
        X007_grade_master BENCH On BENCH.CATEGORY = MASTER.EMPLOYMENT_CATEGORY
                And BENCH.TYPE = MASTER.PERSON_TYPE
                And BENCH.ACADSUPP = MASTER.ACAD_SUPP    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # ADD DETAILS
    print("Add data details...")
    sr_file = "X007cb_detail"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        FIND.ORG,
        FIND.LOC,
        FIND.EMPLOYEE_NUMBER
    From
        X007ca_grade FIND
    Where
        FIND.VALID = 'FALSE'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find = funcsys.tablerowcount(so_curs,sr_file)
    print("*** Found "+str(i_find)+" exceptions ***")
    funcfile.writelog("%t FINDING: "+str(i_find)+" EMPL GRADE invalid finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X007cc_getprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,REMARK TEXT)")
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "empl_grade_invalid":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the impoted data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    sr_file = "X007cd_addprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
          FIND.*,
          'empl_grade_invalid' AS PROCESS,
          '%TODAY%' AS DATE_REPORTED,
          '%TODAYPLUS%' AS DATE_RETEST,
          PREV.PROCESS AS PREV_PROCESS,
          PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
          PREV.DATE_RETEST AS PREV_DATE_RETEST,
          PREV.REMARK
        From
          X007cb_detail FIND Left Join
          X007cc_getprev PREV ON PREV.FIELD1 = FIND.EMPLOYEE_NUMBER AND
              PREV.DATE_RETEST >= Date('%TODAY%')          
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X007ce_newprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE "+sr_file+" AS " + """
        Select
          FIND.PROCESS,
          FIND.EMPLOYEE_NUMBER As FIELD1,
          '' As FIELD2,
          '' AS FIELD3,
          '' AS FIELD4,
          '' AS FIELD5,
          FIND.DATE_REPORTED,
          FIND.DATE_RETEST,
          FIND.REMARK
        From
          X007cd_addprev FIND
        Where
          FIND.PREV_PROCESS Is Null
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: "+sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(so_curs, sr_file)
        if i_coun > 0:
            print("*** " +str(i_coun)+ " Finding(s) to report ***")    
            sr_filet = sr_file
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
                funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
                funcfile.writelog("%t EXPORT DATA: "+sr_file)
            if l_mess:
                s_desc = "Peromnes grade invalid"
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X007cf_officer"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          LOOKUP.LOOKUP,
          LOOKUP.LOOKUP_CODE AS TYPE,
          LOOKUP.LOOKUP_DESCRIPTION AS EMP,
          PERSON.NAME_ADDR AS NAME,
          PERSON.EMAIL_ADDRESS AS MAIL
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS LOOKUP
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR PERSON ON PERSON.EMPLOYEE_NUMBER = LOOKUP.LOOKUP_DESCRIPTION
        WHERE
          LOOKUP.LOOKUP = 'TEST_EMPL_GRADE_INVALID_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X007cg_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          LOOKUP.LOOKUP,
          LOOKUP.LOOKUP_CODE AS TYPE,
          LOOKUP.LOOKUP_DESCRIPTION AS EMP,
          PERSON.NAME_ADDR AS NAME,
          PERSON.EMAIL_ADDRESS AS MAIL
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS LOOKUP
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR PERSON ON PERSON.EMPLOYEE_NUMBER = LOOKUP.LOOKUP_DESCRIPTION
        WHERE
          LOOKUP.LOOKUP = 'TEST_EMPL_GRADE_INVALID_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X007ch_contact"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            FIND.ORG,
            FIND.LOC,
            FIND.EMPLOYEE_NUMBER,
            PEOP.NAME_LIST,
            PEOP.EMPLOYMENT_CATEGORY,
            PEOP.PERSON_TYPE,
            PEOP.ACAD_SUPP,
            PEOP.GRADE As PGRADE,
            CAMP_OFF.EMP As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CAMP_OFF.MAIL As CAMP_OFF_MAIL,
            CAMP_SUP.EMP As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CAMP_SUP.MAIL As CAMP_SUP_MAIL,
            ORG_OFF.EMP As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            ORG_OFF.MAIL As ORG_OFF_MAIL,
            ORG_SUP.EMP As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            ORG_SUP.MAIL As ORG_SUP_MAIL
        From
            X007cd_addprev FIND Left Join
            X007cf_officer CAMP_OFF On CAMP_OFF.TYPE = FIND.LOC Left Join
            X007cf_officer ORG_OFF On ORG_OFF.TYPE = FIND.ORG Left Join
            X007cg_supervisor CAMP_SUP On CAMP_SUP.TYPE = FIND.LOC Left Join
            X007cg_supervisor ORG_SUP On ORG_SUP.TYPE = FIND.ORG Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = FIND.EMPLOYEE_NUMBER
        Where
            FIND.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X007cx_grade_invalid"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'EMPLOYEE GRADE INVALID' As Audit_finding,
            FIND.EMPLOYEE_NUMBER As Employee,
            FIND.NAME_LIST As Name,
            FIND.EMPLOYMENT_CATEGORY As Category,
            FIND.PERSON_TYPE As Person_type,
            FIND.ACAD_SUPP As Acad_supp,
            FIND.PGRADE As Grade,
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail
        From
            X007ch_contact FIND
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
            print("Export findings...")
            sr_filet = sr_file
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "People_test_007cx_grade_invalid_"
            sx_filet = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
            funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    TEST LEAVE CODE INVALID
    *****************************************************************************"""
    print("LEAVE CODE INVALID")
    funcfile.writelog("LEAVE CODE INVALID")

    # DECLARE TEST VARIABLES
    i_find = 0  # Number of findings before previous reported findings
    i_coun = 0  # Number of new findings to report

    # IMPORT LEAVE BENCHMARK
    sr_file = "X007_leave_master"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Import leave benchmark...")
    so_curs.execute(
        "CREATE TABLE " + sr_file + "(CATEGORY TEXT,ACADSUPP TEXT,PERIOD TEXT,WEEK TEXT, GRADE TEXT, LEAVE TEXT)")
    s_cols = ""
    co = open(ed_path + "001_employee_leave.csv", "r")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "CATEGORY":
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[
                3] + "','" + row[4] + "','" + row[5] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the impoted data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_employee_leave.csv (" + sr_file + ")")

    # IDENTIFY FINDING
    print("Identify incorrect data...")
    sr_file = "X007da_leave"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        MASTER.ORG,
        MASTER.LOC,
        MASTER.EMPLOYEE_NUMBER,
        MASTER.LEAVE_CODE,
        MASTER.GRADE,
        MASTER.EMPLOYMENT_CATEGORY,
        MASTER.ACAD_SUPP,
        MASTER.PERSON_TYPE_LEAVE,
        MASTER.ASS_WEEK_LEN,
        MASTER.EMP_START,
        MASTER.SERVICE_START,
        MASTER.PERIOD,
        CASE
            WHEN MASTER.EMPLOYMENT_CATEGORY = 'PERMANENT' And Instr(PERM.GRADE,'.'||Trim(MASTER.GRADE)||'~') > 0 And Instr(PERM.LEAVE,'.'||Trim(MASTER.LEAVE_CODE)||'~') > 0 Then 'TRUE'
            WHEN MASTER.EMPLOYMENT_CATEGORY = 'TEMPORARY' And  Instr(TEMP.LEAVE,'.'||Trim(MASTER.LEAVE_CODE)||'~') > 0 Then 'TRUE'
            WHEN MASTER.EMPLOYMENT_CATEGORY = 'PERMANENT' Then 'FALSE'
            WHEN MASTER.EMPLOYMENT_CATEGORY = 'TEMPORARY' Then 'FALSE'
            ELSE 'OTHER'
        END As VALID,
        PERM.LEAVE As LEAVEP,
        PERM.GRADE As GRADEP,
        TEMP.LEAVE As LEAVET,
        TEMP.GRADE As GRADET
    From
        X007_grade_leave_master MASTER Left Join
        X007_leave_master PERM On PERM.CATEGORY = MASTER.EMPLOYMENT_CATEGORY And
            PERM.ACADSUPP = MASTER.ACAD_SUPP And
            PERM.PERIOD = MASTER.PERIOD And
            PERM.WEEK = MASTER.ASS_WEEK_LEN And
            Instr(PERM.GRADE,'.'||Trim(MASTER.GRADE)||'~') > 0 And
            PERM.CATEGORY = 'PERMANENT' Left Join
        X007_leave_master TEMP On TEMP.CATEGORY = MASTER.EMPLOYMENT_CATEGORY And
            TEMP.GRADE = MASTER.PERSON_TYPE_LEAVE And
            TEMP.CATEGORY = 'TEMPORARY'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # EXPORT TEST DATA
    if l_export == True:
        sr_filet = sr_file
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "X007da_leave_"
        sx_filet = sx_file + funcdate.cur_month()
        print("Export people birthday..." + sx_path + sx_filet)
        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        # Write the data
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # ADD DETAILS
    print("Add data details...")
    sr_file = "X007db_detail"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        FIND.ORG,
        FIND.LOC,
        FIND.EMPLOYEE_NUMBER,
        CASE
            WHEN FIND.LEAVEP Is Null THEN FIND.LEAVET
            ELSE FIND.LEAVEP
        END As LEAVE_PROP,
        FIND.SERVICE_START,
        FIND.PERSON_TYPE_LEAVE
    From
        X007da_leave FIND
    Where
        FIND.VALID = 'FALSE' And
        FIND.PERSON_TYPE_LEAVE <> 'EXTRAORDINARY APPOINTMENT'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_find = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_find) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_find) + " EMPL LEAVE CODE invalid finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X007dc_getprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,REMARK TEXT)")
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "empl_leave_invalid":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + \
                         row[
                             3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[
                             8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the impoted data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    sr_file = "X007dd_addprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
          FIND.*,
          'empl_leave_invalid' AS PROCESS,
          '%TODAY%' AS DATE_REPORTED,
          '%TODAYPLUS%' AS DATE_RETEST,
          PREV.PROCESS AS PREV_PROCESS,
          PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
          PREV.DATE_RETEST AS PREV_DATE_RETEST,
          PREV.REMARK
        From
          X007db_detail FIND Left Join
          X007dc_getprev PREV ON PREV.FIELD1 = FIND.EMPLOYEE_NUMBER AND
              PREV.DATE_RETEST >= Date('%TODAY%')          
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X007de_newprev"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
          FIND.PROCESS,
          FIND.EMPLOYEE_NUMBER As FIELD1,
          '' As FIELD2,
          '' AS FIELD3,
          '' AS FIELD4,
          '' AS FIELD5,
          FIND.DATE_REPORTED,
          FIND.DATE_RETEST,
          FIND.REMARK
        From
          X007dd_addprev FIND
        Where
          FIND.PREV_PROCESS Is Null
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_coun = funcsys.tablerowcount(so_curs, sr_file)
        if i_coun > 0:
            print("*** " + str(i_coun) + " Finding(s) to report ***")
            sr_filet = sr_file
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_coun) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                s_desc = "Leave code invalid"
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_find) + '/' + str(i_coun) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X007df_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          LOOKUP.LOOKUP,
          LOOKUP.LOOKUP_CODE AS TYPE,
          LOOKUP.LOOKUP_DESCRIPTION AS EMP,
          PERSON.NAME_ADDR AS NAME,
          PERSON.EMAIL_ADDRESS AS MAIL
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS LOOKUP
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR PERSON ON PERSON.EMPLOYEE_NUMBER = LOOKUP.LOOKUP_DESCRIPTION
        WHERE
          LOOKUP.LOOKUP = 'TEST_EMPL_LEAVECODE_INVALID_OFFICER'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X007dg_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0 and i_coun > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        SELECT
          LOOKUP.LOOKUP,
          LOOKUP.LOOKUP_CODE AS TYPE,
          LOOKUP.LOOKUP_DESCRIPTION AS EMP,
          PERSON.NAME_ADDR AS NAME,
          PERSON.EMAIL_ADDRESS AS MAIL
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS LOOKUP
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR PERSON ON PERSON.EMPLOYEE_NUMBER = LOOKUP.LOOKUP_DESCRIPTION
        WHERE
          LOOKUP.LOOKUP = 'TEST_EMPL_LEAVECODE_INVALID_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X007dh_contact"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0 and i_coun > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            FIND.ORG,
            FIND.LOC,
            FIND.EMPLOYEE_NUMBER,
            PEOP.NAME_LIST,
            PEOP.LEAVE_CODE,
            FIND.LEAVE_PROP,
            PEOP.EMPLOYMENT_CATEGORY,
            FIND.PERSON_TYPE_LEAVE,
            PEOP.ACAD_SUPP,
            PEOP.GRADE As PGRADE,
            PEOP.ASS_WEEK_LEN,
            PEOP.EMP_START,
            FIND.SERVICE_START,
            CAMP_OFF.EMP As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CAMP_OFF.MAIL As CAMP_OFF_MAIL,
            CAMP_SUP.EMP As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CAMP_SUP.MAIL As CAMP_SUP_MAIL,
            ORG_OFF.EMP As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            ORG_OFF.MAIL As ORG_OFF_MAIL,
            ORG_SUP.EMP As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            ORG_SUP.MAIL As ORG_SUP_MAIL
        From
            X007dd_addprev FIND Left Join
            X007df_officer CAMP_OFF On CAMP_OFF.TYPE = FIND.LOC Left Join
            X007df_officer ORG_OFF On ORG_OFF.TYPE = FIND.ORG Left Join
            X007dg_supervisor CAMP_SUP On CAMP_SUP.TYPE = FIND.LOC Left Join
            X007dg_supervisor ORG_SUP On ORG_SUP.TYPE = FIND.ORG Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = FIND.EMPLOYEE_NUMBER
        Where
            FIND.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X007dx_leavecode_invalid"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_find > 0 and i_coun > 0:
        print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'EMPLOYEE LEAVE CODE INVALID' As Audit_finding,
            FIND.EMPLOYEE_NUMBER As Employee,
            FIND.NAME_LIST As Name,
            FIND.LEAVE_CODE As Invalid_leave_code,
            FIND.LEAVE_PROP As Proposed_leave_code,
            FIND.EMP_START As Start_date,
            FIND.SERVICE_START As Service_start_date,
            FIND.PGRADE As Grade,
            FIND.EMPLOYMENT_CATEGORY As Category,
            FIND.PERSON_TYPE_LEAVE As Person_type,
            FIND.ACAD_SUPP As Acad_supp,
            FIND.ASS_WEEK_LEN As Workdays,
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail
        From
            X007dh_contact FIND
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export == True and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sr_filet = sr_file
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "People_test_007dx_leave_invalid_"
            sx_filet = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    BIO MASTER FILE
    *****************************************************************************"""

    # BUILD TABLE WITH EMPLOYEE BIO INFORMATION
    print("Obtain master list of all employees...")
    sr_file = "X008_bio_master"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PEOP.EMPLOYEE_NUMBER,
        PEOP.LOCATION_DESCRIPTION,
        PEOP.ADDRESS_POST,
        PEOP.ADDRESS_SARS,
        PEOP.ADDRESS_HOME,
        PEOP.ADDRESS_OTHE,
        PEOP.PHONE_WORK
    From
        PEOPLE.X002_PEOPLE_CURR PEOP
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    TEST PHONE WORK INVALID
    *****************************************************************************"""
    print("PHONE WORK INVALID")
    funcfile.writelog("PHONE WORK INVALID")

    # DECLARE TEST VARIABLES
    i_finding_after: int = 0

    # OBTAIN TEST DATA
    print("Obtain test data and add employee details...")
    sr_file: str = "X008aa_phone_work_invalid"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' AS ORG,
        CASE MASTER.LOCATION_DESCRIPTION
            WHEN 'MAFIKENG CAMPUS' THEN 'MAF'
            WHEN 'POTCHEFSTROOM CAMPUS' THEN 'POT'
            WHEN 'VAAL TRIANGLE CAMPUS' THEN 'VAA'
            ELSE 'NWU'
        END AS LOC,
        MASTER.EMPLOYEE_NUMBER As EMP,
        CASE
            WHEN Substr(MASTER.ADDRESS_POST,1,1) = 'Y' THEN 1
            ELSE 0
        END As PRIMARY_VALID,
        CASE 
            WHEN Substr(MASTER.ADDRESS_POST,1,1) = 'Y' AND
                Substr(MASTER.ADDRESS_POST,2) <> Substr(MASTER.ADDRESS_SARS,2) AND
                Substr(MASTER.ADDRESS_SARS,1,1) = 'N' THEN 1
            WHEN Substr(MASTER.ADDRESS_POST,1,1) = 'Y' AND
                Substr(MASTER.ADDRESS_POST,2) <> Substr(MASTER.ADDRESS_HOME,2) AND
                Substr(MASTER.ADDRESS_HOME,1,1) = 'N' THEN 2
            WHEN Substr(MASTER.ADDRESS_POST,1,1) = 'Y' AND
                Substr(MASTER.ADDRESS_POST,2) <> Substr(MASTER.ADDRESS_OTHE,2) AND
                Substr(MASTER.ADDRESS_OTHE,1,1) = 'N' THEN 3
            ELSE 0 
        END As SECONDARY_VALID,
        CASE
            WHEN Length(MASTER.PHONE_WORK) = 10 THEN 1
            ELSE 0
        END As PHONE_VALID,
        MASTER.PHONE_WORK
    From
        X008_bio_master MASTER
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X008ab_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        CURR.ORG,
        CURR.LOC,
        CURR.EMP,
        CURR.PHONE_WORK
    From
        X008aa_phone_work_invalid CURR
    Where
        CURR.PHONE_VALID = 0
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " EMPL PHONE WORK SARS invalid finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X008ac_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 INT,
            FIELD2 TEXT,
            FIELD3 TEXT,
            FIELD4 TEXT,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            REMARK TEXT)
            """)
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "phone_work_invalid":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + \
                         row[
                             3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[
                             8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the imported data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    sr_file = "X008ad_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'phone_work_invalid' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            X008ab_findings FIND Left Join
            X008ac_get_previous PREV ON PREV.FIELD1 = FIND.EMP AND
                PREV.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X008ae_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.EMP AS FIELD1,
            '' AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            X008ad_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                s_desc = "Phone (work) invalid"
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X008af_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if i_finding_after > 0:
            print("Import reporting officers for mail purposes...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                OFFICER.LOOKUP,
                OFFICER.LOOKUP_CODE AS CAMPUS,
                OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
                PEOP.NAME_ADDR As NAME,
                PEOP.EMAIL_ADDRESS
            From
                PEOPLE.X000_OWN_HR_LOOKUPS OFFICER Left Join
                PEOPLE.X002_PEOPLE_CURR PEOP ON
                    PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'TEST_PHONE_WORK_INVALID_OFFICER'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X008ag_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            SUPERVISOR.LOOKUP,
            SUPERVISOR.LOOKUP_CODE AS CAMPUS,
            SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            PEOPLE.X000_OWN_HR_LOOKUPS SUPERVISOR Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON 
                PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'TEST_PHONE_WORK_INVALID_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X008ah_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.EMP,
            PEOP.NAME_LIST,
            PREV.PHONE_WORK,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X008ad_add_previous PREV
            Left Join PEOPLE.X002_PEOPLE_CURR PEOP On PEOP.EMPLOYEE_NUMBER = PREV.EMP
            Left Join X008af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC
            Left Join X008af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
            Left Join X008ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC
            Left Join X008ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X008ax_phone_work_invalid"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'EMPLOYEE WORK PHONE INVALID' As Audit_finding,
            FIND.EMP AS Employee,
            FIND.NAME_LIST As Name,
            FIND.PHONE_WORK As Work_phone,
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail            

        From
            X008ah_detail FIND
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "People_test_008ax_phone_work_invalid_"
            sx_file_dated = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    TEST ADDRESS PRIMARY INVALID
    *****************************************************************************"""
    print("ADDRESS PRIMARY INVALID")
    funcfile.writelog("ADDRESS PRIMARY INVALID")

    # DECLARE TEST VARIABLES
    i_finding_after: int = 0

    # OBTAIN TEST DATA
    print("Obtain test data and add employee details...")
    sr_file: str = "X008ba_address_primary_invalid"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' AS ORG,
        CASE MASTER.LOCATION_DESCRIPTION
            WHEN 'MAFIKENG CAMPUS' THEN 'MAF'
            WHEN 'POTCHEFSTROOM CAMPUS' THEN 'POT'
            WHEN 'VAAL TRIANGLE CAMPUS' THEN 'VAA'
            ELSE 'NWU'
        END AS LOC,
        MASTER.EMPLOYEE_NUMBER As EMP,
        CASE
            WHEN Substr(MASTER.ADDRESS_POST,1,1) = 'Y' THEN 1
            ELSE 0
        END As PRIMARY_VALID,
        MASTER.ADDRESS_POST
    From
        X008_bio_master MASTER
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X008bb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        CURR.ORG,
        CURR.LOC,
        CURR.EMP,
        CURR.ADDRESS_POST
    From
        X008ba_address_primary_invalid CURR
    Where
        CURR.PRIMARY_VALID = 0
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " EMPL ADDRESS PRIMARY SARS invalid finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X008bc_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 INT,
            FIELD2 TEXT,
            FIELD3 TEXT,
            FIELD4 TEXT,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            REMARK TEXT)
            """)
        s_cols = ""
        co = open(ed_path + "001_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "address_primary_invalid":
                continue
            else:
                s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + \
                         row[
                             3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[
                             8] + "')"
                so_curs.execute(s_cols)
        so_conn.commit()
        # Close the imported data file
        co.close()
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    sr_file = "X008bd_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'address_primary_invalid' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.REMARK
        From
            X008bb_findings FIND Left Join
            X008bc_get_previous PREV ON PREV.FIELD1 = FIND.EMP AND
                PREV.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_monthendnext())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X008be_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.EMP AS FIELD1,
            '' AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.REMARK
        From
            X008bd_add_previous PREV
        Where
            PREV.PREV_PROCESS Is Null
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings to previous reported file
        i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
        if i_finding_after > 0:
            print("*** " + str(i_finding_after) + " Finding(s) to report ***")
            sx_path = ed_path
            sx_file = "001_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
            if l_mess:
                s_desc = "Address (post) not primary"
                funcsms.send_telegram('', 'administrator',
                                      '<b>' + str(i_finding_before) + '/' + str(i_finding_after) + '</b> ' + s_desc)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X008bf_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if i_finding_after > 0:
            print("Import reporting officers for mail purposes...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                OFFICER.LOOKUP,
                OFFICER.LOOKUP_CODE AS CAMPUS,
                OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
                PEOP.NAME_ADDR As NAME,
                PEOP.EMAIL_ADDRESS
            From
                PEOPLE.X000_OWN_HR_LOOKUPS OFFICER Left Join
                PEOPLE.X002_PEOPLE_CURR PEOP ON
                    PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'TEST_ADDRESS_PRIMARY_INVALID_OFFICER'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X008bg_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            SUPERVISOR.LOOKUP,
            SUPERVISOR.LOOKUP_CODE AS CAMPUS,
            SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            PEOPLE.X000_OWN_HR_LOOKUPS SUPERVISOR Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON 
                PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'TEST_ADDRESS_PRIMARY_INVALID_SUPERVISOR'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X008bh_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.EMP,
            PEOP.NAME_LIST,
            PREV.ADDRESS_POST,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X008bd_add_previous PREV
            Left Join PEOPLE.X002_PEOPLE_CURR PEOP On PEOP.EMPLOYEE_NUMBER = PREV.EMP
            Left Join X008bf_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC
            Left Join X008bf_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
            Left Join X008bg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC
            Left Join X008bg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X008bx_address_primary_invalid"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'EMPLOYEE POST ADDRESS NOT PRIMARY' As Audit_finding,
            FIND.EMP AS Employee,
            FIND.NAME_LIST As Name,
            FIND.ADDRESS_POST As Address_post,
            FIND.CAMP_OFF_NAME AS Responsible_Officer,
            FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
            FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
            FIND.CAMP_SUP_NAME AS Supervisor,
            FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
            FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
            FIND.ORG_OFF_NAME AS Org_Officer,
            FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
            FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
            FIND.ORG_SUP_NAME AS Org_Supervisor,
            FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail            

        From
            X008bh_detail FIND
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "People_test_008ax_phone_work_invalid_"
            sx_file_dated = sx_file + funcdate.today_file()
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
            funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
    else:
        s_sql = "CREATE TABLE " + sr_file + " (" + """
        BLANK TEXT
        );"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    # if l_mess:
    #     funcsms.send_telegram("", "administrator", "Finished <b>" + s_function + "</b> tests.")

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
        people_test_masterfile()
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, "C001_people_test_masterfile", "C001_people_test_masterfile")
