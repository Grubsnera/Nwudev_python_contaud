""" Script to test PEOPLE master file data *************************************
Created on: 1 Mar 2019
Author: Albert J v Rensburg (NWU21162395)
*****************************************************************************"""

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT

TEST ZA ID EXIST
TEST ZA ID INVALID (Incomplete - test only - no reporting)
TEST ZA DATE OF BIRTH INVALID (Incomplete - test only - no reporting)
TEST ZA GENDER INVALID (Incomplete - test only - no reporting)
TEST ZA ID DUPLICATE (Incomplete - test only - no reporting)

PASSPORT NUMBER MASTER FILE
TEST PASSPORT NUMBER BLANK

BANK NUMBER MASTER FILE
TEST BANK NUMBER DUPLICATE

PAYE NUMBER MASTER FILE
TEST PAYE NUMBER BLANK
TEST PAYE NUMBER INVALID

END OF SCRIPT
*****************************************************************************"""

def People_test_masterfile():

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # IMPORT PYTHON MODULES
    import csv
    import datetime
    import sqlite3
    import sys

    # ADD OWN MODULE PATH
    sys.path.append('S:/_my_modules')

    # IMPORT OWN MODULES
    import funccsv
    import funcdate
    import funcfile
    import funcmail
    import funcmysql
    import funcpeople
    import funcstr
    import funcsys

    # OPEN THE SCRIPT LOG FILE
    print("---------------------------")    
    print("C001_PEOPLE_TEST_MASTERFILE")
    print("---------------------------")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C001_PEOPLE_TEST_MASTERFILE")
    funcfile.writelog("-----------------------------------")
    ilog_severity = 1

    # DECLARE VARIABLES
    so_path = "W:/People/" #Source database path
    re_path = "R:/People/" # Results path
    ed_path = "S:/_external_data/" #external data path
    so_file = "People_test_masterfile.sqlite" # Source database
    s_sql = "" # SQL statements
    l_export = True
    l_mail = False
    l_record = False

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN THE WORKING DATABASE
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("%t OPEN DATABASE: PEOPLE_TEST_MASTERFILE.SQLITE")

    # ATTACH DATA SOURCES
    so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """ ****************************************************************************
    TEST ZA ID EXIST
    *****************************************************************************"""
    print("TEST ZA ID EXIST")
    funcfile.writelog("TEST ZA ID EXIST")

    # IMPORT BASIC ID NUMBER DATA FROM MASTER FILE
    print("Import people id number master file data...")
    sr_file = "X001_people_id_master"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        'NWU' AS ORG,
        CASE LOCATION_DESCRIPTION
            WHEN 'Mafikeng Campus' THEN 'MAF'
            WHEN 'Potchefstroom Campus' THEN 'POT'
            WHEN 'Vaal Triangle Campus' THEN 'VAA'
            ELSE 'NWU'
        END AS LOC,
        PEOPLE.X002_PEOPLE_CURR.OE_CODE,
        PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER,
        PEOPLE.X002_PEOPLE_CURR.FULL_NAME,
        PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
        PEOPLE.X002_PEOPLE_CURR.DATE_OF_BIRTH,
        PEOPLE.X002_PEOPLE_CURR.NATIONALITY,
        PEOPLE.X002_PEOPLE_CURR.IDNO,
        PEOPLE.X002_PEOPLE_CURR.SEX,
        PEOPLE.X002_PEOPLE_CURR.MAILTO
    From
        PEOPLE.X002_PEOPLE_CURR
    Where
        PEOPLE.X002_PEOPLE_CURR.NATIONALITY = 'SAF'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # IDENTIFY BLANK ID NUMBERS
    print("Identify blank ID numbers...")
    print("Identifier: id_blank")
    sr_file = "X002aa_id_blank"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X001_people_id_master.*
    From
        X001_people_id_master
    Where
        X001_people_id_master.IDNO = ''
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # IMPORT PREVIOUS REPORTED FINDINGS
    print("Import previously reported findings...")
    tb_name = "X002ab_impo_reported"
    so_curs.execute("DROP TABLE IF EXISTS " + tb_name)
    so_curs.execute("CREATE TABLE " + tb_name + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT)")
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
            s_cols = "INSERT INTO " + tb_name + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the impoted data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_reported.txt (" + tb_name + ")")

    # JOIN THE NEW FINDING WITH OLD FINDINGS
    print("Join previously reported to current findings...")
    sr_file = "X002ac_join_prev_reported"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
      X002aa_id_blank.*,
      X002ab_impo_reported.PROCESS AS PREV_PROCESS,
      X002ab_impo_reported.DATE_REPORTED AS PREV_DATE_REPORTED,
      X002ab_impo_reported.DATE_RETEST AS PREV_DATE_RETEST,
      'id_blank' AS PROCESS,
      '%TODAY%' AS DATE_REPORTED,
      '%TODAY+14%' AS DATE_RETEST
    FROM
      X002aa_id_blank
      LEFT JOIN X002ab_impo_reported ON X002ab_impo_reported.FIELD1 = X002aa_id_blank.EMPLOYEE_NUMBER AND
        X002ab_impo_reported.DATE_RETEST >= Date('%TODAY%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%",funcdate.today())
    s_sql = s_sql.replace("%TODAY+14%",funcdate.today_plusdays(30))
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE PREVIOUS FINDINGS FILE
    print("Add new findings to previous reported...")
    sr_file = "X002ad_add_prev_reported"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X002ac_join_prev_reported.PROCESS,
      X002ac_join_prev_reported.EMPLOYEE_NUMBER AS FIELD1,
      '' AS FIELD2,
      '' AS FIELD3,
      '' AS FIELD4,
      '' AS FIELD5,
      X002ac_join_prev_reported.DATE_REPORTED,
      X002ac_join_prev_reported.DATE_RETEST
    FROM
      X002ac_join_prev_reported
    WHERE
      X002ac_join_prev_reported.PREV_PROCESS IS NULL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
    # Export findings to previous reported file
    i_coun = funcsys.tablerowcount(so_curs,sr_file)
    if i_coun > 0:
        print("*** " +str(i_coun)+ " Findings to report ***")    
        sr_filet = sr_file
        sx_path = ed_path
        sx_file = "001_reported"
        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        # Write the data
        if l_record == True:
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
            funcfile.writelog("%t FINDING: "+str(i_coun)+" new findings to report")        
            funcfile.writelog("%t EXPORT DATA: "+sr_file)
    else:
        print("*** No new findings to report ***")
        funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT MAILTO CONTACT DETAILS
    print("Import mailto contact details...")
    sr_file = "X002ae_impo_mailto"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      X002ac_join_prev_reported.*,
      PEOPLE.X002_PEOPLE_CURR.MAILTO AS MAIL_NUMB,
      PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME AS MAIL_NAME,
      PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS AS MAIL_MAIL
    FROM
      X002ac_join_prev_reported
      LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X002ac_join_prev_reported.MAILTO
    WHERE
      X002ac_join_prev_reported.PREV_PROCESS IS NULL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT OFFICERS
    print("Import reporting officers for mail purposes...")
    sr_file = "X002af_impo_report_officer"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
      PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
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

    # IMPORT SUPERVISORS
    print("Import reporting supervisors for mail purposes...")
    sr_file = "X002ag_impo_report_supervisor"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP,
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_CODE AS CAMPUS,
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
      PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
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
    print("Add contact details to findings...")
    sr_file = "X002ah_join_contact"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        X002ae_impo_mailto.*,
        CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
        CAMP_OFF.KNOWN_NAME As CAMP_OFF_NAME,
        CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
        CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
        CAMP_SUP.KNOWN_NAME As CAMP_SUP_NAME,
        CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
        ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
        ORG_OFF.KNOWN_NAME As ORG_OFF_NAME,
        ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
        ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
        ORG_SUP.KNOWN_NAME As ORG_SUP_NAME,
        ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
    From
        X002ae_impo_mailto
        Left Join X002af_impo_report_officer CAMP_OFF On CAMP_OFF.CAMPUS = X002ae_impo_mailto.LOC
        Left Join X002af_impo_report_officer ORG_OFF On ORG_OFF.CAMPUS = X002ae_impo_mailto.ORG
        Left Join X002ag_impo_report_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = X002ae_impo_mailto.LOC
        Left Join X002ag_impo_report_supervisor ORG_SUP On ORG_SUP.CAMPUS = X002ae_impo_mailto.ORG
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS X002ai_join_contact")
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL REPORT
    print("Build the final report...")
    sr_file = "X002ax_id_blank"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        X002ah_join_contact.ORG AS ORGANIZATION,
        X002ah_join_contact.LOC AS LOCATION,
        X002ah_join_contact.EMPLOYEE_NUMBER,
        X002ah_join_contact.FULL_NAME,
        X002ah_join_contact.NATIONALITY,
        X002ah_join_contact.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
        X002ah_join_contact.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
        X002ah_join_contact.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
        X002ah_join_contact.CAMP_SUP_NAME AS SUPERVISOR,
        X002ah_join_contact.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
        X002ah_join_contact.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
        X002ah_join_contact.ORG_OFF_NAME AS ORGANIZATION_OFFICER,
        X002ah_join_contact.ORG_OFF_NUMB AS ORGANIZATION_OFFICER_NUMB,
        X002ah_join_contact.ORG_OFF_MAIL AS ORGANIZATION_OFFICER_MAIL,
        X002ah_join_contact.ORG_SUP_NAME AS ORGANIZATION_SUPERVISOR,
        X002ah_join_contact.ORG_SUP_NUMB AS ORGANIZATION_SUPERVISOR_NUMB,
        X002ah_join_contact.ORG_SUP_MAIL AS ORGANIZATION_SUPERVISOR_MAIL,
        X002ah_join_contact.MAIL_NAME AS OE_CODE_OFFICER,
        X002ah_join_contact.MAIL_NUMB AS OE_CODE_OFFICER_NUMB,
        X002ah_join_contact.MAIL_MAIL AS OE_CODE_OFFICER_MAIL    
    From
        X002ah_join_contact
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
        sx_file = "People_test_002ax_idblank_"
        sx_filet = sx_file + funcdate.today_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
        funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    """ ****************************************************************************
    TEST ZA ID INVALID
    *****************************************************************************"""
    print("TEST ZA ID INVALID")
    funcfile.writelog("TEST ZA ID INVALID")

    # BUILD TABLE WITH NOT EMPTY ID NUMBERS
    print("Build not empty ID number table...")
    sr_file = "X002ba_id_invalid"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X001_people_id_master.*,
        SUBSTR(IDNO,1,1)+SUBSTR(IDNO,3,1)+SUBSTR(IDNO,5,1)+SUBSTR(IDNO,7,1)+SUBSTR(IDNO,9,1)+SUBSTR(IDNO,11,1) AS ODDT,
        (SUBSTR(IDNO,2,1)||SUBSTR(IDNO,4,1)||SUBSTR(IDNO,6,1)||SUBSTR(IDNO,8,1)||SUBSTR(IDNO,10,1)||SUBSTR(IDNO,12,1))*2 AS EVEC,
        0 AS EVET,
        '' AS CONT,
        '' AS VALID
    From
        X001_people_id_master
    Where
        X001_people_id_master.IDNO <> ''
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
    # UPDATE COLUMNS
    print("Update column even totals...")
    so_curs.execute("UPDATE X002ba_id_invalid SET EVET = SUBSTR(EVEC,1,1)+SUBSTR(EVEC,2,1)+SUBSTR(EVEC,3,1)+SUBSTR(EVEC,4,1)+SUBSTR(EVEC,5,1)+SUBSTR(EVEC,6,1)+SUBSTR(EVEC,7,1);")
    print("Update column control total...")
    so_curs.execute("UPDATE X002ba_id_invalid SET CONT = SUBSTR(10-SUBSTR(ODDT+EVET,-1,1),-1,1);")
    print("Update column valid...")
    so_curs.execute("UPDATE X002ba_id_invalid " + """
                     SET VALID =
                     CASE
                         WHEN SUBSTR(IDNO,13) = CONT THEN 'T'
                         ELSE 'F'
                     END;""")
    so_conn.commit()

    """ ****************************************************************************
    TEST ZA DATE OF BIRTH INVALID
    *****************************************************************************"""
    print("TEST ZA DATE OF BIRTH INVALID")
    funcfile.writelog("TEST ZA DATE OF BIRTH INVALID")

    # BUILD TABLE WITH NOT EMPTY ID NUMBERS
    print("Build not empty ID number table...")
    sr_file = "X002ca_dateofbirth_invalid"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X001_people_id_master.*,
        SUBSTR(IDNO,1,2)||'-'||SUBSTR(IDNO,3,2)||'-'||SUBSTR(IDNO,5,2) AS DOBC,
        '' AS VALID
    From
        X001_people_id_master
    Where
        X001_people_id_master.IDNO <> ''
    ;"""

    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
    # UPDATE COLUMNS
    print("Update column valid...")
    so_curs.execute("UPDATE X002ca_dateofbirth_invalid " + """
                     SET VALID =
                     CASE
                         WHEN SUBSTR(DATE_OF_BIRTH,3,8) = DOBC THEN 'T'
                         ELSE 'F'
                     END;""")
    so_conn.commit()

    """ ****************************************************************************
    TEST ZA GENDER INVALID
    *****************************************************************************"""
    print("TEST ZA GENDER INVALID")
    funcfile.writelog("TEST ZA GENDER INVALID")

    # BUILD TABLE WITH NOT EMPTY ID NUMBERS
    print("Build not empty ID number table...")
    sr_file = "X002da_gender_invalid"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X001_people_id_master.*,
        CASE
            WHEN CAST(SUBSTR(IDNO,7,1) AS INT) >= 5 THEN 'M'
            WHEN CAST(SUBSTR(IDNO,7,1) AS INT) >= 0 THEN 'F'
            ELSE 'U'
        END AS GENDER,
        '' AS VALID
    From
        X001_people_id_master
    Where
        X001_people_id_master.IDNO <> ''
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
    # UPDATE COLUMNS
    print("Update column valid...")
    so_curs.execute("UPDATE X002da_gender_invalid " + """
                     SET VALID =
                     CASE
                         WHEN SEX = GENDER THEN 'T'
                         ELSE 'F'
                     END;""")
    so_conn.commit()

    """ ****************************************************************************
    TEST ZA ID DUPLICATE
    *****************************************************************************"""
    print("TEST ZA ID DUPLICATE")
    funcfile.writelog("TEST ZA ID DUPLICATE")

    # BUILD TABLE COUNTING ID NUMBERS
    print("Build counting ID number table...")
    sr_file = "X002ea_id_duplicate_count"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X001_people_id_master.IDNO,
        Count(X001_people_id_master.EMPLOYEE_NUMBER) As COUNT
    From
        X001_people_id_master
    Where
        IDNO <> ''
    Group By
        X001_people_id_master.IDNO
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # IDENTIFY DUPLICATES
    print("Identify duplicates...")
    sr_file = "X002eb_id_duplicates"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X002ea_id_duplicate_count.IDNO,
        X002ea_id_duplicate_count.COUNT
    From
        X002ea_id_duplicate_count
    Where
        X002ea_id_duplicate_count.COUNT > 1
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)














    """ ****************************************************************************
    PASSPORT NUMBER MASTER FILE
    *****************************************************************************"""

    # BUILD TABLE WITH EMPLOYEE PASSPORT
    print("Obtain master list of all employees...")
    sr_file = "X003_pass_master"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        'NWU' AS ORG,
        CASE LOCATION_DESCRIPTION
            WHEN 'Mafikeng Campus' THEN 'MAF'
            WHEN 'Potchefstroom Campus' THEN 'POT'
            WHEN 'Vaal Triangle Campus' THEN 'VAA'
            ELSE 'NWU'
        END AS LOC,
        PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER AS EMP,
        PEOPLE.X002_PEOPLE_CURR.PASSPORT AS NUMB,
        PEOPLE.X002_PEOPLE_CURR.NATIONALITY AS NAT
    From
        PEOPLE.X002_PEOPLE_CURR
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
    #l_record = True # Record the findings in the previous reported findings file
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
    funcfile.writelog("%t FINDING: "+str(i_find)+" PASSPORT blank finding(s)")

    # GET PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X003ac_pass_getprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,DATE_MAILED TEXT)")
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

    # ADD PREVIOUS FINDINGS
    # NOTE ADD CODE
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
          X003ac_pass_getprev.PROCESS AS PREV_PROCESS,
          X003ac_pass_getprev.DATE_REPORTED AS PREV_DATE_REPORTED,
          X003ac_pass_getprev.DATE_RETEST AS PREV_DATE_RETEST,
          X003ac_pass_getprev.DATE_MAILED
        FROM
          X003ab_pass_blank
          LEFT JOIN X003ac_pass_getprev ON X003ac_pass_getprev.FIELD1 = X003ab_pass_blank.EMP AND
              X003ac_pass_getprev.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%",funcdate.today_plusdays(30))
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
          X003ad_pass_addprev.PROCESS,
          X003ad_pass_addprev.EMP AS FIELD1,
          '' AS FIELD2,
          '' AS FIELD3,
          '' AS FIELD4,
          '' AS FIELD5,
          X003ad_pass_addprev.DATE_REPORTED,
          X003ad_pass_addprev.DATE_RETEST,
          X003ad_pass_addprev.DATE_MAILED
        FROM
          X003ad_pass_addprev
        WHERE
          X003ad_pass_addprev.PREV_PROCESS IS NULL
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
            if l_record == True:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
                funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
                funcfile.writelog("%t EXPORT DATA: "+sr_file)
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
          PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
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
          PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
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
            CAMP_OFF.KNOWN_NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.KNOWN_NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.KNOWN_NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.KNOWN_NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X003ad_pass_addprev
            Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X003ad_pass_addprev.EMP
            Left Join X003af_offi CAMP_OFF On CAMP_OFF.CAMPUS = X003ad_pass_addprev.LOC
            Left Join X003af_offi ORG_OFF On ORG_OFF.CAMPUS = X003ad_pass_addprev.ORG
            Left Join X003ag_supe CAMP_SUP On CAMP_SUP.CAMPUS = X003ad_pass_addprev.LOC
            Left Join X003ag_supe ORG_SUP On ORG_SUP.CAMPUS = X003ad_pass_addprev.ORG
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X003ax_pass_fina"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
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











    """ ****************************************************************************
    BANK NUMBER MASTER FILE
    *****************************************************************************"""

    # BUILD TABLE WITH EMPLOYEE PAYE NUMBERS
    print("Obtain master list of all employees...")
    sr_file = "X004_bank_master"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        'NWU' AS ORG,
        CASE LOCATION_DESCRIPTION
            WHEN 'Mafikeng Campus' THEN 'MAF'
            WHEN 'Potchefstroom Campus' THEN 'POT'
            WHEN 'Vaal Triangle Campus' THEN 'VAA'
            ELSE 'NWU'
        END AS LOC,
        PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER AS EMP,
        PEOPLE.X002_PEOPLE_CURR.ACC_TYPE,
        PEOPLE.X002_PEOPLE_CURR.ACC_BRANCH,
        PEOPLE.X002_PEOPLE_CURR.ACC_NUMBER,
        PEOPLE.X002_PEOPLE_CURR.ACC_RELATION
    From
        PEOPLE.X002_PEOPLE_CURR
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
            
    """ ****************************************************************************
    TEST BANK NUMBER DUPLICATE
        NOTE 01: SELECT ALL CURRENT EMPLOYEES WITH BANK NUMBERS
    *****************************************************************************"""
    print("TEST BANK NUMBER DUPLICATE")
    funcfile.writelog("TEST BANK NUMBER DUPLICATE")

    # DECLARE TEST VARIABLES
    #l_record = True # Record the findings in the previous reported findings file
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
    funcfile.writelog("%t FINDING: "+str(i_find)+" BANK invalid finding(s)")

    # GET PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X004ac_bank_getprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,DATE_MAILED TEXT)")
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

    # ADD PREVIOUS FINDINGS
    # NOTE ADD CODE
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
          X004ac_bank_getprev.PROCESS AS PREV_PROCESS,
          X004ac_bank_getprev.DATE_REPORTED AS PREV_DATE_REPORTED,
          X004ac_bank_getprev.DATE_RETEST AS PREV_DATE_RETEST,
          X004ac_bank_getprev.DATE_MAILED
        FROM
          X004ab_bank_dupl
          LEFT JOIN X004ac_bank_getprev ON X004ac_bank_getprev.FIELD1 = X004ab_bank_dupl.EMP AND
              X004ac_bank_getprev.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%",funcdate.today_plusdays(20000))
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X004ae_bank_newprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        s_sql = "CREATE TABLE "+sr_file+" AS " + """
        SELECT
          X004ad_bank_addprev.PROCESS,
          X004ad_bank_addprev.EMP AS FIELD1,
          '' AS FIELD2,
          '' AS FIELD3,
          '' AS FIELD4,
          '' AS FIELD5,
          X004ad_bank_addprev.DATE_REPORTED,
          X004ad_bank_addprev.DATE_RETEST,
          X004ad_bank_addprev.DATE_MAILED
        FROM
          X004ad_bank_addprev
        WHERE
          X004ad_bank_addprev.PREV_PROCESS IS NULL
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
            if l_record == True:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
                funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
                funcfile.writelog("%t EXPORT DATA: "+sr_file)
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
          PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
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
          PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
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
            CAMP_OFF.KNOWN_NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.KNOWN_NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.KNOWN_NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.KNOWN_NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X004ad_bank_addprev
            Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X004ad_bank_addprev.EMP
            Left Join X004af_offi CAMP_OFF On CAMP_OFF.CAMPUS = X004ad_bank_addprev.LOC
            Left Join X004af_offi ORG_OFF On ORG_OFF.CAMPUS = X004ad_bank_addprev.ORG
            Left Join X004ag_supe CAMP_SUP On CAMP_SUP.CAMPUS = X004ad_bank_addprev.LOC
            Left Join X004ag_supe ORG_SUP On ORG_SUP.CAMPUS = X004ad_bank_addprev.ORG
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X004ax_bank_fina"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            X004ah_bank_cont.ORG AS ORGANIZATION,
            X004ah_bank_cont.LOC AS LOCATION,
            X004ah_bank_cont.EMP AS EMPLOYEE_NUMBER,
            X004ah_bank_cont.NAME,
            X004ah_bank_cont.ACC_TYPE,
            X004ah_bank_cont.ACC_BRANCH,
            X004ah_bank_cont.ACC_NUMBER,
            X004ah_bank_cont.ACC_RELATION,
            X004ah_bank_cont.COUNT,
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











    """ ****************************************************************************
    PAYE NUMBER MASTER FILE
    *****************************************************************************"""

    # BUILD TABLE WITH EMPLOYEE PAYE NUMBERS
    print("Obtain master list of all employees...")
    sr_file = "X005_paye_master"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        'NWU' AS ORG,
        CASE LOCATION_DESCRIPTION
            WHEN 'Mafikeng Campus' THEN 'MAF'
            WHEN 'Potchefstroom Campus' THEN 'POT'
            WHEN 'Vaal Triangle Campus' THEN 'VAA'
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
    TEST PAYE NUMBER BLANK
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
    print("TEST PAYE NUMBER BLANK")
    funcfile.writelog("TEST PAYE NUMBER BLANK")

    # DECLARE TEST VARIABLES
    #l_record = True # Record the findings in the previous reported findings file
    i_find = 0 # Number of findings before previous reported findings
    i_coun = 0 # Number of new findings to report

    # SELECT ALL EMPLOYEES WITHOUT A PAYE NUMBER
    print("Select all employees with paye number...")
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

    # SELECT ALL EMPLOYEES WITH AN INVALID PAYE NUMBER
    print("Select all employees with an invalid paye number...")
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
    funcfile.writelog("%t FINDING: "+str(i_find)+" PAYE invalid finding(s)")

    # GET PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X005ac_paye_getprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,DATE_MAILED TEXT)")
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
          X005ac_paye_getprev.DATE_MAILED
        FROM
          X005ab_paye_blank
          LEFT JOIN X005ac_paye_getprev ON X005ac_paye_getprev.FIELD1 = X005ab_paye_blank.EMP AND
              X005ac_paye_getprev.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%",funcdate.today_plusdays(30))
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
          X005ad_paye_addprev.DATE_MAILED
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
            if l_record == True:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
                funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
                funcfile.writelog("%t EXPORT DATA: "+sr_file)
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
          PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_PAYE_BLANK_OFFICER'
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
          PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_PAYE_BLANK_SUPERVISOR'
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
            CAMP_OFF.KNOWN_NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.KNOWN_NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.KNOWN_NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.KNOWN_NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X005ad_paye_addprev
            Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X005ad_paye_addprev.EMP
            Left Join X005af_offi CAMP_OFF On CAMP_OFF.CAMPUS = X005ad_paye_addprev.LOC
            Left Join X005af_offi ORG_OFF On ORG_OFF.CAMPUS = X005ad_paye_addprev.ORG
            Left Join X005ag_supe CAMP_SUP On CAMP_SUP.CAMPUS = X005ad_paye_addprev.LOC
            Left Join X005ag_supe ORG_SUP On ORG_SUP.CAMPUS = X005ad_paye_addprev.ORG
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X005ax_paye_fina"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0 and i_coun > 0:
        print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
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

    """ ****************************************************************************
    TEST PAYE NUMBER INVALID
        NOTE 01: SELECT ALL CURRENT EMPLOYEES WITH PAYE TAX NUMBER
    *****************************************************************************"""
    print("TEST PAYE NUMBER INVALID")
    funcfile.writelog("TEST PAYE NUMBER INVALID")

    # DECLARE TEST VARIABLES
    #l_record = True # Record the findings in the previous reported findings file
    i_find = 0 # Number of findings before previous reported findings
    i_coun = 0 # Number of new findings to report

    # SELECT ALL EMPLOYEES WITH A PAYE TAX NUMBER
    print("Select all employees with paye number...")
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

    # SELECT ALL EMPLOYEES WITH AN INVALID PAYE NUMBER
    print("Select all employees with an invalid paye number...")
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
    funcfile.writelog("%t FINDING: "+str(i_find)+" PAYE invalid finding(s)")

    # GET PREVIOUS FINDINGS
    # NOTE ADD CODE
    sr_file = "X005bc_paye_getprev"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    if i_find > 0:
        print("Import previously reported findings...")
        so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,DATE_MAILED TEXT)")
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
          X005bc_paye_getprev.DATE_MAILED
        FROM
          X005bb_paye_inva
          LEFT JOIN X005bc_paye_getprev ON X005bc_paye_getprev.FIELD1 = X005bb_paye_inva.EMP AND
              X005bc_paye_getprev.DATE_RETEST >= Date('%TODAY%')
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%",funcdate.today())
        s_sql = s_sql.replace("%TODAYPLUS%",funcdate.today_plusdays(30))
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
          X005bd_paye_addprev.DATE_MAILED
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
            if l_record == True:
                funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
                funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
                funcfile.writelog("%t EXPORT DATA: "+sr_file)
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
          PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_PAYE_INVALID_OFFICER'
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
          PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
          PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS
        FROM
          PEOPLE.X000_OWN_HR_LOOKUPS
          LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP_DESCRIPTION
        WHERE
          PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_PAYE_INVALID_SUPERVISOR'
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
            CAMP_OFF.KNOWN_NAME As CAMP_OFF_NAME,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.KNOWN_NAME As CAMP_SUP_NAME,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.KNOWN_NAME As ORG_OFF_NAME,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.KNOWN_NAME As ORG_SUP_NAME,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL
        From
            X005bd_paye_addprev
            Left Join PEOPLE.X002_PEOPLE_CURR On PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X005bd_paye_addprev.EMP
            Left Join X005bf_offi CAMP_OFF On CAMP_OFF.CAMPUS = X005bd_paye_addprev.LOC
            Left Join X005bf_offi ORG_OFF On ORG_OFF.CAMPUS = X005bd_paye_addprev.ORG
            Left Join X005bg_supe CAMP_SUP On CAMP_SUP.CAMPUS = X005bd_paye_addprev.LOC
            Left Join X005bg_supe ORG_SUP On ORG_SUP.CAMPUS = X005bd_paye_addprev.ORG
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X005bx_paye_fina"
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute("DROP TABLE IF EXISTS X005bx_paye_cont")
    if i_find > 0 and i_coun > 0:
        print("Build the final report")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
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

    """ ****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    print("Vacuum the database...")
    so_conn.commit()
    so_conn.execute('VACUUM')
    funcfile.writelog("%t DATABASE: Vacuum")
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("--------------------------------------")
    funcfile.writelog("COMPLETED: C001_PEOPLE_TEST_MASTERFILE")

    return
