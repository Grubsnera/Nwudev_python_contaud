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
TEST PASSPORT BLANK
TEST BANK ACC DUPLICATE
TEST TAX NUMBER BLANK
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
    TEST PASSPORT BLANK
    *****************************************************************************"""
    print("TEST PASSPORT BLANK")
    funcfile.writelog("TEST PASSPORT BLANK")

    # IMPORT BASIC PASSPORT DATA FROM MASTER FILE
    print("Import people passport master file data...")
    sr_file = "X001_people_passport_master"
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
        PEOPLE.X002_PEOPLE_CURR.NATIONALITY_NAME,
        PEOPLE.X002_PEOPLE_CURR.IDNO,
        PEOPLE.X002_PEOPLE_CURR.PASSPORT,
        PEOPLE.X002_PEOPLE_CURR.PERMIT,
        PEOPLE.X002_PEOPLE_CURR.SEX,
        PEOPLE.X002_PEOPLE_CURR.MAILTO
    From
        PEOPLE.X002_PEOPLE_CURR
    Where
        PEOPLE.X002_PEOPLE_CURR.NATIONALITY <> 'SAF'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # IDENTIFY BLANK PASSPORT NUMBERS
    print("Identify blank passport numbers...")
    print("Identifier: pass_blank")
    sr_file = "X003aa_pass_blank"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X001_people_passport_master.*
    From
        X001_people_passport_master
    Where
        X001_people_passport_master.PASSPORT = ''
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # IMPORT PREVIOUS REPORTED FINDINGS
    print("Import previously reported findings...")
    tb_name = "X003ab_impo_reported"
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
        elif row[0] != "pass_blank":
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
    sr_file = "X003ac_join_prev_reported"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
      X003aa_pass_blank.*,
      X003ab_impo_reported.PROCESS AS PREV_PROCESS,
      X003ab_impo_reported.DATE_REPORTED AS PREV_DATE_REPORTED,
      X003ab_impo_reported.DATE_RETEST AS PREV_DATE_RETEST,
      'pass_blank' AS PROCESS,
      '%TODAY%' AS DATE_REPORTED,
      '%TODAY+14%' AS DATE_RETEST
    FROM
      X003aa_pass_blank
      LEFT JOIN X003ab_impo_reported ON X003ab_impo_reported.FIELD1 = X003aa_pass_blank.EMPLOYEE_NUMBER AND
        X003ab_impo_reported.DATE_RETEST >= Date('%TODAY%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%",funcdate.today())
    s_sql = s_sql.replace("%TODAY+14%",funcdate.today_plusdays(30))
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE PREVIOUS FINDINGS FILE
    print("Add new findings to previous reported...")
    sr_file = "X003ad_add_prev_reported"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X003ac_join_prev_reported.PROCESS,
      X003ac_join_prev_reported.EMPLOYEE_NUMBER AS FIELD1,
      '' AS FIELD2,
      '' AS FIELD3,
      '' AS FIELD4,
      '' AS FIELD5,
      X003ac_join_prev_reported.DATE_REPORTED,
      X003ac_join_prev_reported.DATE_RETEST
    FROM
      X003ac_join_prev_reported
    WHERE
      X003ac_join_prev_reported.PREV_PROCESS IS NULL
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
            funcfile.writelog("%t EXPORT FILE: "+sr_file)
    else:
        print("*** No new findings to report ***")
        funcfile.writelog("%t FINDING: No new findings to report")

    # IMPORT MAILTO CONTACT DETAILS
    print("Import mailto contact details...")
    sr_file = "X003ae_impo_mailto"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      X003ac_join_prev_reported.*,
      PEOPLE.X002_PEOPLE_CURR.MAILTO AS MAIL_NUMB,
      PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME AS MAIL_NAME,
      PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS AS MAIL_MAIL
    FROM
      X003ac_join_prev_reported
      LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X003ac_join_prev_reported.MAILTO
    WHERE
      X003ac_join_prev_reported.PREV_PROCESS IS NULL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
        
    # IMPORT OFFICERS
    print("Import reporting officers for mail purposes...")
    sr_file = "X003af_impo_report_officer"
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
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_PASS_BLANK_OFFICER'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)        

    # IMPORT SUPERVISORS
    print("Import reporting supervisors for mail purposes...")
    sr_file = "X003ag_impo_report_supervisor"
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
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_PASS_BLANK_SUPERVISOR'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    print("Add contact details to findings...")
    sr_file = "X003ah_join_contact"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        X003ae_impo_mailto.*,
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
        X003ae_impo_mailto
        Left Join X003af_impo_report_officer CAMP_OFF On CAMP_OFF.CAMPUS = X003ae_impo_mailto.LOC
        Left Join X003af_impo_report_officer ORG_OFF On ORG_OFF.CAMPUS = X003ae_impo_mailto.ORG
        Left Join X003ag_impo_report_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = X003ae_impo_mailto.LOC
        Left Join X003ag_impo_report_supervisor ORG_SUP On ORG_SUP.CAMPUS = X003ae_impo_mailto.ORG
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS X002ai_join_contact")
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL REPORT
    print("Build the final report...")
    sr_file = "X003ax_pass_blank"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        X003ah_join_contact.ORG AS ORGANIZATION,
        X003ah_join_contact.LOC AS LOCATION,
        X003ah_join_contact.EMPLOYEE_NUMBER,
        X003ah_join_contact.FULL_NAME,
        X003ah_join_contact.NATIONALITY_NAME AS NATIONALITY,
        X003ah_join_contact.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
        X003ah_join_contact.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
        X003ah_join_contact.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
        X003ah_join_contact.CAMP_SUP_NAME AS SUPERVISOR,
        X003ah_join_contact.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
        X003ah_join_contact.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
        X003ah_join_contact.ORG_OFF_NAME AS ORGANIZATION_OFFICER,
        X003ah_join_contact.ORG_OFF_NUMB AS ORGANIZATION_OFFICER_NUMB,
        X003ah_join_contact.ORG_OFF_MAIL AS ORGANIZATION_OFFICER_MAIL,
        X003ah_join_contact.ORG_SUP_NAME AS ORGANIZATION_SUPERVISOR,
        X003ah_join_contact.ORG_SUP_NUMB AS ORGANIZATION_SUPERVISOR_NUMB,
        X003ah_join_contact.ORG_SUP_MAIL AS ORGANIZATION_SUPERVISOR_MAIL,
        X003ah_join_contact.MAIL_NAME AS OE_CODE_OFFICER,
        X003ah_join_contact.MAIL_NUMB AS OE_CODE_OFFICER_NUMB,
        X003ah_join_contact.MAIL_MAIL AS OE_CODE_OFFICER_MAIL    
    From
        X003ah_join_contact
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS X003ax_id_blank")
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export findings
    if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
        print("Export findings...")
        sr_filet = sr_file
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "People_test_003ax_passblank_"
        sx_filet = sx_file + funcdate.today_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
        funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    """ ****************************************************************************
    TEST BANK ACC DUPLICATE
    *****************************************************************************"""
    print("TEST BANK ACC DUPLICATE")
    funcfile.writelog("TEST BANK ACC DUPLICATE")

    # IMPORT BASIC ID NUMBER DATA FROM MASTER FILE
    print("Import people bank account number data...")
    sr_file = "X004aa_people_bank_acc_master"
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
        PEOPLE.X002_PEOPLE_CURR.ACC_TYPE,
        PEOPLE.X002_PEOPLE_CURR.ACC_BRANCH,
        PEOPLE.X002_PEOPLE_CURR.ACC_NUMBER,
        PEOPLE.X002_PEOPLE_CURR.ACC_RELATION,
        PEOPLE.X002_PEOPLE_CURR.MAILTO
    From
        PEOPLE.X002_PEOPLE_CURR
    Where
        PEOPLE.X002_PEOPLE_CURR.ACC_NUMBER <> ''
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # BUILD TABLE COUNTING BANK ACCOUNTS
    print("Count bank account occurances...")
    sr_file = "X004ab_bank_duplicate_count"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X004aa_people_bank_acc_master.ACC_NUMBER,
        Count(X004aa_people_bank_acc_master.EMPLOYEE_NUMBER) As COUNT
    From
        X004aa_people_bank_acc_master
    Group By
        X004aa_people_bank_acc_master.ACC_NUMBER
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS X004ab_id_duplicate_count")
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # IDENTIFY DUPLICATE ACCOUNTS
    print("Build list of duplicate accounts...")
    sr_file = "X004ac_bank_duplicate_list"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        X004aa_people_bank_acc_master.*,
        X004ab_bank_duplicate_count.COUNT
    From
        X004aa_people_bank_acc_master Left Join
        X004ab_bank_duplicate_count On X004ab_bank_duplicate_count.ACC_NUMBER = X004aa_people_bank_acc_master.ACC_NUMBER
    Where
        X004ab_bank_duplicate_count.COUNT > 1
    Order by
        X004ab_bank_duplicate_count.ACC_NUMBER, EMPLOYEE_NUMBER
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # IMPORT PREVIOUS REPORTED FINDINGS
    print("Import previously reported findings...")
    tb_name = "X004ad_impo_reported"
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
        elif row[0] != "bank_acc_duplicate":
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
    sr_file = "X004ae_join_prev_reported"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
      X004ac_bank_duplicate_list.*,
      X004ad_impo_reported.PROCESS AS PREV_PROCESS,
      X004ad_impo_reported.DATE_REPORTED AS PREV_DATE_REPORTED,
      X004ad_impo_reported.DATE_RETEST AS PREV_DATE_RETEST,
      'bank_acc_duplicate' AS PROCESS,
      '%TODAY%' AS DATE_REPORTED,
      '2099-12-31' AS DATE_RETEST
    FROM
      X004ac_bank_duplicate_list
      LEFT JOIN X004ad_impo_reported ON X004ad_impo_reported.FIELD1 = X004ac_bank_duplicate_list.EMPLOYEE_NUMBER AND
        X004ad_impo_reported.DATE_RETEST >= Date('%TODAY%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%",funcdate.today())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE PREVIOUS FINDINGS FILE
    print("Add new findings to previous reported...")
    sr_file = "X004af_add_prev_reported"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X004ae_join_prev_reported.PROCESS,
      X004ae_join_prev_reported.EMPLOYEE_NUMBER AS FIELD1,
      '' AS FIELD2,
      '' AS FIELD3,
      '' AS FIELD4,
      '' AS FIELD5,
      X004ae_join_prev_reported.DATE_REPORTED,
      X004ae_join_prev_reported.DATE_RETEST
    FROM
      X004ae_join_prev_reported
    WHERE
      X004ae_join_prev_reported.PREV_PROCESS IS NULL
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
            funcfile.writelog("%t EXPORT FILE: "+sr_file)
    else:
        print("*** No new findings to report ***")
        funcfile.writelog("%t FINDING: No new findings to report")

    # IMPORT MAILTO CONTACT DETAILS
    print("Import mailto contact details...")
    sr_file = "X004ag_impo_mailto"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      X004ae_join_prev_reported.*,
      PEOPLE.X002_PEOPLE_CURR.MAILTO AS MAIL_NUMB,
      PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME AS MAIL_NAME,
      PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS AS MAIL_MAIL
    FROM
      X004ae_join_prev_reported
      LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X004ae_join_prev_reported.MAILTO
    WHERE
      X004ae_join_prev_reported.PREV_PROCESS IS NULL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT OFFICERS
    print("Import reporting officers for mail purposes...")
    sr_file = "X004ah_impo_report_officer"
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

    # IMPORT SUPERVISORS
    print("Import reporting supervisors for mail purposes...")
    sr_file = "X004ai_impo_report_supervisor"
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
    print("Add contact details to findings...")
    sr_file = "X004aj_join_contact"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        X004ag_impo_mailto.*,
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
        X004ag_impo_mailto
        Left Join X004ah_impo_report_officer CAMP_OFF On CAMP_OFF.CAMPUS = X004ag_impo_mailto.LOC
        Left Join X004ah_impo_report_officer ORG_OFF On ORG_OFF.CAMPUS = X004ag_impo_mailto.ORG
        Left Join X004ai_impo_report_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = X004ag_impo_mailto.LOC
        Left Join X004ai_impo_report_supervisor ORG_SUP On ORG_SUP.CAMPUS = X004ag_impo_mailto.ORG
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS X004ai_join_contact")
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL REPORT
    print("Build the final report...")
    sr_file = "X004ax_bankacc_duplicate"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        X004aj_join_contact.ORG AS ORGANIZATION,
        X004aj_join_contact.LOC AS LOCATION,
        X004aj_join_contact.EMPLOYEE_NUMBER,
        X004aj_join_contact.FULL_NAME,
        X004aj_join_contact.ACC_TYPE,
        X004aj_join_contact.ACC_BRANCH,
        X004aj_join_contact.ACC_NUMBER,
        X004aj_join_contact.ACC_RELATION,
        X004aj_join_contact.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
        X004aj_join_contact.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
        X004aj_join_contact.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
        X004aj_join_contact.CAMP_SUP_NAME AS SUPERVISOR,
        X004aj_join_contact.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
        X004aj_join_contact.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
        X004aj_join_contact.ORG_OFF_NAME AS ORGANIZATION_OFFICER,
        X004aj_join_contact.ORG_OFF_NUMB AS ORGANIZATION_OFFICER_NUMB,
        X004aj_join_contact.ORG_OFF_MAIL AS ORGANIZATION_OFFICER_MAIL,
        X004aj_join_contact.ORG_SUP_NAME AS ORGANIZATION_SUPERVISOR,
        X004aj_join_contact.ORG_SUP_NUMB AS ORGANIZATION_SUPERVISOR_NUMB,
        X004aj_join_contact.ORG_SUP_MAIL AS ORGANIZATION_SUPERVISOR_MAIL,
        X004aj_join_contact.MAIL_NAME AS OE_CODE_OFFICER,
        X004aj_join_contact.MAIL_NUMB AS OE_CODE_OFFICER_NUMB,
        X004aj_join_contact.MAIL_MAIL AS OE_CODE_OFFICER_MAIL    
    From
        X004aj_join_contact
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
        sx_file = "People_test_004ax_bankacc_duplicate_"
        sx_filet = sx_file + funcdate.today_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
        funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

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

    # IMPORT BASIC ID NUMBER DATA FROM MASTER FILE
    print("Import people bank account number data...")
    sr_file = "X005aa_tax_blank"
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
        PEOPLE.X002_PEOPLE_CURR.TAX_NUMBER,
        PEOPLE.X002_PEOPLE_CURR.NATIONALITY,
        UPPER(PEOPLE.X002_PEOPLE_CURR.PERSON_TYPE) AS PERSON_TYPE,
        PEOPLE.X002_PEOPLE_CURR.EMP_START,
        PEOPLE.X002_PEOPLE_CURR.MAILTO
    From
        PEOPLE.X002_PEOPLE_CURR
    Where
        PEOPLE.X002_PEOPLE_CURR.NATIONALITY = 'SAF' AND
        UPPER(PEOPLE.X002_PEOPLE_CURR.PERSON_TYPE) = 'PERMANENT APPOINTMENT' AND
        PEOPLE.X002_PEOPLE_CURR.TAX_NUMBER = '' AND
        PEOPLE.X002_PEOPLE_CURR.EMP_START<=StrfTime('%Y-%m-%d','now','-1 month')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # IMPORT PREVIOUS REPORTED FINDINGS
    print("Import previously reported findings...")
    tb_name = "X005ab_impo_reported"
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
        elif row[0] != "tax_blank":
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
    sr_file = "X005ac_join_prev_reported"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
      X005aa_tax_blank.*,
      X005ab_impo_reported.PROCESS AS PREV_PROCESS,
      X005ab_impo_reported.DATE_REPORTED AS PREV_DATE_REPORTED,
      X005ab_impo_reported.DATE_RETEST AS PREV_DATE_RETEST,
      'tax_blank' AS PROCESS,
      '%TODAY%' AS DATE_REPORTED,
      '%TODAY+14%' AS DATE_RETEST
    FROM
      X005aa_tax_blank
      LEFT JOIN X005ab_impo_reported ON X005ab_impo_reported.FIELD1 = X005aa_tax_blank.EMPLOYEE_NUMBER AND
        X005ab_impo_reported.DATE_RETEST >= Date('%TODAY%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%",funcdate.today())
    s_sql = s_sql.replace("%TODAY+14%",funcdate.today_plusdays(30))
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE PREVIOUS FINDINGS FILE
    print("Add new findings to previous reported...")
    sr_file = "X005ad_add_prev_reported"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X005ac_join_prev_reported.PROCESS,
      X005ac_join_prev_reported.EMPLOYEE_NUMBER AS FIELD1,
      '' AS FIELD2,
      '' AS FIELD3,
      '' AS FIELD4,
      '' AS FIELD5,
      X005ac_join_prev_reported.DATE_REPORTED,
      X005ac_join_prev_reported.DATE_RETEST
    FROM
      X005ac_join_prev_reported
    WHERE
      X005ac_join_prev_reported.PREV_PROCESS IS NULL
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
    sr_file = "X005ae_impo_mailto"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      X005ac_join_prev_reported.*,
      PEOPLE.X002_PEOPLE_CURR.MAILTO AS MAIL_NUMB,
      PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME AS MAIL_NAME,
      PEOPLE.X002_PEOPLE_CURR.EMAIL_ADDRESS AS MAIL_MAIL
    FROM
      X005ac_join_prev_reported
      LEFT JOIN PEOPLE.X002_PEOPLE_CURR ON PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER = X005ac_join_prev_reported.MAILTO
    WHERE
      X005ac_join_prev_reported.PREV_PROCESS IS NULL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT OFFICERS
    print("Import reporting officers for mail purposes...")
    sr_file = "X005af_impo_report_officer"
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
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_TAX_BLANK_OFFICER'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS
    print("Import reporting supervisors for mail purposes...")
    sr_file = "X005ag_impo_report_supervisor"
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
      PEOPLE.X000_OWN_HR_LOOKUPS.LOOKUP = 'TEST_TAX_BLANK_SUPERVISOR'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    print("Add contact details to findings...")
    sr_file = "X005ah_join_contact"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        X005ae_impo_mailto.*,
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
        X005ae_impo_mailto
        Left Join X005af_impo_report_officer CAMP_OFF On CAMP_OFF.CAMPUS = X005ae_impo_mailto.LOC
        Left Join X005af_impo_report_officer ORG_OFF On ORG_OFF.CAMPUS = X005ae_impo_mailto.ORG
        Left Join X005ag_impo_report_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = X005ae_impo_mailto.LOC
        Left Join X005ag_impo_report_supervisor ORG_SUP On ORG_SUP.CAMPUS = X005ae_impo_mailto.ORG
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS X002a5_impo_report_supervisor")
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL REPORT
    print("Build the final report...")
    sr_file = "X005ax_tax_blank"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        X005ah_join_contact.ORG AS ORGANIZATION,
        X005ah_join_contact.LOC AS LOCATION,
        X005ah_join_contact.EMPLOYEE_NUMBER,
        X005ah_join_contact.FULL_NAME,
        X005ah_join_contact.CAMP_OFF_NAME AS RESPONSIBLE_OFFICER,
        X005ah_join_contact.CAMP_OFF_NUMB AS RESPONSIBLE_OFFICER_NUMB,
        X005ah_join_contact.CAMP_OFF_MAIL AS RESPONSIBLE_OFFICER_MAIL,
        X005ah_join_contact.CAMP_SUP_NAME AS SUPERVISOR,
        X005ah_join_contact.CAMP_SUP_NUMB AS SUPERVISOR_NUMB,
        X005ah_join_contact.CAMP_SUP_MAIL AS SUPERVISOR_MAIL,
        X005ah_join_contact.ORG_OFF_NAME AS ORGANIZATION_OFFICER,
        X005ah_join_contact.ORG_OFF_NUMB AS ORGANIZATION_OFFICER_NUMB,
        X005ah_join_contact.ORG_OFF_MAIL AS ORGANIZATION_OFFICER_MAIL,
        X005ah_join_contact.ORG_SUP_NAME AS ORGANIZATION_SUPERVISOR,
        X005ah_join_contact.ORG_SUP_NUMB AS ORGANIZATION_SUPERVISOR_NUMB,
        X005ah_join_contact.ORG_SUP_MAIL AS ORGANIZATION_SUPERVISOR_MAIL,
        X005ah_join_contact.MAIL_NAME AS OE_CODE_OFFICER,
        X005ah_join_contact.MAIL_NUMB AS OE_CODE_OFFICER_NUMB,
        X005ah_join_contact.MAIL_MAIL AS OE_CODE_OFFICER_MAIL    
    From
        X005ah_join_contact
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS X005ax_id_blank")
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export findings
    if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
        print("Export findings...")
        sr_filet = sr_file
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "People_test_005ax_taxblank_"
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
