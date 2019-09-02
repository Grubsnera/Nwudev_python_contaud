"""
Script to test STUDENT FEES
Created on: 28 Aug 2019
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3
import csv

# IMPORT OWN MODULES
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcstat
from _my_modules import funcsys

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
OBTAIN STUDENTS
OBTAIN STUDENT TRANSACTIONS AND ISOLATE FEES
TEST REGISTRATION FEE ABNORMAL
END OF SCRIPT
*****************************************************************************"""


def student_fee(s_period='curr', s_year='2019'):
    """
    Script to test STUDENT FEE INCOME
    :param s_period: str: The financial period
    :param s_year: str: The financial year
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # SCRIPT LOG FILE
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C302_TEST_STUDENT_FEE")
    funcfile.writelog("-----------------------------")
    print("---------------------")    
    print("C302_TEST_STUDENT_FEE")
    print("---------------------")

    # DECLARE VARIABLES
    ed_path = "S:/_external_data/"  # External data path
    so_path = "W:/Vss_fee/"  # Source database path
    so_file = "Vss_test_fee.sqlite"  # Source database
    re_path = "R:/Vss/"
    l_export: bool = True
    l_mail: bool = False
    l_record: bool = False
    l_vacuum: bool = False

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN SQLITE SOURCE table
    print("Open sqlite database...")
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH VSS DATABASE
    print("Attach vss database...")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
    funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")
    so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: People.sqlite")

    """ ****************************************************************************
    TEMPORARY AREA
    *****************************************************************************"""
    print("TEMPORARY AREA")
    funcfile.writelog("TEMPORARY AREA")

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")      

    """ ****************************************************************************
    OBTAIN STUDENTS
    *****************************************************************************"""
    print("OBTAIN STUDENTS")
    funcfile.writelog("OBTAIN STUDENTS")      

    # OBTAIN THE LIST STUDENTS
    print("Obtain the registered students...")
    sr_file = "X000_Student"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
      STUD.*,
      CASE
          WHEN DATEENROL < STARTDATE THEN STARTDATE
          ELSE DATEENROL
      END AS DATEENROL_CALC
    FROM
      VSS.X001_Student_%PERIOD% STUD
    WHERE
      UPPER(STUD.QUAL_TYPE) Not Like '%SHORT COURSE%' AND
      STUD.ISMAINQUALLEVEL = 1 AND
      UPPER(STUD.ACTIVE_IND) = 'ACTIVE'
    """
    s_sql = s_sql.replace("%PERIOD%", s_period)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    OBTAIN STUDENT TRANSACTIONS AND ISOLATE FEES
    *****************************************************************************"""
    print("OBTAIN STUDENT TRANSACTIONS")
    funcfile.writelog("OBTAIN STUDENT TRANSACTIONS")      

    # OBTAIN STUDENT ACCOUNT TRANSACTIONS
    print("Import student transactions...")
    sr_file = "X000_Transaction"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
      TRAN.FBUSENTID As STUDENT,
      CASE
        WHEN TRAN.FDEBTCOLLECTIONSITE = '-9' THEN 'MAFIKENG'
        WHEN TRAN.FDEBTCOLLECTIONSITE = '-2' THEN 'VAAL TRIANGLE'
        ELSE 'POTCHEFSTROOM'
      END AS CAMPUS,
      TRAN.TRANSDATE,
      TRAN.TRANSDATETIME,
      CASE
        WHEN SUBSTR(TRAN.TRANSDATE,6,5)='01-01' AND INSTR('001z031z061',TRAN.TRANSCODE)>0 THEN '00'
        WHEN strftime('%Y',TRAN.TRANSDATE)>strftime('%Y',TRAN.POSTDATEDTRANSDATE) THEN strftime('%m',TRAN.TRANSDATE)
        ELSE strftime('%m',TRAN.TRANSDATE)
      END AS MONTH,
      TRAN.TRANSCODE,
      TRAN.AMOUNT,
      CASE
        WHEN TRAN.AMOUNT > 0 THEN TRAN.AMOUNT
        ELSE 0.00
      END AS AMOUNT_DT,
      CASE
        WHEN TRAN.AMOUNT < 0 THEN TRAN.AMOUNT
        ELSE 0.00
      END AS AMOUNT_CR,
      TRAN.DESCRIPTION_E As TRANSDESC
    FROM
      VSS.X010_Studytrans_%PERIOD% TRAN
    WHERE
      TRAN.TRANSCODE <> ''
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%PERIOD%", s_period)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # CALCULATE THE REGISTRATION FEES LEVIED
    print("Calculate the registration fee transactions...")
    sr_file = "X001aa_Trans_feereg"
    s_sql = "CREATE VIEW " + sr_file + " AS" + """
    SELECT
      TRAN.STUDENT,
      CAST(TOTAL(TRAN.AMOUNT) AS REAL) AS FEE_REG
    FROM
      X000_Transaction TRAN
    WHERE
      TRAN.TRANSCODE = "002" Or
      TRAN.TRANSCODE = "095"
    GROUP BY
      TRAN.STUDENT
    """
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    """*****************************************************************************
    TEST REGISTRATION FEE ABNORMAL
    *****************************************************************************"""
    print("TEST REGISTRATION FEE ABNORMAL")
    funcfile.writelog("TEST REGISTRATION FEE ABNORMAL")

    # DECLARE VARIABLES
    i_finding_after: int = 0

    # CALCULATE THE REGISTRATION FEE MODE
    i_calc = funcstat.stat_mode(so_curs, "X001aa_Trans_feereg", "FEE_REG")
    funcfile.writelog("%t STATISTIC MODE: " + str(i_calc))
    print(i_calc)

    # IDENTIFY REGISTRATION FEE AMOUNTS NOT MODE
    print("Identify abnormal registration fees...")
    sr_file = "X010aa_Regfee_abnormal"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        'NWU' As ORG,
        CASE
            WHEN STUD.FSITEORGUNITNUMBER = -1 THEN 'POTCHEFSTROOM'
            WHEN STUD.FSITEORGUNITNUMBER = -2 THEN 'VAAL TRIANGLE'
            WHEN STUD.FSITEORGUNITNUMBER = -9 THEN 'MAFIKENG'
            ELSE 'OTH'
        END As LOC, 
        FEE.STUDENT,
        FEE.FEE_REG
    From
        X001aa_Trans_feereg FEE Left Join
        X000_Student STUD On STUD.KSTUDBUSENTID = FEE.STUDENT
    Where
        FEE.FEE_REG != 0 And 
        FEE.FEE_REG != %AMOUNT% 
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%AMOUNT%", str(i_calc))
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X010ab_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        FIND.*
    From
        X010aa_Regfee_abnormal FIND
    Where
        FIND.LOC != "OTH"    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " REGISTRATION FEE ABNORMAL finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X010ac_get_previous"
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
            DATE_MAILED TEXT)
            """)
        s_cols = ""
        co = open(ed_path + "302_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "registration fee abnormal":
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
        funcfile.writelog("%t IMPORT TABLE: " + ed_path + "302_reported.txt (" + sr_file + ")")

    # ADD PREVIOUS FINDINGS
    sr_file = "X010ad_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'registration fee abnormal' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.DATE_MAILED
        From
            X010ab_findings FIND Left Join
            X010ac_get_previous PREV ON PREV.FIELD1 = FIND.STUDENT
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_yearend())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X010ae_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.STUDENT AS FIELD1,
            '' AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.DATE_MAILED
        From
            X010ad_add_previous PREV
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
            sx_file = "302_reported"
            # Read the header data
            s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
            # Write the data
            if l_record:
                funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
                funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
                funcfile.writelog("%t EXPORT DATA: " + sr_file)
        else:
            print("*** No new findings to report ***")
            funcfile.writelog("%t FINDING: No new findings to export")

    # IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
    sr_file = "X010af_officer"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        if i_finding_after > 0:
            print("Import reporting officers for mail purposes...")
            s_sql = "CREATE TABLE " + sr_file + " AS " + """
            Select
                OFFICER.LOOKUP,
                Upper(OFFICER.LOOKUP_CODE) AS CAMPUS,
                OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
                PEOP.NAME_ADDR As NAME,
                PEOP.EMAIL_ADDRESS
            From
                VSS.X000_OWN_LOOKUPS OFFICER Left Join
                PEOPLE.X002_PEOPLE_CURR PEOP ON
                    PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_fee_test_reg_fee_abnormal_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X010ag_supervisor"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Import reporting supervisors for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            SUPERVISOR.LOOKUP,
            Upper(SUPERVISOR.LOOKUP_CODE) AS CAMPUS,
            SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            VSS.X000_OWN_LOOKUPS SUPERVISOR Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON 
                PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_fee_test_reg_fee_abnormal_officer'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X010ah_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.STUDENT,
            PREV.FEE_REG,
            STUD.DATEENROL,
            Upper(STUD.PRESENT_CAT) As PRESENT_CAT,
            Upper(STUD.ENROL_CAT) As ENROL_CAT,
            Upper(STUD.QUAL_TYPE) As QUAL_TYPE,
            CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
            CAMP_OFF.NAME As CAMP_OFF_NAME,
            CASE
                WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_OFF.EMAIL_ADDRESS
            END As CAMP_OFF_MAIL,
            CAMP_OFF.EMAIL_ADDRESS As CAMP_OFF_MAIL2,
            CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
            CAMP_SUP.NAME As CAMP_SUP_NAME,
            CASE
                WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE CAMP_SUP.EMAIL_ADDRESS
            END As CAMP_SUP_MAIL,
            CAMP_SUP.EMAIL_ADDRESS As CAMP_SUP_MAIL2,
            ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
            ORG_OFF.NAME As ORG_OFF_NAME,
            CASE
                WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_OFF.EMAIL_ADDRESS
            END As ORG_OFF_MAIL,
            ORG_OFF.EMAIL_ADDRESS As ORG_OFF_MAIL2,
            ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
            ORG_SUP.NAME As ORG_SUP_NAME,
            CASE
                WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ORG_SUP.EMAIL_ADDRESS
            END As ORG_SUP_MAIL,
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2
        From
            X010ad_add_previous PREV Left Join
            X010af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            X010af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            X010ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            X010ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            X000_Student STUD On STUD.KSTUDBUSENTID = PREV.STUDENT
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X010ax_Regfee_abnormal"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'REGISTRATION FEE ABNORMAL' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.STUDENT As 'Student',
            FIND.FEE_REG As 'Registration_fee',
            FIND.DATEENROL As 'Date_enrol',
            FIND.PRESENT_CAT As 'Present_cat',
            FIND.ENROL_CAT As 'Enrol_cat',
            FIND.QUAL_TYPE As 'Qualification_type',
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
            X010ah_detail FIND
        Order by
            FIND.LOC,
            FIND.PRESENT_CAT,
            FIND.ENROL_CAT,
            FIND.QUAL_TYPE,
            FIND.STUDENT
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "Student_fee_test_010ax_reg_fee_abnormal_"
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
    END OF SCRIPT
    *****************************************************************************"""
    print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    if l_vacuum:
        print("Vacuum the database...")
        so_conn.commit()
        so_conn.execute('VACUUM')
        funcfile.writelog("%t VACUUM DATABASE: " + so_file)
    so_conn.commit()
    so_conn.close()

    # CLOSE THE LOG WRITER *********************************************************
    funcfile.writelog("--------------------------------")
    funcfile.writelog("COMPLETED: C302_TEST_STUDENT_FEE")

    return
