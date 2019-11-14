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
OBTAIN STUDENT TRANSACTIONS

REGISTRATION FEE MASTER
REGISTRATION FEE REPORTS
TEST REGISTRATION FEE CONTACT NULL
TEST REGISTRATION FEE CONTACT NEGATIVE
TEST REGISTRATION FEE CONTACT ZERO
TEST REGISTRATION FEE CONTACT ABNORMAL

QUALIFICATION FEE MASTER 1
QUALIFICATION FEE TEST NO FEE LOADED
QUALIFICATION FEE MASTER 2
QUALIFICATION FEE TEST NO TRANSACTION (1 NO TRANSACTION)
QUALIFICATION FEE TEST NEGATIVE TRANSACTION (2 NEGATIVE TRANSACTION)
QUALIFICATION FEE TEST ZERO TRANSACTION (3 ZERO TRANSACTION)

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
    l_export: bool = False
    l_record: bool = True
    l_vacuum: bool = False
    l_mail: bool = False

    s_reg_trancode: str = "095"
    s_qual_trancode: str = "004"
    s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
    s_mpa: str = "665566"  # Exclude these FQUALLEVELAPID
    # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded

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
        UPPER(STUD.QUAL_TYPE) Not Like '%SHORT COURSE%'
    """
    """
    To exclude some students
    STUD.ISMAINQUALLEVEL = 1 AND
    UPPER(STUD.ACTIVE_IND) = 'ACTIVE'
    """
    s_sql = s_sql.replace("%PERIOD%", s_period)
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    OBTAIN STUDENT TRANSACTIONS
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
            WHEN strftime('%Y',TRANSDATE)>strftime('%Y',POSTDATEDTRANSDATE) And
             Strftime('%Y',POSTDATEDTRANSDATE) = '%CYEAR%' THEN strftime('%m',POSTDATEDTRANSDATE)
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
        TRAN.DESCRIPTION_E As TRANSDESC,
        TRAN.FUSERBUSINESSENTITYID,
        TRAN.AUDITDATETIME,
        TRAN.FMODAPID,
        TRAN.MODULE,
        TRAN.MODULE_NAME,
        TRAN.FQUALLEVELAPID,
        TRAN.QUALIFICATION,
        TRAN.QUALIFICATION_NAME,
        TRAN.FENROLPRESID
    FROM
        VSS.X010_Studytrans_%PERIOD% TRAN
    WHERE
        TRAN.TRANSCODE <> ''
    ORDER BY
        AUDITDATETIME
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%PERIOD%", s_period)
    s_sql = s_sql.replace("%CYEAR%", funcdate.cur_year())
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    REGISTRATION FEE MASTER
    *****************************************************************************"""
    print("REGISTRATION FEE MASTER")
    funcfile.writelog("REGISTRATION FEE MASTER")

    # CALCULATE THE REGISTRATION FEES LEVIED
    print("Calculate the registration fee transactions...")
    sr_file = "X010_Trans_feereg"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        TRAN.STUDENT,
        CAST(TOTAL(TRAN.AMOUNT) AS REAL) AS FEE_REG,
        COUNT(TRAN.STUDENT) As TRAN_COUNT,
        MAX(AUDITDATETIME),
        TRAN.FUSERBUSINESSENTITYID
    From
        X000_Transaction TRAN
    Where
        Instr('%TRANCODE%', Trim(TRAN.TRANSCODE)) > 0
    Group by
      TRAN.STUDENT
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TRANCODE%", s_reg_trancode)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # CALCULATE THE REGISTRATION FEE MODE
    i_calc = funcstat.stat_mode(so_curs, "X010_Trans_feereg", "FEE_REG")
    funcfile.writelog("%t STATISTIC MODE: Registration fee R" + str(i_calc))
    print("Registration fee: R" + str(i_calc))

    # ADD REGISTRATION LEVIED FEES TO THE STUDENTS LIST
    print("Join students and registration fees...")
    sr_file = "X010_Student_feereg"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        STUD.KSTUDBUSENTID,
        FEE.FEE_REG,
        CASE
            When STUD.ENROL_CAT = "POST DOC" Then '9 EXCLUDE FROM TEST'
            When FEE.FEE_REG Is Null Then '1 NO TRANSACTION'
            When FEE.FEE_REG < 0 Then '2 NEGATIVE TRANSACTION'
            When FEE.FEE_REG = 0 Then '3 ZERO TRANSACTION'
            When FEE.FEE_REG = %LEVY% Then '4 NORMAL TRANSACTION'
            Else '5 ABNORMAL TRANSACTION'
        END As FEE_TYPE,
        Cast(Case
            When FEE.FEE_REG Is Null Then 0
            Else FEE.FEE_REG
        End As REAL) As FEE_CALC,
        Upper(STUD.ACTIVE_IND) As ACTIVE_IND,
        Upper(STUD.LEVY_CATEGORY) As LEVY_CATEGORY,
        STUD.QUALIFICATION,
        STUD.QUALIFICATION_NAME,
        Upper(STUD.PRESENT_CAT) As PRESENT_CAT,
        Upper(STUD.ENROL_CAT) As ENROL_CAT,
        Upper(STUD.QUAL_TYPE) As QUAL_TYPE,
        Upper(STUD.ENTRY_LEVEL) As ENTRY_LEVEL,
        Upper(STUD.STATUS_FINAL) As STATUS_FINAL,
        STUD.FSITEORGUNITNUMBER,
        STUD.CAMPUS,
        STUD.ORGUNIT_NAME,
        STUD.DATEQUALLEVELSTARTED,
        STUD.STARTDATE,
        STUD.DATEENROL,
        STUD.DATEENROL_CALC,
        STUD.DISCONTINUEDATE,
        STUD.RESULTPASSDATE,
        STUD.RESULT,
        STUD.ISHEMISSUBSIDY,
        STUD.ISMAINQUALLEVEL,
        STUD.ENROLACADEMICYEAR,
        STUD.ENROLHISTORYYEAR,
        STUD.MIN,
        STUD.MIN_UNIT,
        STUD.MAX,
        STUD.MAX_UNIT,
        STUD.CERT_TYPE,
        STUD.LEVY_TYPE,
        STUD.BLACKLIST,
        STUD.LONG,
        STUD.DISCONTINUE_REAS,
        STUD.POSTPONE_REAS,
        STUD.FBUSINESSENTITYID,
        STUD.ORGUNIT_TYPE,
        STUD.ISCONDITIONALREG,
        STUD.MARKSFINALISEDDATE,
        STUD.RESULTISSUEDATE,
        STUD.EXAMSUBMINIMUM,
        STUD.ISCUMLAUDE,
        STUD.ISPOSSIBLEGRADUATE,
        STUD.FACCEPTANCETESTCODEID,
        STUD.FENROLMENTPRESENTATIONID,
        STUD.FQUALLEVELAPID,
        STUD.FPROGRAMAPID,
        FEE.TRAN_COUNT,
        FEE.FUSERBUSINESSENTITYID
    From
        X000_Student STUD Left Join
        X010_Trans_feereg FEE On FEE.STUDENT = STUD.KSTUDBUSENTID
    Where
        STUD.ISMAINQUALLEVEL = 1
    Order by
        STUD.ENROL_CAT,
        STUD.KSTUDBUSENTID      
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%LEVY%", str(i_calc))
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    REGISTRATION FEE REPORTS
    *****************************************************************************"""
    print("REGISTRATION FEE REPORTS")
    funcfile.writelog("REGISTRATION FEE REPORTS")

    # ADD REGISTRATION LEVIED FEES TO THE STUDENTS LIST
    print("Report presentation category...")
    sr_file = "X010_Report_feereg_present"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        Upper(FEE.CAMPUS) As CAMPUS,
        Upper(FEE.FEE_TYPE) As FEE_TYPE,
        Upper(FEE.PRESENT_CAT) As PRESENT_CAT,
        Count(FEE.FBUSINESSENTITYID) As COUNT,
        Total(FEE.FEE_CALC) As TOTAL
    From
        X010_Student_feereg FEE
    Group By
        FEE.CAMPUS,
        FEE.FEE_TYPE,
        FEE.PRESENT_CAT
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%LEVY%", str(i_calc))
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD REGISTRATION LEVIED FEES TO THE STUDENTS LIST
    print("Report enrolment category...")
    sr_file = "X010_Report_feereg_enrol"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        Upper(FEE.CAMPUS) As CAMPUS,
        Upper(FEE.FEE_TYPE) As FEE_TYPE,
        Upper(FEE.PRESENT_CAT) As PRESENT_CAT,
        Upper(FEE.ENROL_CAT) As ENROL_CAT,
        Count(FEE.FBUSINESSENTITYID) As COUNT,
        Total(FEE.FEE_CALC) As TOTAL
    From
        X010_Student_feereg FEE
    Group By
        FEE.CAMPUS,
        FEE.FEE_TYPE,
        FEE.PRESENT_CAT,
        FEE.ENROL_CAT
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%LEVY%", str(i_calc))
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    TEST REGISTRATION FEE CONTACT NULL
    *****************************************************************************"""
    print("REGISTRATION FEE CONTACT NULL")
    funcfile.writelog("REGISTRATION FEE CONTACT NULL")

    # EXCLUSIONS
    # Only 1 NO TRANSACTION
    # Contact students only
    # Exclude if conditional registration

    # DECLARE VARIABLES
    i_finding_after: int = 0

    # IDENTIFY REGISTRATION FEES CONTACT NOT LEVIED
    print("Identify null registration fees...")
    sr_file = "X010aa_Regfee_null"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        FIND.*
    From
        X010_Student_feereg FIND
    Where
        FIND.FEE_TYPE Like '1%' And
        FIND.PRESENT_CAT Like 'C%'
    Order by
        FIND.ENROL_CAT,
        FIND.QUAL_TYPE,
        FIND.QUALIFICATION,
        FIND.ENTRY_LEVEL,
        FIND.KSTUDBUSENTID    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        print("Export findings...")
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "Student_fee_test_010aa_reg_fee_contact_null_"
        sx_file_dated = sx_file + funcdate.today_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X010ab_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.KSTUDBUSENTID As STUDENT,
        FIND.FEE_CALC,
        FIND.FUSERBUSINESSENTITYID As USER
    From
        X010aa_Regfee_null FIND
    Where
        FIND.ISCONDITIONALREG != 1
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " REGISTRATION FEE NULL finding(s)")

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
        co = open(ed_path + "302_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "registration fee null":
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
            'registration fee null' AS PROCESS,
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
                PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_fee_test_reg_fee_null_contact_officer'
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
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_fee_test_reg_fee_null_contact_supervisor'
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
            PREV.FEE_CALC,
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
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2,
            PREV.USER As USER_NUMB,
            CASE
                WHEN PREV.USER != '' THEN PEOP.NAME_ADDR
                WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
                ELSE ''
            END As USER_NAME,
            CASE
                WHEN PREV.USER != '' THEN PREV.USER||'@nwu.ac.za'
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ''
            END As USER_MAIL,
            CASE
                WHEN PREV.USER != '' THEN PEOP.EMAIL_ADDRESS
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMAIL_ADDRESS
                ELSE ''
            END As USER_MAIL2
        From
            X010ad_add_previous PREV Left Join
            X010af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            X010af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            X010ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            X010ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            X000_Student STUD On STUD.KSTUDBUSENTID = PREV.STUDENT Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = PREV.USER
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        """
        WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X010ax_Regfee_null"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'REGISTRATION FEE NULL' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.STUDENT As 'Student',
            FIND.FEE_CALC As 'Registration_fee',
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
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.USER_NAME As User,
            FIND.USER_NUMB As User_Numb,
            FIND.USER_MAIL As User_Mail
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
            sx_file = "Student_fee_test_010ex_reg_fee_contact_null_"
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
    TEST REGISTRATION FEE CONTACT NEGATIVE
    *****************************************************************************"""
    print("REGISTRATION FEE CONTACT NEGATIVE")
    funcfile.writelog("REGISTRATION FEE CONTACT NEGATIVE")

    # DECLARE VARIABLES
    i_finding_after: int = 0

    # IDENTIFY REGISTRATION FEES CONTACT NEGATIVES
    print("Identify negative registration fees...")
    sr_file = "X010ba_Regfee_negative"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        FIND.*
    From
        X010_Student_feereg FIND
    Where
        FIND.FEE_TYPE Like '2%' And
        FIND.PRESENT_CAT Like 'C%'
    Order by
        FIND.ENROL_CAT,
        FIND.QUAL_TYPE,
        FIND.QUALIFICATION,
        FIND.ENTRY_LEVEL,
        FIND.KSTUDBUSENTID    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        print("Export findings...")
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "Student_fee_test_010ba_reg_fee_contact_negative_"
        sx_file_dated = sx_file + funcdate.today_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X010bb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.KSTUDBUSENTID As STUDENT,
        FIND.FEE_CALC,
        FIND.FUSERBUSINESSENTITYID As USER
    From
        X010ba_Regfee_negative FIND
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " REGISTRATION FEE NEGATIVE finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X010bc_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 INT,
            FIELD2 INT,
            FIELD3 TEXT,
            FIELD4 TEXT,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            DATE_MAILED TEXT)
            """)
        co = open(ed_path + "302_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "registration fee negative":
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
    sr_file = "X010bd_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'registration fee negative' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.DATE_MAILED
        From
            X010bb_findings FIND Left Join
            X010bc_get_previous PREV ON PREV.FIELD1 = FIND.STUDENT And
                PREV.FIELD2 = FIND.FEE_CALC
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_yearend())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X010be_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.STUDENT AS FIELD1,
            FEE_CALC AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.DATE_MAILED
        From
            X010bd_add_previous PREV
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
    sr_file = "X010bf_officer"
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
                PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_fee_test_reg_fee_negative_contact_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X010bg_supervisor"
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
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_fee_test_reg_fee_negative_contact_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X010bh_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.STUDENT,
            PREV.FEE_CALC,
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
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2,
            PREV.USER As USER_NUMB,
            CASE
                WHEN PREV.USER != '' THEN PEOP.NAME_ADDR
                WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
                ELSE ''
            END As USER_NAME,
            CASE
                WHEN PREV.USER != '' THEN PREV.USER||'@nwu.ac.za'
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ''
            END As USER_MAIL,
            CASE
                WHEN PREV.USER != '' THEN PEOP.EMAIL_ADDRESS
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMAIL_ADDRESS
                ELSE ''
            END As USER_MAIL2
        From
            X010bd_add_previous PREV Left Join
            X010bf_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            X010bf_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            X010bg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            X010bg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            X000_Student STUD On STUD.KSTUDBUSENTID = PREV.STUDENT Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = PREV.USER
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        """
        WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X010bx_Regfee_negative"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'REGISTRATION FEE NEGATIVE' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.STUDENT As 'Student',
            FIND.FEE_CALC As 'Registration_fee',
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
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.USER_NAME As User,
            FIND.USER_NUMB As User_Numb,
            FIND.USER_MAIL As User_Mail
        From
            X010bh_detail FIND
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
            sx_file = "Student_fee_test_010ex_reg_fee_contact_negative_"
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
    TEST REGISTRATION FEE CONTACT ZERO
    *****************************************************************************"""
    print("REGISTRATION FEE CONTACT ZERO")
    funcfile.writelog("REGISTRATION FEE CONTACT ZERO")

    # DECLARE VARIABLES
    i_finding_after: int = 0

    # IDENTIFY REGISTRATION FEES CONTACT NOT LEVIED
    print("Identify zero registration fees...")
    sr_file = "X010ca_Regfee_zero"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        FIND.*
    From
        X010_Student_feereg FIND
    Where
        FIND.FEE_TYPE Like '3%' And
        FIND.PRESENT_CAT Like 'C%'
    Order by
        FIND.ENROL_CAT,
        FIND.QUAL_TYPE,
        FIND.QUALIFICATION,
        FIND.ENTRY_LEVEL,
        FIND.KSTUDBUSENTID    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        print("Export findings...")
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "Student_fee_test_010ca_reg_fee_contact_zero_"
        sx_file_dated = sx_file + funcdate.today_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X010cb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.KSTUDBUSENTID As STUDENT,
        FIND.FEE_CALC,
        FIND.FUSERBUSINESSENTITYID As USER
    From
        X010ca_Regfee_zero FIND
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " REGISTRATION FEE ZERO finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X010cc_get_previous"
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
        co = open(ed_path + "302_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "registration fee zero":
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
    sr_file = "X010cd_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'registration fee zero' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.DATE_MAILED
        From
            X010cb_findings FIND Left Join
            X010cc_get_previous PREV ON PREV.FIELD1 = FIND.STUDENT
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_yearend())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X010ce_new_previous"
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
            X010cd_add_previous PREV
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
    sr_file = "X010cf_officer"
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
                PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_fee_test_reg_fee_zero_contact_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X010cg_supervisor"
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
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_fee_test_reg_fee_zero_contact_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X010ch_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.STUDENT,
            PREV.FEE_CALC,
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
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2,
            PREV.USER As USER_NUMB,
            CASE
                WHEN PREV.USER != '' THEN PEOP.NAME_ADDR
                WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
                ELSE ''
            END As USER_NAME,
            CASE
                WHEN PREV.USER != '' THEN PREV.USER||'@nwu.ac.za'
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ''
            END As USER_MAIL,
            CASE
                WHEN PREV.USER != '' THEN PEOP.EMAIL_ADDRESS
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMAIL_ADDRESS
                ELSE ''
            END As USER_MAIL2
        From
            X010cd_add_previous PREV Left Join
            X010cf_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            X010cf_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            X010cg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            X010cg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            X000_Student STUD On STUD.KSTUDBUSENTID = PREV.STUDENT Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = PREV.USER
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        """
        WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X010cx_Regfee_zero"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'REGISTRATION FEE ZERO' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.STUDENT As 'Student',
            FIND.FEE_CALC As 'Registration_fee',
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
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.USER_NAME As User,
            FIND.USER_NUMB As User_Numb,
            FIND.USER_MAIL As User_Mail
        From
            X010ch_detail FIND
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
            sx_file = "Student_fee_test_010ex_reg_fee_contact_zero_"
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
    TEST REGISTRATION FEE CONTACT ABNORMAL
    *****************************************************************************"""
    print("TEST REGISTRATION FEE CONTACT ABNORMAL")
    funcfile.writelog("TEST REGISTRATION FEE CONTACT ABNORMAL")

    # DECLARE VARIABLES
    i_finding_after: int = 0

    # IDENTIFY REGISTRATION FEE AMOUNTS NOT MODE
    print("Identify abnormal registration fees...")
    sr_file = "X010ea_Regfee_abnormal"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        FIND.*
    From
        X010_Student_feereg FIND
    Where
        FIND.FEE_TYPE Like '5%' And
        FIND.PRESENT_CAT Like 'C%'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%AMOUNT%", str(i_calc))
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        print("Export findings...")
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "Student_fee_test_010ea_reg_fee_contact_abnormal_"
        sx_file_dated = sx_file + funcdate.today_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X010eb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.KSTUDBUSENTID As STUDENT,
        FIND.FEE_CALC,
        FIND.FUSERBUSINESSENTITYID As USER
    From
        X010ea_Regfee_abnormal FIND
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
    sr_file = "X010ec_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 INT,
            FIELD2 REAL,
            FIELD3 TEXT,
            FIELD4 TEXT,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            DATE_MAILED TEXT)
            """)
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
    sr_file = "X010ed_add_previous"
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
            X010eb_findings FIND Left Join
            X010ec_get_previous PREV ON PREV.FIELD1 = FIND.STUDENT And
                PREV.FIELD2 = FIND.FEE_CALC
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_yearend())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    # NOTE ADD CODE
    sr_file = "X010ee_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.STUDENT AS FIELD1,
            FEE_CALC AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.DATE_MAILED
        From
            X010ed_add_previous PREV
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
    sr_file = "X010ef_officer"
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
                PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_fee_test_reg_fee_abnormal_contact_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X010eg_supervisor"
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
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_fee_test_reg_fee_abnormal_contact_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X010eh_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.STUDENT,
            PREV.FEE_CALC,
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
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2,
            PREV.USER As USER_NUMB,
            CASE
                WHEN PREV.USER != '' THEN PEOP.NAME_ADDR
                WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
                ELSE ''
            END As USER_NAME,
            CASE
                WHEN PREV.USER != '' THEN PREV.USER||'@nwu.ac.za'
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ''
            END As USER_MAIL,
            CASE
                WHEN PREV.USER != '' THEN PEOP.EMAIL_ADDRESS
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMAIL_ADDRESS
                ELSE ''
            END As USER_MAIL2
        From
            X010ed_add_previous PREV Left Join
            X010ef_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            X010ef_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            X010eg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            X010eg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            X000_Student STUD On STUD.KSTUDBUSENTID = PREV.STUDENT Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = PREV.USER
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        """
        WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X010ex_Regfee_abnormal"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'REGISTRATION FEE ABNORMAL' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.STUDENT As 'Student',
            FIND.FEE_CALC As 'Registration_fee',
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
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.USER_NAME As User,
            FIND.USER_NUMB As User_Numb,
            FIND.USER_MAIL As User_Mail
        From
            X010eh_detail FIND
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
            sx_file = "Student_fee_test_010ex_reg_fee_contact_abnormal_"
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
    QUALIFICATION FEE MASTER 1
    *****************************************************************************"""
    print("QUALIFICATION FEE MASTER")
    funcfile.writelog("QUALIFICATION FEE MASTER")

    # TODO Remove after first run
    sr_file = "X020ad_Trans_feequal_semester1"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file = "X020ae_Trans_feequal_semester2"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_conn.commit()

    # BUILD LIST OF QUALIFICATIONS PLUS STATS
    print("Build summary of qualifications levied...")
    sr_file = "X020aa_Trans_feequal"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        TRAN.FQUALLEVELAPID,
        TRAN.QUALIFICATION,
        TRAN.QUALIFICATION_NAME,
        CAST(COUNT(TRAN.STUDENT) As INT) As TRAN_COUNT_ALL,
        CAST(TOTAL(TRAN.AMOUNT) AS REAL) AS FEE_QUAL_ALL
    From
        X000_Transaction TRAN
    Where
        Instr('%TRANCODE%', Trim(TRAN.TRANSCODE)) > 0 And
        TRAN.FMODAPID = 0
    Group by
        TRAN.FQUALLEVELAPID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TRANCODE%", s_qual_trancode)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # CALCULATE THE QUALIFICATION FEES LEVIED PER STUDENT
    print("Calculate the qualification fees levied per student...")
    sr_file = "X020ab_Trans_feequal_stud"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        TRAN.STUDENT,
        TRAN.FQUALLEVELAPID,
        TRAN.FUSERBUSINESSENTITYID,
        CAST(COUNT(TRAN.STUDENT) As INT) As TRAN_COUNT,
        CAST(TOTAL(TRAN.AMOUNT) AS REAL) AS FEE_QUAL,
        MAX(TRAN.AUDITDATETIME)
    From
        X000_Transaction TRAN
    Where
        Instr('%TRANCODE%', Trim(TRAN.TRANSCODE)) > 0 And
        TRAN.FMODAPID = 0
    Group by
        TRAN.STUDENT,
        TRAN.FQUALLEVELAPID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TRANCODE%", s_qual_trancode)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # CALCULATE THE STATISTIC MODE FOR EACH QUALIFICATION
    print("Calculate the qualification statistic mode...")
    i_value: int = 0
    sr_file = "X020ac_Trans_feequal_mode"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute("CREATE TABLE " + sr_file + " (FQUALLEVELAPID INT, FEE_MODE REAL)")
    for qual in so_curs.execute("SELECT FQUALLEVELAPID FROM X020aa_Trans_feequal").fetchall():
        try:
            i_value = funcstat.stat_mode(so_curs, "X020ab_Trans_feequal_stud",
                                         "FEE_QUAL", "FQUALLEVELAPID = " + str(qual[0]))
            if i_value < 0:
                i_value = 0
        except Exception as e:
            # funcsys.ErrMessage(e) if you want error to log
            if "".join(e.args).find("no unique mode") >= 0:
                i_value = funcstat.stat_highest_value(so_curs, "X020ab_Trans_feequal_stud", "FEE_QUAL",
                                                      "FQUALLEVELAPID = " + str(qual[0]))
            else:
                i_value = 0
        s_cols = "INSERT INTO " + sr_file + " VALUES(" + str(qual[0]) + ", " + str(i_value) + ")"
        so_curs.execute(s_cols)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()

    # CALCULATE THE NUMBER OF MODULES
    print("Calculate the number modules...")
    sr_file = "X020ad_Student_module_calc"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        MODU.KSTUDBUSENTID,
        MODU.FQUALLEVELAPID,
        MODU.COURSESEMESTER,
        Cast(Case
            When MODU.COURSESEMESTER = '0' Then 1
            Else 0
        End As INT) As S0,
        Cast(Case
            When MODU.COURSESEMESTER = '1' Then 1
            Else 0
        End As INT) As S1,
        Cast(Case
            When MODU.COURSESEMESTER = '2' Then 1
            Else 0
        End As INT) As S2,
        Cast(Case
            When MODU.COURSESEMESTER = '3' Then 1
            Else 0
        End As INT) As S3,
        Cast(Case
            When MODU.COURSESEMESTER = '4' Then 1
            Else 0
        End As INT) As S4,
        Cast(Case
            When MODU.COURSESEMESTER = '5' Then 1
            Else 0
        End As INT) As S5,
        Cast(Case
            When MODU.COURSESEMESTER = '6' Then 1
            Else 0
        End As INT) As S6,
        Cast(Case
            When MODU.COURSESEMESTER = '7' Then 1
            Else 0
        End As INT) As S7,
        Cast(Case
            When MODU.COURSESEMESTER = '8' Then 1
            Else 0
        End As INT) As S8,
        Cast(Case
            When MODU.COURSESEMESTER = '9' Then 1
            Else 0
        End As INT) As S9
    From
        VSS.X001_Student_module_curr MODU
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()

    # CALCULATE THE NUMBER OF MODULES PER STUDENT
    print("Calculate the number modules per student...")
    sr_file = "X020ad_Student_module_summ"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        MODU.KSTUDBUSENTID,
        MODU.FQUALLEVELAPID,
        Sum(MODU.S0) As SEM0,
        Sum(MODU.S1) As SEM1,
        Sum(MODU.S2) As SEM2,
        Sum(MODU.S3) As SEM3,
        Sum(MODU.S4) As SEM4,
        Sum(MODU.S5) As SEM5,
        Sum(MODU.S6) As SEM6,
        Sum(MODU.S7) As SEM7,
        Sum(MODU.S8) As SEM8,
        Sum(MODU.S9) As SEM9
    From
        X020ad_Student_module_calc MODU
    Group By
        MODU.KSTUDBUSENTID,
        MODU.FQUALLEVELAPID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()

    """*****************************************************************************
    QUALIFICATION FEE TEST NO FEE LOADED
    *****************************************************************************"""
    print("QUALIFICATION FEE TEST NO FEE LOADED")
    funcfile.writelog("QUALIFICATION FEE TEST NO FEE LOADED")

    # FILES NEEDED
    # X000_Student
    # X020aa_Trans_feequal
    # EXCLUDE
    # Distance students
    # MBA and MPA students
    # Occasional students

    # DECLARE VARIABLES
    i_finding_after: int = 0

    # ISOLATE QUALIFICATIONS WITH NO LINKED LEVIES - CONTACT STUDENTS ONLY
    print("Isolate qualifications with no linked levies...")
    sr_file = "X021aa_Qual_nofee_loaded"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        STUD.FQUALLEVELAPID,
        STUD.CAMPUS,
        STUD.QUALIFICATION,
        STUD.QUALIFICATION_NAME,
        Count(STUD.KSTUDBUSENTID) As COUNT_STUD,
        Cast(Case
            When Instr('%MBA%',STUD.FQUALLEVELAPID) > 0 Then 0
            When Instr('%MPA%',STUD.FQUALLEVELAPID) > 0 Then 0
            When STUD.QUALIFICATION_NAME Like ('%NON DEGREE%') Then 0
            When STUD.QUALIFICATION_NAME Like ('%OCCASIONAL STUD%') Then 0
            When STUD.QUALIFICATION_NAME Like ('%OCCATIONAL STUD%') Then 0
            When STUD.QUALIFICATION_NAME Like ('%OCC. STUD.%') Then 0
            When STUD.QUALIFICATION_NAME Like ('%MBA%') Then 0
            When STUD.QUALIFICATION_NAME Like ('%MASTER OF BUSINESS ADMINISTRATION%') Then 0
            Else 1
        End As INT) As FINDING
    From
        X000_Student STUD Left Join
        X020aa_Trans_feequal TRAN On TRAN.FQUALLEVELAPID = STUD.FQUALLEVELAPID
    Where
        (STUD.PRESENT_CAT Like ('C%') And
        TRAN.TRAN_COUNT_ALL Is Null) Or
        (STUD.PRESENT_CAT Like ('C%') And
        Instr('%MPA%',STUD.FQUALLEVELAPID) > 0)
    Group By
        STUD.FQUALLEVELAPID,
        STUD.CAMPUS
    Order By
        STUD.CAMPUS,
        STUD.QUALIFICATION
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%MBA%", s_mba)
    s_sql = s_sql.replace("%MPA%", s_mpa)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X021ab_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.FQUALLEVELAPID As ID,
        FIND.QUALIFICATION,
        FIND.QUALIFICATION_NAME,
        FIND.COUNT_STUD
    From
        X021aa_Qual_nofee_loaded FIND
    Where
        FIND.FINDING = 1
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IDENTIFY FINDINGS LIST - NOT DONE NORMALLY BUT WANT DETAIL HERE TO EXPORT
    print("Identify findings list...")
    sr_file = "X021ab_findings_list"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        FIND.CAMPUS,
        FIND.QUALIFICATION,
        FIND.QUALIFICATION_NAME,
        STUD.KSTUDBUSENTID,
        STUD.DATEQUALLEVELSTARTED,
        STUD.DATEENROL,
        STUD.STARTDATE,
        STUD.ENDDATE,
        STUD.QUAL_TYPE,
        STUD.ACTIVE_IND,
        STUD.ENTRY_LEVEL,
        STUD.BLACKLIST,
        STUD.ENROL_CAT,
        STUD.PRESENT_CAT,
        STUD.STATUS_FINAL,
        STUD.LEVY_CATEGORY,
        STUD.SITEID,
        STUD.CAMPUS,
        STUD.ORGUNIT_NAME,
        STUD.KSTUDQUALFOSRESULTID,
        STUD.DISCONTINUEDATE,
        STUD.FDISCONTINUECODEID,
        STUD.RESULT,
        STUD.DISCONTINUE_REAS,
        STUD.POSTPONE_REAS,
        STUD.FPOSTPONEMENTCODEID,
        STUD.ENROLACADEMICYEAR,
        STUD.ENROLHISTORYYEAR,
        STUD.ISHEMISSUBSIDY,
        STUD.ISMAINQUALLEVEL,
        STUD.ISCONDITIONALREG,
        STUD.ISCUMLAUDE,
        STUD.ISPOSSIBLEGRADUATE,
        STUD.FACCEPTANCETESTCODEID,
        STUD.ISVERIFICATIONREQUIRED,
        STUD.EXAMSUBMINIMUM,
        STUD.ISVATAPPLICABLE,
        STUD.ISPRESENTEDBEFOREAPPROVAL,
        STUD.ISDIRECTED
    From
        X021aa_Qual_nofee_loaded FIND Inner Join
        X000_Student STUD On STUD.QUALIFICATION = FIND.QUALIFICATION
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if funcsys.tablerowcount(so_curs, sr_file) > 0:  # Ignore l_export flag - should export every time
        print("Export findings...")
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "Student_fee_test_021ax_qual_fee_not_loaded_studentlist_"  # File X021_findings_list
        sx_file_dated = sx_file + funcdate.today_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " QUALIFICATION NO FEE LOADED finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X021ac_get_previous"
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
        co = open(ed_path + "302_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "qualification no fee loaded":
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
    sr_file = "X021ad_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'qualification no fee loaded' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.DATE_MAILED
        From
            X021ab_findings FIND Left Join
            X021ac_get_previous PREV ON PREV.FIELD1 = FIND.ID And
                PREV.FIELD2 = FIND.QUALIFICATION
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_yearend())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X021ae_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.ID AS FIELD1,
            PREV.QUALIFICATION AS FIELD2,
            '' AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.DATE_MAILED
        From
            X021ad_add_previous PREV
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
    sr_file = "X021af_officer"
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
                PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_fee_test_qual_no_fee_loaded_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X021ag_supervisor"
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
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_fee_test_qual_no_fee_loaded_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X021ah_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.ID,
            PREV.QUALIFICATION,
            PREV.QUALIFICATION_NAME,
            PREV.COUNT_STUD,
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
            X021ad_add_previous PREV Left Join
            X021af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            X021af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            X021ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            X021ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        """
        WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X021ax_Qual_nofee_loaded"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'QUALIFICATION NO FEE LOADED' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.ID As 'Quallevelid',
            FIND.QUALIFICATION As 'Qualification',
            FIND.QUALIFICATION_NAME As 'Qualification_name',
            FIND.COUNT_STUD As 'Student_count',
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
            X021ah_detail FIND
        Order by
            FIND.LOC,
            FIND.QUALIFICATION
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "Student_fee_test_021ax_qual_fee_not_loaded_"
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
    QUALIFICATION FEE MASTER 2 - JOIN STUDENT AND TRANSACTION AND NO FEE LOADED TEST
    *****************************************************************************"""
    print("QUALIFICATION FEE MASTER")
    funcfile.writelog("QUALIFICATION FEE MASTER")

    # Short course students already removed in OBTAIN STUDENTS
    # Remove NON CONTACT students
    # Remove students identified in QUALIFICATION FEE TEST NO FEE LOADED

    # JOIN STUDENTS AND TRANSACTIONS
    print("Join students and transactions...")
    sr_file = "X020ba_Student_master"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        Cast(0 As INT) As VALID,
        STUD.CAMPUS,
        STUD.KSTUDBUSENTID,
        Cast(Case
            When FEES.FEE_QUAL Is Null Then 0
            Else Round(FEES.FEE_QUAL,2)
        End As REAL) As FEE_LEVIED,
        Cast(Case
            When MODE.FEE_MODE Is Null Then 0
            Else Round(MODE.FEE_MODE,2)
        End As REAL) As FEE_MODE,
        Cast(Case
            When MODE.FEE_MODE Is Null Then 0
            Else Round(MODE.FEE_MODE/2,2)
        End As REAL) As FEE_MODE_HALF,
        '' As FEE_SHOULD_BE,
        Case
            When STUD.ENROL_CAT = "POST DOC" Then '9 EXCLUDE POST DOC'
            When STUD.ISCONDITIONALREG = '1' Then  '9 EXCLUDE CONDITIONAL REG'
            When Instr('%MBA%',STUD.FQUALLEVELAPID) != 0 Then '9 EXCLUDE MBA'
            When Instr('%MPA%',STUD.FQUALLEVELAPID) != 0 Then '9 EXCLUDE MPA'
            When STUD.QUALIFICATION_NAME Like ('%NON DEGREE%') Then '9 EXCLUDE NON DEGREE'
            When STUD.QUALIFICATION_NAME Like ('%OCCASIONAL STUD%') Then '9 OCCASIONAL STUDENT'
            When STUD.QUALIFICATION_NAME Like ('%OCCATIONAL STUD%') Then '9 OCCASIONAL STUDENT'
            When STUD.QUALIFICATION_NAME Like ('%MTH IN%') Then '9 MTH STUDENT'
            When FEES.FEE_QUAL Is Null Then '1 NO TRANSACTION'
            When FEES.FEE_QUAL < 0 Then '2 NEGATIVE TRANSACTION'
            When FEES.FEE_QUAL = 0 Then '3 ZERO TRANSACTION'
            When FEES.FEE_QUAL = Round(MODE.FEE_MODE/2,2) Then '4 HALF TRANSACTION'
            When FEES.FEE_QUAL = MODE.FEE_MODE Then '5 NORMAL TRANSACTION'
            Else '6 ABNORMAL TRANSACTION'
        End As FEE_LEVIED_TYPE,
        STUD.QUALIFICATION,
        STUD.QUALIFICATION_NAME,
        STUD.QUAL_TYPE,
        Case
            When STUD.QUAL_TYPE Like ('%DOCTORAL%') Then 'POSTGRADUATE'
            When STUD.QUAL_TYPE Like ('%MASTERS%') Then 'POSTGRADUATE'
            Else 'UNDERGRADUATE'
        End As QUAL_TYPE_FEE,
        STUD.ACTIVE_IND,
        STUD.LEVY_CATEGORY,
        STUD.PRESENT_CAT,
        STUD.ENROL_CAT,
        STUD.ENTRY_LEVEL,
        STUD.STATUS_FINAL,
        STUD.FSITEORGUNITNUMBER As FSITE,
        STUD.ORGUNIT_NAME,
        STUD.DATEQUALLEVELSTARTED,
        STUD.STARTDATE,
        STUD.DATEENROL,
        STUD.DISCONTINUEDATE,
        Cast(Case
            When STUD.DISCONTINUEDATE > STUD.DATEENROL Then Julianday(STUD.DISCONTINUEDATE) - Julianday(STUD.DATEENROL) 
            Else 0
        End As INT) As DAYS_REG,
        Case
            When Upper(STUD.RESULT) Like '%PASS%' Then ''
            Else STUD.DISCONTINUEDATE 
        End As DISCDATE_CALC,
        Case
            When Upper(STUD.RESULT) Like '%PASS%' Then STUD.RESULTPASSDATE
            Else '' 
        End As RESULTPASSDATE,
        Case
            When Upper(STUD.RESULT) Like '%PASS%' Then STUD.RESULTISSUEDATE
            Else '' 
        End As RESULTISSUEDATE,
        STUD.RESULT,
        STUD.ISHEMISSUBSIDY,
        STUD.ISMAINQUALLEVEL,
        STUD.ENROLACADEMICYEAR,
        STUD.ENROLHISTORYYEAR,
        SEME.SEM1,
        SEME.SEM2,
        SEME.SEM7,
        SEME.SEM8,
        SEME.SEM9,
        REGF.FEE_TYPE As REG_FEE_TYPE,
        STUD.MIN,
        STUD.MIN_UNIT,
        STUD.MAX,
        STUD.MAX_UNIT,
        STUD.CERT_TYPE,
        STUD.LEVY_TYPE,
        STUD.BLACKLIST,
        STUD.LONG,
        STUD.DISCONTINUE_REAS,
        STUD.POSTPONE_REAS,
        STUD.FBUSINESSENTITYID,
        STUD.ORGUNIT_TYPE,
        STUD.ISCONDITIONALREG,
        STUD.MARKSFINALISEDDATE,
        STUD.EXAMSUBMINIMUM,
        STUD.ISCUMLAUDE,
        STUD.ISPOSSIBLEGRADUATE,
        STUD.FACCEPTANCETESTCODEID,
        STUD.FENROLMENTPRESENTATIONID,
        STUD.FQUALLEVELAPID,
        STUD.FPROGRAMAPID,
        FEES.TRAN_COUNT,    
        FEES.FUSERBUSINESSENTITYID
    From
        X000_Student STUD Left Join
        X021ab_findings_list FIND On FIND.KSTUDBUSENTID = STUD.KSTUDBUSENTID And
            FIND.QUALIFICATION = STUD.QUALIFICATION Left Join
        X020ab_Trans_feequal_stud FEES On FEES.STUDENT = STUD.KSTUDBUSENTID And
            FEES.FQUALLEVELAPID = STUD.FQUALLEVELAPID Left Join
        X020ac_Trans_feequal_mode MODE On MODE.FQUALLEVELAPID = STUD.FQUALLEVELAPID Left Join
        X020ad_Student_module_summ SEME On SEME.KSTUDBUSENTID = STUD.KSTUDBUSENTID And
            SEME.FQUALLEVELAPID = STUD.FQUALLEVELAPID Left Join
        X010_Student_feereg REGF On REGF.KSTUDBUSENTID = STUD.KSTUDBUSENTID    
    Where
        STUD.PRESENT_CAT Like ('C%') And
        FIND.KSTUDBUSENTID Is Null    
    ;"""

    # To exclude all MBA and MPA include the following in the where clause
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # UPDATE FEE SHOULD BE COLUMN
    print("Update qualification fee should be column...")
    so_curs.execute("UPDATE " + sr_file + """
                    SET FEE_SHOULD_BE = 
                    CASE
                        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM9 > 0 Then '4 CP FULL PAYMENT RQD' 
                        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM8 > 0 Then '4 CP FULL PAYMENT RQD' 
                        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM7 > 0 Then '4 CP FULL PAYMENT RQD' 
                        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM1 = 0 Then '6 CP 2ND SEM HALF PAYMENT RQD' 
                        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null Then '4 CP FULL PAYMENT RQD' 
                        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC = '' Then '5 CP PASS FULL PAYMENT RQD' 
                        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC <= '2019-03-31' Then '1 CP NO PAYMENT RQD' 
                        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC <= '2019-07-31' Then '2 CP DISC 1ST HALF PAYMENT RQD' 
                        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC > '2019-07-31' Then '3 CP DISC 2ND FULL PAYMENT RQD'
                        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'D%' And DISCDATE_CALC Is Null Then '4 DP FULL PAYMENT RQD' 
                        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'D%' And DISCDATE_CALC = '' Then '5 DP PASS FULL PAYMENT RQD' 
                        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'D%' And DISCDATE_CALC <= '2019-03-31' Then '1 DP NO PAYMENT RQD' 
                        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'D%' And DISCDATE_CALC <= '2019-07-31' Then '2 DP DISC 1ST HALF PAYMENT RQD' 
                        When QUAL_TYPE_FEE Like 'P%' And PRESENT_CAT Like 'D%' And DISCDATE_CALC > '2019-07-31' Then '3 DP DISC 2ND FULL PAYMENT RQD'
                        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM8 > 0 Then '4 CU FULL PAYMENT RQD' 
                        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM7 > 0 Then '4 CU FULL PAYMENT RQD' 
                        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null And SEM1 = 0 Then '6 CU 2ND SEM HALF PAYMENT RQD' 
                        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC Is Null Then '4 CU FULL PAYMENT RQD'
                        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC = '' Then '5 CU PASS FULL PAYMENT RQD'
                        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC <= '2019-02-17' Then '1 CU NO PAYMENT RQD'
                        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC <= '2019-07-31' Then '2 CU DISC 1ST HALF PAYMENT RQD'
                        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'C%' And DISCDATE_CALC > '2019-07-31' Then '3 CU DISC 2ND FULL PAYMENT RQD'
                        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'D%' And DISCDATE_CALC Is Null Then '4 DU FULL PAYMENT RQD'
                        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'D%' And DISCDATE_CALC = '' Then '5 DU PASS FULL PAYMENT RQD'
                        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'D%' And DISCDATE_CALC <= '2019-03-09' Then '1 DU NO PAYMENT RQD'
                        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'D%' And DISCDATE_CALC <= '2019-08-15' Then '2 DU DISC 1ST HALF PAYMENT RQD'
                        When QUAL_TYPE_FEE Like 'U%' And PRESENT_CAT Like 'D%' And DISCDATE_CALC > '2019-08-15' Then '3 DU DISC 2ND FULL PAYMENT RQD'
                        Else '7 NO ALLOCATION FULL PAYMENT RQD' 
                    END
                    ;""")
    so_conn.commit()
    funcfile.writelog("%t UPDATE COLUMN: Valid qualification fee")

    # UPDATE VALID COLUMN
    print("Update qualification fee valid column...")
    so_curs.execute("UPDATE " + sr_file + """
                    SET VALID = 
                    CASE
                        When FEE_LEVIED_TYPE Like '2%' Then 0
                        When FEE_LEVIED_TYPE Like '9%' Then 1
                        When FEE_SHOULD_BE Like '1%' And FEE_LEVIED_TYPE Like '1%' Then 1
                        When FEE_SHOULD_BE Like '1%' And FEE_LEVIED_TYPE Like '3%' Then 1
                        When FEE_SHOULD_BE Like '1%' And FEE_LEVIED_TYPE Like '4%' Then 2
                        When FEE_SHOULD_BE Like '2%' And FEE_LEVIED_TYPE Like '4%' Then 1
                        When FEE_SHOULD_BE Like '2%' And FEE_LEVIED_TYPE Like '5%' Then 2
                        When FEE_SHOULD_BE Like '3%' And FEE_LEVIED_TYPE Like '5%' Then 1
                        When FEE_SHOULD_BE Like '4%' And FEE_LEVIED_TYPE Like '5%' Then 1
                        When FEE_SHOULD_BE Like '5%' And FEE_LEVIED_TYPE Like '5%' Then 1
                        When FEE_SHOULD_BE Like '6%' And FEE_LEVIED_TYPE Like '4%' Then 1
                        When FEE_SHOULD_BE Like '6%' And FEE_LEVIED_TYPE Like '5%' Then 2
                        When FEE_SHOULD_BE Like '7%' And FEE_LEVIED_TYPE Like '5%' Then 1
                        Else 0
                    END
                    ;""")
    so_conn.commit()
    funcfile.writelog("%t UPDATE COLUMN: Valid qualification fee")

    # IDENTIFY STUDENTS WITH TWO OR MORE HALF LEVY TRANSACTIONS
    print("Identify multiple half levy students...")
    sr_file = "X020bb_Student_multiple_half"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        STUD.KSTUDBUSENTID,
        Cast(Count(STUD.VALID) As INT) As FEE_COUNT_HALF
    From
        X020ba_Student_master STUD
    Where
        STUD.FEE_LEVIED_TYPE Like ('4%')
    Group By
        STUD.KSTUDBUSENTID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()

    # JOIN STUDENTS AND TRANSACTIONS
    print("Join students and transactions...")
    sr_file = "X020bx_Student_master_sort"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        STUD.VALID,
        STUD.CAMPUS,
        STUD.KSTUDBUSENTID,
        STUD.FEE_LEVIED,
        STUD.FEE_MODE,
        STUD.FEE_MODE_HALF,
        HALF.FEE_COUNT_HALF,
        STUD.FEE_SHOULD_BE,
        STUD.FEE_LEVIED_TYPE,
        STUD.QUALIFICATION,
        STUD.QUALIFICATION_NAME,
        STUD.QUAL_TYPE,
        STUD.QUAL_TYPE_FEE,
        STUD.ACTIVE_IND,
        STUD.LEVY_CATEGORY,
        STUD.PRESENT_CAT,
        STUD.ENROL_CAT,
        STUD.ENTRY_LEVEL,
        STUD.STATUS_FINAL,
        STUD.FSITE,
        STUD.ORGUNIT_NAME,
        STUD.DATEQUALLEVELSTARTED,
        STUD.STARTDATE,
        STUD.DATEENROL,
        STUD.DISCONTINUEDATE,
        STUD.DAYS_REG,
        STUD.DISCDATE_CALC,
        STUD.RESULTPASSDATE,
        STUD.RESULTISSUEDATE,
        STUD.RESULT,
        STUD.ISHEMISSUBSIDY,
        STUD.ISMAINQUALLEVEL,
        STUD.ENROLACADEMICYEAR,
        STUD.ENROLHISTORYYEAR,
        STUD.SEM1,
        STUD.SEM2,
        STUD.SEM7,
        STUD.SEM8,
        STUD.SEM9,
        STUD.REG_FEE_TYPE,
        STUD.MIN,
        STUD.MIN_UNIT,
        STUD.MAX,
        STUD.MAX_UNIT,
        STUD.CERT_TYPE,
        STUD.LEVY_TYPE,
        STUD.BLACKLIST,
        STUD.LONG,
        STUD.DISCONTINUE_REAS,
        STUD.POSTPONE_REAS,
        STUD.FBUSINESSENTITYID,
        STUD.ORGUNIT_TYPE,
        STUD.ISCONDITIONALREG,
        STUD.MARKSFINALISEDDATE,
        STUD.EXAMSUBMINIMUM,
        STUD.ISCUMLAUDE,
        STUD.ISPOSSIBLEGRADUATE,
        STUD.FACCEPTANCETESTCODEID,
        STUD.FENROLMENTPRESENTATIONID,
        STUD.FQUALLEVELAPID,
        STUD.FPROGRAMAPID,
        STUD.TRAN_COUNT,
        STUD.FUSERBUSINESSENTITYID
    From
        X020ba_Student_master STUD Left Join
        X020bb_Student_multiple_half HALF On HALF.KSTUDBUSENTID = STUD.KSTUDBUSENTID
    Order By
        STUD.FEE_LEVIED_TYPE,
        STUD.FEE_SHOULD_BE
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*****************************************************************************
    QUALIFICATION FEE TEST NO TRANSACTION
    *****************************************************************************"""
    print("QUALIFICATION FEE TEST NO TRANSACTION")
    funcfile.writelog("QUALIFICATION FEE TEST NO TRANSACTION")

    # FILES NEEDED
    # X020ba_Student_master

    # DECLARE VARIABLES
    i_finding_after: int = 0

    # ISOLATE QUALIFICATIONS WITH NO TRANSACTIONS - CONTACT STUDENTS ONLY
    print("Isolate qualifications with no transactions...")
    sr_file = "X021ba_Qual_nofee_transaction"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        STUD.*
    From
        X020ba_Student_master STUD
    Where
        STUD.VALID = 0 And
        STUD.FEE_LEVIED_TYPE Like ('1%')
    Order By
        STUD.CAMPUS,
        STUD.FEE_SHOULD_BE,
        STUD.KSTUDBUSENTID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()
    if funcsys.tablerowcount(so_curs, sr_file) > 0:  # Ignore l_export flag - should export every time
        print("Export findings...")
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "Student_fee_test_021bx_qual_fee_no_transaction_studentlist_"
        sx_file_dated = sx_file + funcdate.today_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X021bb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.KSTUDBUSENTID As ID,
        FIND.QUALIFICATION,
        FIND.FEE_LEVIED,
        FIND.FUSERBUSINESSENTITYID As USER
    From
        X021ba_Qual_nofee_transaction FIND
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " QUALIFICATION NULL FEE NOTRAN finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X021bc_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 INT,
            FIELD2 TEXT,
            FIELD3 REAL,
            FIELD4 TEXT,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            DATE_MAILED TEXT)
            """)
        co = open(ed_path + "302_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "qualification no transaction":
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
    sr_file = "X021bd_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'qualification no transaction' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.DATE_MAILED
        From
            X021bb_findings FIND Left Join
            X021bc_get_previous PREV ON PREV.FIELD1 = FIND.ID And
                PREV.FIELD2 = FIND.QUALIFICATION And
                PREV.FIELD3 = FIND.FEE_LEVIED
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_yearend())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X021be_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.ID AS FIELD1,
            PREV.QUALIFICATION AS FIELD2,
            PREV.FEE_LEVIED AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.DATE_MAILED
        From
            X021bd_add_previous PREV
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
    sr_file = "X021bf_officer"
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
                PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_fee_test_qual_no_fee_transaction_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X021bg_supervisor"
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
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_fee_test_qual_no_fee_transaction_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X021bh_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.ID,
            PREV.QUALIFICATION,
            MAST.QUALIFICATION_NAME,
            MAST.FEE_LEVIED,
            MAST.FEE_SHOULD_BE,
            MAST.FEE_MODE,
            MAST.DATEENROL,
            MAST.RESULTPASSDATE,
            MAST.DISCONTINUEDATE,
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
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2,
            PREV.USER
        From
            X021bd_add_previous PREV Left Join
            X021ba_Qual_nofee_transaction MAST On MAST.KSTUDBUSENTID = PREV.ID And
                MAST.QUALIFICATION = PREV.QUALIFICATION Left Join
            X021bf_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            X021bf_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            X021bg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            X021bg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        """
        WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X021bx_Qual_nofee_transaction"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'QUALIFICATION FEE NO TRANSACTION' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.ID As 'Student',
            FIND.QUALIFICATION As 'Qualification',
            FIND.QUALIFICATION_NAME As 'Qualification_name',
            FIND.FEE_LEVIED As 'Fee_levied',
            FIND.FEE_SHOULD_BE As 'Fee_should_be',
            FIND.FEE_MODE As 'Qual_fee',
            FIND.DATEENROL As 'Date_enrol',
            FIND.RESULTPASSDATE As 'Date_pass',
            FIND.DISCONTINUEDATE As 'Date_discontinue',
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
            X021bh_detail FIND
        Order by
            FIND.LOC,
            FIND.QUALIFICATION,
            FIND.FEE_SHOULD_BE,
            FIND.ID
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "Student_fee_test_021bx_qual_fee_no_transaction_"
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
    QUALIFICATION FEE TEST NEGATIVE TRANSACTION
    *****************************************************************************"""
    print("QUALIFICATION FEE TEST NEGATIVE TRANSACTION")
    funcfile.writelog("QUALIFICATION FEE TEST NEGATIVE TRANSACTION")

    # FILES NEEDED
    # X020ba_Student_master

    # DECLARE VARIABLES
    i_finding_after: int = 0

    # ISOLATE QUALIFICATIONS WITH NO TRANSACTIONS - CONTACT STUDENTS ONLY
    print("Isolate qualifications with negative value transactions...")
    sr_file = "X021da_Qual_negativefee_transaction"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        STUD.*
    From
        X020ba_Student_master STUD
    Where
        STUD.VALID = 0 And
        STUD.FEE_LEVIED_TYPE Like ('2%')
    Order By
        STUD.CAMPUS,
        STUD.FEE_SHOULD_BE,
        STUD.KSTUDBUSENTID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()
    if funcsys.tablerowcount(so_curs, sr_file) > 0:  # Ignore l_export flag - should export every time
        print("Export findings...")
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "Student_fee_test_021dx_qual_fee_negative_transaction_studentlist_"
        sx_file_dated = sx_file + funcdate.today_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X021db_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.KSTUDBUSENTID As ID,
        FIND.QUALIFICATION,
        FIND.FEE_LEVIED,
        FIND.FUSERBUSINESSENTITYID As USER
    From
        X021da_Qual_negativefee_transaction FIND
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " QUALIFICATION NEGATIVE FEE TRAN finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X021dc_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 INT,
            FIELD2 TEXT,
            FIELD3 REAL,
            FIELD4 TEXT,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            DATE_MAILED TEXT)
            """)
        co = open(ed_path + "302_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "qualification negative transaction":
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
    sr_file = "X021dd_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'qualification negative transaction' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.DATE_MAILED
        From
            X021db_findings FIND Left Join
            X021dc_get_previous PREV ON PREV.FIELD1 = FIND.ID And
                PREV.FIELD2 = FIND.QUALIFICATION And
                PREV.FIELD3 = FIND.FEE_LEVIED
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_yearend())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X021de_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.ID AS FIELD1,
            PREV.QUALIFICATION AS FIELD2,
            PREV.FEE_LEVIED AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.DATE_MAILED
        From
            X021dd_add_previous PREV
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
    sr_file = "X021df_officer"
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
                PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_fee_test_qual_negative_fee_transaction_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X021dg_supervisor"
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
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_fee_test_qual_negative_fee_transaction_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X021dh_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.ID,
            PREV.QUALIFICATION,
            MAST.QUALIFICATION_NAME,
            MAST.FEE_LEVIED,
            MAST.FEE_SHOULD_BE,
            MAST.FEE_MODE,
            MAST.DATEENROL,
            MAST.RESULTPASSDATE,
            MAST.DISCONTINUEDATE,
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
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2,
            PREV.USER As USER_NUMB,
            CASE
                WHEN PREV.USER != '' THEN PEOP.NAME_ADDR
                WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
                ELSE ''
            END As USER_NAME,
            CASE
                WHEN PREV.USER != '' THEN PREV.USER||'@nwu.ac.za'
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ''
            END As USER_MAIL,
            CASE
                WHEN PREV.USER != '' THEN PEOP.EMAIL_ADDRESS
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMAIL_ADDRESS
                ELSE ''
            END As USER_MAIL2
        From
            X021dd_add_previous PREV Left Join
            X021da_Qual_negativefee_transaction MAST On MAST.KSTUDBUSENTID = PREV.ID And
                MAST.QUALIFICATION = PREV.QUALIFICATION Left Join
            X021df_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            X021df_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            X021dg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            X021dg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = PREV.USER
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        """
        WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X021dx_Qual_negativefee_transaction"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'QUALIFICATION FEE NEGATIVE VALUE TRANSACTION' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.ID As 'Student',
            FIND.QUALIFICATION As 'Qualification',
            FIND.QUALIFICATION_NAME As 'Qualification_name',
            FIND.FEE_LEVIED As 'Fee_levied',
            FIND.FEE_SHOULD_BE As 'Fee_should_be',
            FIND.FEE_MODE As 'Qual_fee',
            FIND.DATEENROL As 'Date_enrol',
            FIND.RESULTPASSDATE As 'Date_pass',
            FIND.DISCONTINUEDATE As 'Date_discontinue',
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
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.USER_NAME As User,
            FIND.USER_NUMB As User_Numb,
            FIND.USER_MAIL As User_Mail
        From
            X021dh_detail FIND
        Order by
            FIND.LOC,
            FIND.QUALIFICATION,
            FIND.FEE_SHOULD_BE,
            FIND.ID
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "Student_fee_test_021dx_qual_fee_negative_transaction_"
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
    QUALIFICATION FEE TEST ZERO TRANSACTION
    *****************************************************************************"""
    print("QUALIFICATION FEE TEST ZERO TRANSACTION")
    funcfile.writelog("QUALIFICATION FEE TEST ZERO TRANSACTION")

    # FILES NEEDED
    # X020ba_Student_master

    # DECLARE VARIABLES
    i_finding_after: int = 0

    # ISOLATE QUALIFICATIONS WITH NO TRANSACTIONS - CONTACT STUDENTS ONLY
    print("Isolate qualifications with zero value transactions...")
    sr_file = "X021ca_Qual_zerofee_transaction"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        STUD.*
    From
        X020ba_Student_master STUD
    Where
        STUD.VALID = 0 And
        STUD.FEE_LEVIED_TYPE Like ('3%')
    Order By
        STUD.CAMPUS,
        STUD.FEE_SHOULD_BE,
        STUD.KSTUDBUSENTID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()
    if funcsys.tablerowcount(so_curs, sr_file) > 0:  # Ignore l_export flag - should export every time
        print("Export findings...")
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "Student_fee_test_021cx_qual_fee_zero_transaction_studentlist_"
        sx_file_dated = sx_file + funcdate.today_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

    # IDENTIFY FINDINGS
    print("Identify findings...")
    sr_file = "X021cb_findings"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'NWU' As ORG,
        FIND.CAMPUS As LOC,
        FIND.KSTUDBUSENTID As ID,
        FIND.QUALIFICATION,
        FIND.FEE_LEVIED,
        FIND.FUSERBUSINESSENTITYID As USER
    From
        X021ca_Qual_zerofee_transaction FIND
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # COUNT THE NUMBER OF FINDINGS
    i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
    print("*** Found " + str(i_finding_before) + " exceptions ***")
    funcfile.writelog("%t FINDING: " + str(i_finding_before) + " QUALIFICATION ZERO FEE TRAN finding(s)")

    # GET PREVIOUS FINDINGS
    sr_file = "X021cc_get_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Import previously reported findings...")
        so_curs.execute(
            "CREATE TABLE " + sr_file + """
            (PROCESS TEXT,
            FIELD1 INT,
            FIELD2 TEXT,
            FIELD3 REAL,
            FIELD4 TEXT,
            FIELD5 TEXT,
            DATE_REPORTED TEXT,
            DATE_RETEST TEXT,
            DATE_MAILED TEXT)
            """)
        co = open(ed_path + "302_reported.txt", "r")
        co_reader = csv.reader(co)
        # Read the COLUMN database data
        for row in co_reader:
            # Populate the column variables
            if row[0] == "PROCESS":
                continue
            elif row[0] != "qualification zero transaction":
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
    sr_file = "X021cd_add_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        print("Join previously reported to current findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS" + """
        Select
            FIND.*,
            'qualification zero transaction' AS PROCESS,
            '%TODAY%' AS DATE_REPORTED,
            '%DAYS%' AS DATE_RETEST,
            PREV.PROCESS AS PREV_PROCESS,
            PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
            PREV.DATE_RETEST AS PREV_DATE_RETEST,
            PREV.DATE_MAILED
        From
            X021cb_findings FIND Left Join
            X021cc_get_previous PREV ON PREV.FIELD1 = FIND.ID And
                PREV.FIELD2 = FIND.QUALIFICATION And
                PREV.FIELD3 = FIND.FEE_LEVIED
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        s_sql = s_sql.replace("%TODAY%", funcdate.today())
        s_sql = s_sql.replace("%DAYS%", funcdate.cur_yearend())
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST TO UPDATE FINDINGS
    sr_file = "X021ce_new_previous"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.PROCESS,
            PREV.ID AS FIELD1,
            PREV.QUALIFICATION AS FIELD2,
            PREV.FEE_LEVIED AS FIELD3,
            '' AS FIELD4,
            '' AS FIELD5,
            PREV.DATE_REPORTED,
            PREV.DATE_RETEST,
            PREV.DATE_MAILED
        From
            X021cd_add_previous PREV
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
    sr_file = "X021cf_officer"
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
                PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
            Where
                OFFICER.LOOKUP = 'stud_fee_test_qual_zero_fee_transaction_officer'
            ;"""
            so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
            so_curs.execute(s_sql)
            so_conn.commit()
            funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
    sr_file = "X021cg_supervisor"
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
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
        Where
            SUPERVISOR.LOOKUP = 'stud_fee_test_qual_zero_fee_transaction_supervisor'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD CONTACT DETAILS TO FINDINGS
    sr_file = "X021ch_detail"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if i_finding_before > 0 and i_finding_after > 0:
        print("Add contact details to findings...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            PREV.ORG,
            PREV.LOC,
            PREV.ID,
            PREV.QUALIFICATION,
            MAST.QUALIFICATION_NAME,
            MAST.FEE_LEVIED,
            MAST.FEE_SHOULD_BE,
            MAST.FEE_MODE,
            MAST.DATEENROL,
            MAST.RESULTPASSDATE,
            MAST.DISCONTINUEDATE,
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
            ORG_SUP.EMAIL_ADDRESS As ORG_SUP_MAIL2,
            PREV.USER As USER_NUMB,
            CASE
                WHEN PREV.USER != '' THEN PEOP.NAME_ADDR
                WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
                ELSE ''
            END As USER_NAME,
            CASE
                WHEN PREV.USER != '' THEN PREV.USER||'@nwu.ac.za'
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
                ELSE ''
            END As USER_MAIL,
            CASE
                WHEN PREV.USER != '' THEN PEOP.EMAIL_ADDRESS
                WHEN CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMAIL_ADDRESS
                ELSE ''
            END As USER_MAIL2
        From
            X021cd_add_previous PREV Left Join
            X021ca_Qual_zerofee_transaction MAST On MAST.KSTUDBUSENTID = PREV.ID And
                MAST.QUALIFICATION = PREV.QUALIFICATION Left Join
            X021cf_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.LOC Left Join
            X021cf_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
            X021cg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.LOC Left Join
            X021cg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = PREV.USER
        Where
          PREV.PREV_PROCESS IS NULL
        ;"""
        """
        WHEN CAMP_OFF.NAME != '' THEN CAMP_OFF.NAME 
        """
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD THE FINAL TABLE FOR EXPORT AND REPORT
    sr_file = "X021cx_Qual_zerofee_transaction"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    print("Build the final report")
    if i_finding_before > 0 and i_finding_after > 0:
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            'QUALIFICATION FEE ZERO VALUE TRANSACTION' As Audit_finding,
            FIND.ORG As 'Organization',
            FIND.LOC As 'Campus',
            FIND.ID As 'Student',
            FIND.QUALIFICATION As 'Qualification',
            FIND.QUALIFICATION_NAME As 'Qualification_name',
            FIND.FEE_LEVIED As 'Fee_levied',
            FIND.FEE_SHOULD_BE As 'Fee_should_be',
            FIND.FEE_MODE As 'Qual_fee',
            FIND.DATEENROL As 'Date_enrol',
            FIND.RESULTPASSDATE As 'Date_pass',
            FIND.DISCONTINUEDATE As 'Date_discontinue',
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
            FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail,
            FIND.USER_NAME As User,
            FIND.USER_NUMB As User_Numb,
            FIND.USER_MAIL As User_Mail
        From
            X021ch_detail FIND
        Order by
            FIND.LOC,
            FIND.QUALIFICATION,
            FIND.FEE_SHOULD_BE,
            FIND.ID
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)
        # Export findings
        if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
            print("Export findings...")
            sx_path = re_path + funcdate.cur_year() + "/"
            sx_file = "Student_fee_test_021cx_qual_fee_zero_transaction_"
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
