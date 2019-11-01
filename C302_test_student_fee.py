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

QUALIFICATION FEE MASTER
QUALIFICATION FEE TEST NO FEE LOADED

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
    s_exclude = s_mba + 'z' + s_mpa

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
        AUDITDATETIME DESC
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
    QUALIFICATION FEE MASTER
    *****************************************************************************"""
    print("QUALIFICATION FEE MASTER")
    funcfile.writelog("QUALIFICATION FEE MASTER")

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
        CAST(TOTAL(TRAN.AMOUNT) AS REAL) AS FEE_QUAL
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

    # CALCULATE THE NUMBER OF FIRST SEMESTER MODULES
    print("Calculate the number of first semester modules...")
    sr_file = "X020ad_Trans_feequal_semester1"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        MODU.KSTUDBUSENTID,
        MODU.FQUALLEVELAPID,    
        Count(MODU.KENROLSTUDID) As SEM1
    From
        VSS.X001_Student_module_curr MODU
    Where
        Substr(MODU.COURSEMODULE,1,1) = '1' Or
        Substr(MODU.COURSEMODULE,1,1) = '3' Or
        Substr(MODU.COURSEMODULE,1,1) = '4' Or
        Substr(MODU.COURSEMODULE,1,1) = '7' Or
        Substr(MODU.COURSEMODULE,1,1) = '8' Or
        Substr(MODU.COURSEMODULE,1,1) = '9'
    Group By
        MODU.KSTUDBUSENTID,
        MODU.FQUALLEVELAPID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()

    # CALCULATE THE NUMBER OF SECOND SEMESTER MODULES
    print("Calculate the number of second semester modules...")
    sr_file = "X020ae_Trans_feequal_semester2"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        MODU.KSTUDBUSENTID,
        MODU.FQUALLEVELAPID,    
        Count(MODU.KENROLSTUDID) As SEM2
    From
        VSS.X001_Student_module_curr MODU
    Where
        Substr(MODU.COURSEMODULE,1,1) = '2' Or
        Substr(MODU.COURSEMODULE,1,1) = '5' Or
        Substr(MODU.COURSEMODULE,1,1) = '6'
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
        TRAN.TRAN_COUNT_ALL As COUNT_TRAN
    From
        X000_Student STUD Left Join
        X020aa_Trans_feequal TRAN On TRAN.FQUALLEVELAPID = STUD.FQUALLEVELAPID
    Where
        STUD.PRESENT_CAT Like ('C%')
    Group By
        STUD.FQUALLEVELAPID,
        STUD.CAMPUS
    Order By
        COUNT_TRAN,
        STUD.CAMPUS,
        STUD.QUALIFICATION
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
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
        FIND.COUNT_TRAN Is Null And
        Instr('%EXCLUDE%',FIND.FQUALLEVELAPID) = 0
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%EXCLUDE%", s_exclude)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

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
            SUPERVISOR.LOOKUP = 'stud_fee_test_qual_no_fee_loaded_officer_supervisor'
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
