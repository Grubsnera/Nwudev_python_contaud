"""
Script to test STUDENT BURSARIES
Created on: 29 Jan 2021
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3
import csv

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcstat
from _my_modules import funcsys
from _my_modules import funcsms
from _my_modules import functest

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
IMPORT BURSARY MASTER LIST
OBTAIN STUDENTS
OBTAIN STUDENT TRANSACTIONS
OBTAIN STAFF DISCOUNT STUDENTS
END OF SCRIPT
"""

# SCRIPT WIDE VARIABLES
s_function: str = "C303 Student bursary tests"


def student_bursary(s_period: str = "curr"):
    """
    Script to test STUDENT BURSARIES
    :param s_period: str: The financial period
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # FUNCTION WIDE VARIABLES
    if s_period == "prev":
        s_year = funcdate.prev_year()
    else:
        s_year = funcdate.cur_year()
    ed_path: str = "S:/_external_data/"  # External data path
    # re_path: str = "R:/Vss/" + s_year
    so_path: str = "W:/Vss_fee/"  # Source database path
    so_file: str = "Vss_test_bursary.sqlite"
    l_debug: bool = False
    l_mail: bool = funcconf.l_mail_project
    # l_mail: bool = True
    l_mess: bool = funcconf.l_mess_project
    # l_mess: bool = True
    # l_record: bool = True
    # l_export: bool = True
    s_burs_code: str = "('042', '052', '381', '500')"  # Current bursary transaction codes
    s_staff_code = "('021')"  # Staff discount transaction code

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
    funcfile.writelog("OPEN THE DATABASES")
    if l_debug:
        print("OPEN THE DATABASES")

    # OPEN SQLITE SOURCE table
    if l_debug:
        print("Open sqlite database...")
    with sqlite3.connect(so_path + so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH VSS DATABASE
    if l_debug:
        print("Attach vss database...")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
    funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss_curr.sqlite' AS 'VSSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: Vss_curr.sqlite")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss_prev.sqlite' AS 'VSSPREV'")
    funcfile.writelog("%t ATTACH DATABASE: Vss_prev.sqlite")
    so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: People.sqlite")

    """************************************************************************
    TEMPORARY AREA
    ************************************************************************"""
    funcfile.writelog("TEMPORARY AREA")
    if l_debug:
        print("TEMPORARY AREA")

    """************************************************************************
    BEGIN OF SCRIPT
    ************************************************************************"""
    funcfile.writelog("BEGIN OF SCRIPT")
    if l_debug:
        print("BEGIN OF SCRIPT")

    """*****************************************************************************
    DO NOT DELETE
    IMPORT BURSARY MASTER LIST
    DO NOT DELETE
    *****************************************************************************"""
    if l_debug:
        print("Import bursary master list...")
    sr_file = "X000_OWN_HR_LOOKUPS"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file = "X000_Bursary_master"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute("CREATE TABLE " + sr_file + "(ACTIVE TEXT,"
                                                "FINAIDCODE INT,"
                                                "FINAIDNAME TEXT,"
                                                "SOURCE TEXT,"
                                                "DEGREE_TYPE TEXT,"
                                                "COST_STRING TEXT,"
                                                "APPLICATION_PROCESS TEXT,"
                                                "SBL_EVALUATION TEXT,"
                                                "BURSARY_OFFICE_PROCESS TEXT,"
                                                "NOTE TEXT)")

    co = open(ed_path + "303_bursary_master.csv", newline=None)
    co_reader = csv.reader(co)
    for row in co_reader:
        if row[0] == "ACTIVE":
            continue
        else:
            s_cols: str = "INSERT INTO " + sr_file + " VALUES('" +\
                                                row[0] + "'," +\
                                                row[1] + ",'" +\
                                                row[2] + "','" +\
                                                row[3] + "','" +\
                                                row[4] + "','" +\
                                                row[5] + "','" +\
                                                row[6] + "','" +\
                                                row[7] + "','" +\
                                                row[8] + "','" +\
                                                row[9] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the imported data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + sr_file)

    """************************************************************************
    OBTAIN STUDENTS
    ************************************************************************"""
    funcfile.writelog("OBTAIN STUDENTS")
    if l_debug:
        print("OBTAIN STUDENTS")

    # OBTAIN THE LIST STUDENTS
    # EXCLUDE SHORT COURSE STUDENTS
    if l_debug:
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
        %VSS%.X001_Student STUD
    WHERE
        UPPER(STUD.QUAL_TYPE) Not Like '%SHORT COURSE%'
    """
    """
    To exclude some students
    STUD.ISMAINQUALLEVEL = 1 AND
    UPPER(STUD.ACTIVE_IND) = 'ACTIVE'
    """
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """************************************************************************
    OBTAIN STUDENT TRANSACTIONS
    ************************************************************************"""
    funcfile.writelog("OBTAIN STUDENT TRANSACTIONS")
    if l_debug:
        print("OBTAIN STUDENT TRANSACTIONS")

    # OBTAIN STUDENT ACCOUNT TRANSACTIONS
    if l_debug:
        print("Import student transactions...")
    sr_file = "X000_Transaction"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        TRAN.FBUSENTID As STUDENT,
        CASE
            WHEN TRAN.FDEBTCOLLECTIONSITE = '-9' THEN 'MAHIKENG'
            WHEN TRAN.FDEBTCOLLECTIONSITE = '-2' THEN 'VANDERBIJLPARK'
            ELSE 'POTCHEFSTROOM'
        END AS CAMPUS,
        TRAN.TRANSDATE,
        TRAN.TRANSDATETIME,
        CASE
            WHEN SUBSTR(TRAN.TRANSDATE,6,5)='01-01' AND INSTR('001z031z061',TRAN.TRANSCODE)>0 THEN '00'
            WHEN strftime('%Y',TRANSDATE)>strftime('%Y',POSTDATEDTRANSDATE) And
             Strftime('%Y',POSTDATEDTRANSDATE) = '%YEAR%' THEN strftime('%m',POSTDATEDTRANSDATE)
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
        TRAN.AUDITDATETIME,
        TRAN.FUSERBUSINESSENTITYID,
        TRAN.FAUDITUSERCODE,
        TRAN.SYSTEM_DESC,
        TRAN.FFINAIDSITEID,
        TRAN.FINAIDCODE,
        TRAN.FINAIDNAME,
        BURS.SOURCE
    FROM
        %VSS%.X010_Studytrans TRAN LEFT JOIN
        X000_Bursary_master BURS ON BURS.FINAIDCODE = TRAN.FINAIDCODE
    WHERE
        TRAN.TRANSCODE IN %BURSARY%
    ORDER BY
        AUDITDATETIME
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%YEAR%", s_year)
    s_sql = s_sql.replace("%BURSARY%", s_burs_code)
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # OBTAIN STAFF DISCOUNT STUDENTS
    if l_debug:
        print("Import staff discount students...")
    sr_file = "X000_Transaction_staffdisc_student"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        s.FBUSENTID As STUDENT,
        Count(s.FSERVICESITE) As TRAN_COUNT,
        Total(s.AMOUNT) As TRAN_VALUE
    From
        %VSS%.X010_Studytrans s
    Where
        s.TRANSCODE In %STAFF%
    Group By
        s.FBUSENTID
    ;"""
    s_sql = s_sql.replace("%STAFF%", s_staff_code)
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """************************************************************************
    BUILD BURSARY TEST DATA
    ************************************************************************"""
    funcfile.writelog("BUILD BURSARY TEST DATA")
    if l_debug:
        print("BUILD BURSARY TEST DATA")

    # BUILD BURSARY VALUE PER STUDENT, BURSARY AND QUALIFICATION TYPE
    if l_debug:
        print("Build bursary value summary per student...")
    sr_file = "X001_Bursary_value_student"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        x000t.STUDENT,
        x000t.FFINAIDSITEID,
        x000t.FINAIDCODE,
        x000t.FINAIDNAME,
        x000s.LEVY_CATEGORY,
        x000b.SOURCE,
        Cast(Round(Total(x000t.AMOUNT),2) As Real) As AMOUNT_TOTAL,
        Cast(Count(x000t.CAMPUS) As Int) As TRAN_COUNT
    From
        X000_Transaction x000t Left Join
        X000_Student x000s On x000s.KSTUDBUSENTID = x000t.STUDENT Left Join
        X000_Bursary_master x000b ON x000b.FINAIDCODE = x000t.FINAIDCODE
    Group By
        x000t.STUDENT,
        x000t.FINAIDCODE,
        x000s.LEVY_CATEGORY
    """
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BURSARY SUMMARY PER STUDENT
    if l_debug:
        print("Build bursary summary per student...")
    sr_file = "X001_Bursary_summary_student"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        tran.STUDENT As student,
        Total(Distinct tran.AMOUNT) As total_burs,
        Total(Distinct loan.AMOUNT) As total_loan,
        Total(Distinct exte.AMOUNT) As total_external,
        Total(Distinct inte.AMOUNT) As total_internal,
        Total(Distinct rese.AMOUNT) As total_research,
        Total(Distinct trus.AMOUNT) As total_trust,
        Total(Distinct othe.AMOUNT) As total_other,
        staf.TRAN_VALUE As staff_discount,
        stud.ACTIVE_IND As active,
        stud.LEVY_CATEGORY As levy_category,
        stud.QUALIFICATION_NAME As qualification,
        stud.QUAL_TYPE As qualification_type,
        stud.DISCONTINUEDATE As discontinue_date,
        stud.RESULT As discontinue_result,
        stud.DISCONTINUE_REAS As discontinue_reason
    From
        X000_Transaction tran Left Join
        X000_Transaction loan On loan.STUDENT = tran.STUDENT
                And loan.SOURCE = 'BURSARY-LOAN SCHEMA' Left Join
        X000_Transaction exte On exte.STUDENT = tran.STUDENT
                And exte.SOURCE = 'EXTERNAL FUND' Left Join
        X000_Transaction inte On inte.STUDENT = tran.STUDENT
                And inte.SOURCE = 'UNIVERSITY FUND' Left Join
        X000_Transaction rese On rese.STUDENT = tran.STUDENT
                And rese.SOURCE = 'NRF (RESEARCH FUND)' Left Join
        X000_Transaction trus On trus.STUDENT = tran.STUDENT
                And trus.SOURCE = 'DONATE/TRUST FUND' Left Join
        X000_Transaction othe On othe.STUDENT = tran.STUDENT
                And othe.SOURCE Not In (
                'BURSARY-LOAN SCHEMA',
                'EXTERNAL FUND',
                'UNIVERSITY FUND',
                'NRF (RESEARCH FUND)',
                'DONATE/TRUST FUND') Left Join
        X000_Student stud On stud.KSTUDBUSENTID = tran.STUDENT Left Join
        X000_Transaction_staffdisc_student staf On staf.STUDENT = tran.STUDENT
    Group By
        tran.STUDENT    
    ;"""
    s_sql = s_sql.replace("%STAFF%", s_staff_code)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_mess:
        so_conn.commit()
        i = funcsys.tablerowcount(so_curs, sr_file)
        funcsms.send_telegram("", "administrator", "<b>" + str(i) + "</b> Bursary students")

    # MESSAGE
    # if l_mess:
    #     funcsms.send_telegram("", "administrator", "<b>" + s_function + "</b> end")

    """************************************************************************
    END OF SCRIPT
    ************************************************************************"""
    funcfile.writelog("END OF SCRIPT")
    if l_debug:
        print("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    so_conn.commit()
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("-" * len("completed: "+s_function))
    funcfile.writelog("COMPLETED: " + s_function.upper())

    return


if __name__ == '__main__':
    try:
        student_bursary("curr")
    except Exception as e:
        funcsys.ErrMessage(e)
