"""
Script to test STUDENT BURSARIES
Created on: 29 Jan 2021
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3
# import csv

# IMPORT OWN MODULES
from _my_modules import funcconf
# from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcstat
from _my_modules import funcsys
from _my_modules import funcsms
# from _my_modules import functest

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
END OF SCRIPT
"""

# SCRIPT WIDE VARIABLES
s_function: str = "C303_test_student_bursary_xdev"


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
    """
    if s_period == "prev":
        s_year = funcdate.prev_year()
    else:
        s_year = funcdate.cur_year()
    """
    # ed_path: str = "S:/_external_data/"  # External data path
    # re_path: str = "R:/Vss/" + s_year
    so_path: str = "W:/Vss_fee/"  # Source database path
    so_file: str = "Vss_test_bursary.sqlite"
    l_debug: bool = True
    # l_mail: bool = funcconf.l_mail_project
    l_mail: bool = True
    # l_mess: bool = funcconf.l_mess_project
    l_mess: bool = True
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

    """************************************************************************
    BUILD MASTER TABLES
    ************************************************************************"""
    funcfile.writelog("BUILD MASTER TABLES")
    if l_debug:
        print("BUILD MASTER TABLES")

    # BUILD BURSARY VALUE PER STUDENT
    if l_debug:
        print("Build bursary value summary per student...")
    sr_file = "X001_Bursary_student_value_total"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        x000t.STUDENT,
        x000s.LEVY_CATEGORY,
        Cast(Round(Total(x000t.AMOUNT),2) As Real) As AMOUNT_TOTAL,
        Cast(Count(x000t.CAMPUS) As Int) As TRAN_COUNT
    From
        X000_Transaction x000t Left Join
        X000_Student x000s On x000s.KSTUDBUSENTID = x000t.STUDENT
    Group By
        x000t.STUDENT
    """
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD BURSARY VALUE PER STUDENT, BURSARY AND QUALIFICATION TYPE
    if l_debug:
        print("Build bursary value summary per student...")
    sr_file = "X001_Bursary_student_value"
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

    # OBTAIN STAFF DISCOUNT STUDENTS
    if l_debug:
        print("Import staff discount students...")
    sr_file = "X001_Bursary_student_value_staffdisc"
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
    print(s_sql)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD STUDENT RELATIONSHIPS
    if l_debug:
        print("Build student relationships...")
    sr_file = "X000_Student_relationship"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        s.KSTUDBUSENTID,
        e.name_full As EMP_NAME_FULL,
        e.user_person_type As EMP_PERSON_TYPE,
        r.KBUSENTRELID,
        r.KRELATIONSHIPTYPECODEID,
        Upper(c.CODELONGDESCRIPTION) As REL_TYPE,
        r.KRELATEDBUSINESSENTITYID,
        p.name_full As REL_NAME_FULL,
        p.user_person_type As REL_PERSON_TYPE,
        Max(r.STARTDATE) As STARTDATE,
        r.ENDDATE,
        r.LOCKSTAMP,
        r.AUDITDATETIME,
        r.FAUDITSYSTEMFUNCTIONID,
        r.FAUDITUSERCODE        
    From
        X000_Student s Inner Join
        VSS.BUSINESSENTITYRELATIONSHIP r On r.KBUSINESSENTITYID = s.KSTUDBUSENTID And
            r.KRELATEDBUSINESSENTITYID >= 10000000 And
            r.KRELATIONSHIPTYPECODEID in (6713, 6712, 9719, 6573, 6574) And
            strftime('%Y-%m-%d', '%DATE%') between r.STARTDATE and ifnull(r.ENDDATE, '4712-12-31') Left Join
        VSS.CODEDESCRIPTION c On c.KCODEDESCID = r.KRELATIONSHIPTYPECODEID And
            c.KSYSTEMLANGUAGECODEID = 3 Left Join
        PEOPLE.X000_PEOPLE e On e.employee_number = Cast(s.KSTUDBUSENTID As TEXT) Left Join
        PEOPLE.X000_PEOPLE p On p.employee_number = Cast(r.KRELATEDBUSINESSENTITYID As TEXT)
    Group By
        s.KSTUDBUSENTID,
        r.KRELATIONSHIPTYPECODEID            
    ;"""
    s_sql = s_sql.replace("%DATE%", funcdate.today())
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD STUDENT EMPLOYEES
    if l_debug:
        print("Build student employees...")
    sr_file = "X001_Student_employee"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "CREATE VIEW " + sr_file + " AS" + """
    Select
        ST.KSTUDBUSENTID,
        ST.EMP_NAME_FULL,
        ST.EMP_PERSON_TYPE
    From
        X000_Student_relationship ST
    Where
        ST.EMP_NAME_FULL != ""
    Group By
        ST.KSTUDBUSENTID
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD BURSARY STUDENT EMPLOYEES TYPE
    if l_debug:
        print("Build bursary employee types...")
    sr_file = "X002_Bursary_student_employee_type"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        BU.STUDENT,
        ST.EMP_NAME_FULL,
        ST.EMP_PERSON_TYPE,
        BU.FINAIDCODE,
        BU.FINAIDNAME,
        BU.LEVY_CATEGORY,
        BU.AMOUNT_TOTAL,
        Cast(SD.TRAN_VALUE As REAL) AS STAFF_DISC
    From
        X001_Bursary_student_value BU Inner Join
        X001_Student_employee ST On ST.KSTUDBUSENTID = BU.STUDENT Left Join
        X001_Bursary_student_value_staffdisc SD On SD.STUDENT = BU.STUDENT        
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD BURSARY EMPLOYEES TYPE COUNT
    if l_debug:
        print("Build bursary employee type count...")
    sr_file = "X002_Bursary_student_employee_type_count"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "CREATE VIEW " + sr_file + " AS" + """
    Select
        BU.FINAIDCODE,
        BU.FINAIDNAME,
        BU.LEVY_CATEGORY,
        Count(BU.AMOUNT_TOTAL) As BURSARY_COUNT,
        Total(BU.AMOUNT_TOTAL) As BURSARY_VALUE,
        BM.SOURCE,
        BM.DEGREE_TYPE,
        BM.COST_STRING,
        BM.APPLICATION_PROCESS,
        BM.SBL_EVALUATION,
        BM.BURSARY_OFFICE_PROCESS
    From
        X002_Bursary_student_employee_type BU Left Join
        X000_Bursary_master BM On BM.FINAIDCODE = BU.FINAIDCODE
    Group By
        BU.FINAIDCODE,
        BM.SOURCE,
        BM.DEGREE_TYPE
    Order By
        BM.SOURCE Desc,
        BM.DEGREE_TYPE Desc
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD STUDENT PARENT
    if l_debug:
        print("Build student parents...")
    sr_file = "X001_Student_parent"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "CREATE VIEW " + sr_file + " AS" + """
    Select
        ST.KSTUDBUSENTID,
        ST.REL_TYPE,
        ST.KRELATEDBUSINESSENTITYID,
        ST.REL_NAME_FULL,
        Min(ST.REL_PERSON_TYPE) As REL_PERSON_TYPE
    From
        X000_Student_relationship ST
    Where
        ST.EMP_NAME_FULL Is Null And
        ST.REL_NAME_FULL != ""
    Group By
        ST.KSTUDBUSENTID
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD BURSARY STUDENT PARENT TYPE
    if l_debug:
        print("Build bursary parent types...")
    sr_file = "X002_Bursary_student_parent_type"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        BU.STUDENT,
        ST.REL_TYPE,
        ST.KRELATEDBUSINESSENTITYID,
        ST.REL_NAME_FULL,
        ST.REL_PERSON_TYPE,
        BU.FINAIDCODE,
        BU.FINAIDNAME,
        BU.LEVY_CATEGORY,
        BU.AMOUNT_TOTAL,
        Cast(SD.TRAN_VALUE As REAL) AS STAFF_DISC
    From
        X001_Bursary_student_value BU Inner Join
        X001_Student_parent ST On ST.KSTUDBUSENTID = BU.STUDENT Left Join
        X001_Bursary_student_value_staffdisc SD On SD.STUDENT = BU.STUDENT 
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD BURSARY EMPLOYEES TYPE COUNT
    if l_debug:
        print("Build bursary parent type count...")
    sr_file = "X002_Bursary_student_parent_type_count"
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    s_sql = "CREATE VIEW " + sr_file + " AS" + """
    Select
        PA.FINAIDCODE,
        PA.FINAIDNAME,
        PA.LEVY_CATEGORY,
        Count(PA.AMOUNT_TOTAL) As BURSARY_COUNT,
        Total(PA.AMOUNT_TOTAL) As BURSARY_VALUE,
        BM.FINAIDCODE As FINAIDCODE1,
        BM.SOURCE,
        BM.DEGREE_TYPE,
        BM.COST_STRING,
        BM.APPLICATION_PROCESS,
        BM.SBL_EVALUATION,
        BM.BURSARY_OFFICE_PROCESS
    From
        X002_Bursary_student_parent_type PA Left Join
        X000_Bursary_master BM On BM.FINAIDCODE = PA.FINAIDCODE
    Group By
        PA.FINAIDCODE
    Order By
        BM.SOURCE Desc,
        BM.DEGREE_TYPE Desc
    ;"""
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

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
