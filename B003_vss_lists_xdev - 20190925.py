"""
Development area for VSS lists
12 Sep 2019
AB Janse van Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcstudent

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
END OF SCRIPT
*****************************************************************************"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# SCRIPT LOG FILE
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B003_VSS_LISTS_XDEV")
funcfile.writelog("---------------------------")
print("-------------------")
print("B003_VSS_LISTS_XDEV")
print("-------------------")

# DECLARE VARIABLES
ed_path = "S:/_external_data/"  # External data path
so_path = "W:/Vss/"  # Source database path
so_file = "Vss.sqlite"  # Source database
re_path = "R:/Vss/"
l_vacuum: bool = False
s_period:str = "curr"

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
# print("Attach vss database...")

"""*****************************************************************************
TEMPORARY AREA
*****************************************************************************"""
print("TEMPORARY AREA")
funcfile.writelog("TEMPORARY AREA")

# IMPORT OWN LOOKUPS
print("Import own lookups...")
sr_file = "X000_Own_lookups"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute("CREATE TABLE " + sr_file + "(LOOKUP TEXT,LOOKUP_CODE TEXT,LOOKUP_DESCRIPTION TEXT)")
s_cols = ""
co = open(ed_path + "001_own_vss_lookups.csv", newline=None)
co_reader = csv.reader(co)
for row in co_reader:
    if row[0] == "LOOKUP":
        continue
    else:
        s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "')"
        so_curs.execute(s_cols)
so_conn.commit()
co.close()
funcfile.writelog("%t IMPORT TABLE: " + sr_file)

"""*****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

"""*************************************************************************
BUILD MODULES
*************************************************************************"""
print("BUILD MODULES")
funcfile.writelog("BUILD MODULES")

# BUILD MODULES
print("Build module...")
sr_file: str = "X002aa_Module"
s_sql = "CREATE VIEW " + sr_file + " AS " + """
SELECT
    MODU.KACADEMICPROGRAMID AS MODULE_ID,
    MODU.STARTDATE,
    MODU.ENDDATE,
    COUR.COURSECODE,
    COUL.COURSELEVEL,
    MODU.COURSEMODULE
FROM
    MODULE MODU Left Join
    COURSELEVEL COUL ON COUL.KACADEMICPROGRAMID = MODU.FCOURSELEVELAPID Left Join
    COURSE COUR ON COUR.KACADEMICPROGRAMID = COUL.FCOURSEAPID
;"""
so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: " + sr_file)

# BUILD MODULE PRESENTING ORGANIZATION
print("Build module present organization...")
sr_file: str = "X002ba_Module_present_org"
s_sql = "CREATE VIEW " + sr_file + " AS " + """
SELECT
    PRES.KPRESENTINGOUID,
    PRES.STARTDATE,
    PRES.ENDDATE,
    PRES.FBUSINESSENTITYID,
    ORGA.FSITEORGUNITNUMBER,
    ORGA.ORGUNIT_TYPE,
    ORGA.ORGUNIT_NAME,
    PRES.FMODULEAPID,
    MODU.COURSECODE,
    MODU.COURSELEVEL,
    MODU.COURSEMODULE,
    PRES.FCOURSEGROUPCODEID,
    NAME.LONG AS NAME_GROUP,
    NAME.LANK AS NAAM_GROEP,
    PRES.ISEXAMMODULE,
    PRES.LOCKSTAMP,
    PRES.AUDITDATETIME,
    PRES.FAUDITSYSTEMFUNCTIONID,
    PRES.FAUDITUSERCODE
FROM
    MODULEPRESENTINGOU PRES Left Join
    X000_Orgunitinstance ORGA ON ORGA.KBUSINESSENTITYID = PRES.FBUSINESSENTITYID Left Join
    X002aa_Module MODU ON MODU.MODULE_ID = PRES.FMODULEAPID Left Join
    X000_Codedescription NAME ON NAME.KCODEDESCID = PRES.FCOURSEGROUPCODEID
;"""
so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: " + sr_file)

# BUILD MODULE ENROLMENT CATEGORY
print("Build module enrolment category...")
sr_file = "X002bb_Module_present_enrol"
s_sql = "CREATE VIEW " + sr_file + " AS " + """
SELECT
    ENRC.KENROLMENTPRESENTATIONID,
    ENRC.ENROL_CAT_E,
    ENRC.ENROL_CAT_A,
    ENRC.PRESENT_CAT_E,
    ENRC.PRESENT_CAT_A,
    ENRC.STARTDATE,
    ENRC.ENDDATE,
    ENRO.FSITEORGUNITNUMBER,
    ENRO.ORGUNIT_TYPE,
    ENRO.ORGUNIT_NAME,
    ENRO.FMODULEAPID,
    ENRO.COURSECODE,
    ENRO.COURSELEVEL,
    ENRO.COURSEMODULE,
    ENRO.FCOURSEGROUPCODEID,
    ENRO.NAME_GROUP,
    ENRO.NAAM_GROEP,
    ENRO.ISEXAMMODULE,
    ENRC.EXAMSUBMINIMUM
FROM
    X000_Present_enrol_category ENRC Inner Join
    X002ba_Module_present_org ENRO ON ENRO.KPRESENTINGOUID = ENRC.FMODULEPRESENTINGOUID
;"""
so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: " + sr_file)

# BUILD CURRENT STUDENT MODULE LIST
print("Build student module enrolments...")
sr_file = "X002_Module_curr"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    STUD.KENROLSTUDID,
    STUD.KSTUDBUSENTID,
    MODU.FSITEORGUNITNUMBER,
    STUD.STARTDATE,
    STUD.ENDDATE,
    STUD.DATEENROL,
    Upper(MODU.COURSECODE) As COURSECODE,
    MODU.COURSELEVEL,
    MODU.COURSEMODULE,
    Upper(MODU.COURSECODE)||' '||MODU.COURSELEVEL||' '||MODU.COURSEMODULE As MODULE,
    Substr(MODU.COURSEMODULE,1,1) As COURSESEM,
    CASE
        When Substr(MODU.COURSEMODULE,1,1) = '2' Then '2'
        When Substr(MODU.COURSEMODULE,1,1) = '5' Then '2'
        When Substr(MODU.COURSEMODULE,1,1) = '6' Then '2'
        Else '1'
    END As COURSESECS,
    Upper(MODU.ORGUNIT_TYPE) As ORGUNIT_TYPE,
    Upper(MODU.ORGUNIT_NAME) As ORGUNIT_NAME,
    Upper(MODU.NAME_GROUP) As NAME_GROUP,
    Upper(MODU.NAAM_GROEP) As NAAM_GROEP,
    STUD.FMODULETYPECODEID,
    Upper(TYPE.LONG) AS NAME_TYPE,
    Upper(TYPE.LANK) AS NAAM_TYPE,
    STUD.DATEDISCONTINUED,
    STUD.FCOMPLETEREASONCODEID,
    Upper(REAS.LONG) AS NAME_REAS,
    Upper(REAS.LANK) AS NAAM_REAS,
    STUD.FSTUDYCENTREMODAPID,
    STUD.FQUALLEVELENROLSTUDID,
    STUD.FENROLMENTPRESENTATIONID,
    STUD.FEXAMCENTREMODAPID,
    STUD.FPRESENTATIONLANGUAGEID,
    STUD.FMODPERIODENROLPRESCATID,
    STUD.ACADEMICYEAR,
    STUD.ISNEWENROLMENT,
    STUD.ISPROCESSEDONLINE,
    STUD.ISREPEATINGMODULE,
    STUD.ISEXEMPTION,
    STUD.FACKTYPECODEID,
    STUD.FACKSTUDBUSENTID,
    STUD.FACKENROLSTUDID,
    STUD.FACKMODENROLSTUDID,
    STUD.FACKMODSTUDBUSENTID,
    STUD.ISCONDITIONALREG,
    STUD.LOCKSTAMP,
    STUD.AUDITDATETIME,
    STUD.FAUDITSYSTEMFUNCTIONID,
    STUD.FAUDITUSERCODE,
    STUD.REGALLOWED,
    STUD.ISDISCOUNTED,
    MODU.KENROLMENTPRESENTATIONID,
    Upper(MODU.ENROL_CAT_E) As ENROL_CAT_E,
    Upper(MODU.ENROL_CAT_A) As ENROL_CAT_A,
    Upper(MODU.PRESENT_CAT_E) As PRESENT_CAT_E,
    Upper(MODU.PRESENT_CAT_A) As PRESENT_CAT_A,
    MODU.STARTDATE,
    MODU.ENDDATE,
    MODU.FMODULEAPID,
    MODU.FCOURSEGROUPCODEID,
    MODU.ISEXAMMODULE,
    MODU.EXAMSUBMINIMUM
From
    MODULEENROLSTUD_CURR STUD Left Join
    X002bb_Module_present_enrol MODU On MODU.KENROLMENTPRESENTATIONID = STUD.FENROLMENTPRESENTATIONID Left Join
    X000_Codedescription TYPE ON TYPE.KCODEDESCID = STUD.FMODULETYPECODEID Left Join
    X000_Codedescription REAS ON REAS.KCODEDESCID = STUD.FCOMPLETEREASONCODEID
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

"""*****************************************************************************
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
funcfile.writelog("------------------------------")
funcfile.writelog("COMPLETED: B003_VSS_LISTS_XDEV")
