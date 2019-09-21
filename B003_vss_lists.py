"""
Script to build standard VSS lists
Created on: 01 Mar 2018
Copyright: Albert J v Rensburg
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
BUILD STANDARD LOOKUP TABLES
BUILD QUALIFICATION MASTER LIST
BUILD STUDENTS
BUILD MODULES
BUILD PROGRAMS
BUILD BURSARIES
*****************************************************************************"""

def vss_lists():
    """
    Function to build vss master lists
    :return: Nothing
    """

    """*************************************************************************
    ENVIRONMENT
    *************************************************************************"""
    print("ENVIRONMENT")
    funcfile.writelog("ENVIRONMENT")

    # LOG
    print("--------------")
    print("B003_VSS_LISTS")
    print("--------------")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: B003_VSS_LISTS")
    funcfile.writelog("----------------------")
    ilog_severity = 1

    # DECLARE VARIABLES
    so_path: str = "W:/Vss/"  # Source database path
    so_file: str = "Vss.sqlite"  # Source database
    re_path: str = "R:/Vss/"  # Results
    ed_path: str = "S:/_external_data/"  # External data location
    s_sql: str = ""  # SQL statements
    l_export: bool = False  # Export files

    # OPEN DATABASE
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    """*************************************************************************
    BUILD STANDARD LOOKUP TABLES
    *************************************************************************"""
    print("BUILD STANDARD LOOKUP TABLES")
    funcfile.writelog("BUILD STANDARD LOOKUP TABLES")

    # IMPORT OWN LOOKUPS
    print("Import own lookups...")
    tb_name = "X000_Own_lookups"
    so_curs.execute("DROP TABLE IF EXISTS " + tb_name)
    so_curs.execute("CREATE TABLE " + tb_name + "(LOOKUP TEXT,LOOKUP_CODE TEXT,LOOKUP_DESCRIPTION TEXT)")
    s_cols = ""
    co = open(ed_path + "001_own_vss_lookups.csv", "rU")
    co_reader = csv.reader(co)
    for row in co_reader:
        if row[0] == "LOOKUP":
            continue
        else:
            s_cols = "INSERT INTO " + tb_name + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the impoted data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + tb_name)

    # BUILD CODE DESCRIPTIONS
    print("Build code descriptions...")
    s_sql = "CREATE TABLE X000_Codedescription AS " + """
    SELECT
        CODE.KCODEDESCID,
        CODE.CODELONGDESCRIPTION AS LANK,
        CODE.CODESHORTDESCRIPTION AS KORT,
        LONG.CODELONGDESCRIPTION AS LONG,
        LONG.CODESHORTDESCRIPTION AS SHORT
    FROM
        CODEDESCRIPTION CODE Inner Join
        CODEDESCRIPTION LONG ON LONG.KCODEDESCID = CODE.KCODEDESCID
    WHERE
        CODE.KSYSTEMLANGUAGECODEID = 2 AND
        LONG.KSYSTEMLANGUAGECODEID = 3
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS X000_Codedescription")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: X000_Codedescription")

    # BUILD ORGANIZATION UNIT NAME
    print("Build org unit name...")
    s_sql = "CREATE VIEW X000_Orgunitname AS " + """
    SELECT
        NAME.KORGUNITNUMBER,
        NAME.KSTARTDATE,
        NAME.ENDDATE,
        NAME.SHORTNAME AS KORT,
        NAME.LONGNAME AS LANK,
        ENGL.SHORTNAME AS SHORT,
        ENGL.LONGNAME AS LONG
    FROM
        ORGUNITNAME NAME Left Join
        ORGUNITNAME ENGL ON ENGL.KORGUNITNUMBER = NAME.KORGUNITNUMBER AND
            ENGL.KSTARTDATE = NAME.KSTARTDATE
    WHERE
        NAME.KSYSTEMLANGUAGECODEID = 2 AND
        ENGL.KSYSTEMLANGUAGECODEID = 3
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS X000_Orgunitname")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_Orgunitname")

    # BUILD ORGANIZATION UNIT
    print("Build org unit...")
    s_sql = "CREATE VIEW X000_Orgunit AS " + """
    SELECT
        ORGU.KORGUNITNUMBER,
        ORGU.STARTDATE,
        ORGU.ENDDATE,
        ORGU.FORGUNITTYPECODEID,
        ORGU.FORGUNITTYPECODE,
        ORGU.ISSITE,
        ORGU.LOCKSTAMP,
        ORGU.AUDITDATETIME,
        ORGU.FAUDITSYSTEMFUNCTIONID,
        ORGU.FAUDITUSERCODE,
        NAME.KSTARTDATE,
        NAME.ENDDATE AS ENDDATE1,
        NAME.KORT,
        NAME.LANK,
        NAME.SHORT,
        NAME.LONG,
        DESC.LONG AS UNIT_TYPE
    FROM
        ORGUNIT ORGU Left Join
        X000_Orgunitname NAME ON NAME.KORGUNITNUMBER = ORGU.KORGUNITNUMBER Left Join
        X000_Codedescription DESC ON DESC.KCODEDESCID = ORGU.FORGUNITTYPECODEID
    WHERE
        NAME.KSTARTDATE <= ORGU.STARTDATE AND
        NAME.ENDDATE >= ORGU.STARTDATE
    ORDER BY
        ORGU.KORGUNITNUMBER
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_Orgunit")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_Orgunit")

    # BUILD ORGANIZATION UNIT INSTANCE
    print("Build org unit instance...")
    s_sql = "CREATE TABLE X000_Orgunitinstance AS " + """
    SELECT
        ORGI.KBUSINESSENTITYID,
        ORGI.FORGUNITNUMBER,  
        ORGU.UNIT_TYPE AS ORGUNIT_TYPE,
        ORGU.LONG AS ORGUNIT_NAME,
        ORGI.FSITEORGUNITNUMBER,
        ORGI.STARTDATE,  
        ORGI.ENDDATE,
        ORGI.FMANAGERTYPECODEID,
        MANT.LONG AS MANAGER_TYPE,
        ORGI.PLANNEDRESTRUCTUREDATE,
        ORGI.FMANAGERTYPECODE,
        ORGI.LOCKSTAMP,
        ORGI.AUDITDATETIME,
        ORGI.FAUDITSYSTEMFUNCTIONID,
        ORGI.FAUDITUSERCODE,
        ORGU.ISSITE,
        ORGU.KORT,
        ORGU.LANK,
        ORGU.SHORT,
        ORGI.FNEWBUSINESSENTITYID  
    FROM
        ORGUNITINSTANCE ORGI Left Join
        X000_Orgunit ORGU ON ORGU.KORGUNITNUMBER = ORGI.FORGUNITNUMBER Left Join
        X000_Codedescription MANT ON MANT.KCODEDESCID = ORGI.FMANAGERTYPECODEID
    ORDER BY
        ORGI.KBUSINESSENTITYID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS X000_Orgunitinstance")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: X000_Orgunitinstance")

    # BUILD STUDENT RESULTS
    funcfile.writelog("STUDENT RESULTS")
    print("Build student results...")
    sr_file = "X000_Student_qualfos_result"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        RESU.KBUSINESSENTITYID,
        RESU.KACADEMICPROGRAMID,
        RESU.KQUALFOSRESULTCODEID,
        Upper(REAS.LONG) AS RESULT,
        RESU.KRESULTYYYYMM,
        RESU.KSTUDQUALFOSRESULTID,
        RESU.FGRADUATIONCEREMONYID,
        RESU.FPOSTPONEMENTCODEID,
        Upper(POST.LONG) AS POSTPONE_REAS,
        RESU.RESULTISSUEDATE,
        RESU.DISCONTINUEDATE,
        RESU.FDISCONTINUECODEID,
        Upper(DISC.LONG) AS DISCONTINUE_REAS,
        RESU.RESULTPASSDATE,
        RESU.FLANGUAGECODEID,
        RESU.ISSUESURNAME,
        RESU.CERTIFICATESEQNUMBER,
        RESU.AVGMARKACHIEVED,
        RESU.PROCESSSEQNUMBER,
        RESU.FRECEIPTID,
        RESU.FRECEIPTLINEID,
        RESU.ISINABSENTIA,
        RESU.FPROGRAMAPID,
        RESU.FISSUETYPECODEID,
        Upper(TYPE.LONG) AS ISSUE_TYPE,
        RESU.DATEPRINTED,
        RESU.LOCKSTAMP,
        RESU.AUDITDATETIME,
        RESU.FAUDITSYSTEMFUNCTIONID,
        RESU.FAUDITUSERCODE,
        RESU.FAPPROVEDBYCODEID,
        RESU.FAPPROVEDBYUSERCODE,
        RESU.DATERESULTAPPROVED,
        RESU.FENROLMENTPRESENTATIONID,
        RESU.CERTDISPATCHDATE,
        RESU.CERTDISPATCHREFNO,
        RESU.ISSUEFIRSTNAMES
    From
        STUDQUALFOSRESULT RESU
        LEFT JOIN X000_Codedescription REAS ON REAS.KCODEDESCID = RESU.KQUALFOSRESULTCODEID
        LEFT JOIN X000_Codedescription POST ON POST.KCODEDESCID = RESU.FPOSTPONEMENTCODEID
        LEFT JOIN X000_Codedescription DISC ON DISC.KCODEDESCID = RESU.FDISCONTINUECODEID
        LEFT JOIN X000_Codedescription TYPE ON TYPE.KCODEDESCID = RESU.FISSUETYPECODEID
    Order By
        RESU.KBUSINESSENTITYID,
        RESU.AUDITDATETIME DESC
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD ACADEMIC PROGRAM NAME
    print("Build academic program name 1 ...")
    sr_file = "X000ba_Academicprog_shortname"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        PROG.KACADEMICPROGRAMNAMEID,
        PROG.FACADEMICPROGRAMID,
        PROG.FNAMEPURPOSECODEID,
        Upper(PURP.LONG) As PURPOSE,
        PROG.FSYSTEMLANGUAGECODEID,
        Upper(LANG.LONG) As LANGUAGE,
        PROG.STARTDATE,
        PROG.ENDDATE,
        PROG.SHORTDESCRIPTION,
        PROG.WFSHORTDESC,
        PROG.LOCKSTAMP,
        PROG.AUDITDATETIME,
        PROG.FAUDITSYSTEMFUNCTIONID,
        PROG.FAUDITUSERCODE
    From
        ACADEMICPROGRAMSHORTNAME PROG Left Join
        X000_Codedescription PURP On PURP.KCODEDESCID = PROG.FNAMEPURPOSECODEID Left Join
        X000_Codedescription LANG On LANG.KCODEDESCID = PROG.FSYSTEMLANGUAGECODEID
    Order By
        PROG.FACADEMICPROGRAMID,
        LANGUAGE,
        PURPOSE,
        PROG.STARTDATE
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # BUILD ACADEMIC PROGRAM NAME SUMMARY
    print("Build academic program name summary 2...")
    sr_file = "X000bb_Academicprog_summary"
    s_sql = "CREATE VIEW " + sr_file + " AS " + """
    Select
        PROG.FNAMEPURPOSECODEID,
        PROG.PURPOSE,
        Count(PROG.KACADEMICPROGRAMNAMEID) As PURPOSE_COUNT
    From
        X000ba_Academicprog_shortname PROG
    Where
        PROG.FSYSTEMLANGUAGECODEID = 3
    Group By
        PROG.FNAMEPURPOSECODEID
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)

    # QUALIFICATION, MODULE, PROGRAM MASTER
    print("Build present enrol master...")
    sr_file = "X000aa_QMP_Master"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PRES.KENROLMENTPRESENTATIONID,
        PRES.FQUALPRESENTINGOUID,
        PRES.FMODULEPRESENTINGOUID,
        PRES.FPROGRAMPRESENTINGOUID,
        PRES.FPROGRAMAPID,
        PRES.FENROLMENTCATEGORYCODEID,
        Upper(ENRO.LONG) As ENROL_CATEGORY,
        PRES.FPRESENTATIONCATEGORYCODEID,
        Upper(PRES.LONG) As PRESENT_CATEGORY,
        PRES.MAXNOOFSTUDENTS,
        PRES.MINNOOFSTUDENTS,
        PRES.ISVERIFICATIONREQUIRED,
        PRES.EXAMSUBMINIMUM,
        PRES.STARTDATE AS MASTER_STARTDATE,
        PRES.ENDDATE AS MASTER_ENDDATE,
        PRES.AUDITDATETIME MASTER_AUDITDATETIME,
        PRES.FAUDITSYSTEMFUNCTIONID AS MASTER_SYSID,
        PRES.FAUDITUSERCODE AS MASTER_USERCODE
    From
        PRESENTOUENROLPRESENTCAT PRES Left Join
        X000_Codedescription ENRO ON ENRO.KCODEDESCID = PRES.FENROLMENTCATEGORYCODEID Left Join 
        X000_Codedescription PRES ON PRES.KCODEDESCID = PRES.FPRESENTATIONCATEGORYCODEID 
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """*************************************************************************
    BUILD QUALIFICATION MASTER LIST
    *************************************************************************"""
    print("BUILD QUALIFICATION MASTER LIST")
    funcfile.writelog("BUILD QUALIFICATION MASTER LIST")

    # BUILD QUALIFICATION STEP 1 - QUALIFICATION LEVEL
    print("Build qualification level (step 1)...")
    sr_file = "X001aa_Qual_level"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        QUAL.KACADEMICPROGRAMID,
        QUAL.STARTDATE,
        QUAL.ENDDATE,
        QUAL.QUALIFICATIONLEVEL,
        Upper(FINA.LONG) AS FINAL_STATUS,
        Upper(LEVY.LONG) AS LEVY_CATEGORY,
        QUAL.FFIELDOFSTUDYAPID,
        QUAL.FFINALSTATUSCODEID,
        QUAL.FLEVYCATEGORYCODEID,
        QUAL.LOCKSTAMP,
        QUAL.AUDITDATETIME,
        QUAL.FAUDITSYSTEMFUNCTIONID,
        QUAL.FAUDITUSERCODE,
        QUAL.PHASEOUTDATE
    From
        QUALIFICATIONLEVEL QUAL Left Join
        X000_Codedescription FINA ON FINA.KCODEDESCID = QUAL.FFINALSTATUSCODEID Left Join
        X000_Codedescription LEVY ON LEVY.KCODEDESCID = QUAL.FLEVYCATEGORYCODEID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD QUALIFICATION STEP 2 - QUALIFICATION LEVEL
    print("Build qualification presentation (step 2)...")
    sr_file = "X001ab_Qual_level_present"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        MAST.KENROLMENTPRESENTATIONID,
        LEVE.QUALIFICATIONLEVEL,
        PRES.FQUALLEVELAPID,
        MAST.ENROL_CATEGORY,
        MAST.PRESENT_CATEGORY,
        LEVE.FINAL_STATUS,
        LEVE.LEVY_CATEGORY,
        LEVE.FFIELDOFSTUDYAPID,
        MAST.FPROGRAMAPID,
        PRES.FBUSINESSENTITYID,
        ORGA.FSITEORGUNITNUMBER As SITEID,
        Upper(SITE.LONG) As CAMPUS,
        Upper(ORGA.ORGUNIT_TYPE) As ORGUNIT_TYPE,
        Upper(ORGA.ORGUNIT_NAME) As ORGUNIT_NAME,
        Upper(ORGA.MANAGER_TYPE) As ORGUNIT_MANAGER,
        MAST.MAXNOOFSTUDENTS,
        MAST.MINNOOFSTUDENTS,
        PRES.NUMBEROFSTUDENTS,
        MAST.ISVERIFICATIONREQUIRED,
        MAST.EXAMSUBMINIMUM,
        MAST.FQUALPRESENTINGOUID,
        MAST.MASTER_STARTDATE,
        MAST.MASTER_ENDDATE,
        MAST.MASTER_AUDITDATETIME,
        MAST.MASTER_SYSID,
        MAST.MASTER_USERCODE,
        PRES.KPRESENTINGOUID,
        PRES.STARTDATE As PRESENT_STARTDATE,
        PRES.ENDDATE As PRESENT_ENDDATE,
        PRES.AUDITDATETIME As PRESENT_AUDITDATETIME,
        PRES.FAUDITSYSTEMFUNCTIONID AS PRESENT_SYSID,
        PRES.FAUDITUSERCODE As PRESENT_USERCODE,
        LEVE.KACADEMICPROGRAMID As LEVEL_KACADEMICPROGRAMID,
        LEVE.STARTDATE As LEVEL_STARTDATE,
        LEVE.ENDDATE As LEVEL_ENDDATE,
        LEVE.PHASEOUTDATE As LEVEL_PHASEOUTDATE,    
        LEVE.AUDITDATETIME As LEVEL_AUDITDATETIME,
        LEVE.FAUDITSYSTEMFUNCTIONID As LEVEL_SYSID,
        LEVE.FAUDITUSERCODE As LEVEL_USERCODE
    From
        X000aa_QMP_Master MAST Inner Join
        QUALLEVELPRESENTINGOU PRES On PRES.KPRESENTINGOUID = MAST.FQUALPRESENTINGOUID Left Join
        X000_Orgunitinstance ORGA On ORGA.KBUSINESSENTITYID = PRES.FBUSINESSENTITYID Left Join
        X000_Orgunit SITE On SITE.KORGUNITNUMBER = ORGA.FSITEORGUNITNUMBER Left Join
        X001aa_Qual_level LEVE On LEVE.KACADEMICPROGRAMID = PRES.FQUALLEVELAPID 
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD QUALIFICATION STEP 3 - FIELD OF STUDY
    print("Build qualification field of study (step 3)...")
    sr_file = "X001ac_Qual_fieldofstudy"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        QUAL.KENROLMENTPRESENTATIONID,
        FOFS.QUALIFICATIONFIELDOFSTUDY,
        QUAL.QUALIFICATIONLEVEL,
        QUAL.FQUALLEVELAPID,
        QUAL.ENROL_CATEGORY,
        QUAL.PRESENT_CATEGORY,
        QUAL.FINAL_STATUS,
        QUAL.LEVY_CATEGORY,
        Upper(SELE.LONG) As FOS_SELECTION,
        QUAL.FFIELDOFSTUDYAPID,
        FOFS.FQUALIFICATIONAPID,
        QUAL.FPROGRAMAPID,
        QUAL.FBUSINESSENTITYID,
        QUAL.SITEID,
        QUAL.CAMPUS,
        QUAL.ORGUNIT_TYPE,
        QUAL.ORGUNIT_NAME,
        QUAL.ORGUNIT_MANAGER,
        QUAL.MAXNOOFSTUDENTS,
        QUAL.MINNOOFSTUDENTS,
        QUAL.NUMBEROFSTUDENTS,
        QUAL.ISVERIFICATIONREQUIRED,
        QUAL.EXAMSUBMINIMUM,
        QUAL.FQUALPRESENTINGOUID,
        QUAL.MASTER_STARTDATE,
        QUAL.MASTER_ENDDATE,
        QUAL.MASTER_AUDITDATETIME,
        QUAL.MASTER_SYSID,
        QUAL.MASTER_USERCODE,
        QUAL.KPRESENTINGOUID,
        QUAL.PRESENT_STARTDATE,
        QUAL.PRESENT_ENDDATE,
        QUAL.PRESENT_AUDITDATETIME,
        QUAL.PRESENT_SYSID,
        QUAL.PRESENT_USERCODE,
        QUAL.LEVEL_KACADEMICPROGRAMID,
        QUAL.LEVEL_STARTDATE,
        QUAL.LEVEL_ENDDATE,
        QUAL.LEVEL_PHASEOUTDATE,
        QUAL.LEVEL_AUDITDATETIME,
        QUAL.LEVEL_SYSID,
        QUAL.LEVEL_USERCODE,
        FOFS.KACADEMICPROGRAMID As FOS_KACADEMICPROGRAMID,
        FOFS.STARTDATE As FOS_STARTDATE,
        FOFS.ENDDATE As FOS_ENDDATE,
        FOFS.AUDITDATETIME As FOS_AUDITDATETIME,
        FOFS.FAUDITSYSTEMFUNCTIONID As FOS_SYSID,
        FOFS.FAUDITUSERCODE As FOS_USERCODE
    From
        X001ab_Qual_level_present QUAL Left Join
        FIELDOFSTUDY FOFS On FOFS.KACADEMICPROGRAMID = QUAL.FFIELDOFSTUDYAPID Left Join
        X000_Codedescription SELE On SELE.KCODEDESCID = FOFS.FSELECTIONCODEID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD QUALIFICATION STEP 4 - QUALIFICATION
    print("Build qualification (step 4)...")
    sr_file = "X001ad_Qual"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        QUAL.KACADEMICPROGRAMID,
        QUAL.STARTDATE,
        QUAL.ENDDATE,
        QUAL.QUALIFICATIONCODE,
        Upper(QUALTYPE.LONG) AS QUALIFICATION_TYPE,
        Cast(MINDUR.LANK As REAL) AS MIN,
        Upper(MINUNI.LONG) AS MIN_UNIT,
        Cast(MAXDUR.LONG As REAL) AS MAX,
        Upper(MAXUNI.LONG) AS MAX_UNIT,
        Upper(CERTTYPE.LONG) AS CERT_TYPE,
        Upper(LEVY.LONG) AS LEVY_TYPE,
        QUAL.ISVATAPPLICABLE,
        QUAL.ISPRESENTEDBEFOREAPPROVAL,
        QUAL.ISDIRECTED,
        QUAL.AUDITDATETIME,
        QUAL.FAUDITSYSTEMFUNCTIONID,
        QUAL.FAUDITUSERCODE
    From
        QUALIFICATION QUAL Left Join
        X000_Codedescription MINDUR ON MINDUR.KCODEDESCID = QUAL.FMINDURATIONCODEID Left Join
        X000_Codedescription MINUNI ON MINUNI.KCODEDESCID = QUAL.FMINDURPERIODUNITCODEID Left Join
        X000_Codedescription MAXDUR ON MAXDUR.KCODEDESCID = QUAL.FMAXDURATIONCODEID Left Join
        X000_Codedescription MAXUNI ON MAXUNI.KCODEDESCID = QUAL.FMAXDURPERIODUNITCODEID Left Join
        X000_Codedescription QUALTYPE ON QUALTYPE.KCODEDESCID = QUAL.FQUALIFICATIONTYPECODEID Left Join
        X000_Codedescription CERTTYPE ON CERTTYPE.KCODEDESCID = QUAL.FCERTIFICATETYPECODEID Left Join
        X000_Codedescription LEVY ON LEVY.KCODEDESCID = QUAL.FLEVYLEVELCODEID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD QUALIFICATION STEP 5 - ADD QUALIFICATION
    print("Build qualification final (step 5)...")
    sr_file = "X001ae_Qual_final"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        QUAL.KENROLMENTPRESENTATIONID,
        Upper(NAME.SHORTDESCRIPTION) As QUALIFICATION_NAME,
        Trim(QAUD.QUALIFICATIONCODE)||' '||
            Trim(QUAL.QUALIFICATIONFIELDOFSTUDY)||' '||
            Trim(QUAL.QUALIFICATIONLEVEL) As QUALIFICATION,
        QAUD.QUALIFICATIONCODE,
        QUAL.QUALIFICATIONFIELDOFSTUDY,
        QUAL.QUALIFICATIONLEVEL,
        QAUD.QUALIFICATION_TYPE,
        QUAL.FQUALLEVELAPID,
        QUAL.ENROL_CATEGORY,
        QUAL.PRESENT_CATEGORY,
        QUAL.FINAL_STATUS,
        QUAL.LEVY_CATEGORY,
        QAUD.CERT_TYPE,
        QAUD.LEVY_TYPE,
        QUAL.FOS_SELECTION,
        QUAL.FPROGRAMAPID,
        QUAL.FBUSINESSENTITYID,
        QUAL.SITEID,
        QUAL.CAMPUS,
        QUAL.ORGUNIT_TYPE,
        QUAL.ORGUNIT_NAME,
        QUAL.ORGUNIT_MANAGER,
        QAUD.MIN,
        QAUD.MIN_UNIT,
        QAUD.MAX,
        QAUD.MAX_UNIT,
        QUAL.MAXNOOFSTUDENTS,
        QUAL.MINNOOFSTUDENTS,
        QUAL.NUMBEROFSTUDENTS,
        QUAL.ISVERIFICATIONREQUIRED,
        QUAL.EXAMSUBMINIMUM,
        QAUD.ISVATAPPLICABLE,
        QAUD.ISPRESENTEDBEFOREAPPROVAL,
        QAUD.ISDIRECTED,
        QUAL.FQUALPRESENTINGOUID,
        QUAL.MASTER_STARTDATE,
        QUAL.MASTER_ENDDATE,
        QUAL.MASTER_AUDITDATETIME,
        QUAL.MASTER_SYSID,
        QUAL.MASTER_USERCODE,
        QUAL.KPRESENTINGOUID,
        QUAL.PRESENT_STARTDATE,
        QUAL.PRESENT_ENDDATE,
        QUAL.PRESENT_AUDITDATETIME,
        QUAL.PRESENT_SYSID,
        QUAL.PRESENT_USERCODE,
        QUAL.LEVEL_KACADEMICPROGRAMID,
        QUAL.LEVEL_STARTDATE,
        QUAL.LEVEL_ENDDATE,
        QUAL.LEVEL_PHASEOUTDATE,
        QUAL.LEVEL_AUDITDATETIME,
        QUAL.LEVEL_SYSID,
        QUAL.LEVEL_USERCODE,
        QUAL.FOS_KACADEMICPROGRAMID,
        QUAL.FOS_STARTDATE,
        QUAL.FOS_ENDDATE,
        QUAL.FOS_AUDITDATETIME,
        QUAL.FOS_SYSID,
        QUAL.FOS_USERCODE,
        QAUD.KACADEMICPROGRAMID As QUAL_KACADEMICPROGRAMID,
        QAUD.STARTDATE As QUAL_STARTDATE,
        QAUD.ENDDATE As QUAL_ENDDATE,
        QAUD.AUDITDATETIME As QUAL_AUDITDATETIME,
        QAUD.FAUDITSYSTEMFUNCTIONID As QUAL_SYSID,
        QAUD.FAUDITUSERCODE As QUAL_USERCODE
    From
        X001ac_Qual_fieldofstudy QUAL Left Join
        X001ad_Qual QAUD On QAUD.KACADEMICPROGRAMID = QUAL.FQUALIFICATIONAPID Left Join
        ACADEMICPROGRAMSHORTNAME NAME On NAME.FACADEMICPROGRAMID = QUAL.LEVEL_KACADEMICPROGRAMID And
            NAME.STARTDATE <= QUAL.LEVEL_STARTDATE And
            NAME.ENDDATE >= QUAL.LEVEL_ENDDATE And
            NAME.FSYSTEMLANGUAGECODEID = 3 And
            NAME.FNAMEPURPOSECODEID = 7294
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD QUALIFICATION FINAL
    print("Build qualification final...")
    sr_file = "X000_Qualifications"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        QUAL.KENROLMENTPRESENTATIONID,
        QUAL.QUALIFICATION,
        QUAL.QUALIFICATION_NAME,
        QUAL.QUALIFICATION_TYPE,
        QUAL.FQUALLEVELAPID,
        QUAL.ENROL_CATEGORY,
        QUAL.PRESENT_CATEGORY,
        QUAL.FINAL_STATUS,
        QUAL.LEVY_CATEGORY,
        QUAL.CERT_TYPE,
        QUAL.LEVY_TYPE,
        QUAL.FOS_SELECTION,
        QUAL.FPROGRAMAPID,
        QUAL.FBUSINESSENTITYID,
        QUAL.SITEID,
        QUAL.CAMPUS,
        QUAL.ORGUNIT_TYPE,
        QUAL.ORGUNIT_NAME,
        QUAL.ORGUNIT_MANAGER,
        QUAL.QUALIFICATIONCODE,
        QUAL.QUALIFICATIONFIELDOFSTUDY,
        QUAL.QUALIFICATIONLEVEL,
        QUAL.MIN,
        QUAL.MIN_UNIT,
        QUAL.MAX,
        QUAL.MAX_UNIT,
        QUAL.MAXNOOFSTUDENTS,
        QUAL.MINNOOFSTUDENTS,
        QUAL.NUMBEROFSTUDENTS,
        QUAL.ISVERIFICATIONREQUIRED,
        QUAL.EXAMSUBMINIMUM,
        QUAL.ISVATAPPLICABLE,
        QUAL.ISPRESENTEDBEFOREAPPROVAL,
        QUAL.ISDIRECTED,
        QUAL.FQUALPRESENTINGOUID,
        QUAL.MASTER_STARTDATE,
        QUAL.MASTER_ENDDATE,
        QUAL.MASTER_AUDITDATETIME,
        QUAL.MASTER_SYSID,
        QUAL.MASTER_USERCODE,
        QUAL.KPRESENTINGOUID,
        QUAL.PRESENT_STARTDATE,
        QUAL.PRESENT_ENDDATE,
        QUAL.PRESENT_AUDITDATETIME,
        QUAL.PRESENT_SYSID,
        QUAL.PRESENT_USERCODE,
        QUAL.LEVEL_KACADEMICPROGRAMID,
        QUAL.LEVEL_STARTDATE,
        QUAL.LEVEL_ENDDATE,
        QUAL.LEVEL_PHASEOUTDATE,
        QUAL.LEVEL_AUDITDATETIME,
        QUAL.LEVEL_SYSID,
        QUAL.LEVEL_USERCODE,
        QUAL.FOS_KACADEMICPROGRAMID,
        QUAL.FOS_STARTDATE,
        QUAL.FOS_ENDDATE,
        QUAL.FOS_AUDITDATETIME,
        QUAL.FOS_SYSID,
        QUAL.FOS_USERCODE,
        QUAL.QUAL_KACADEMICPROGRAMID,
        QUAL.QUAL_STARTDATE,
        QUAL.QUAL_ENDDATE,
        QUAL.QUAL_AUDITDATETIME,
        QUAL.QUAL_SYSID,
        QUAL.QUAL_USERCODE
    From
        X001ae_Qual_final QUAL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # VACUUM TEMP DEVELOPMENT FILES
    sr_file = "X001aa_Qual_level"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file = "X001ab_Qual_level_present"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file = "X001ac_Qual_fieldofstudy"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file = "X001ad_Qual"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    sr_file = "X001ae_Qual_final"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_conn.commit()

    """*************************************************************************
    BUILD STUDENTS
    *************************************************************************"""
    print("BUILD STUDENTS")
    funcfile.writelog("BUILD STUDENTS")

    funcstudent.studentlist(so_conn,re_path,'curr','0',True)
    # funcstudent.studentlist(so_conn,re_path,'prev','0',True)
    # funcstudent.studentlist(so_conn,re_path,'peri','2017',True)

    """*************************************************************************
    BUILD MODULES
    *************************************************************************"""
    print("BUILD MODULES")
    funcfile.writelog("BUILD MODULES")

    """*************************************************************************
    BUILD PROGRAMS
    *************************************************************************"""
    print("BUILD PROGRAMS")
    funcfile.writelog("BUILD PROGRAMS")

    """*************************************************************************
    BUILD BURSARIES
    *************************************************************************"""
    print("BUILD BURSARIES")
    funcfile.writelog("BUILD BURSARIES")

    funcfile.writelog("STUDENT BURSARIES")

    print("Build bursaries...")

    s_sql = "CREATE VIEW X004aa_Bursaries AS " + """
    SELECT
      FINAID.KFINAIDID,
      FINAID.FFINAIDINSTBUSENTID,
      FINAID.FINAIDCODE,
      FINAIDNAME.FINAIDNAME,
      FINAIDNAAM.FINAIDNAME AS FINAIDNAAM,
      FINAID.FTYPECODEID,
      X000_Codedescription.LONG AS TYPE_E,
      X000_Codedescription.LANK AS TYPE_A,
      FINAID.FFINAIDCATCODEID,
      X000_CODEDESC_FINAIDCATE.LONG AS BURS_CATE_E,
      X000_CODEDESC_FINAIDCATE.LANK AS BURS_CATE_A,
      FINAID.ISAUTOAPPL,
      FINAID.ISWWWAPPLALLOWED,
      FINAID.FINAIDYEARS,
      FINAID.STARTDATE,
      FINAID.ENDDATE,
      FINAID.FFUNDTYPECODEID,
      X000_Codedesc_fundtype.LONG AS FUND_TYPE_E,
      X000_Codedesc_fundtype.LANK AS FUND_TYPE_A,
      FINAID.FSTUDYTYPECODEID,
      X000_Codedesc_studytype.LONG AS STUDY_TYPE_E,
      X000_Codedesc_studytype.LANK AS STUDY_TYPE_A,
      FINAID.FPROCESSID,
      FINAID.AUDITDATETIME,
      FINAID.FAUDITUSERCODE,
      FINAID.FAUDITSYSTEMFUNCTIONID
    FROM
      FINAID
      LEFT JOIN X000_Codedescription X000_CODEDESC_FINAIDCATE ON X000_CODEDESC_FINAIDCATE.KCODEDESCID =
        FINAID.FFINAIDCATCODEID
      LEFT JOIN FINAIDNAME ON FINAIDNAME.FFINAIDID = FINAID.KFINAIDID AND FINAIDNAME.KSYSTEMLANGUAGECODEID = '3'
      LEFT JOIN FINAIDNAME FINAIDNAAM ON FINAIDNAAM.FFINAIDID = FINAID.KFINAIDID AND FINAIDNAAM.KSYSTEMLANGUAGECODEID = '2'
      LEFT JOIN X000_Codedescription ON X000_Codedescription.KCODEDESCID = FINAID.FTYPECODEID
      LEFT JOIN X000_Codedescription X000_Codedesc_fundtype ON X000_Codedesc_fundtype.KCODEDESCID = FINAID.FFUNDTYPECODEID
      LEFT JOIN X000_Codedescription X000_Codedesc_studytype ON X000_Codedesc_studytype.KCODEDESCID =
        FINAID.FSTUDYTYPECODEID
    """
    so_curs.execute("DROP VIEW IF EXISTS X004aa_Bursaries")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X004aa_Bursaries")

    # Build bursary site *******************************************************

    print("Build bursary sites...")

    s_sql = "CREATE VIEW X004ab_Bursary_site AS " + """
    SELECT
      FINAIDSITE.KFINAIDSITEID,
      FINAIDSITE.FFINAIDID,
      FINAIDSITE.FSITEORGUNITNUMBER,
      X004aa_Bursaries.FFINAIDINSTBUSENTID,
      X004aa_Bursaries.FINAIDCODE,
      X004aa_Bursaries.FINAIDNAME,
      X004aa_Bursaries.FINAIDNAAM,
      X004aa_Bursaries.FTYPECODEID,
      X004aa_Bursaries.TYPE_E,
      X004aa_Bursaries.TYPE_A,
      X004aa_Bursaries.FFINAIDCATCODEID,
      X004aa_Bursaries.BURS_CATE_E,
      X004aa_Bursaries.BURS_CATE_A,
      X004aa_Bursaries.ISAUTOAPPL,
      X004aa_Bursaries.ISWWWAPPLALLOWED,
      X004aa_Bursaries.FINAIDYEARS,
      X004aa_Bursaries.FFUNDTYPECODEID,
      X004aa_Bursaries.FUND_TYPE_E,
      X004aa_Bursaries.FUND_TYPE_A,
      X004aa_Bursaries.FSTUDYTYPECODEID,
      X004aa_Bursaries.STUDY_TYPE_E,
      X004aa_Bursaries.STUDY_TYPE_A,
      FINAIDSITE.CC,
      FINAIDSITE.ACC,
      FINAIDSITE.LOANTYPECODE,
      FINAIDSITE.STARTDATE,
      FINAIDSITE.ENDDATE,
      FINAIDSITE.FCOAID,
      FINAIDSITE.AUDITDATETIME,
      FINAIDSITE.FAUDITSYSTEMFUNCTIONID,
      FINAIDSITE.FAUDITUSERCODE
    FROM
      FINAIDSITE
      LEFT JOIN X004aa_Bursaries ON X004aa_Bursaries.KFINAIDID = FINAIDSITE.FFINAIDID"""
    so_curs.execute("DROP VIEW IF EXISTS X004ab_Bursary_site")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X004ab_Bursary_site")

    # Build bursary master table ***********************************************

    print("Build bursary master table...")

    s_sql = "CREATE TABLE X004_Bursaries AS " + """
    SELECT
      X004ab_Bursary_site.KFINAIDSITEID,
      X004ab_Bursary_site.FFINAIDID,
      X004ab_Bursary_site.FSITEORGUNITNUMBER,
      X004ab_Bursary_site.FFINAIDINSTBUSENTID,
      X004ab_Bursary_site.FINAIDCODE,
      X004ab_Bursary_site.FINAIDNAME,
      X004ab_Bursary_site.FINAIDNAAM,
      X004ab_Bursary_site.TYPE_E,
      X004ab_Bursary_site.BURS_CATE_E,
      X004ab_Bursary_site.ISAUTOAPPL,
      X004ab_Bursary_site.ISWWWAPPLALLOWED,
      X004ab_Bursary_site.FINAIDYEARS,
      X004ab_Bursary_site.FUND_TYPE_E,
      X004ab_Bursary_site.STUDY_TYPE_E,
      X004ab_Bursary_site.CC,
      X004ab_Bursary_site.ACC,
      X004ab_Bursary_site.LOANTYPECODE,
      X004ab_Bursary_site.STARTDATE,
      X004ab_Bursary_site.ENDDATE,
      X004ab_Bursary_site.FCOAID
    FROM
      X004ab_Bursary_site
    ORDER BY
      X004ab_Bursary_site.KFINAIDSITEID
    """
    so_curs.execute("DROP TABLE IF EXISTS X000_Bursaries")
    so_curs.execute("DROP TABLE IF EXISTS X004_Bursaries")    
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: X004_Bursaries")

    """ PARTY STUDENT BIO INFORMATION ******************************************
    *** Build party external references like ID and PASSPORT numbers
    *** Extract ID number list for today
    *** Build the student bio PARTY file
    *************************************************************************"""

    funcfile.writelog("STUDENT BIO INFORMATION")

    # Build student party external reference file **********************************
    print("Build student party external ref file...")
    sr_file = "X005aa_Party_extref"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      PARTYEXTERNALREFERENCE.KBUSINESSENTITYID,
      PARTYEXTERNALREFERENCE.KEXTERNALREFERENCECODEID,
      X000_Codedesc_partyextref.LONG,
      X000_Codedesc_partyextref.LANK,
      PARTYEXTERNALREFERENCE.EXTERNALREFERENCENUMBER,
      PARTYEXTERNALREFERENCE.STARTDATE,
      PARTYEXTERNALREFERENCE.ENDDATE,
      PARTYEXTERNALREFERENCE.REFERENCECODE,
      PARTYEXTERNALREFERENCE.OTHERDESCRIPTION,
      PARTYEXTERNALREFERENCE.LOCKSTAMP,
      PARTYEXTERNALREFERENCE.AUDITDATETIME,
      PARTYEXTERNALREFERENCE.FAUDITSYSTEMFUNCTIONID,
      PARTYEXTERNALREFERENCE.FAUDITUSERCODE
    FROM
      PARTYEXTERNALREFERENCE
      LEFT JOIN X000_Codedescription X000_Codedesc_partyextref ON X000_Codedesc_partyextref.KCODEDESCID =
        PARTYEXTERNALREFERENCE.KEXTERNALREFERENCECODEID
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)

    # BUILD CURRENT STUDENT ID NUMBER TABLE
    print("Build current student party id numbers...")
    sr_file = "X005ab_Party_idno_curr"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT DISTINCT
      X005aa_Party_extref.KBUSINESSENTITYID,
      X005aa_Party_extref.KEXTERNALREFERENCECODEID,
      X005aa_Party_extref.LONG,
      X005aa_Party_extref.LANK,
      X005aa_Party_extref.EXTERNALREFERENCENUMBER
    FROM
      X005aa_Party_extref
    WHERE
      X005aa_Party_extref.STARTDATE <= Date('%TODAY%') AND X005aa_Party_extref.ENDDATE >= Date('%TODAY%') AND
      X005aa_Party_extref.KEXTERNALREFERENCECODEID = '6525'
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    s_sql = s_sql.replace("%TODAY%",funcdate.today())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)

    # BUILD CURRENT STUDENT PASSPORT TABLE
    print("Build current student passport table...")
    sr_file = "X005ac_Party_pass_curr"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT DISTINCT
      X005aa_Party_extref.KBUSINESSENTITYID,
      X005aa_Party_extref.KEXTERNALREFERENCECODEID,
      X005aa_Party_extref.LONG,
      X005aa_Party_extref.LANK,
      X005aa_Party_extref.EXTERNALREFERENCENUMBER
    FROM
      X005aa_Party_extref
    WHERE
      X005aa_Party_extref.STARTDATE <= Date('%TODAY%') AND X005aa_Party_extref.ENDDATE >= Date('%TODAY%') AND
      X005aa_Party_extref.KEXTERNALREFERENCECODEID = '6526'
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    s_sql = s_sql.replace("%TODAY%",funcdate.today())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)

    # BUILD CURRENT STUDENT PASSPORT FILE
    print("Build current student study permit table...")
    sr_file = "X005ad_Party_perm_curr"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT DISTINCT
      X005aa_Party_extref.KBUSINESSENTITYID,
      X005aa_Party_extref.KEXTERNALREFERENCECODEID,
      X005aa_Party_extref.LONG,
      X005aa_Party_extref.LANK,
      X005aa_Party_extref.EXTERNALREFERENCENUMBER
    FROM
      X005aa_Party_extref
    WHERE
      X005aa_Party_extref.STARTDATE <= Date('%TODAY%') AND X005aa_Party_extref.ENDDATE >= Date('%TODAY%') AND
      X005aa_Party_extref.KEXTERNALREFERENCECODEID = '9690'
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    s_sql = s_sql.replace("%TODAY%",funcdate.today())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)    

    # BUILD STUDENT PARTY FILE
    print("Build student party file...")
    sr_file = "X000_Party"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      PARTY.KBUSINESSENTITYID,
      PARTY.PARTYTYPE,
      PARTY.NAME,
      PARTY.SURNAME,
      PARTY.INITIALS,
      PARTY.FIRSTNAMES,
      PARTY.NICKNAME,
      PARTY.MAIDENNAME,
      PARTY.DATEOFBIRTH,
      ID.EXTERNALREFERENCENUMBER AS IDNO,
      Upper(PASSPORT.EXTERNALREFERENCENUMBER) As PASSPORT,
      Upper(SPERMIT.EXTERNALREFERENCENUMBER) AS STUDYPERMIT,
      PARTY.FTITLECODEID,
      X000_Codedesc_title.LONG AS TITLE,
      X000_Codedesc_title.LANK AS TITEL,
      PARTY.FGENDERCODEID,
      PARTY.FGENDERCODE,
      X000_Codedesc_gender.LONG AS GENDER,
      X000_Codedesc_gender.LANK AS GESLAG,
      PARTY.FNATIONALITYCODEID,
      X000_Codedesc_nationality.LONG AS NATIONALITY,
      X000_Codedesc_nationality.LANK AS NASIONALITEIT,
      PARTY.FPOPULATIONGROUPCODEID,
      X000_Codedesc_population.LONG AS POPULATION,
      X000_Codedesc_population.LANK AS POPULASIE,
      PARTY.FRACECODEID,
      X000_Codedesc_race.LONG AS RACE,
      X000_Codedesc_race.LANK AS RAS,
      PARTY.ISFOREIGN,
      PARTY.CONTACTPERSONNAME,
      PARTY.FRELIGIOUSAFFILIATIONCODEID,
      PARTY.FPREFERREDCORRCODEID,
      PARTY.FPREFACCCORRCODEID,
      PARTY.LOCKSTAMP,
      PARTY.AUDITDATETIME,
      PARTY.FAUDITSYSTEMFUNCTIONID,
      PARTY.FAUDITUSERCODE,
      Upper(Trim(SURNAME))||' '||Replace(Upper(Trim(INITIALS)),' ','') As SURN_INIT,
      Upper(Trim(SURNAME))||' ('||Replace(Upper(Trim(INITIALS)),' ','')||') '||Upper(Trim(FIRSTNAMES)) As FULL_NAME
    FROM
      PARTY
      LEFT JOIN X000_Codedescription X000_Codedesc_title ON X000_Codedesc_title.KCODEDESCID = PARTY.FTITLECODEID
      LEFT JOIN X000_Codedescription X000_Codedesc_gender ON X000_Codedesc_gender.KCODEDESCID = PARTY.FGENDERCODEID
      LEFT JOIN X000_Codedescription X000_Codedesc_nationality ON X000_Codedesc_nationality.KCODEDESCID =
        PARTY.FNATIONALITYCODEID
      LEFT JOIN X000_Codedescription X000_Codedesc_population ON X000_Codedesc_population.KCODEDESCID =
        PARTY.FPOPULATIONGROUPCODEID
      LEFT JOIN X000_Codedescription X000_Codedesc_race ON X000_Codedesc_race.KCODEDESCID = PARTY.FRACECODEID
      LEFT JOIN X005ab_Party_idno_curr ID ON ID.KBUSINESSENTITYID = PARTY.KBUSINESSENTITYID
      LEFT JOIN X005ac_Party_pass_curr PASSPORT ON PASSPORT.KBUSINESSENTITYID = PARTY.KBUSINESSENTITYID
      LEFT JOIN X005ad_Party_perm_curr SPERMIT ON SPERMIT.KBUSINESSENTITYID = PARTY.KBUSINESSENTITYID
    WHERE
      PARTY.PARTYTYPE = '1'
    ORDER BY
      PARTY.KBUSINESSENTITYID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    """ STUDENT ACCOUNT TRANSACTIONS *******************************************
    *** 
    *** 
    *** 
    *************************************************************************"""

    funcfile.writelog("STUDENT TRANSACTIONS")

    # Build transaction master *************************************************

    print("Build transaction master...")

    s_sql = "CREATE VIEW X000_Transmaster AS " + """
    SELECT
      TRANSMASTER.KTRANSMASTERID,
      TRANSMASTER.TRANSCODE,
      TRANSMASTER.FBUSAREAID,
      TRANSMASTERDESC_E.DESCRIP AS DESCRIPTION_E,
      TRANSMASTERDESC_A.DESCRIP AS DESCRIPTION_A,
      TRANSMASTER.FENROLCHECKCODEID,
      X000_CODEDESC_ENROLCHECK.LONG AS ENROL_CHECK_E,
      X000_CODEDESC_ENROLCHECK.LANK AS ENROL_CHECK_A,
      TRANSMASTER.FSUBACCTYPECODEID,
      X000_CODEDESC_SUBACCTYPE.LONG AS SYBACCTYPE_E,
      X000_CODEDESC_SUBACCTYPE.LANK AS SUBACCTYPE_A,
      TRANSMASTER.FAGEANALYSISCTCODEID,
      X000_CODEDESC_AGEANALYSIS.LONG AS AGEANAL_E,
      X000_CODEDESC_AGEANALYSIS.LANK AS AGEANAL_A,
      TRANSMASTER.FGENERALLEDGERTYPECODEID,
      X000_CODEDESC_GLTYPE.LONG AS GLTYPE_E,
      X000_CODEDESC_GLTYPE.LANK AS GLTYPE_A,
      TRANSMASTER.FTRANSGROUPCODEID,
      X000_CODEDESC_TRANSGROUP.LONG AS TRANGROUP_E,
      X000_CODEDESC_TRANSGROUP.LANK AS TRANGROUP_A,
      TRANSMASTER.STARTDATE,
      TRANSMASTER.ENDDATE,
      TRANSMASTER.FREBATETRANSTYPECODEID,
      TRANSMASTER.FINSTALLMENTCODEID,
      TRANSMASTER.FSUBSYSTRANSCODEID,
      TRANSMASTER.ISPERMITTEDTOCREATEMANUALLY,
      TRANSMASTER.ISSUMMARISED,
      TRANSMASTER.ISCONSOLIDATIONNEEDED,
      TRANSMASTER.ISEXTERNALTRANS,
      TRANSMASTER.ISONLYDEBITSSHOWN,
      TRANSMASTER.ISDEBTEXCLUDED,
      TRANSMASTER.ISMISCELLANEOUS,
      TRANSMASTER.LOCKSTAMP,
      TRANSMASTER.AUDITDATETIME,
      TRANSMASTER.FAUDITSYSTEMFUNCTIONID,
      TRANSMASTER.FAUDITUSERCODE,
      TRANSMASTER.ISMAF,
      TRANSMASTER.ISNONREGSTUDENTALLOWED
    FROM
      TRANSMASTER
      LEFT JOIN TRANSMASTERDESC TRANSMASTERDESC_A ON TRANSMASTERDESC_A.KTRANSMASTERID = TRANSMASTER.KTRANSMASTERID
        AND TRANSMASTERDESC_A.KSYSLANGUAGECODEID = '2'
      LEFT JOIN TRANSMASTERDESC TRANSMASTERDESC_E ON TRANSMASTERDESC_E.KTRANSMASTERID = TRANSMASTER.KTRANSMASTERID
        AND TRANSMASTERDESC_E.KSYSLANGUAGECODEID = '3'
      LEFT JOIN X000_Codedescription X000_CODEDESC_ENROLCHECK ON X000_CODEDESC_ENROLCHECK.KCODEDESCID =
        TRANSMASTER.FENROLCHECKCODEID
      LEFT JOIN X000_Codedescription X000_CODEDESC_SUBACCTYPE ON X000_CODEDESC_SUBACCTYPE.KCODEDESCID =
        TRANSMASTER.FSUBACCTYPECODEID
      LEFT JOIN X000_Codedescription X000_CODEDESC_AGEANALYSIS ON X000_CODEDESC_AGEANALYSIS.KCODEDESCID =
        TRANSMASTER.FAGEANALYSISCTCODEID
      LEFT JOIN X000_Codedescription X000_CODEDESC_GLTYPE ON X000_CODEDESC_GLTYPE.KCODEDESCID =
        TRANSMASTER.FGENERALLEDGERTYPECODEID
      LEFT JOIN X000_Codedescription X000_CODEDESC_TRANSGROUP ON X000_CODEDESC_TRANSGROUP.KCODEDESCID =
        TRANSMASTER.FTRANSGROUPCODEID
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_Transmaster")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_Transmaster")

    # BUILD CURRENT YEAR TRANSACTIONS ******************************************

    print("Build current year transactions...")
    sr_file = "X010_Studytrans_curr"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      STUDYTRANS_CURR.KACCTRANSID,
      STUDYTRANS_CURR.FACCID,
      STUDACC.FBUSENTID,
      STUDYTRANS_CURR.FSERVICESITE,
      STUDYTRANS_CURR.FDEBTCOLLECTIONSITE,
      STUDYTRANS_CURR.TRANSDATE,
      STUDYTRANS_CURR.AMOUNT,
      STUDYTRANS_CURR.FTRANSMASTERID,
      X000_Transmaster.TRANSCODE,
      X000_Transmaster.DESCRIPTION_E,
      X000_Transmaster.DESCRIPTION_A,
      STUDYTRANS_CURR.TRANSDATETIME,
      STUDYTRANS_CURR.MONTHENDDATE,
      STUDYTRANS_CURR.POSTDATEDTRANSDATE,
      STUDYTRANS_CURR.FFINAIDSITEID,
      X004_Bursaries.FINAIDCODE,
      X004_Bursaries.FINAIDNAAM,
      STUDYTRANS_CURR.FRESIDENCELOGID,
      STUDYTRANS_CURR.FLEVYLOGID,
      STUDYTRANS_CURR.FMODAPID,
      STUDYTRANS_CURR.FQUALLEVELAPID,
      STUDYTRANS_CURR.FPROGAPID,
      STUDYTRANS_CURR.FENROLPRESID,
      STUDYTRANS_CURR.FRESIDENCEID,
      STUDYTRANS_CURR.FRECEIPTID,
      STUDYTRANS_CURR.FROOMTYPECODEID,
      STUDYTRANS_CURR.REFERENCENO,
      STUDYTRANS_CURR.FSUBACCTYPECODEID,
      STUDYTRANS_CURR.FDEPOSITCODEID,
      STUDYTRANS_CURR.FDEPOSITTYPECODEID,
      STUDYTRANS_CURR.FVARIABLEAMOUNTTYPECODEID,
      STUDYTRANS_CURR.FDEPOSITTRANSTYPECODEID,
      STUDYTRANS_CURR.RESIDENCETRANSTYPE,
      STUDYTRANS_CURR.FSTUDYTRANSTYPECODEID,
      STUDYTRANS_CURR.ISSHOWN,
      STUDYTRANS_CURR.ISCREATEDMANUALLY,
      STUDYTRANS_CURR.FTRANSINSTID,
      STUDYTRANS_CURR.FMONTHENDORGUNITNO,
      STUDYTRANS_CURR.LOCKSTAMP,
      STUDYTRANS_CURR.AUDITDATETIME,
      STUDYTRANS_CURR.FAUDITSYSTEMFUNCTIONID,
      STUDYTRANS_CURR.FAUDITUSERCODE,
      SYSTEMUSER.FUSERBUSINESSENTITYID,
      STUDYTRANS_CURR.FORIGINSYSTEMFUNCTIONID,
      STUDYTRANS_CURR.FPAYMENTREQUESTID
    FROM
      STUDYTRANS_CURR
      LEFT JOIN STUDACC ON STUDACC.KACCID = STUDYTRANS_CURR.FACCID
      LEFT JOIN X000_Transmaster ON X000_Transmaster.KTRANSMASTERID = STUDYTRANS_CURR.FTRANSMASTERID
      LEFT JOIN X004_Bursaries ON X004_Bursaries.KFINAIDSITEID = STUDYTRANS_CURR.FFINAIDSITEID
      LEFT JOIN SYSTEMUSER ON SYSTEMUSER.KUSERCODE = STUDYTRANS_CURR.FAUDITUSERCODE
    ORDER BY
      STUDYTRANS_CURR.TRANSDATETIME
    """
    so_curs.execute("DROP TABLE IF EXISTS X010_Studytrans")
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)    

    # Close the connection *********************************************************
    so_conn.close()

    # Close the log writer *********************************************************
    funcfile.writelog("-------------------------")
    funcfile.writelog("COMPLETED: B003_VSS_LISTS")

    return
