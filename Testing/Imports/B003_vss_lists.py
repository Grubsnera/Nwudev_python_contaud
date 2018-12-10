"""
Script to build standard VSS lists
Created on: 01 Mar 2018
Copyright: Albert J v Rensburg
"""

def Vss_lists():
    
    # Import python modules
    import datetime
    import sqlite3
    import sys

    # Add own module path
    sys.path.append('S:/_my_modules')
    #print(sys.path)

    # Import own modules
    import funcdate
    import funccsv
    import funcfile

    # Open the script log file ******************************************************

    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: B003_VSS_LISTS")
    funcfile.writelog("----------------------")
    print("--------------")
    print("B003_VSS_LISTS")
    print("--------------")
    ilog_severity = 1

    # Declare variables
    so_path = "W:/" #Source database path
    so_file = "Vss.sqlite" #Source database
    s_sql = "" #SQL statements

    # Open the SOURCE file
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()

    funcfile.writelog("%t OPEN DATABASE: VSS.SQLITE")

    # Build code descriptions ******************************************************

    print("Build code descriptions...")

    s_sql = "CREATE TABLE X000_Codedescription AS " + """
    SELECT
      CODEDESCRIPTION.KCODEDESCID,
      CODEDESCRIPTION.CODELONGDESCRIPTION AS LANK,
      CODEDESCRIPTION.CODESHORTDESCRIPTION AS KORT,
      CODEDESCRIPTION1.CODELONGDESCRIPTION AS LONG,
      CODEDESCRIPTION1.CODESHORTDESCRIPTION AS SHORT
    FROM
      CODEDESCRIPTION
      INNER JOIN CODEDESCRIPTION CODEDESCRIPTION1 ON CODEDESCRIPTION1.KCODEDESCID = CODEDESCRIPTION.KCODEDESCID
    WHERE
      CODEDESCRIPTION.KSYSTEMLANGUAGECODEID = 2 AND
      CODEDESCRIPTION1.KSYSTEMLANGUAGECODEID = 3
    """
    so_curs.execute("DROP TABLE IF EXISTS X000_Codedescription")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: X000_Codedescription")

    # Build org unit name **********************************************************

    print("Build org unit name...")

    s_sql = "CREATE VIEW X000_Orgunitname AS " + """
    SELECT
      ORGUNITNAME.KORGUNITNUMBER,
      ORGUNITNAME.KSTARTDATE,
      ORGUNITNAME.ENDDATE,
      ORGUNITNAME.SHORTNAME AS KORT,
      ORGUNITNAME.LONGNAME AS LANK,
      ORGUNITNAME1.SHORTNAME AS SHORT,
      ORGUNITNAME1.LONGNAME AS LONG
    FROM
      ORGUNITNAME
      LEFT JOIN ORGUNITNAME ORGUNITNAME1 ON ORGUNITNAME1.KORGUNITNUMBER = ORGUNITNAME.KORGUNITNUMBER AND
        ORGUNITNAME1.KSTARTDATE = ORGUNITNAME.KSTARTDATE
    WHERE
      ORGUNITNAME.KSYSTEMLANGUAGECODEID = 2 AND
      ORGUNITNAME1.KSYSTEMLANGUAGECODEID = 3
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_Orgunitname")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_Orgunitname")

    # Build org unit ***************************************************************

    print("Build org unit...")

    s_sql = "CREATE VIEW X000_Orgunit AS " + """
    SELECT
      ORGUNIT.KORGUNITNUMBER,
      ORGUNIT.STARTDATE,
      ORGUNIT.ENDDATE,
      ORGUNIT.FORGUNITTYPECODEID,
      ORGUNIT.FORGUNITTYPECODE,
      ORGUNIT.ISSITE,
      ORGUNIT.LOCKSTAMP,
      ORGUNIT.AUDITDATETIME,
      ORGUNIT.FAUDITSYSTEMFUNCTIONID,
      ORGUNIT.FAUDITUSERCODE,
      X000_Orgunitname.KSTARTDATE,
      X000_Orgunitname.ENDDATE AS ENDDATE1,
      X000_Orgunitname.KORT,
      X000_Orgunitname.LANK,
      X000_Orgunitname.SHORT,
      X000_Orgunitname.LONG,
      X000_Codedescription.LONG AS UNIT_TYPE
    FROM
      ORGUNIT
      LEFT JOIN X000_Orgunitname ON X000_Orgunitname.KORGUNITNUMBER = ORGUNIT.KORGUNITNUMBER
      LEFT JOIN X000_Codedescription ON X000_Codedescription.KCODEDESCID = ORGUNIT.FORGUNITTYPECODEID
    WHERE
      X000_Orgunitname.KSTARTDATE <= ORGUNIT.STARTDATE AND
      X000_Orgunitname.ENDDATE >= ORGUNIT.STARTDATE
    ORDER BY
      ORGUNIT.KORGUNITNUMBER
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_Orgunit")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_Orgunit")

    # Build org unit ***************************************************************

    print("Build org unit instance...")

    s_sql = "CREATE TABLE X000_Orgunitinstance AS " + """
    SELECT
      ORGUNITINSTANCE.KBUSINESSENTITYID,
      ORGUNITINSTANCE.FORGUNITNUMBER,  
      X000_Orgunit.UNIT_TYPE AS ORGUNIT_TYPE,
      X000_Orgunit.LONG AS ORGUNIT_NAME,
      ORGUNITINSTANCE.FSITEORGUNITNUMBER,
      ORGUNITINSTANCE.STARTDATE,  
      ORGUNITINSTANCE.ENDDATE,
      ORGUNITINSTANCE.FMANAGERTYPECODEID,
      X000_Codedescription.LONG AS MANAGER_TYPE,
      ORGUNITINSTANCE.PLANNEDRESTRUCTUREDATE,
      ORGUNITINSTANCE.FMANAGERTYPECODE,
      ORGUNITINSTANCE.LOCKSTAMP,
      ORGUNITINSTANCE.AUDITDATETIME,
      ORGUNITINSTANCE.FAUDITSYSTEMFUNCTIONID,
      ORGUNITINSTANCE.FAUDITUSERCODE,
      X000_Orgunit.ISSITE,
      X000_Orgunit.KORT,
      X000_Orgunit.LANK,
      X000_Orgunit.SHORT,
      ORGUNITINSTANCE.FNEWBUSINESSENTITYID  
    FROM
      ORGUNITINSTANCE
      LEFT JOIN X000_Orgunit ON X000_Orgunit.KORGUNITNUMBER = ORGUNITINSTANCE.FORGUNITNUMBER
      LEFT JOIN X000_Codedescription ON X000_Codedescription.KCODEDESCID = ORGUNITINSTANCE.FMANAGERTYPECODEID
    ORDER BY
      ORGUNITINSTANCE.KBUSINESSENTITYID
    """
    so_curs.execute("DROP TABLE IF EXISTS X000_Orgunitinstance")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: X000_Orgunitinstance")

    # Build student results ********************************************************

    print("Build student results...")

    s_sql = "CREATE VIEW X000_Student_qual_result AS " + """
    SELECT
      STUDQUALFOSRESULT.KBUSINESSENTITYID,
      STUDQUALFOSRESULT.KACADEMICPROGRAMID,
      STUDQUALFOSRESULT.KQUALFOSRESULTCODEID,
      X000_Codedescription.LONG AS RESULT,
      STUDQUALFOSRESULT.KRESULTYYYYMM,
      STUDQUALFOSRESULT.KSTUDQUALFOSRESULTID,
      STUDQUALFOSRESULT.FGRADUATIONCEREMONYID,
      STUDQUALFOSRESULT.FPOSTPONEMENTCODEID,
      X000_Codedescription1.LONG AS POSTPONE_REAS,
      STUDQUALFOSRESULT.RESULTISSUEDATE,
      STUDQUALFOSRESULT.DISCONTINUEDATE,
      STUDQUALFOSRESULT.FDISCONTINUECODEID,
      X000_Codedescription2.LONG AS DISCONTINUE_REAS,
      STUDQUALFOSRESULT.RESULTPASSDATE,
      STUDQUALFOSRESULT.FLANGUAGECODEID,
      STUDQUALFOSRESULT.ISSUESURNAME,
      STUDQUALFOSRESULT.CERTIFICATESEQNUMBER,
      STUDQUALFOSRESULT.AVGMARKACHIEVED,
      STUDQUALFOSRESULT.PROCESSSEQNUMBER,
      STUDQUALFOSRESULT.FRECEIPTID,
      STUDQUALFOSRESULT.FRECEIPTLINEID,
      STUDQUALFOSRESULT.ISINABSENTIA,
      STUDQUALFOSRESULT.FPROGRAMAPID,
      STUDQUALFOSRESULT.FISSUETYPECODEID,
      X000_Codedescription3.LONG AS ISSUE_TYPE,
      STUDQUALFOSRESULT.DATEPRINTED,
      STUDQUALFOSRESULT.LOCKSTAMP,
      STUDQUALFOSRESULT.AUDITDATETIME,
      STUDQUALFOSRESULT.FAUDITSYSTEMFUNCTIONID,
      STUDQUALFOSRESULT.FAUDITUSERCODE,
      STUDQUALFOSRESULT.FAPPROVEDBYCODEID,
      STUDQUALFOSRESULT.FAPPROVEDBYUSERCODE,
      STUDQUALFOSRESULT.DATERESULTAPPROVED,
      STUDQUALFOSRESULT.FENROLMENTPRESENTATIONID,
      STUDQUALFOSRESULT.CERTDISPATCHDATE,
      STUDQUALFOSRESULT.CERTDISPATCHREFNO,
      STUDQUALFOSRESULT.ISSUEFIRSTNAMES
    FROM
      STUDQUALFOSRESULT
      LEFT JOIN X000_Codedescription ON X000_Codedescription.KCODEDESCID =
        STUDQUALFOSRESULT.KQUALFOSRESULTCODEID
      LEFT JOIN X000_Codedescription X000_Codedescription1 ON X000_Codedescription1.KCODEDESCID =
        STUDQUALFOSRESULT.FPOSTPONEMENTCODEID
      LEFT JOIN X000_Codedescription X000_Codedescription2 ON X000_Codedescription2.KCODEDESCID =
        STUDQUALFOSRESULT.FDISCONTINUECODEID
      LEFT JOIN X000_Codedescription X000_Codedescription3 ON X000_Codedescription3.KCODEDESCID =
        STUDQUALFOSRESULT.FISSUETYPECODEID
    ORDER BY
      STUDQUALFOSRESULT.KBUSINESSENTITYID,
      STUDQUALFOSRESULT.AUDITDATETIME DESC
    """

    so_curs.execute("DROP VIEW IF EXISTS X000_Student_qualification_result")
    so_curs.execute("DROP VIEW IF EXISTS X000_Student_qual_result")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_Student_qual_result")

    # Build qualification step one *************************************************

    print("Build qualification...")

    s_sql = "CREATE VIEW X001aa_Qualification AS " + """
    SELECT
      QUALIFICATION.KACADEMICPROGRAMID,
      QUALIFICATION.STARTDATE,
      QUALIFICATION.ENDDATE,
      QUALIFICATION.QUALIFICATIONCODE,
      X000_Codedescription4.LONG AS QUAL_TYPE,
      X000_Codedescription.LANK AS MIN,
      X000_Codedescription1.LONG AS MIN_UNIT,
      X000_Codedescription2.LONG AS MAX,
      X000_Codedescription3.LONG AS MAX_UNIT,
      QUALIFICATION.AUDITDATETIME,
      QUALIFICATION.FAUDITSYSTEMFUNCTIONID,
      QUALIFICATION.FAUDITUSERCODE,
      X000_Codedescription5.LONG AS CERT_TYPE,
      X000_Codedescription6.LONG AS LEVY_TYPE,
      QUALIFICATION.ISVATAPPLICABLE,
      QUALIFICATION.ISPRESENTEDBEFOREAPPROVAL,
      QUALIFICATION.ISDIRECTED
    FROM
      QUALIFICATION
      LEFT JOIN X000_Codedescription ON X000_Codedescription.KCODEDESCID = QUALIFICATION.FMINDURATIONCODEID
      LEFT JOIN X000_Codedescription X000_Codedescription1 ON X000_Codedescription1.KCODEDESCID =
        QUALIFICATION.FMINDURPERIODUNITCODEID
      LEFT JOIN X000_Codedescription X000_Codedescription2 ON X000_Codedescription2.KCODEDESCID =
        QUALIFICATION.FMAXDURATIONCODEID
      LEFT JOIN X000_Codedescription X000_Codedescription3 ON X000_Codedescription3.KCODEDESCID =
        QUALIFICATION.FMAXDURPERIODUNITCODEID
      LEFT JOIN X000_Codedescription X000_Codedescription4 ON X000_Codedescription4.KCODEDESCID =
        QUALIFICATION.FQUALIFICATIONTYPECODEID
      LEFT JOIN X000_Codedescription X000_Codedescription5 ON X000_Codedescription5.KCODEDESCID =
        QUALIFICATION.FCERTIFICATETYPECODEID
      LEFT JOIN X000_Codedescription X000_Codedescription6 ON X000_Codedescription6.KCODEDESCID =
        QUALIFICATION.FLEVYLEVELCODEID
    """
    so_curs.execute("DROP VIEW IF EXISTS X001aa_Qualification")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X001aa_Qualification")

    # Build qualification step two *************************************************

    print("Build qualification level...")

    s_sql = "CREATE VIEW X001ba_Qualification_level AS " + """
    SELECT
      QUALIFICATIONLEVEL.KACADEMICPROGRAMID,
      QUALIFICATIONLEVEL.STARTDATE,
      QUALIFICATIONLEVEL.ENDDATE,
      QUALIFICATIONLEVEL.QUALIFICATIONLEVEL,
      X000_Codedescription.LONG AS STATUS_FINAL,
      X000_Codedescription1.LONG AS LEVY_CATEGORY,
      QUALIFICATIONLEVEL.FFIELDOFSTUDYAPID,
      QUALIFICATIONLEVEL.FFINALSTATUSCODEID,
      QUALIFICATIONLEVEL.FLEVYCATEGORYCODEID,
      QUALIFICATIONLEVEL.LOCKSTAMP,
      QUALIFICATIONLEVEL.AUDITDATETIME,
      QUALIFICATIONLEVEL.FAUDITSYSTEMFUNCTIONID,
      QUALIFICATIONLEVEL.FAUDITUSERCODE,
      QUALIFICATIONLEVEL.PHASEOUTDATE
    FROM
      QUALIFICATIONLEVEL
      LEFT JOIN X000_Codedescription ON X000_Codedescription.KCODEDESCID =
        QUALIFICATIONLEVEL.FFINALSTATUSCODEID
      LEFT JOIN X000_Codedescription X000_Codedescription1 ON X000_Codedescription1.KCODEDESCID =
        QUALIFICATIONLEVEL.FLEVYCATEGORYCODEID
    """
    so_curs.execute("DROP VIEW IF EXISTS X100_Qualification_level")
    so_curs.execute("DROP VIEW IF EXISTS X001ba_Qualification_level")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X001ba_Qualification_level")

    # Build student qualification current 1 ****************************************
    # QUALLEVELENROLSTUD + PRESENTOUENROLPRESENTCAT

    print("Build current student qualification step 1...")

    s_sql = "CREATE VIEW X001ca_Stud_qual_curr AS " + """
    SELECT
      QUALLEVELENROLSTUD_CURR.KSTUDBUSENTID,
      QUALLEVELENROLSTUD_CURR.KENROLSTUDID,
      QUALLEVELENROLSTUD_CURR.DATEQUALLEVELSTARTED,  
      QUALLEVELENROLSTUD_CURR.DATEENROL,  
      QUALLEVELENROLSTUD_CURR.STARTDATE,
      QUALLEVELENROLSTUD_CURR.ENDDATE,
      QUALLEVELENROLSTUD_CURR.ISHEMISSUBSIDY,
      QUALLEVELENROLSTUD_CURR.ISMAINQUALLEVEL,
      QUALLEVELENROLSTUD_CURR.ENROLACADEMICYEAR,
      QUALLEVELENROLSTUD_CURR.ENROLHISTORYYEAR,  
      QUALLEVELENROLSTUD_CURR.FSTUDACTIVECODEID,
      X000_Codedescription1.LONG AS ACTIVE_IND,  
      QUALLEVELENROLSTUD_CURR.FENTRYLEVELCODEID,
      X000_Codedescription2.LONG AS ENTRY_LEVEL,
      PRESENTOUENROLPRESENTCAT.FENROLMENTCATEGORYCODEID,  
      X000_Codedescription3.LONG AS ENROL_CAT,  
      PRESENTOUENROLPRESENTCAT.FPRESENTATIONCATEGORYCODEID,
      X000_Codedescription4.LONG AS PRESENT_CAT,
      QUALLEVELENROLSTUD_CURR.FBLACKLISTCODEID,
      X000_Codedescription.LONG AS BLACKLIST,  
      QUALLEVELENROLSTUD_CURR.ISCONDITIONALREG,
      QUALLEVELENROLSTUD_CURR.MARKSFINALISEDDATE,
      PRESENTOUENROLPRESENTCAT.EXAMSUBMINIMUM,  
      QUALLEVELENROLSTUD_CURR.ISCUMLAUDE,
      QUALLEVELENROLSTUD_CURR.FGRADCERTLANGUAGECODEID,
      QUALLEVELENROLSTUD_CURR.ISPOSSIBLEGRADUATE,
      QUALLEVELENROLSTUD_CURR.FGRADUATIONCEREMONYID,
      QUALLEVELENROLSTUD_CURR.FACCEPTANCETESTCODEID,
      QUALLEVELENROLSTUD_CURR.FENROLMENTPRESENTATIONID,
      QUALLEVELENROLSTUD_CURR.FPROGRAMAPID, 
      PRESENTOUENROLPRESENTCAT.FQUALPRESENTINGOUID
    FROM
      QUALLEVELENROLSTUD_CURR
      LEFT JOIN PRESENTOUENROLPRESENTCAT ON PRESENTOUENROLPRESENTCAT.KENROLMENTPRESENTATIONID =
        QUALLEVELENROLSTUD_CURR.FENROLMENTPRESENTATIONID
      LEFT JOIN X000_Codedescription ON X000_Codedescription.KCODEDESCID =
        QUALLEVELENROLSTUD_CURR.FBLACKLISTCODEID
      LEFT JOIN X000_Codedescription X000_Codedescription1 ON X000_Codedescription1.KCODEDESCID =
        QUALLEVELENROLSTUD_CURR.FSTUDACTIVECODEID
      LEFT JOIN X000_Codedescription X000_Codedescription2 ON X000_Codedescription2.KCODEDESCID =
        QUALLEVELENROLSTUD_CURR.FENTRYLEVELCODEID
      LEFT JOIN X000_Codedescription X000_Codedescription3 ON X000_Codedescription3.KCODEDESCID =
        PRESENTOUENROLPRESENTCAT.FENROLMENTCATEGORYCODEID
      INNER JOIN X000_Codedescription X000_Codedescription4 ON X000_Codedescription4.KCODEDESCID =
        PRESENTOUENROLPRESENTCAT.FPRESENTATIONCATEGORYCODEID
    ORDER BY
      QUALLEVELENROLSTUD_CURR.KSTUDBUSENTID,
      QUALLEVELENROLSTUD_CURR.DATEQUALLEVELSTARTED  
    """
    so_curs.execute("DROP VIEW IF EXISTS X001ca_Stud_qual_curr")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X001ca_Stud_qual_curr")

    # Build student qualification current 2 ****************************************
    # QUALLEVELENROLSTUD + PRESENTOUENROLPRESENTCAT
    # QUALIFICATIONLEVEL

    print("Build current student qualification step 2...")

    s_sql = "CREATE VIEW X001cb_Stud_qual_curr AS " + """
    SELECT
      X001ca_Stud_qual_curr.KSTUDBUSENTID,
      X001ca_Stud_qual_curr.KENROLSTUDID,
      X001ca_Stud_qual_curr.DATEQUALLEVELSTARTED,
      X001ca_Stud_qual_curr.DATEENROL,
      X001ca_Stud_qual_curr.STARTDATE,
      X001ca_Stud_qual_curr.ENDDATE,
      QUALIFICATIONLEVEL.QUALIFICATIONLEVEL,  
      X001ca_Stud_qual_curr.ISHEMISSUBSIDY,
      X001ca_Stud_qual_curr.ISMAINQUALLEVEL,
      X001ca_Stud_qual_curr.ENROLACADEMICYEAR,
      X001ca_Stud_qual_curr.ENROLHISTORYYEAR,
      X001ca_Stud_qual_curr.FSTUDACTIVECODEID,
      X001ca_Stud_qual_curr.ACTIVE_IND,
      X001ca_Stud_qual_curr.FENTRYLEVELCODEID,
      X001ca_Stud_qual_curr.ENTRY_LEVEL,
      X001ca_Stud_qual_curr.FENROLMENTCATEGORYCODEID,
      X001ca_Stud_qual_curr.ENROL_CAT,
      X001ca_Stud_qual_curr.FPRESENTATIONCATEGORYCODEID,
      X001ca_Stud_qual_curr.PRESENT_CAT,
      QUALIFICATIONLEVEL.FFINALSTATUSCODEID,
      X000_Codedescription.LONG AS QUAL_LEVEL_STATUS_FINAL,  
      QUALIFICATIONLEVEL.FLEVYCATEGORYCODEID,
      X000_Codedescription1.LONG AS QUAL_LEVEL_LEVY_CAT,  
      X001ca_Stud_qual_curr.FBLACKLISTCODEID,
      X001ca_Stud_qual_curr.BLACKLIST,
      QUALLEVELPRESENTINGOU.FBUSINESSENTITYID,
      X000_Orgunitinstance.FORGUNITNUMBER,
      X000_Orgunitinstance.ORGUNIT_TYPE,
      X000_Orgunitinstance.ORGUNIT_NAME,
      X000_Orgunitinstance.FSITEORGUNITNUMBER,  
      X001ca_Stud_qual_curr.ISCONDITIONALREG,
      X001ca_Stud_qual_curr.MARKSFINALISEDDATE,
      X001ca_Stud_qual_curr.EXAMSUBMINIMUM,
      X001ca_Stud_qual_curr.ISCUMLAUDE,
      X001ca_Stud_qual_curr.FGRADCERTLANGUAGECODEID,
      X001ca_Stud_qual_curr.ISPOSSIBLEGRADUATE,
      X001ca_Stud_qual_curr.FGRADUATIONCEREMONYID,
      X001ca_Stud_qual_curr.FACCEPTANCETESTCODEID,
      X001ca_Stud_qual_curr.FENROLMENTPRESENTATIONID,
      X001ca_Stud_qual_curr.FQUALPRESENTINGOUID,
      X001ca_Stud_qual_curr.FPROGRAMAPID,   
      QUALLEVELPRESENTINGOU.FQUALLEVELAPID,
      QUALIFICATIONLEVEL.FFIELDOFSTUDYAPID,
      QUALIFICATIONLEVEL.STARTDATE AS QUAL_LEVEL_STARTDATE,
      QUALIFICATIONLEVEL.ENDDATE AS QUAL_LEVEL_ENDDATE
    FROM
      X001ca_Stud_qual_curr
      LEFT JOIN QUALLEVELPRESENTINGOU ON QUALLEVELPRESENTINGOU.KPRESENTINGOUID =
        X001ca_Stud_qual_curr.FQUALPRESENTINGOUID
      LEFT JOIN QUALIFICATIONLEVEL ON QUALIFICATIONLEVEL.KACADEMICPROGRAMID =
        QUALLEVELPRESENTINGOU.FQUALLEVELAPID
      LEFT JOIN X000_Orgunitinstance ON X000_Orgunitinstance.KBUSINESSENTITYID =
        QUALLEVELPRESENTINGOU.FBUSINESSENTITYID
      LEFT JOIN X000_Codedescription ON X000_Codedescription.KCODEDESCID =
        QUALIFICATIONLEVEL.FFINALSTATUSCODEID
      LEFT JOIN X000_Codedescription X000_Codedescription1 ON X000_Codedescription1.KCODEDESCID =
        QUALIFICATIONLEVEL.FLEVYCATEGORYCODEID
    """
    so_curs.execute("DROP VIEW IF EXISTS X001cb_Stud_qual_curr")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X001cb_Stud_qual_curr")

    # Build student qualification current 3 ****************************************
    # QUALLEVELENROLSTUD + PRESENTOUENROLPRESENTCAT
    # QUALIFICATIONLEVEL

    print("Build current student qualification step 3...")

    s_sql = "CREATE VIEW X001cc_Stud_qual_curr AS " + """
    SELECT
      X001cb_Stud_qual_curr.KSTUDBUSENTID,
      X001cb_Stud_qual_curr.KENROLSTUDID,
      X001cb_Stud_qual_curr.DATEQUALLEVELSTARTED,
      X001cb_Stud_qual_curr.DATEENROL,
      X001cb_Stud_qual_curr.STARTDATE,
      X001cb_Stud_qual_curr.ENDDATE,
      X001aa_Qualification.QUALIFICATIONCODE,  
      FIELDOFSTUDY.QUALIFICATIONFIELDOFSTUDY,  
      X001cb_Stud_qual_curr.QUALIFICATIONLEVEL,
      X001aa_Qualification.QUAL_TYPE,  
      X001cb_Stud_qual_curr.ISHEMISSUBSIDY,
      X001cb_Stud_qual_curr.ISMAINQUALLEVEL,
      X001cb_Stud_qual_curr.ENROLACADEMICYEAR,
      X001cb_Stud_qual_curr.ENROLHISTORYYEAR,
      X001aa_Qualification.MIN,
      X001aa_Qualification.MIN_UNIT,
      X001aa_Qualification.MAX,
      X001aa_Qualification.MAX_UNIT,
      X001cb_Stud_qual_curr.FSTUDACTIVECODEID,
      X001cb_Stud_qual_curr.ACTIVE_IND,
      X001cb_Stud_qual_curr.FENTRYLEVELCODEID,
      X001cb_Stud_qual_curr.ENTRY_LEVEL,
      X001cb_Stud_qual_curr.FENROLMENTCATEGORYCODEID,
      X001cb_Stud_qual_curr.ENROL_CAT,
      X001cb_Stud_qual_curr.FPRESENTATIONCATEGORYCODEID,
      X001cb_Stud_qual_curr.PRESENT_CAT,
      X001cb_Stud_qual_curr.FFINALSTATUSCODEID,
      X001cb_Stud_qual_curr.QUAL_LEVEL_STATUS_FINAL,
      X001cb_Stud_qual_curr.FLEVYCATEGORYCODEID,
      X001cb_Stud_qual_curr.QUAL_LEVEL_LEVY_CAT,
      X001aa_Qualification.CERT_TYPE,
      X001aa_Qualification.LEVY_TYPE,
      X001cb_Stud_qual_curr.FBLACKLISTCODEID,
      X001cb_Stud_qual_curr.BLACKLIST,
      FIELDOFSTUDY.FSELECTIONCODEID,
      X000_Codedescription.LONG AS FOS_SELECTION,
      X001cb_Stud_qual_curr.FBUSINESSENTITYID,
      X001cb_Stud_qual_curr.FORGUNITNUMBER,
      X001cb_Stud_qual_curr.ORGUNIT_TYPE,
      X001cb_Stud_qual_curr.ORGUNIT_NAME,
      X001cb_Stud_qual_curr.FSITEORGUNITNUMBER,
      X001cb_Stud_qual_curr.ISCONDITIONALREG,
      X001cb_Stud_qual_curr.MARKSFINALISEDDATE,
      X001cb_Stud_qual_curr.EXAMSUBMINIMUM,
      X001cb_Stud_qual_curr.ISCUMLAUDE,
      X001cb_Stud_qual_curr.FGRADCERTLANGUAGECODEID,
      X001cb_Stud_qual_curr.ISPOSSIBLEGRADUATE,
      X001cb_Stud_qual_curr.FGRADUATIONCEREMONYID,
      X001cb_Stud_qual_curr.FACCEPTANCETESTCODEID,
      X001cb_Stud_qual_curr.FENROLMENTPRESENTATIONID,
      X001cb_Stud_qual_curr.FQUALPRESENTINGOUID,
      X001cb_Stud_qual_curr.FQUALLEVELAPID,
      X001cb_Stud_qual_curr.FFIELDOFSTUDYAPID,
      X001cb_Stud_qual_curr.FPROGRAMAPID,  
      FIELDOFSTUDY.FQUALIFICATIONAPID
    FROM
      X001cb_Stud_qual_curr
      LEFT JOIN FIELDOFSTUDY ON FIELDOFSTUDY.KACADEMICPROGRAMID = X001cb_Stud_qual_curr.FFIELDOFSTUDYAPID
      LEFT JOIN X000_Codedescription ON X000_Codedescription.KCODEDESCID = FIELDOFSTUDY.FSELECTIONCODEID
      LEFT JOIN X001aa_Qualification ON X001aa_Qualification.KACADEMICPROGRAMID = FIELDOFSTUDY.FQUALIFICATIONAPID
    """
    so_curs.execute("DROP VIEW IF EXISTS X001cc_Stud_qual_curr")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X001cc_Stud_qual_curr")

    # Build current student results ************************************************

    print("Build current student results...")

    s_sql = "CREATE VIEW X000_Student_qual_result_curr AS " + """
    SELECT
      X000_Student_qual_result.KBUSINESSENTITYID,
      X000_Student_qual_result.KACADEMICPROGRAMID,
      X000_Student_qual_result.KQUALFOSRESULTCODEID,
      X000_Student_qual_result.RESULT,
      X000_Student_qual_result.KRESULTYYYYMM,
      X000_Student_qual_result.KSTUDQUALFOSRESULTID,
      X000_Student_qual_result.FGRADUATIONCEREMONYID,
      X000_Student_qual_result.FPOSTPONEMENTCODEID,
      X000_Student_qual_result.POSTPONE_REAS,
      X000_Student_qual_result.RESULTISSUEDATE,
      X000_Student_qual_result.DISCONTINUEDATE,
      X000_Student_qual_result.FDISCONTINUECODEID,
      X000_Student_qual_result.DISCONTINUE_REAS,
      X000_Student_qual_result.RESULTPASSDATE,
      X000_Student_qual_result.FLANGUAGECODEID,
      X000_Student_qual_result.ISSUESURNAME,
      X000_Student_qual_result.CERTIFICATESEQNUMBER,
      X000_Student_qual_result.AVGMARKACHIEVED,
      X000_Student_qual_result.PROCESSSEQNUMBER,
      X000_Student_qual_result.FRECEIPTID,
      X000_Student_qual_result.FRECEIPTLINEID,
      X000_Student_qual_result.ISINABSENTIA,
      X000_Student_qual_result.FPROGRAMAPID,
      X000_Student_qual_result.FISSUETYPECODEID,
      X000_Student_qual_result.ISSUE_TYPE,
      X000_Student_qual_result.DATEPRINTED,
      X000_Student_qual_result.LOCKSTAMP,
      X000_Student_qual_result.AUDITDATETIME,
      X000_Student_qual_result.FAUDITSYSTEMFUNCTIONID,
      X000_Student_qual_result.FAUDITUSERCODE,
      X000_Student_qual_result.FAPPROVEDBYCODEID,
      X000_Student_qual_result.FAPPROVEDBYUSERCODE,
      X000_Student_qual_result.DATERESULTAPPROVED,
      X000_Student_qual_result.FENROLMENTPRESENTATIONID,
      X000_Student_qual_result.CERTDISPATCHDATE,
      X000_Student_qual_result.CERTDISPATCHREFNO,
      X000_Student_qual_result.ISSUEFIRSTNAMES,
      X001cc_Stud_qual_curr.KSTUDBUSENTID,
      X001cc_Stud_qual_curr.KENROLSTUDID,
      X001cc_Stud_qual_curr.DATEQUALLEVELSTARTED,
      X001cc_Stud_qual_curr.DATEENROL,
      X001cc_Stud_qual_curr.STARTDATE,
      X001cc_Stud_qual_curr.ENDDATE,
      X001cc_Stud_qual_curr.QUALIFICATIONCODE,
      X001cc_Stud_qual_curr.QUALIFICATIONFIELDOFSTUDY,
      X001cc_Stud_qual_curr.QUALIFICATIONLEVEL,
      X001cc_Stud_qual_curr.QUAL_TYPE,
      X001cc_Stud_qual_curr.ISHEMISSUBSIDY,
      X001cc_Stud_qual_curr.ISMAINQUALLEVEL,
      X001cc_Stud_qual_curr.ENROLACADEMICYEAR,
      X001cc_Stud_qual_curr.ENROLHISTORYYEAR,
      X001cc_Stud_qual_curr.MIN,
      X001cc_Stud_qual_curr.MIN_UNIT,
      X001cc_Stud_qual_curr.MAX,
      X001cc_Stud_qual_curr.MAX_UNIT,
      X001cc_Stud_qual_curr.FSTUDACTIVECODEID,
      X001cc_Stud_qual_curr.ACTIVE_IND,
      X001cc_Stud_qual_curr.FENTRYLEVELCODEID,
      X001cc_Stud_qual_curr.ENTRY_LEVEL,
      X001cc_Stud_qual_curr.FENROLMENTCATEGORYCODEID,
      X001cc_Stud_qual_curr.ENROL_CAT,
      X001cc_Stud_qual_curr.FPRESENTATIONCATEGORYCODEID,
      X001cc_Stud_qual_curr.PRESENT_CAT,
      X001cc_Stud_qual_curr.FFINALSTATUSCODEID,
      X001cc_Stud_qual_curr.QUAL_LEVEL_STATUS_FINAL,
      X001cc_Stud_qual_curr.FLEVYCATEGORYCODEID,
      X001cc_Stud_qual_curr.QUAL_LEVEL_LEVY_CAT,
      X001cc_Stud_qual_curr.CERT_TYPE,
      X001cc_Stud_qual_curr.LEVY_TYPE,
      X001cc_Stud_qual_curr.FBLACKLISTCODEID,
      X001cc_Stud_qual_curr.BLACKLIST,
      X001cc_Stud_qual_curr.FSELECTIONCODEID,
      X001cc_Stud_qual_curr.FOS_SELECTION,
      X001cc_Stud_qual_curr.FBUSINESSENTITYID,
      X001cc_Stud_qual_curr.FORGUNITNUMBER,
      X001cc_Stud_qual_curr.ORGUNIT_TYPE,
      X001cc_Stud_qual_curr.ORGUNIT_NAME,
      X001cc_Stud_qual_curr.FSITEORGUNITNUMBER,
      X001cc_Stud_qual_curr.ISCONDITIONALREG,
      X001cc_Stud_qual_curr.MARKSFINALISEDDATE,
      X001cc_Stud_qual_curr.EXAMSUBMINIMUM,
      X001cc_Stud_qual_curr.ISCUMLAUDE,
      X001cc_Stud_qual_curr.FGRADCERTLANGUAGECODEID,
      X001cc_Stud_qual_curr.ISPOSSIBLEGRADUATE,
      X001cc_Stud_qual_curr.FGRADUATIONCEREMONYID AS FGRADUATIONCEREMONYID1,
      X001cc_Stud_qual_curr.FACCEPTANCETESTCODEID,
      X001cc_Stud_qual_curr.FENROLMENTPRESENTATIONID AS FENROLMENTPRESENTATIONID1,
      X001cc_Stud_qual_curr.FQUALPRESENTINGOUID,
      X001cc_Stud_qual_curr.FQUALLEVELAPID,
      X001cc_Stud_qual_curr.FFIELDOFSTUDYAPID,
      X001cc_Stud_qual_curr.FPROGRAMAPID AS FPROGRAMAPID1,
      X001cc_Stud_qual_curr.FQUALIFICATIONAPID
    FROM
      X000_Student_qual_result
      INNER JOIN X001cc_Stud_qual_curr ON X001cc_Stud_qual_curr.KSTUDBUSENTID =
        X000_Student_qual_result.KBUSINESSENTITYID AND X001cc_Stud_qual_curr.FFIELDOFSTUDYAPID =
        X000_Student_qual_result.KACADEMICPROGRAMID AND X001cc_Stud_qual_curr.FPROGRAMAPID =
        X000_Student_qual_result.FPROGRAMAPID
    WHERE
      X000_Student_qual_result.DISCONTINUEDATE >= Date('%CYEARB%') AND
      X000_Student_qual_result.DISCONTINUEDATE <= Date('%CYEARE%')
    ORDER BY
      X000_Student_qual_result.KBUSINESSENTITYID,
      X000_Student_qual_result.AUDITDATETIME DESC
    """

    s_sql = s_sql.replace("%CYEARB%",funcdate.cur_yearbegin())
    s_sql = s_sql.replace("%CYEARE%",funcdate.cur_yearend())
    so_curs.execute("DROP VIEW IF EXISTS X000_Student_qual_result_curr")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_Student_qual_result_curr")


    # Build student qualification current 4 ****************************************
    # QUALLEVELENROLSTUD
    # PRESENTOUENROLPRESENTCAT
    # QUALIFICATIONLEVEL
    # FIELD OF STUDY
    # QUALIFICATION

    print("Build current student qualification step 4...")

    s_sql = "CREATE VIEW X001cd_Stud_qual_curr AS " + """
    SELECT
      X001cc_Stud_qual_curr.KSTUDBUSENTID,
      X001cc_Stud_qual_curr.KENROLSTUDID,
      X001cc_Stud_qual_curr.DATEQUALLEVELSTARTED,
      X001cc_Stud_qual_curr.DATEENROL,
      X001cc_Stud_qual_curr.STARTDATE,
      X001cc_Stud_qual_curr.ENDDATE,
      X000_Student_qual_result_curr.DISCONTINUEDATE,
      X001cc_Stud_qual_curr.QUALIFICATIONCODE,
      X001cc_Stud_qual_curr.QUALIFICATIONFIELDOFSTUDY,
      X001cc_Stud_qual_curr.QUALIFICATIONLEVEL,
      X001cc_Stud_qual_curr.QUAL_TYPE,
      X001cc_Stud_qual_curr.ISHEMISSUBSIDY,
      X001cc_Stud_qual_curr.ISMAINQUALLEVEL,
      X001cc_Stud_qual_curr.ENROLACADEMICYEAR,
      X001cc_Stud_qual_curr.ENROLHISTORYYEAR,
      X001cc_Stud_qual_curr.MIN,
      X001cc_Stud_qual_curr.MIN_UNIT,
      X001cc_Stud_qual_curr.MAX,
      X001cc_Stud_qual_curr.MAX_UNIT,
      X001cc_Stud_qual_curr.FSTUDACTIVECODEID,
      X001cc_Stud_qual_curr.ACTIVE_IND,
      X001cc_Stud_qual_curr.FENTRYLEVELCODEID,
      X001cc_Stud_qual_curr.ENTRY_LEVEL,
      X001cc_Stud_qual_curr.FENROLMENTCATEGORYCODEID,
      X001cc_Stud_qual_curr.ENROL_CAT,
      X001cc_Stud_qual_curr.FPRESENTATIONCATEGORYCODEID,
      X001cc_Stud_qual_curr.PRESENT_CAT,
      X001cc_Stud_qual_curr.FFINALSTATUSCODEID,
      X001cc_Stud_qual_curr.QUAL_LEVEL_STATUS_FINAL,
      X001cc_Stud_qual_curr.FLEVYCATEGORYCODEID,
      X001cc_Stud_qual_curr.QUAL_LEVEL_LEVY_CAT,
      X001cc_Stud_qual_curr.CERT_TYPE,
      X001cc_Stud_qual_curr.LEVY_TYPE,
      X001cc_Stud_qual_curr.FBLACKLISTCODEID,
      X001cc_Stud_qual_curr.BLACKLIST,
      X001cc_Stud_qual_curr.FSELECTIONCODEID,
      X001cc_Stud_qual_curr.FOS_SELECTION,
      X000_Student_qual_result_curr.RESULT,
      X000_Student_qual_result_curr.DISCONTINUE_REAS,
      X000_Student_qual_result_curr.POSTPONE_REAS,
      X001cc_Stud_qual_curr.FBUSINESSENTITYID,
      X001cc_Stud_qual_curr.FORGUNITNUMBER,
      X001cc_Stud_qual_curr.ORGUNIT_TYPE,
      X001cc_Stud_qual_curr.ORGUNIT_NAME,
      X001cc_Stud_qual_curr.FSITEORGUNITNUMBER,
      X001cc_Stud_qual_curr.ISCONDITIONALREG,
      X001cc_Stud_qual_curr.MARKSFINALISEDDATE,
      X000_Student_qual_result_curr.RESULTPASSDATE,
      X000_Student_qual_result_curr.RESULTISSUEDATE,
      X001cc_Stud_qual_curr.EXAMSUBMINIMUM,
      X001cc_Stud_qual_curr.ISCUMLAUDE,
      X001cc_Stud_qual_curr.FGRADCERTLANGUAGECODEID,
      X001cc_Stud_qual_curr.ISPOSSIBLEGRADUATE,
      X001cc_Stud_qual_curr.FGRADUATIONCEREMONYID,
      X001cc_Stud_qual_curr.FACCEPTANCETESTCODEID,
      X001cc_Stud_qual_curr.FENROLMENTPRESENTATIONID,
      X001cc_Stud_qual_curr.FQUALPRESENTINGOUID,
      X001cc_Stud_qual_curr.FQUALLEVELAPID,
      X001cc_Stud_qual_curr.FFIELDOFSTUDYAPID,
      X001cc_Stud_qual_curr.FPROGRAMAPID,
      X001cc_Stud_qual_curr.FQUALIFICATIONAPID,
      PROGRAM.FFIELDOFSTUDYAPID AS FFIELDOFSTUDYAPID1,
      PROGRAM.PROGRAMCODE
    FROM
      X001cc_Stud_qual_curr
      LEFT JOIN PROGRAM ON PROGRAM.KACADEMICPROGRAMID = X001cc_Stud_qual_curr.FPROGRAMAPID
      LEFT JOIN X000_Student_qual_result_curr ON X000_Student_qual_result_curr.KBUSINESSENTITYID =
        X001cc_Stud_qual_curr.KSTUDBUSENTID
    """
    so_curs.execute("DROP VIEW IF EXISTS X001cd_Stud_qual_curr")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X001cd_Stud_qual_curr")

    # Build modules ************************************************************

    print("Build module...")

    s_sql = "CREATE VIEW X002aa_Module AS " + """
    SELECT
      MODULE.KACADEMICPROGRAMID AS MODULE_ID,
      MODULE.STARTDATE,
      MODULE.ENDDATE,
      COURSE.COURSECODE,
      COURSELEVEL.COURSELEVEL,
      MODULE.COURSEMODULE
    FROM
      MODULE
      LEFT JOIN COURSELEVEL ON COURSELEVEL.KACADEMICPROGRAMID = MODULE.FCOURSELEVELAPID
      LEFT JOIN COURSE ON COURSE.KACADEMICPROGRAMID = COURSELEVEL.FCOURSEAPID
    """
    so_curs.execute("DROP VIEW IF EXISTS X002aa_Module")    
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X002aa_Module")    

    # Build module presenting organization *************************************

    print("Build module present organization...")

    s_sql = "CREATE VIEW X002ba_Module_present_org AS " + """
    SELECT
      MODULEPRESENTINGOU.KPRESENTINGOUID,
      MODULEPRESENTINGOU.STARTDATE,
      MODULEPRESENTINGOU.ENDDATE,
      MODULEPRESENTINGOU.FBUSINESSENTITYID,
      X000_Orgunitinstance.FSITEORGUNITNUMBER,
      X000_Orgunitinstance.ORGUNIT_TYPE,
      X000_Orgunitinstance.ORGUNIT_NAME,
      MODULEPRESENTINGOU.FMODULEAPID,
      X002aa_Module.COURSECODE,
      X002aa_Module.COURSELEVEL,
      X002aa_Module.COURSEMODULE,
      MODULEPRESENTINGOU.FCOURSEGROUPCODEID,
      X000_Codedescription_coursegroup.LONG AS NAME_GROUP,
      X000_Codedescription_coursegroup.LANK AS NAAM_GROEP,
      MODULEPRESENTINGOU.ISEXAMMODULE,
      MODULEPRESENTINGOU.LOCKSTAMP,
      MODULEPRESENTINGOU.AUDITDATETIME,
      MODULEPRESENTINGOU.FAUDITSYSTEMFUNCTIONID,
      MODULEPRESENTINGOU.FAUDITUSERCODE
    FROM
      MODULEPRESENTINGOU
      LEFT JOIN X000_Orgunitinstance ON X000_Orgunitinstance.KBUSINESSENTITYID = MODULEPRESENTINGOU.FBUSINESSENTITYID
      LEFT JOIN X002aa_Module ON X002aa_Module.MODULE_ID = MODULEPRESENTINGOU.FMODULEAPID
      LEFT JOIN X000_Codedescription X000_Codedescription_coursegroup ON X000_Codedescription_coursegroup.KCODEDESCID =
        MODULEPRESENTINGOU.FCOURSEGROUPCODEID
    """
    so_curs.execute("DROP VIEW IF EXISTS X002ba_Module_present_org")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X002ba_Module_present_org")

    # Close the connection *********************************************************
    so_conn.close()

    # Close the log writer *********************************************************
    funcfile.writelog("-------------------------")
    funcfile.writelog("COMPLETED: B003_VSS_LISTS")

    return
