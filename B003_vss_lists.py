"""
Script to build standard VSS lists
Created on: 01 Mar 2018
Copyright: Albert J v Rensburg
"""

def Vss_lists():
    
    # Import python modules
    import csv
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

    print("--------------")
    print("B003_VSS_LISTS")
    print("--------------")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: B003_VSS_LISTS")
    funcfile.writelog("----------------------")
    ilog_severity = 1

    # Declare variables
    so_path = "W:/Vss/" #Source database path
    so_file = "Vss.sqlite" #Source database
    re_path = "R:/Vss/" #Results
    ed_path = "S:/_external_data/"
    s_sql = "" #SQL statements

    # Open the SOURCE file
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()

    funcfile.writelog("OPEN DATABASE: VSS.SQLITE")

    # Import OWN LOOKUPS table *************************************************
    print("Import own lookups...")
    tb_name = "X000_OWN_LOOKUPS"
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

    # Build present enrolment category *****************************************

    print("Build present enrol category...")

    s_sql = "CREATE TABLE X000_Present_enrol_category AS " + """
    SELECT
      PRESENTOUENROLPRESENTCAT.KENROLMENTPRESENTATIONID,
      PRESENTOUENROLPRESENTCAT.FENROLMENTCATEGORYCODEID,
      X000_CODEDESC_ENROLCAT.LONG AS ENROL_CAT_E,
      PRESENTOUENROLPRESENTCAT.FPRESENTATIONCATEGORYCODEID,
      X000_CODEDESC_PRESENTCAT.LONG AS PRESENT_CAT_E,
      PRESENTOUENROLPRESENTCAT.STARTDATE,
      PRESENTOUENROLPRESENTCAT.ENDDATE,
      PRESENTOUENROLPRESENTCAT.FQUALPRESENTINGOUID,
      PRESENTOUENROLPRESENTCAT.FMODULEPRESENTINGOUID,
      PRESENTOUENROLPRESENTCAT.FPROGRAMPRESENTINGOUID,
      PRESENTOUENROLPRESENTCAT.EXAMSUBMINIMUM,
      PRESENTOUENROLPRESENTCAT.FAUDITSYSTEMFUNCTIONID,
      PRESENTOUENROLPRESENTCAT.AUDITDATETIME,
      PRESENTOUENROLPRESENTCAT.FAUDITUSERCODE,
      X000_CODEDESC_ENROLCAT.LANK AS ENROL_CAT_A,
      X000_CODEDESC_PRESENTCAT.LANK AS PRESENT_CAT_A
    FROM
      PRESENTOUENROLPRESENTCAT
      LEFT JOIN X000_Codedescription X000_CODEDESC_ENROLCAT ON X000_CODEDESC_ENROLCAT.KCODEDESCID =
        PRESENTOUENROLPRESENTCAT.FENROLMENTCATEGORYCODEID
      LEFT JOIN X000_Codedescription X000_CODEDESC_PRESENTCAT ON X000_CODEDESC_PRESENTCAT.KCODEDESCID =
        PRESENTOUENROLPRESENTCAT.FPRESENTATIONCATEGORYCODEID
    """
    so_curs.execute("DROP TABLE IF EXISTS X000_Present_enrol_category")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: X000_Present_enrol_category")

    # Build student results ********************************************************

    funcfile.writelog("STUDENT RESULTS")

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

    # Build current student qualification results **********************************
    print("Build current student qualification results...")
    sr_file = "X000_Student_qual_result_curr"
    s_sql = "CREATE VIEW "+ sr_file +" AS " + """
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
      X000_Student_qual_result.ISSUEFIRSTNAMES
    FROM
      X000_Student_qual_result
    WHERE
      X000_Student_qual_result.KRESULTYYYYMM >= SubStr('%CYEARB%',1,4)||SubStr('%CYEARB%',6,2) AND
      X000_Student_qual_result.KRESULTYYYYMM <= SubStr('%CYEARE%',1,4)||SubStr('%CYEARE%',6,2)
    ORDER BY
      X000_Student_qual_result.KBUSINESSENTITYID
    """
    s_sql = s_sql.replace("%CYEARB%",funcdate.cur_yearbegin())
    s_sql = s_sql.replace("%CYEARE%",funcdate.cur_yearend())
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_Student_qual_result_curr")

    # Build qualification step one *************************************************

    funcfile.writelog("STUDENT QUALIFICATIONS")

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

    # Build current student qualification results **********************************
    print("Build current student qualification step 4...")
    sr_file = "X001cd_Stud_qual_curr"
    s_sql = "CREATE VIEW "+ sr_file +" AS " + """
    SELECT
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
      X001cc_Stud_qual_curr.FGRADUATIONCEREMONYID,
      X001cc_Stud_qual_curr.FACCEPTANCETESTCODEID,
      X001cc_Stud_qual_curr.FENROLMENTPRESENTATIONID,
      X001cc_Stud_qual_curr.FQUALPRESENTINGOUID,
      X001cc_Stud_qual_curr.FQUALLEVELAPID,
      X001cc_Stud_qual_curr.FFIELDOFSTUDYAPID,
      X001cc_Stud_qual_curr.FPROGRAMAPID,
      X001cc_Stud_qual_curr.FQUALIFICATIONAPID,
      X000_Student_qual_result_curr.KBUSINESSENTITYID,
      X000_Student_qual_result_curr.KACADEMICPROGRAMID,
      X000_Student_qual_result_curr.KQUALFOSRESULTCODEID,
      X000_Student_qual_result_curr.RESULT,
      X000_Student_qual_result_curr.KRESULTYYYYMM,
      X000_Student_qual_result_curr.KSTUDQUALFOSRESULTID,
      X000_Student_qual_result_curr.FGRADUATIONCEREMONYID AS FGRADUATIONCEREMONYID1,
      X000_Student_qual_result_curr.FPOSTPONEMENTCODEID,
      X000_Student_qual_result_curr.POSTPONE_REAS,
      X000_Student_qual_result_curr.RESULTISSUEDATE,
      X000_Student_qual_result_curr.DISCONTINUEDATE,
      X000_Student_qual_result_curr.FDISCONTINUECODEID,
      X000_Student_qual_result_curr.DISCONTINUE_REAS,
      X000_Student_qual_result_curr.RESULTPASSDATE,
      X000_Student_qual_result_curr.FLANGUAGECODEID,
      X000_Student_qual_result_curr.ISSUESURNAME,
      X000_Student_qual_result_curr.CERTIFICATESEQNUMBER,
      X000_Student_qual_result_curr.AVGMARKACHIEVED,
      X000_Student_qual_result_curr.PROCESSSEQNUMBER,
      X000_Student_qual_result_curr.FRECEIPTID,
      X000_Student_qual_result_curr.FRECEIPTLINEID,
      X000_Student_qual_result_curr.ISINABSENTIA,
      X000_Student_qual_result_curr.FPROGRAMAPID AS FPROGRAMAPID1,
      X000_Student_qual_result_curr.FISSUETYPECODEID,
      X000_Student_qual_result_curr.ISSUE_TYPE,
      X000_Student_qual_result_curr.DATEPRINTED,
      X000_Student_qual_result_curr.LOCKSTAMP,
      X000_Student_qual_result_curr.AUDITDATETIME,
      X000_Student_qual_result_curr.FAUDITSYSTEMFUNCTIONID,
      X000_Student_qual_result_curr.FAUDITUSERCODE,
      X000_Student_qual_result_curr.FAPPROVEDBYCODEID,
      X000_Student_qual_result_curr.FAPPROVEDBYUSERCODE,
      X000_Student_qual_result_curr.DATERESULTAPPROVED,
      X000_Student_qual_result_curr.FENROLMENTPRESENTATIONID AS FENROLMENTPRESENTATIONID1,
      X000_Student_qual_result_curr.CERTDISPATCHDATE,
      X000_Student_qual_result_curr.CERTDISPATCHREFNO,
      X000_Student_qual_result_curr.ISSUEFIRSTNAMES
    FROM
      X001cc_Stud_qual_curr
      LEFT JOIN X000_Student_qual_result_curr ON X000_Student_qual_result_curr.KBUSINESSENTITYID =
        X001cc_Stud_qual_curr.KSTUDBUSENTID AND X000_Student_qual_result_curr.FPROGRAMAPID =
        X001cc_Stud_qual_curr.FPROGRAMAPID AND X000_Student_qual_result_curr.KACADEMICPROGRAMID =
        X001cc_Stud_qual_curr.FFIELDOFSTUDYAPID
    """
    so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X001cd_Stud_qual_curr")

    # Build student qualification current 5 or final list **************************
    # QUALLEVELENROLSTUD
    # PRESENTOUENROLPRESENTCAT
    # QUALIFICATIONLEVEL
    # FIELD OF STUDY
    # QUALIFICATION

    print("Build current student qualification step 5...")
    sr_file = "X001cx_Stud_qual_curr"
    s_sql = "CREATE TABLE "+ sr_file +" AS " + """
    SELECT
      X001cd_Stud_qual_curr.KSTUDBUSENTID,
      X001cd_Stud_qual_curr.KENROLSTUDID,
      X001cd_Stud_qual_curr.FORGUNITNUMBER,      
      X001cd_Stud_qual_curr.DATEQUALLEVELSTARTED,
      X001cd_Stud_qual_curr.DATEENROL,
      X001cd_Stud_qual_curr.STARTDATE,
      X001cd_Stud_qual_curr.ENDDATE,
      X001cd_Stud_qual_curr.DISCONTINUEDATE,
      X001cd_Stud_qual_curr.RESULT,      
      X001cd_Stud_qual_curr.QUALIFICATIONCODE,
      X001cd_Stud_qual_curr.QUALIFICATIONFIELDOFSTUDY,
      X001cd_Stud_qual_curr.QUALIFICATIONLEVEL,
      X001cd_Stud_qual_curr.QUAL_TYPE,
      X001cd_Stud_qual_curr.ISHEMISSUBSIDY,
      X001cd_Stud_qual_curr.ISMAINQUALLEVEL,
      X001cd_Stud_qual_curr.ENROLACADEMICYEAR,
      X001cd_Stud_qual_curr.ENROLHISTORYYEAR,
      X001cd_Stud_qual_curr.MIN,
      X001cd_Stud_qual_curr.MIN_UNIT,
      X001cd_Stud_qual_curr.MAX,
      X001cd_Stud_qual_curr.MAX_UNIT,
      X001cd_Stud_qual_curr.FSTUDACTIVECODEID,
      X001cd_Stud_qual_curr.ACTIVE_IND,
      X001cd_Stud_qual_curr.FENTRYLEVELCODEID,
      X001cd_Stud_qual_curr.ENTRY_LEVEL,
      X001cd_Stud_qual_curr.FENROLMENTCATEGORYCODEID,
      X001cd_Stud_qual_curr.ENROL_CAT,
      X001cd_Stud_qual_curr.FPRESENTATIONCATEGORYCODEID,
      X001cd_Stud_qual_curr.PRESENT_CAT,
      X001cd_Stud_qual_curr.FFINALSTATUSCODEID,
      X001cd_Stud_qual_curr.QUAL_LEVEL_STATUS_FINAL,
      X001cd_Stud_qual_curr.FLEVYCATEGORYCODEID,
      X001cd_Stud_qual_curr.QUAL_LEVEL_LEVY_CAT,
      X001cd_Stud_qual_curr.CERT_TYPE,
      X001cd_Stud_qual_curr.LEVY_TYPE,
      X001cd_Stud_qual_curr.FBLACKLISTCODEID,
      X001cd_Stud_qual_curr.BLACKLIST,
      X001cd_Stud_qual_curr.FSELECTIONCODEID,
      X001cd_Stud_qual_curr.FOS_SELECTION,
      X001cd_Stud_qual_curr.DISCONTINUE_REAS,
      X001cd_Stud_qual_curr.POSTPONE_REAS,
      X001cd_Stud_qual_curr.FBUSINESSENTITYID,
      X001cd_Stud_qual_curr.ORGUNIT_TYPE,
      X001cd_Stud_qual_curr.ORGUNIT_NAME,
      X001cd_Stud_qual_curr.FSITEORGUNITNUMBER,
      X001cd_Stud_qual_curr.ISCONDITIONALREG,
      X001cd_Stud_qual_curr.MARKSFINALISEDDATE,
      X001cd_Stud_qual_curr.RESULTPASSDATE,
      X001cd_Stud_qual_curr.RESULTISSUEDATE,
      X001cd_Stud_qual_curr.EXAMSUBMINIMUM,
      X001cd_Stud_qual_curr.ISCUMLAUDE,
      X001cd_Stud_qual_curr.FGRADCERTLANGUAGECODEID,
      X001cd_Stud_qual_curr.ISPOSSIBLEGRADUATE,
      X001cd_Stud_qual_curr.FGRADUATIONCEREMONYID,
      X001cd_Stud_qual_curr.FACCEPTANCETESTCODEID,
      X001cd_Stud_qual_curr.FENROLMENTPRESENTATIONID,
      X001cd_Stud_qual_curr.FQUALPRESENTINGOUID,
      X001cd_Stud_qual_curr.FQUALLEVELAPID,
      X001cd_Stud_qual_curr.FFIELDOFSTUDYAPID,
      X001cd_Stud_qual_curr.FPROGRAMAPID,
      X001cd_Stud_qual_curr.FQUALIFICATIONAPID,
      PROGRAM.FFIELDOFSTUDYAPID AS FFIELDOFSTUDYAPID1,
      PROGRAM.PROGRAMCODE
    FROM
      X001cd_Stud_qual_curr
      LEFT JOIN PROGRAM ON PROGRAM.KACADEMICPROGRAMID = X001cd_Stud_qual_curr.FPROGRAMAPID
    ORDER BY
      X001cd_Stud_qual_curr.KSTUDBUSENTID,
      X001cd_Stud_qual_curr.DATEENROL
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: X001cx_Stud_qual_curr")

    # Export the data
    print("Export gl student debtor transactions...")
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "Student_001_all_"
    sx_filet = sx_file + funcdate.prev_monthendfile()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)    
    funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

    # Build modules ************************************************************

    funcfile.writelog("STUDENT MODULES")

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

    # Build module presenting enrolment ****************************************

    print("Build module present enrolment...")

    s_sql = "CREATE VIEW X002bb_Module_present_enrol AS " + """
    SELECT
      X000_Present_enrol_category.KENROLMENTPRESENTATIONID,
      X000_Present_enrol_category.ENROL_CAT_E,
      X000_Present_enrol_category.ENROL_CAT_A,
      X000_Present_enrol_category.PRESENT_CAT_E,
      X000_Present_enrol_category.PRESENT_CAT_A,
      X000_Present_enrol_category.STARTDATE,
      X000_Present_enrol_category.ENDDATE,
      X002ba_Module_present_org.FSITEORGUNITNUMBER,
      X002ba_Module_present_org.ORGUNIT_TYPE,
      X002ba_Module_present_org.ORGUNIT_NAME,
      X002ba_Module_present_org.FMODULEAPID,
      X002ba_Module_present_org.COURSECODE,
      X002ba_Module_present_org.COURSELEVEL,
      X002ba_Module_present_org.COURSEMODULE,
      X002ba_Module_present_org.FCOURSEGROUPCODEID,
      X002ba_Module_present_org.NAME_GROUP,
      X002ba_Module_present_org.NAAM_GROEP,
      X002ba_Module_present_org.ISEXAMMODULE,
      X000_Present_enrol_category.EXAMSUBMINIMUM
    FROM
      X000_Present_enrol_category
      INNER JOIN X002ba_Module_present_org ON X002ba_Module_present_org.KPRESENTINGOUID =
        X000_Present_enrol_category.FMODULEPRESENTINGOUID
    """
    so_curs.execute("DROP VIEW IF EXISTS X002bb_Module_present_enrol")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X002bb_Module_present_enrol")

    # Build bursary master *****************************************************

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
    sr_file = "X010_Studytrans"
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
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: " + sr_file)    

    # BUILD PREVIOUS YEAR TRANSACTIONS *****************************************

    print("Build previous year transactions...")
    sr_file = "X010_Studytrans_prev"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    SELECT
      STUDYTRANS_PREV.KACCTRANSID,
      STUDYTRANS_PREV.FACCID,
      STUDACC.FBUSENTID,
      STUDYTRANS_PREV.FSERVICESITE,
      STUDYTRANS_PREV.FDEBTCOLLECTIONSITE,
      STUDYTRANS_PREV.TRANSDATE,
      STUDYTRANS_PREV.AMOUNT,
      STUDYTRANS_PREV.FTRANSMASTERID,
      X000_Transmaster.TRANSCODE,
      X000_Transmaster.DESCRIPTION_E,
      X000_Transmaster.DESCRIPTION_A,
      STUDYTRANS_PREV.TRANSDATETIME,
      STUDYTRANS_PREV.MONTHENDDATE,
      STUDYTRANS_PREV.POSTDATEDTRANSDATE,
      STUDYTRANS_PREV.FFINAIDSITEID,
      X004_Bursaries.FINAIDCODE,
      X004_Bursaries.FINAIDNAAM,
      STUDYTRANS_PREV.FRESIDENCELOGID,
      STUDYTRANS_PREV.FLEVYLOGID,
      STUDYTRANS_PREV.FMODAPID,
      STUDYTRANS_PREV.FQUALLEVELAPID,
      STUDYTRANS_PREV.FPROGAPID,
      STUDYTRANS_PREV.FENROLPRESID,
      STUDYTRANS_PREV.FRESIDENCEID,
      STUDYTRANS_PREV.FRECEIPTID,
      STUDYTRANS_PREV.FROOMTYPECODEID,
      STUDYTRANS_PREV.REFERENCENO,
      STUDYTRANS_PREV.FSUBACCTYPECODEID,
      STUDYTRANS_PREV.FDEPOSITCODEID,
      STUDYTRANS_PREV.FDEPOSITTYPECODEID,
      STUDYTRANS_PREV.FVARIABLEAMOUNTTYPECODEID,
      STUDYTRANS_PREV.FDEPOSITTRANSTYPECODEID,
      STUDYTRANS_PREV.RESIDENCETRANSTYPE,
      STUDYTRANS_PREV.FSTUDYTRANSTYPECODEID,
      STUDYTRANS_PREV.ISSHOWN,
      STUDYTRANS_PREV.ISCREATEDMANUALLY,
      STUDYTRANS_PREV.FTRANSINSTID,
      STUDYTRANS_PREV.FMONTHENDORGUNITNO,
      STUDYTRANS_PREV.LOCKSTAMP,
      STUDYTRANS_PREV.AUDITDATETIME,
      STUDYTRANS_PREV.FAUDITSYSTEMFUNCTIONID,
      STUDYTRANS_PREV.FAUDITUSERCODE,
      SYSTEMUSER.FUSERBUSINESSENTITYID,
      STUDYTRANS_PREV.FORIGINSYSTEMFUNCTIONID,
      STUDYTRANS_PREV.FPAYMENTREQUESTID
    FROM
      STUDYTRANS_PREV
      LEFT JOIN STUDACC ON STUDACC.KACCID = STUDYTRANS_PREV.FACCID
      LEFT JOIN X000_Transmaster ON X000_Transmaster.KTRANSMASTERID = STUDYTRANS_PREV.FTRANSMASTERID
      LEFT JOIN X004_Bursaries ON X004_Bursaries.KFINAIDSITEID = STUDYTRANS_PREV.FFINAIDSITEID
      LEFT JOIN SYSTEMUSER ON SYSTEMUSER.KUSERCODE = STUDYTRANS_PREV.FAUDITUSERCODE
    ORDER BY
      STUDYTRANS_PREV.TRANSDATETIME
    """
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
