"""
Script to build standard VSS lists
Created on: 14 Sep 2018
Copyright: Albert J v Rensburg
"""

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
re_path = "R:/Vss/" #Results
ed_path = "S:/_external_data/"
s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("OPEN DATABASE: VSS.SQLITE")

# Ask student list dates *******************************************************
print("")
d_from = input("Students from? (yyyy-mm-dd) ")
d_toto = input("  Students to? (yyyy-mm-dd) ")
print("")

# Build period student qualification results **********************************
print("Build period student qualification results...")
sr_file = "X000_Student_qual_result_peri"
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
s_sql = s_sql.replace("%CYEARB%",d_from)
s_sql = s_sql.replace("%CYEARE%",d_toto)
so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD VIEW: X000_Student_qual_result_peri")

# Build qualification step one *************************************************

funcfile.writelog("STUDENT QUALIFICATIONS")
print("Build qualification...")
sr_file = "X001aa_Qualification"
s_sql = "CREATE VIEW "+sr_file+" AS " + """
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
so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: "+sr_file)

# Build qualification step two *************************************************

print("Build qualification level...")
sr_file = "X001ba_Qualification_level"
s_sql = "CREATE VIEW "+sr_file+" AS " + """
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
so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: "+sr_file)

# Build student qualification period 1 *****************************************
# QUALLEVELENROLSTUD + PRESENTOUENROLPRESENTCAT

print("Build period student qualification step 1...")
sr_file = "X001ca_Stud_qual_peri"
s_sql = "CREATE VIEW "+sr_file+" AS " + """
SELECT
  QUALLEVELENROLSTUD_PERI.KSTUDBUSENTID,
  QUALLEVELENROLSTUD_PERI.KENROLSTUDID,
  QUALLEVELENROLSTUD_PERI.DATEQUALLEVELSTARTED,  
  QUALLEVELENROLSTUD_PERI.DATEENROL,  
  QUALLEVELENROLSTUD_PERI.STARTDATE,
  QUALLEVELENROLSTUD_PERI.ENDDATE,
  QUALLEVELENROLSTUD_PERI.ISHEMISSUBSIDY,
  QUALLEVELENROLSTUD_PERI.ISMAINQUALLEVEL,
  QUALLEVELENROLSTUD_PERI.ENROLACADEMICYEAR,
  QUALLEVELENROLSTUD_PERI.ENROLHISTORYYEAR,  
  QUALLEVELENROLSTUD_PERI.FSTUDACTIVECODEID,
  X000_Codedescription1.LONG AS ACTIVE_IND,  
  QUALLEVELENROLSTUD_PERI.FENTRYLEVELCODEID,
  X000_Codedescription2.LONG AS ENTRY_LEVEL,
  PRESENTOUENROLPRESENTCAT.FENROLMENTCATEGORYCODEID,  
  X000_Codedescription3.LONG AS ENROL_CAT,  
  PRESENTOUENROLPRESENTCAT.FPRESENTATIONCATEGORYCODEID,
  X000_Codedescription4.LONG AS PRESENT_CAT,
  QUALLEVELENROLSTUD_PERI.FBLACKLISTCODEID,
  X000_Codedescription.LONG AS BLACKLIST,  
  QUALLEVELENROLSTUD_PERI.ISCONDITIONALREG,
  QUALLEVELENROLSTUD_PERI.MARKSFINALISEDDATE,
  PRESENTOUENROLPRESENTCAT.EXAMSUBMINIMUM,  
  QUALLEVELENROLSTUD_PERI.ISCUMLAUDE,
  QUALLEVELENROLSTUD_PERI.FGRADCERTLANGUAGECODEID,
  QUALLEVELENROLSTUD_PERI.ISPOSSIBLEGRADUATE,
  QUALLEVELENROLSTUD_PERI.FGRADUATIONCEREMONYID,
  QUALLEVELENROLSTUD_PERI.FACCEPTANCETESTCODEID,
  QUALLEVELENROLSTUD_PERI.FENROLMENTPRESENTATIONID,
  QUALLEVELENROLSTUD_PERI.FPROGRAMAPID, 
  PRESENTOUENROLPRESENTCAT.FQUALPRESENTINGOUID
FROM
  QUALLEVELENROLSTUD_PERI
  LEFT JOIN PRESENTOUENROLPRESENTCAT ON PRESENTOUENROLPRESENTCAT.KENROLMENTPRESENTATIONID =
    QUALLEVELENROLSTUD_PERI.FENROLMENTPRESENTATIONID
  LEFT JOIN X000_Codedescription ON X000_Codedescription.KCODEDESCID =
    QUALLEVELENROLSTUD_PERI.FBLACKLISTCODEID
  LEFT JOIN X000_Codedescription X000_Codedescription1 ON X000_Codedescription1.KCODEDESCID =
    QUALLEVELENROLSTUD_PERI.FSTUDACTIVECODEID
  LEFT JOIN X000_Codedescription X000_Codedescription2 ON X000_Codedescription2.KCODEDESCID =
    QUALLEVELENROLSTUD_PERI.FENTRYLEVELCODEID
  LEFT JOIN X000_Codedescription X000_Codedescription3 ON X000_Codedescription3.KCODEDESCID =
    PRESENTOUENROLPRESENTCAT.FENROLMENTCATEGORYCODEID
  INNER JOIN X000_Codedescription X000_Codedescription4 ON X000_Codedescription4.KCODEDESCID =
    PRESENTOUENROLPRESENTCAT.FPRESENTATIONCATEGORYCODEID
ORDER BY
  QUALLEVELENROLSTUD_PERI.KSTUDBUSENTID,
  QUALLEVELENROLSTUD_PERI.DATEQUALLEVELSTARTED  
"""
so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: "+sr_file)

# Build student qualification period 2 *****************************************
# QUALLEVELENROLSTUD + PRESENTOUENROLPRESENTCAT
# QUALIFICATIONLEVEL

print("Build period student qualification step 2...")
sr_file = "X001cb_Stud_qual_peri"
s_sql = "CREATE VIEW "+sr_file+" AS " + """
SELECT
  X001ca_Stud_qual_peri.KSTUDBUSENTID,
  X001ca_Stud_qual_peri.KENROLSTUDID,
  X001ca_Stud_qual_peri.DATEQUALLEVELSTARTED,
  X001ca_Stud_qual_peri.DATEENROL,
  X001ca_Stud_qual_peri.STARTDATE,
  X001ca_Stud_qual_peri.ENDDATE,
  QUALIFICATIONLEVEL.QUALIFICATIONLEVEL,  
  X001ca_Stud_qual_peri.ISHEMISSUBSIDY,
  X001ca_Stud_qual_peri.ISMAINQUALLEVEL,
  X001ca_Stud_qual_peri.ENROLACADEMICYEAR,
  X001ca_Stud_qual_peri.ENROLHISTORYYEAR,
  X001ca_Stud_qual_peri.FSTUDACTIVECODEID,
  X001ca_Stud_qual_peri.ACTIVE_IND,
  X001ca_Stud_qual_peri.FENTRYLEVELCODEID,
  X001ca_Stud_qual_peri.ENTRY_LEVEL,
  X001ca_Stud_qual_peri.FENROLMENTCATEGORYCODEID,
  X001ca_Stud_qual_peri.ENROL_CAT,
  X001ca_Stud_qual_peri.FPRESENTATIONCATEGORYCODEID,
  X001ca_Stud_qual_peri.PRESENT_CAT,
  QUALIFICATIONLEVEL.FFINALSTATUSCODEID,
  X000_Codedescription.LONG AS QUAL_LEVEL_STATUS_FINAL,  
  QUALIFICATIONLEVEL.FLEVYCATEGORYCODEID,
  X000_Codedescription1.LONG AS QUAL_LEVEL_LEVY_CAT,  
  X001ca_Stud_qual_peri.FBLACKLISTCODEID,
  X001ca_Stud_qual_peri.BLACKLIST,
  QUALLEVELPRESENTINGOU.FBUSINESSENTITYID,
  X000_Orgunitinstance.FORGUNITNUMBER,
  X000_Orgunitinstance.ORGUNIT_TYPE,
  X000_Orgunitinstance.ORGUNIT_NAME,
  X000_Orgunitinstance.FSITEORGUNITNUMBER,  
  X001ca_Stud_qual_peri.ISCONDITIONALREG,
  X001ca_Stud_qual_peri.MARKSFINALISEDDATE,
  X001ca_Stud_qual_peri.EXAMSUBMINIMUM,
  X001ca_Stud_qual_peri.ISCUMLAUDE,
  X001ca_Stud_qual_peri.FGRADCERTLANGUAGECODEID,
  X001ca_Stud_qual_peri.ISPOSSIBLEGRADUATE,
  X001ca_Stud_qual_peri.FGRADUATIONCEREMONYID,
  X001ca_Stud_qual_peri.FACCEPTANCETESTCODEID,
  X001ca_Stud_qual_peri.FENROLMENTPRESENTATIONID,
  X001ca_Stud_qual_peri.FQUALPRESENTINGOUID,
  X001ca_Stud_qual_peri.FPROGRAMAPID,   
  QUALLEVELPRESENTINGOU.FQUALLEVELAPID,
  QUALIFICATIONLEVEL.FFIELDOFSTUDYAPID,
  QUALIFICATIONLEVEL.STARTDATE AS QUAL_LEVEL_STARTDATE,
  QUALIFICATIONLEVEL.ENDDATE AS QUAL_LEVEL_ENDDATE
FROM
  X001ca_Stud_qual_peri
  LEFT JOIN QUALLEVELPRESENTINGOU ON QUALLEVELPRESENTINGOU.KPRESENTINGOUID =
    X001ca_Stud_qual_peri.FQUALPRESENTINGOUID
  LEFT JOIN QUALIFICATIONLEVEL ON QUALIFICATIONLEVEL.KACADEMICPROGRAMID =
    QUALLEVELPRESENTINGOU.FQUALLEVELAPID
  LEFT JOIN X000_Orgunitinstance ON X000_Orgunitinstance.KBUSINESSENTITYID =
    QUALLEVELPRESENTINGOU.FBUSINESSENTITYID
  LEFT JOIN X000_Codedescription ON X000_Codedescription.KCODEDESCID =
    QUALIFICATIONLEVEL.FFINALSTATUSCODEID
  LEFT JOIN X000_Codedescription X000_Codedescription1 ON X000_Codedescription1.KCODEDESCID =
    QUALIFICATIONLEVEL.FLEVYCATEGORYCODEID
"""
so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: "+sr_file)

# Build student qualification period 3 ****************************************
# QUALLEVELENROLSTUD + PRESENTOUENROLPRESENTCAT
# QUALIFICATIONLEVEL

print("Build period student qualification step 3...")
sr_file = "X001cc_Stud_qual_peri"
s_sql = "CREATE VIEW "+sr_file+" AS " + """
SELECT
  X001cb_Stud_qual_peri.KSTUDBUSENTID,
  X001cb_Stud_qual_peri.KENROLSTUDID,
  X001cb_Stud_qual_peri.DATEQUALLEVELSTARTED,
  X001cb_Stud_qual_peri.DATEENROL,
  X001cb_Stud_qual_peri.STARTDATE,
  X001cb_Stud_qual_peri.ENDDATE,
  X001aa_Qualification.QUALIFICATIONCODE,  
  FIELDOFSTUDY.QUALIFICATIONFIELDOFSTUDY,  
  X001cb_Stud_qual_peri.QUALIFICATIONLEVEL,
  X001aa_Qualification.QUAL_TYPE,  
  X001cb_Stud_qual_peri.ISHEMISSUBSIDY,
  X001cb_Stud_qual_peri.ISMAINQUALLEVEL,
  X001cb_Stud_qual_peri.ENROLACADEMICYEAR,
  X001cb_Stud_qual_peri.ENROLHISTORYYEAR,
  X001aa_Qualification.MIN,
  X001aa_Qualification.MIN_UNIT,
  X001aa_Qualification.MAX,
  X001aa_Qualification.MAX_UNIT,
  X001cb_Stud_qual_peri.FSTUDACTIVECODEID,
  X001cb_Stud_qual_peri.ACTIVE_IND,
  X001cb_Stud_qual_peri.FENTRYLEVELCODEID,
  X001cb_Stud_qual_peri.ENTRY_LEVEL,
  X001cb_Stud_qual_peri.FENROLMENTCATEGORYCODEID,
  X001cb_Stud_qual_peri.ENROL_CAT,
  X001cb_Stud_qual_peri.FPRESENTATIONCATEGORYCODEID,
  X001cb_Stud_qual_peri.PRESENT_CAT,
  X001cb_Stud_qual_peri.FFINALSTATUSCODEID,
  X001cb_Stud_qual_peri.QUAL_LEVEL_STATUS_FINAL,
  X001cb_Stud_qual_peri.FLEVYCATEGORYCODEID,
  X001cb_Stud_qual_peri.QUAL_LEVEL_LEVY_CAT,
  X001aa_Qualification.CERT_TYPE,
  X001aa_Qualification.LEVY_TYPE,
  X001cb_Stud_qual_peri.FBLACKLISTCODEID,
  X001cb_Stud_qual_peri.BLACKLIST,
  FIELDOFSTUDY.FSELECTIONCODEID,
  X000_Codedescription.LONG AS FOS_SELECTION,
  X001cb_Stud_qual_peri.FBUSINESSENTITYID,
  X001cb_Stud_qual_peri.FORGUNITNUMBER,
  X001cb_Stud_qual_peri.ORGUNIT_TYPE,
  X001cb_Stud_qual_peri.ORGUNIT_NAME,
  X001cb_Stud_qual_peri.FSITEORGUNITNUMBER,
  X001cb_Stud_qual_peri.ISCONDITIONALREG,
  X001cb_Stud_qual_peri.MARKSFINALISEDDATE,
  X001cb_Stud_qual_peri.EXAMSUBMINIMUM,
  X001cb_Stud_qual_peri.ISCUMLAUDE,
  X001cb_Stud_qual_peri.FGRADCERTLANGUAGECODEID,
  X001cb_Stud_qual_peri.ISPOSSIBLEGRADUATE,
  X001cb_Stud_qual_peri.FGRADUATIONCEREMONYID,
  X001cb_Stud_qual_peri.FACCEPTANCETESTCODEID,
  X001cb_Stud_qual_peri.FENROLMENTPRESENTATIONID,
  X001cb_Stud_qual_peri.FQUALPRESENTINGOUID,
  X001cb_Stud_qual_peri.FQUALLEVELAPID,
  X001cb_Stud_qual_peri.FFIELDOFSTUDYAPID,
  X001cb_Stud_qual_peri.FPROGRAMAPID,  
  FIELDOFSTUDY.FQUALIFICATIONAPID
FROM
  X001cb_Stud_qual_peri
  LEFT JOIN FIELDOFSTUDY ON FIELDOFSTUDY.KACADEMICPROGRAMID = X001cb_Stud_qual_peri.FFIELDOFSTUDYAPID
  LEFT JOIN X000_Codedescription ON X000_Codedescription.KCODEDESCID = FIELDOFSTUDY.FSELECTIONCODEID
  LEFT JOIN X001aa_Qualification ON X001aa_Qualification.KACADEMICPROGRAMID = FIELDOFSTUDY.FQUALIFICATIONAPID
"""
so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: "+sr_file)

# Build period student qualification results **********************************
print("Build period student qualification step 4...")
sr_file = "X001cd_Stud_qual_peri"
s_sql = "CREATE VIEW "+ sr_file +" AS " + """
SELECT
  X001cc_Stud_qual_peri.KSTUDBUSENTID,
  X001cc_Stud_qual_peri.KENROLSTUDID,
  X001cc_Stud_qual_peri.DATEQUALLEVELSTARTED,
  X001cc_Stud_qual_peri.DATEENROL,
  X001cc_Stud_qual_peri.STARTDATE,
  X001cc_Stud_qual_peri.ENDDATE,
  X001cc_Stud_qual_peri.QUALIFICATIONCODE,
  X001cc_Stud_qual_peri.QUALIFICATIONFIELDOFSTUDY,
  X001cc_Stud_qual_peri.QUALIFICATIONLEVEL,
  X001cc_Stud_qual_peri.QUAL_TYPE,
  X001cc_Stud_qual_peri.ISHEMISSUBSIDY,
  X001cc_Stud_qual_peri.ISMAINQUALLEVEL,
  X001cc_Stud_qual_peri.ENROLACADEMICYEAR,
  X001cc_Stud_qual_peri.ENROLHISTORYYEAR,
  X001cc_Stud_qual_peri.MIN,
  X001cc_Stud_qual_peri.MIN_UNIT,
  X001cc_Stud_qual_peri.MAX,
  X001cc_Stud_qual_peri.MAX_UNIT,
  X001cc_Stud_qual_peri.FSTUDACTIVECODEID,
  X001cc_Stud_qual_peri.ACTIVE_IND,
  X001cc_Stud_qual_peri.FENTRYLEVELCODEID,
  X001cc_Stud_qual_peri.ENTRY_LEVEL,
  X001cc_Stud_qual_peri.FENROLMENTCATEGORYCODEID,
  X001cc_Stud_qual_peri.ENROL_CAT,
  X001cc_Stud_qual_peri.FPRESENTATIONCATEGORYCODEID,
  X001cc_Stud_qual_peri.PRESENT_CAT,
  X001cc_Stud_qual_peri.FFINALSTATUSCODEID,
  X001cc_Stud_qual_peri.QUAL_LEVEL_STATUS_FINAL,
  X001cc_Stud_qual_peri.FLEVYCATEGORYCODEID,
  X001cc_Stud_qual_peri.QUAL_LEVEL_LEVY_CAT,
  X001cc_Stud_qual_peri.CERT_TYPE,
  X001cc_Stud_qual_peri.LEVY_TYPE,
  X001cc_Stud_qual_peri.FBLACKLISTCODEID,
  X001cc_Stud_qual_peri.BLACKLIST,
  X001cc_Stud_qual_peri.FSELECTIONCODEID,
  X001cc_Stud_qual_peri.FOS_SELECTION,
  X001cc_Stud_qual_peri.FBUSINESSENTITYID,
  X001cc_Stud_qual_peri.FORGUNITNUMBER,
  X001cc_Stud_qual_peri.ORGUNIT_TYPE,
  X001cc_Stud_qual_peri.ORGUNIT_NAME,
  X001cc_Stud_qual_peri.FSITEORGUNITNUMBER,
  X001cc_Stud_qual_peri.ISCONDITIONALREG,
  X001cc_Stud_qual_peri.MARKSFINALISEDDATE,
  X001cc_Stud_qual_peri.EXAMSUBMINIMUM,
  X001cc_Stud_qual_peri.ISCUMLAUDE,
  X001cc_Stud_qual_peri.FGRADCERTLANGUAGECODEID,
  X001cc_Stud_qual_peri.ISPOSSIBLEGRADUATE,
  X001cc_Stud_qual_peri.FGRADUATIONCEREMONYID,
  X001cc_Stud_qual_peri.FACCEPTANCETESTCODEID,
  X001cc_Stud_qual_peri.FENROLMENTPRESENTATIONID,
  X001cc_Stud_qual_peri.FQUALPRESENTINGOUID,
  X001cc_Stud_qual_peri.FQUALLEVELAPID,
  X001cc_Stud_qual_peri.FFIELDOFSTUDYAPID,
  X001cc_Stud_qual_peri.FPROGRAMAPID,
  X001cc_Stud_qual_peri.FQUALIFICATIONAPID,
  X000_Student_qual_result_peri.KBUSINESSENTITYID,
  X000_Student_qual_result_peri.KACADEMICPROGRAMID,
  X000_Student_qual_result_peri.KQUALFOSRESULTCODEID,
  X000_Student_qual_result_peri.RESULT,
  X000_Student_qual_result_peri.KRESULTYYYYMM,
  X000_Student_qual_result_peri.KSTUDQUALFOSRESULTID,
  X000_Student_qual_result_peri.FGRADUATIONCEREMONYID AS FGRADUATIONCEREMONYID1,
  X000_Student_qual_result_peri.FPOSTPONEMENTCODEID,
  X000_Student_qual_result_peri.POSTPONE_REAS,
  X000_Student_qual_result_peri.RESULTISSUEDATE,
  X000_Student_qual_result_peri.DISCONTINUEDATE,
  X000_Student_qual_result_peri.FDISCONTINUECODEID,
  X000_Student_qual_result_peri.DISCONTINUE_REAS,
  X000_Student_qual_result_peri.RESULTPASSDATE,
  X000_Student_qual_result_peri.FLANGUAGECODEID,
  X000_Student_qual_result_peri.ISSUESURNAME,
  X000_Student_qual_result_peri.CERTIFICATESEQNUMBER,
  X000_Student_qual_result_peri.AVGMARKACHIEVED,
  X000_Student_qual_result_peri.PROCESSSEQNUMBER,
  X000_Student_qual_result_peri.FRECEIPTID,
  X000_Student_qual_result_peri.FRECEIPTLINEID,
  X000_Student_qual_result_peri.ISINABSENTIA,
  X000_Student_qual_result_peri.FPROGRAMAPID AS FPROGRAMAPID1,
  X000_Student_qual_result_peri.FISSUETYPECODEID,
  X000_Student_qual_result_peri.ISSUE_TYPE,
  X000_Student_qual_result_peri.DATEPRINTED,
  X000_Student_qual_result_peri.LOCKSTAMP,
  X000_Student_qual_result_peri.AUDITDATETIME,
  X000_Student_qual_result_peri.FAUDITSYSTEMFUNCTIONID,
  X000_Student_qual_result_peri.FAUDITUSERCODE,
  X000_Student_qual_result_peri.FAPPROVEDBYCODEID,
  X000_Student_qual_result_peri.FAPPROVEDBYUSERCODE,
  X000_Student_qual_result_peri.DATERESULTAPPROVED,
  X000_Student_qual_result_peri.FENROLMENTPRESENTATIONID AS FENROLMENTPRESENTATIONID1,
  X000_Student_qual_result_peri.CERTDISPATCHDATE,
  X000_Student_qual_result_peri.CERTDISPATCHREFNO,
  X000_Student_qual_result_peri.ISSUEFIRSTNAMES
FROM
  X001cc_Stud_qual_peri
  LEFT JOIN X000_Student_qual_result_peri ON X000_Student_qual_result_peri.KBUSINESSENTITYID =
    X001cc_Stud_qual_peri.KSTUDBUSENTID AND X000_Student_qual_result_peri.FPROGRAMAPID =
    X001cc_Stud_qual_peri.FPROGRAMAPID AND X000_Student_qual_result_peri.KACADEMICPROGRAMID =
    X001cc_Stud_qual_peri.FFIELDOFSTUDYAPID
"""
so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: "+sr_file)

# Build student qualification period 5 or final list **************************
# QUALLEVELENROLSTUD
# PRESENTOUENROLPRESENTCAT
# QUALIFICATIONLEVEL
# FIELD OF STUDY
# QUALIFICATION

print("Build period student qualification step 5...")
sr_file = "X001cx_Stud_qual_peri"
s_sql = "CREATE TABLE "+ sr_file +" AS " + """
SELECT
  X001cd_Stud_qual_peri.KSTUDBUSENTID,
  X001cd_Stud_qual_peri.KENROLSTUDID,
  X001cd_Stud_qual_peri.FORGUNITNUMBER,      
  X001cd_Stud_qual_peri.DATEQUALLEVELSTARTED,
  X001cd_Stud_qual_peri.DATEENROL,
  X001cd_Stud_qual_peri.STARTDATE,
  X001cd_Stud_qual_peri.ENDDATE,
  X001cd_Stud_qual_peri.DISCONTINUEDATE,
  X001cd_Stud_qual_peri.RESULT,      
  X001cd_Stud_qual_peri.QUALIFICATIONCODE,
  X001cd_Stud_qual_peri.QUALIFICATIONFIELDOFSTUDY,
  X001cd_Stud_qual_peri.QUALIFICATIONLEVEL,
  X001cd_Stud_qual_peri.QUAL_TYPE,
  X001cd_Stud_qual_peri.ISHEMISSUBSIDY,
  X001cd_Stud_qual_peri.ISMAINQUALLEVEL,
  X001cd_Stud_qual_peri.ENROLACADEMICYEAR,
  X001cd_Stud_qual_peri.ENROLHISTORYYEAR,
  X001cd_Stud_qual_peri.MIN,
  X001cd_Stud_qual_peri.MIN_UNIT,
  X001cd_Stud_qual_peri.MAX,
  X001cd_Stud_qual_peri.MAX_UNIT,
  X001cd_Stud_qual_peri.FSTUDACTIVECODEID,
  X001cd_Stud_qual_peri.ACTIVE_IND,
  X001cd_Stud_qual_peri.FENTRYLEVELCODEID,
  X001cd_Stud_qual_peri.ENTRY_LEVEL,
  X001cd_Stud_qual_peri.FENROLMENTCATEGORYCODEID,
  X001cd_Stud_qual_peri.ENROL_CAT,
  X001cd_Stud_qual_peri.FPRESENTATIONCATEGORYCODEID,
  X001cd_Stud_qual_peri.PRESENT_CAT,
  X001cd_Stud_qual_peri.FFINALSTATUSCODEID,
  X001cd_Stud_qual_peri.QUAL_LEVEL_STATUS_FINAL,
  X001cd_Stud_qual_peri.FLEVYCATEGORYCODEID,
  X001cd_Stud_qual_peri.QUAL_LEVEL_LEVY_CAT,
  X001cd_Stud_qual_peri.CERT_TYPE,
  X001cd_Stud_qual_peri.LEVY_TYPE,
  X001cd_Stud_qual_peri.FBLACKLISTCODEID,
  X001cd_Stud_qual_peri.BLACKLIST,
  X001cd_Stud_qual_peri.FSELECTIONCODEID,
  X001cd_Stud_qual_peri.FOS_SELECTION,
  X001cd_Stud_qual_peri.DISCONTINUE_REAS,
  X001cd_Stud_qual_peri.POSTPONE_REAS,
  X001cd_Stud_qual_peri.FBUSINESSENTITYID,
  X001cd_Stud_qual_peri.ORGUNIT_TYPE,
  X001cd_Stud_qual_peri.ORGUNIT_NAME,
  X001cd_Stud_qual_peri.FSITEORGUNITNUMBER,
  X001cd_Stud_qual_peri.ISCONDITIONALREG,
  X001cd_Stud_qual_peri.MARKSFINALISEDDATE,
  X001cd_Stud_qual_peri.RESULTPASSDATE,
  X001cd_Stud_qual_peri.RESULTISSUEDATE,
  X001cd_Stud_qual_peri.EXAMSUBMINIMUM,
  X001cd_Stud_qual_peri.ISCUMLAUDE,
  X001cd_Stud_qual_peri.FGRADCERTLANGUAGECODEID,
  X001cd_Stud_qual_peri.ISPOSSIBLEGRADUATE,
  X001cd_Stud_qual_peri.FGRADUATIONCEREMONYID,
  X001cd_Stud_qual_peri.FACCEPTANCETESTCODEID,
  X001cd_Stud_qual_peri.FENROLMENTPRESENTATIONID,
  X001cd_Stud_qual_peri.FQUALPRESENTINGOUID,
  X001cd_Stud_qual_peri.FQUALLEVELAPID,
  X001cd_Stud_qual_peri.FFIELDOFSTUDYAPID,
  X001cd_Stud_qual_peri.FPROGRAMAPID,
  X001cd_Stud_qual_peri.FQUALIFICATIONAPID,
  PROGRAM.FFIELDOFSTUDYAPID AS FFIELDOFSTUDYAPID1,
  PROGRAM.PROGRAMCODE
FROM
  X001cd_Stud_qual_peri
  LEFT JOIN PROGRAM ON PROGRAM.KACADEMICPROGRAMID = X001cd_Stud_qual_peri.FPROGRAMAPID
ORDER BY
  X001cd_Stud_qual_peri.KSTUDBUSENTID,
  X001cd_Stud_qual_peri.DATEENROL
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)

# Export the data
print("Export period list of students...")
sr_filet = sr_file
sx_path = re_path + funcdate.cur_year() + "/"                
sx_file = "Student_001_all_"
sx_filet = sx_file + d_from + "_" + d_toto
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
#funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)    
funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("-------------------------")
funcfile.writelog("COMPLETED: B003_VSS_LISTS")

