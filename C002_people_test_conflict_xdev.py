"""
Script to test PEOPLE conflict of interest
Created on: 8 Apr 2019
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3
import sys

# IMPORT OWN MODULES
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcsms
from _my_modules import funcsys
from _my_modules import functest

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
END OF SCRIPT
*****************************************************************************"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# DECLARE VARIABLES
so_path = "W:/People_conflict/"  # Source database path
re_path = "R:/People/" + funcdate.cur_year() + "/"  # Results path
ed_path = "S:/_external_data/"  # external data path
so_file = "People_conflict.sqlite"  # Source database
s_sql = ""  # SQL statements
l_export = False
l_mess = False
l_mail = False
l_record = False

# OPEN THE SCRIPT LOG FILE
print("-----------------------------")
print("C002_PEOPLE_TEST_CONFLICT_DEV")
print("-----------------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C002_PEOPLE_TEST_CONFLICT_DEV")
funcfile.writelog("-------------------------------------")
ilog_severity = 1

if l_mess:
    funcsms.send_telegram('', 'administrator', 'Testing employee <b>conflict of interest</b>.')

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")

"""*****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

"""*****************************************************************************
TEST EMPLOYEE VENDOR SHARE EMAIL ADDRESS
*****************************************************************************"""
print("TEST EMPLOYEE VENDOR SHARE EMAIL ADDRESS")
funcfile.writelog("TEST EMPLOYEE VENDOR SHARE EMAIL ADDRESS")

# FILES NEEDED
# X020bx_Student_master_sort

# DECLARE TEST VARIABLES
i_finding_before: int = 0
i_finding_after: int = 0
s_description = "Employee and vendor <b>share an email address.</b>"
s_file_name: str = "employee_vendor_share_email"
s_file_prefix: str = "X100b"
s_finding: str = "EMPLOYEE VENDOR SHARE EMAIL ADDRESS"
s_report_file: str = "002_reported.txt"

# OBTAIN TEST DATA
print("Obtain test data...")
sr_file: str = s_file_prefix + "a_" + s_file_name
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    VEND.VENDOR_ID,
    VEND.VNDR_NM As VENDOR_NAME,
    VEND.VNDR_TYP_CD As VENDOR_TYPE,
    SUMM.MAX_PMT_DT As LAST_PAYMENT_DATE,
    Cast(SUMM.SUM_NET_PMT_AMT As REAL) As PAYMENT_VALUE,
    Cast(SUMM.COUNT_TRAN As INT) As PAYMENT_TRANSACTION_COUNT,
    Lower(VEND.VEND_MAIL) As VENDOR_MAIL,
    Lower(VEND.EMAIL) As VENDOR_MAIL2,
    Lower(VEND.EMAIL_CONTACT) As VENDOR_MAIL_CONTACT
From
    KFS.X000_Vendor VEND Left Join
    KFSCURR.X002aa_Report_payments_summary SUMM On SUMM.VENDOR_ID = VEND.VENDOR_ID Left Join
    PEOPLE.X002_PEOPLE_CURR PEOP On PEOP.EMPLOYEE_NUMBER = Substr(VEND.VENDOR_ID,1,8)
Where
    Lower(VEND.VEND_MAIL) Like ('%nwu.ac.za%') And
    VEND.DOBJ_MAINT_CD_ACTV_IND = 'Y' And
    Substr(VEND.VENDOR_ID,1,8) != Substr(VEND.VEND_MAIL,1,8) And
    PEOP.EMPLOYEE_NUMBER Is Null And
    SUMM.VENDOR_ID Is Not Null
;"""
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)
if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
    print("Export findings...")
    sx_path = re_path
    sx_file = s_file_prefix + "_" + s_finding.lower() + "_list_"
    sx_file_dated = sx_file + funcdate.today_file()
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
    funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
    # funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
    funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# SELECT TEST DATA
print("Identify findings...")
sr_file = s_file_prefix + "b_finding"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    'NWU' As ORG,
    FIND.VENDOR_ID,
    FIND.VENDOR_TYPE,
    FIND.VENDOR_MAIL
From
    %FILEP%%FILEN% FIND
Order by
    VENDOR_ID
;"""
s_sql = s_sql.replace("%FILEP%", s_file_prefix)
s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# COUNT THE NUMBER OF FINDINGS
i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
print("*** Found " + str(i_finding_before) + " exceptions ***")
funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")

# GET PREVIOUS FINDINGS
if i_finding_before > 0:
    i = functest.get_previous_finding(so_curs, ed_path, s_report_file, s_finding, "TTTTT")
    so_conn.commit()

# SET PREVIOUS FINDINGS
if i_finding_before > 0:
    i = functest.set_previous_finding(so_curs)
    so_conn.commit()

# ADD PREVIOUS FINDINGS
sr_file = s_file_prefix + "d_addprev"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    print("Join previously reported to current findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        FIND.*,
        Lower('%FINDING%') AS PROCESS,
        '%TODAY%' AS DATE_REPORTED,
        '%DAYS%' AS DATE_RETEST,
        PREV.PROCESS AS PREV_PROCESS,
        PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
        PREV.DATE_RETEST AS PREV_DATE_RETEST,
        PREV.REMARK
    From
        %FILEP%b_finding FIND Left Join
        Z001ab_setprev PREV ON PREV.FIELD1 = FIND.VENDOR_ID And
            PREV.FIELD2 = FIND.VENDOR_MAIL
    ;"""
    s_sql = s_sql.replace("%FINDING%", s_finding)
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%TODAY%", funcdate.today())
    s_sql = s_sql.replace("%DAYS%", funcdate.cur_monthendnext())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD LIST TO UPDATE FINDINGS
sr_file = s_file_prefix + "e_newprev"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.PROCESS,
        PREV.VENDOR_ID AS FIELD1,
        PREV.VENDOR_MAIL AS FIELD2,
        '' AS FIELD3,
        '' AS FIELD4,
        '' AS FIELD5,
        PREV.DATE_REPORTED,
        PREV.DATE_RETEST,
        PREV.REMARK
    From
        %FILEP%d_addprev PREV
    Where
        PREV.PREV_PROCESS Is Null Or
        PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""        
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export findings to previous reported file
    i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
    if i_finding_after > 0:
        print("*** " + str(i_finding_after) + " Finding(s) to report ***")
        sx_path = ed_path
        sx_file = s_report_file[:-4]
        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        # Write the data
        if l_record:
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
            funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
            funcfile.writelog("%t EXPORT DATA: " + sr_file)
        if l_mess:
            funcsms.send_telegram('', 'administrator', '<b>' + str(i_finding_after) + '/' + str(i_finding_before) + '</b> ' + s_description)
    else:
        print("*** No new findings to report ***")
        funcfile.writelog("%t FINDING: No new findings to export")

# IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
if i_finding_before > 0 and i_finding_after > 0:
    i = functest.get_officer(so_curs, "HR", "TEST " + s_finding + " OFFICER")
    so_conn.commit()

# IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
if i_finding_before > 0 and i_finding_after > 0:
    i = functest.get_supervisor(so_curs, "HR", "TEST " + s_finding + " SUPERVISOR")
    so_conn.commit()

# ADD CONTACT DETAILS TO FINDINGS
sr_file = s_file_prefix + "h_detail"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0 and i_finding_after > 0:
    print("Add contact details to findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.ORG,
        PREV.VENDOR_ID,
        FIND.VENDOR_NAME,
        PREV.VENDOR_TYPE,
        PREV.VENDOR_MAIL,
        CAMP_OFF.EMPLOYEE_NUMBER AS CAMP_OFF_NUMB,
        CAMP_OFF.NAME_ADDR AS CAMP_OFF_NAME,
        CASE
            WHEN  CAMP_OFF.EMPLOYEE_NUMBER != '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
            ELSE CAMP_OFF.EMAIL_ADDRESS
        END AS CAMP_OFF_MAIL,
        CAMP_OFF.EMAIL_ADDRESS AS CAMP_OFF_MAIL2,        
        CAMP_SUP.EMPLOYEE_NUMBER AS CAMP_SUP_NUMB,
        CAMP_SUP.NAME_ADDR AS CAMP_SUP_NAME,
        CASE
            WHEN CAMP_SUP.EMPLOYEE_NUMBER != '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
            ELSE CAMP_SUP.EMAIL_ADDRESS
        END AS CAMP_SUP_MAIL,
        CAMP_SUP.EMAIL_ADDRESS AS CAMP_SUP_MAIL2,
        ORG_OFF.EMPLOYEE_NUMBER AS ORG_OFF_NUMB,
        ORG_OFF.NAME_ADDR AS ORG_OFF_NAME,
        CASE
            WHEN ORG_OFF.EMPLOYEE_NUMBER != '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
            ELSE ORG_OFF.EMAIL_ADDRESS
        END AS ORG_OFF_MAIL,
        ORG_OFF.EMAIL_ADDRESS AS ORG_OFF_MAIL2,
        ORG_SUP.EMPLOYEE_NUMBER AS ORG_SUP_NUMB,
        ORG_SUP.NAME_ADDR AS ORG_SUP_NAME,
        CASE
            WHEN ORG_SUP.EMPLOYEE_NUMBER != '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
            ELSE ORG_SUP.EMAIL_ADDRESS
        END AS ORG_SUP_MAIL,
        ORG_SUP.EMAIL_ADDRESS AS ORG_SUP_MAIL2,
        AUD_OFF.EMPLOYEE_NUMBER As AUD_OFF_NUMB,
        AUD_OFF.NAME_ADDR As AUD_OFF_NAME,
        AUD_OFF.EMAIL_ADDRESS As AUD_OFF_MAIL,
        AUD_SUP.EMPLOYEE_NUMBER As AUD_SUP_NUMB,
        AUD_SUP.NAME_ADDR As AUD_SUP_NAME,
        AUD_SUP.EMAIL_ADDRESS As AUD_SUP_MAIL
    From
        %FILEP%d_addprev PREV Left Join
        Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.VENDOR_TYPE Left Join
        Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
        Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD' Left Join
        Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.VENDOR_TYPE Left Join
        Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
        Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD' Left Join
        %FILEP%%FILEN% FIND On FIND.VENDOR_ID = PREV.VENDOR_ID
    Where
        PREV.PREV_PROCESS Is Null Or
        PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD THE FINAL TABLE FOR EXPORT AND REPORT
sr_file = s_file_prefix + "x_" + s_file_name
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
print("Build the final report")
if i_finding_before > 0 and i_finding_after > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        '%FIND%' As Audit_finding,
        FIND.ORG As Organization,
        FIND.VENDOR_ID As Vendor_id,
        FIND.VENDOR_NAME As Vendor_name,
        FIND.VENDOR_TYPE As Vendor_type,
        FIND.VENDOR_MAIL Responsible_Mail,
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
        FIND.AUD_OFF_NAME AS Audit_Officer,
        FIND.AUD_OFF_NUMB AS Audit_Officer_Numb,
        FIND.AUD_OFF_MAIL AS Audit_Officer_Mail,
        FIND.AUD_SUP_NAME AS Audit_Supervisor,
        FIND.AUD_SUP_NUMB AS Audit_Supervisor_Numb,
        FIND.AUD_SUP_MAIL AS Audit_Supervisor_Mail
    From
        %FILEP%h_detail FIND
    ;"""
    s_sql = s_sql.replace("%FIND%", s_finding)
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export findings
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        print("Export findings...")
        sx_path = re_path
        sx_file = s_file_prefix + "_" + s_finding.lower() + "_"
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
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C002_PEOPLE_TEST_CONFLICT_DEV")
