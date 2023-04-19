""" Script to build kfs creditor payment tests *********************************
Created on: 16 Apr 2019
Author: Albert J v Rensburg (NWU21162395)
*****************************************************************************"""

# IMPORT PYTHON MODULES
import csv
import sqlite3
import datetime

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcfile
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcsms
from _my_modules import funcstat
from _my_modules import funcsys
from _my_modules import functest

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# DECLARE VARIABLES
l_debug: bool = True
so_path: str = "W:/Kfs/"  # Source database path
so_file: str = "Kfs_test_creditor.sqlite"  # Source database
re_path: str = "R:/Kfs/"  # Results path
ed_path: str = "S:/_external_data/"  # External data path
l_export: bool = True
# l_mail: bool = funcconf.l_mail_project
l_mail: bool = False
# l_mess: bool = funcconf.l_mess_project
l_mess: bool = False
l_record: bool = False

# OPEN THE SCRIPT LOG FILE
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C201_CREDITOR_TEST_DEV")
funcfile.writelog("------------------------------")
if l_debug:
    print("----------------------")
    print("C201_CREDITOR_TEST_DEV")
    print("----------------------")

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
funcfile.writelog("OPEN THE DATABASES")
if l_debug:
    print("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs_prev.sqlite' AS 'KFSPREV'")
funcfile.writelog("%t ATTACH DATABASE: KFS_PREV.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
funcfile.writelog("BEGIN OF SCRIPT")
if l_debug:
    print("BEGIN OF SCRIPT")

"""*****************************************************************************
TEST VENDOR QUOTE 250K SPLIT PAYMENTS
*****************************************************************************"""
funcfile.writelog("TEST VENDOR QUOTE 250K SPLIT PAYMENTS")
if l_debug:
    print("TEST VENDOR QUOTE SPLIT PAYMENTS")

# FILES NEEDED

# DECLARE TEST VARIABLES
s_days: str = '14'  # Test days between payments - Note - Not in all tests. Remove in other tests
s_limit: str = '250000'  # Test ceiling limit - Note - Not in all tests. Remove in other tests
i_finding_after: int = 0
s_description = "Vendor quote 250k split payment"
s_file_prefix: str = "X001f"
s_file_name: str = "vendor_quote_250k_split_payment"
s_finding: str = "VENDOR QUOTE 250K SPLIT PAYMENT"
s_report_file: str = "201_reported.txt"

# IDENTIFY AND SUMMARIZE QUOTE PAYMENTS
if l_debug:
    print("Identify quote payments...")
sr_file: str = s_file_prefix + "a_a_" + s_file_name
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "Create Table " + sr_file + " As " + """
Select
    a.ACC_COST_STRING,
    a.ORG_NM,
    a.FIN_OBJ_CD_NM,
    a.INIT_EMP_NO,
    a.INIT_EMP_NAME,
    a.VENDOR_ID,
    a.PAYEE_NAME,
    v.VNDR_TYP_CD,
    a.VENDOR_TYPE,
    a.DOC_TYPE,    
    a.PMT_DT,
    a.EDOC,
    Cast(Total(a.ACC_AMOUNT) As Real) As TOT_AMOUNT,
    oe.LOOKUP_DESCRIPTION As EXCLUDE_OBJECT,
    ve.LOOKUP_DESCRIPTION As EXCLUDE_VENDOR    
From
    KFSCURR.X001ad_Report_payments_accroute a Left Join
    KFS.X000_Vendor v on v.vendor_id = a.vendor_id Left Join
    KFS.X000_Own_kfs_lookups oe On oe.LOOKUP = '%OBJECT%' And
        oe.LOOKUP_CODE = Substr(a.ACC_COST_STRING, -4) Left Join
    KFS.X000_Own_kfs_lookups ve On ve.LOOKUP = '%VENDOR%' And
        ve.LOOKUP_CODE = a.VENDOR_ID
Where
    a.PAYEE_TYPE = 'V' And
    a.DOC_TYPE in ('DV', 'PDV', 'PREQ') And
    Cast(Substr(a.ACC_COST_STRING, -4) As Int) Between 2051 and 4213 And
    oe.LOOKUP_CODE Is Null And
    ve.LOOKUP_CODE Is Null
Group By
    a.ORG_NM,
    a.FIN_OBJ_CD_NM,    
    a.VENDOR_ID,
    a.PMT_DT,
    a.EDOC
Having
    TOT_AMOUNT > 0 And
    TOT_AMOUNT < %LIMIT%
Order By
    a.ORG_NM,
    a.FIN_OBJ_CD_NM,    
    a.VENDOR_ID,
    a.PMT_DT,
    a.EDOC
;"""
s_sql = s_sql.replace("%OBJECT%", "EXCLUDE OBJECT " + s_finding)
s_sql = s_sql.replace("%VENDOR%", "EXCLUDE VENDOR " + s_finding)
s_sql = s_sql.replace("%LIMIT%", s_limit)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
if l_debug:
    so_conn.commit()

# GROUP PAYMENTS BY DATE
if l_debug:
    print("Group payments by date...")
sr_file: str = s_file_prefix + "a_b_" + s_file_name
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "Create Table " + sr_file + " As " + """
Select
    a.ORG_NM,
    a.FIN_OBJ_CD_NM,
    a.VENDOR_ID,
    a.PMT_DT,
    Min(a.EDOC) As EDOC,
    a.DOC_TYPE,
    Cast(Count(a.EDOC) As Int) As TRAN_COUNT,
    Cast(a.TOT_AMOUNT As Real) As TOT_AMOUNT
From
    %FILEP%a_a_%FILEN% a
Group By
    a.ORG_NM,
    a.FIN_OBJ_CD_NM,    
    a.VENDOR_ID,
    a.PMT_DT
;"""
s_sql = s_sql.replace("%FILEP%", s_file_prefix)
s_sql = s_sql.replace("%FILEN%", s_file_name)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
if l_debug:
    so_conn.commit()

# IDENTIFY PAYMENT TRANSACTIONS
if l_debug:
    print("Identify payment transactions...")
sr_file: str = s_file_prefix + "a_" + s_file_name
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "Create Table " + sr_file + " As " + """
Select
    a.ACC_COST_STRING,
    a.ORG_NM,
    a.FIN_OBJ_CD_NM,
    a.VENDOR_ID,
    a.PAYEE_NAME,
    a.VNDR_TYP_CD,        
    a.VENDOR_TYPE,
    a.DOC_TYPE,
    a.INIT_EMP_NO,
    a.INIT_EMP_NAME,    
    a.EDOC As EDOC_A,
    a.DOC_TYPE As DOC_TYPE_A,
    a.PMT_DT As PMT_DATE_A,
    a.TOT_AMOUNT As AMOUNT_PD_A,
    b.EDOC As EDOC_B,
    b.DOC_TYPE As DOC_TYPE_B,
    cast(julianday(b.PMT_DT) - julianday(a.PMT_DT) As int) As DAYS_AFTER,
    b.PMT_DT As PMT_DATE_B,
    b.TOT_AMOUNT As AMOUNT_PD_B,
    b.TRAN_COUNT,
    cast(a.TOT_AMOUNT + b.TOT_AMOUNT As real) As TOTAL_AMOUNT_PD
From
    %FILEP%a_a_%FILEN% a Inner Join
    %FILEP%a_b_%FILEN% b On b.ORG_NM = a.ORG_NM
            And b.FIN_OBJ_CD_NM = a.FIN_OBJ_CD_NM
            And b.VENDOR_ID = a.VENDOR_ID
            And julianday(b.PMT_DT) - julianday(a.PMT_DT) >= 0
            And julianday(b.PMT_DT) - julianday(a.PMT_DT) <= %DAYSBETWEEN%
            And cast(a.TOT_AMOUNT + b.TOT_AMOUNT As real) > %LIMIT%
            And a.EDOC != b.EDOC
Order By
    a.ORG_NM,
    a.VENDOR_ID,
    a.PMT_DT
;"""
s_sql = s_sql.replace("%FILEP%", s_file_prefix)
s_sql = s_sql.replace("%FILEN%", s_file_name)
s_sql = s_sql.replace("%DAYSBETWEEN%", s_days)
s_sql = s_sql.replace("%LIMIT%", s_limit)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
if l_debug:
    so_conn.commit()

# IDENTIFY FINDINGS
if l_debug:
    print("Identify findings...")
sr_file = s_file_prefix + "b_finding"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    'NWU' As ORG,
    FIND.VNDR_TYP_CD As VENDOR_TYPE,
    FIND.ACC_COST_STRING,
    FIND.EDOC_A,
    FIND.AMOUNT_PD_A,
    FIND.EDOC_B,
    FIND.AMOUNT_PD_B
From
    %FILEP%%FILEN% FIND
;"""
s_sql = s_sql.replace("%FILEP%", s_file_prefix)
s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
if l_debug:
    so_conn.commit()

# COUNT THE NUMBER OF FINDINGS
if l_debug:
    print("Count the number of findings...")
i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
funcfile.writelog("%t FINDING: " + str(i_finding_before) + " " + s_finding + " finding(s)")
if l_debug:
    print("*** Found " + str(i_finding_before) + " exceptions ***")

# GET PREVIOUS FINDINGS
if i_finding_before > 0:
    functest.get_previous_finding(so_curs, ed_path, s_report_file, s_finding, "TIRIR")
    if l_debug:
        so_conn.commit()

# SET PREVIOUS FINDINGS
if i_finding_before > 0:
    functest.set_previous_finding(so_curs)
    if l_debug:
        so_conn.commit()

# ADD PREVIOUS FINDINGS
sr_file = s_file_prefix + "d_addprev"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    if l_debug:
        print("Join previously reported to current findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        FIND.*,
        Lower('%FINDING%') AS PROCESS,
        '%TODAY%' AS DATE_REPORTED,
        '%DATETEST%' AS DATE_RETEST,
        PREV.PROCESS AS PREV_PROCESS,
        PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
        PREV.DATE_RETEST AS PREV_DATE_RETEST,
        PREV.REMARK
    From
        %FILEP%b_finding FIND Left Join
        Z001ab_setprev PREV ON PREV.FIELD1 = FIND.ACC_COST_STRING And
            PREV.FIELD2 = FIND.EDOC_A And
            PREV.FIELD3 = FIND.AMOUNT_PD_A And
            PREV.FIELD4 = FIND.EDOC_B And
            PREV.FIELD5 = FIND.AMOUNT_PD_B
    ;"""
    s_sql = s_sql.replace("%FINDING%", s_finding)
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%TODAY%", funcdate.today())
    s_sql = s_sql.replace("%DATETEST%", funcdate.cur_yearend())
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()

# BUILD LIST TO UPDATE FINDINGS
sr_file = s_file_prefix + "e_newprev"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    if l_debug:
        print("Build list to update findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.PROCESS,
        PREV.ACC_COST_STRING AS FIELD1,
        PREV.EDOC_A AS FIELD2,
        PREV.AMOUNT_PD_A AS FIELD3,
        PREV.EDOC_B AS FIELD4,
        AMOUNT_PD_B AS FIELD5,
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
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    if l_debug:
        so_conn.commit()
    # Export findings to previous reported file
    i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
    if i_finding_after > 0:
        if l_debug:
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
            funcsms.send_telegram('', 'administrator', '<b>' + str(i_finding_before) + '/' + str(
                i_finding_after) + '</b> ' + s_description)
    else:
        funcfile.writelog("%t FINDING: No new findings to export")
        if l_debug:
            print("*** No new findings to report ***")

# IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
if i_finding_before > 0 and i_finding_after > 0:
    functest.get_officer(so_curs, "HR", "TEST " + s_finding + " OFFICER")
    if l_debug:
        print("TEST " + s_finding + " OFFICER")
    so_conn.commit()

# IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
if i_finding_before > 0 and i_finding_after > 0:
    functest.get_supervisor(so_curs, "HR", "TEST " + s_finding + " SUPERVISOR")
    if l_debug:
        print("TEST " + s_finding + " SUPERVISOR")
    so_conn.commit()

# ADD CONTACT DETAILS TO FINDINGS
sr_file = s_file_prefix + "h_detail"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0 and i_finding_after > 0:
    if l_debug:
        print("Add contact details to findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.ORG,
        FIND.VENDOR_ID,
        FIND.PAYEE_NAME,
        PREV.VENDOR_TYPE,
        PREV.ACC_COST_STRING,
        FIND.ORG_NM,
        FIND.FIN_OBJ_CD_NM,
        FIND.INIT_EMP_NO,
        FIND.INIT_EMP_NAME,
        PREV.EDOC_A,
        FIND.DOC_TYPE_A,
        FIND.PMT_DATE_A,
        PREV.AMOUNT_PD_A,
        PREV.EDOC_B,
        FIND.DOC_TYPE_B,
        FIND.PMT_DATE_B,
        PREV.AMOUNT_PD_B,
        FIND.DAYS_AFTER,
        FIND.TRAN_COUNT,
        FIND.TOTAL_AMOUNT_PD,
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
        %FILEP%a_%FILEN% FIND on FIND.ACC_COST_STRING = PREV.ACC_COST_STRING And
            FIND.EDOC_A = PREV.EDOC_A And
            FIND.EDOC_B = PREV.EDOC_B Left Join
        Z001af_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.VENDOR_TYPE Left Join
        Z001af_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG Left Join
        Z001af_officer AUD_OFF On AUD_OFF.CAMPUS = 'AUD' Left Join
        Z001ag_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.VENDOR_TYPE Left Join
        Z001ag_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG Left Join
        Z001ag_supervisor AUD_SUP On AUD_SUP.CAMPUS = 'AUD'
    Where
        PREV.PREV_PROCESS Is Null Or
        PREV.DATE_REPORTED > PREV.PREV_DATE_RETEST And PREV.REMARK = ""
    ;"""
    s_sql = s_sql.replace("%FILEP%", s_file_prefix)
    s_sql = s_sql.replace("%FILEN%", s_file_name)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD THE FINAL TABLE FOR EXPORT AND REPORT
sr_file = s_file_prefix + "x_" + s_file_name
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0 and i_finding_after > 0:
    if l_debug:
        print("Build the final report")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        '%FIND%' As Audit_finding,
        FIND.ACC_COST_STRING As Cost_string,
        FIND.ORG_NM As Organization_name,
        FIND.FIN_OBJ_CD_NM As Object_name,
        FIND.VENDOR_ID As Vendor_id,
        FIND.PAYEE_NAME As Vendor_name,
        FIND.VENDOR_TYPE As Vendor_type,        
        FIND.INIT_EMP_NO As Initiator_number,
        FIND.INIT_EMP_NAME As Initiator_name,
        FIND.EDOC_A As Payment1_edoc,
        FIND.DOC_TYPE_A As Payment1_doctype,
        FIND.PMT_DATE_A As Payment1_date,
        FIND.AMOUNT_PD_A As Payment1_amount,
        FIND.DAYS_AFTER As Payment2_days,
        FIND.EDOC_B As Payment2_edoc,
        FIND.DOC_TYPE_B As Payment2_doctype,        
        FIND.PMT_DATE_B As Payment2_date,
        FIND.AMOUNT_PD_B As Payment2_amount,
        FIND.TRAN_COUNT As Tran_count,
        FIND.TOTAL_AMOUNT_PD As Total_paid,
        FIND.ORG As Organization,
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
        if l_debug:
            print("Export findings...")
        sx_path = re_path + funcdate.cur_year() + "/"
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
PAYEE FISCAL OFFICER SAME
*****************************************************************************"""
funcfile.writelog("PAYEE FISCAL OFFICER SAME")
if l_debug:
    print("PAYEE FISCAL OFFICER SAME")

# DECLARE TEST VARIABLES
i_finding_after: int = 0
s_description = "Payee fiscal officer same person"
s_file_prefix: str = "X001f"
s_file_name: str = "payee_fiscal_officer_same"
s_finding: str = "PAYEE FISCAL OFFICER SAME"
s_report_file: str = "201_reported.txt"

# IDENTIFY PAYMENTS WHERE THE PAYEE IS ALSO THE FISCAL OFFICER
if l_debug:
    print("Identify payment transactions...")
sr_file: str = s_file_prefix + "a_" + s_file_name
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "Create Table " + sr_file + " As " + """
Select
    LIST.*
From
    KFSCURR.X001ad_Report_payments_accroute LIST
Where
    LIST.VENDOR_ID = LIST.ACCT_FSC_OFC_UID
Order By
    VENDOR_ID,
    PMT_DT    
;"""
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
if l_debug:
    so_conn.commit()

# IDENTIFY FINDINGS
if l_debug:
    print("Identify findings...")
sr_file = s_file_prefix + "b_finding"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    'NWU' As ORG,
    FIND.VENDOR_ID,
    FIND.PAYEE_NAME,
    FIND.TRAN_COUNT,
    FIND.AMOUNT_TOTAL,
    FIND.TRAN_VALUE
From
    %FILEP%%FILEN% FIND
;"""
s_sql = s_sql.replace("%FILEP%", s_file_prefix)
s_sql = s_sql.replace("%FILEN%", "a_" + s_file_name)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
if l_debug:
    so_conn.commit()

"""*****************************************************************************
END OF SCRIPT
*****************************************************************************"""
funcfile.writelog("END OF SCRIPT")
if l_debug:
    print("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("---------------------------------")
funcfile.writelog("COMPLETED: C201_CREDITOR_TEST_DEV")
