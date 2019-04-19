""" Script to build kfs creditor payment tests *********************************
Created on: 16 Apr 2019
Author: Albert J v Rensburg (NWU21162395)
*****************************************************************************"""

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
END OF SCRIPT
*****************************************************************************"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# IMPORT PYTHON MODULES
import csv
import datetime
import sqlite3
import sys

# ADD OWN MODULE PATH
sys.path.append('S:/_my_modules')

# IMPORT OWN MODULES
import funcfile
import funccsv
import funcdate
#import funcmail
#import funcmysql
#import funcpeople
#import funcstr
import funcsys

# OPEN THE SCRIPT LOG FILE
print("----------------------")    
print("C200_CREDITOR_TEST_DEV")
print("----------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C200_CREDITOR_TEST_DEV")
funcfile.writelog("------------------------------")
ilog_severity = 1

# DECLARE VARIABLES
so_path = "W:/Kfs/" #Source database path
re_path = "R:/Kfs/" # Results path
ed_path = "S:/_external_data/" #external data path
so_file = "Kfs_test_creditor.sqlite" # Source database
s_sql = "" # SQL statements
l_export = False
l_mail = False
l_record = False

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

""" ****************************************************************************
TEST CREDITOR DUPLICATE PAYMENT
*****************************************************************************"""
print("TEST CREDITOR DUPLICATE PAYMENT")
funcfile.writelog("TEST CREDITOR DUPLICATE PAYMENT")


so_curs.execute("DROP TABLE IF EXISTS X001_payment_prev")
so_curs.execute("DROP TABLE IF EXISTS X001aa_dupl")

# DECLARE TEST VARIABLES
l_record = False # Record the findings in the previous reported findings file
i_find = 0 # Number of findings before previous reported findings
i_coun = 0 # Number of new findings to report

# BUILD CREDITOR PAYMENTS MASTER FILES - LAST DAY
print("Build creditor payment master tables...")
sr_file = "X001_payment_totest"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    PAYMENT.VENDOR_ID,
    PAYMENT.CUST_PMT_DOC_NBR,
    PAYMENT.INV_NBR,
    PAYMENT.INV_DT,
    PAYMENT.PMT_DT,
    PAYMENT.NET_PMT_AMT,
    Cast(Trim(PAYMENT.INV_NBR,' -/ABCDEFGHIJKLMNOPQRSTUVWXYZ') As INT) As INV_CALC
From
    KFS.X001aa_Report_payments_curr PAYMENT
Where
    StrfTime('%Y-%m-%d',PAYMENT.PMT_DT) >= StrfTime('%Y-%m-%d','now','%DAYS%')
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if funcdate.today_dayname() == "Mon":
    s_sql = s_sql.replace('%DAYS%','-3 day')
else:
    s_sql = s_sql.replace('%DAYS%','-1 day')
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD CREDITOR PAYMENTS MASTER FILES - PREVIOUS YEAR PAYMENTS
print("Build previous year payments...")
sr_file = "X001_payments"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    PAYMENT.VENDOR_ID,
    PAYMENT.CUST_PMT_DOC_NBR,
    PAYMENT.INV_NBR,
    PAYMENT.INV_DT,
    PAYMENT.PMT_DT,    
    PAYMENT.NET_PMT_AMT,
    Cast(Trim(Replace(Replace(Replace(Replace(INV_NBR,'*',''),'/',''),'-',''),' ',''),'ABCDEFGHIJKLMNOPQRSTUVWXYZ') As INT) As INV_CALC
From
    KFS.X001aa_Report_payments_prev PAYMENT
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# ADD CURRENT YEAR PAYMENTS
print("Add current year payments...")
s_sql = "INSERT INTO X001_payments " + """
Select
    VENDOR_ID,
    CUST_PMT_DOC_NBR,
    INV_NBR,
    INV_DT,
    PMT_DT,    
    NET_PMT_AMT,
    Cast(Trim(Replace(Replace(Replace(Replace(INV_NBR,'*',''),'/',''),'-',''),' ',''),'ABCDEFGHIJKLMNOPQRSTUVWXYZ') As INT)
FROM
    KFS.X001aa_Report_payments_curr
"""
"""

WHERE
    StrfTime('%Y-%m-%d',PMT_DT) < StrfTime('%Y-%m-%d','now','%DAYS%')

if funcdate.today_dayname() in "Mon":
    s_sql = s_sql.replace('%DAYS%','-3 day')
else:
    s_sql = s_sql.replace('%DAYS%','-1 day')
"""    
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IDENTIFY DUPLICATES
print("Identify possible duplicates...")
sr_file = "X001aa_paym_dupl"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    TEST.VENDOR_ID As VENDOR,
    TEST.CUST_PMT_DOC_NBR As EDOC,
    TEST.INV_NBR As INVOICE,
    TEST.INV_DT As INVOICE_DATE,
    TEST.PMT_DT As PAYMENT_DATE,
    TEST.NET_PMT_AMT As AMOUNT,
    TEST.INV_CALC As CALC,
    BASE.CUST_PMT_DOC_NBR As DUP_EDOC,
    BASE.INV_NBR As DUP_INVOICE,
    BASE.INV_DT As DUP_INVOICE_DATE,
    BASE.PMT_DT As DUP_PAYMENT_DATE,
    BASE.NET_PMT_AMT As DUP_AMOUNT,
    BASE.INV_CALC As DUP_CALC
From
    X001_payment_totest TEST Inner Join
    X001_payments BASE On BASE.CUST_PMT_DOC_NBR <> TEST.CUST_PMT_DOC_NBR And
        BASE.VENDOR_ID = TEST.VENDOR_ID And
        BASE.INV_DT = TEST.INV_DT And
        BASE.NET_PMT_AMT = TEST.NET_PMT_AMT And
        BASE.INV_CALC = TEST.INV_CALC
"""
"""
BASE.CUST_PMT_DOC_NBR <> TEST.CUST_PMT_DOC_NBR And
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IDENTIFY DUPLICATES
print("Identify possible duplicates...")
sr_file = "X001ab_paym_dupl"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    FIND.*
From
    X001aa_paym_dupl FIND
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# COUNT THE NUMBER OF FINDINGS
i_find = funcsys.tablerowcount(so_curs,sr_file)
print("*** Found "+str(i_find)+" exceptions ***")
funcfile.writelog("%t FINDING: "+str(i_find)+" PAYMENT duplicate finding(s)")

# GET PREVIOUS FINDINGS
sr_file = "X001ac_paym_getprev"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0:
    print("Import previously reported findings...")
    so_curs.execute("CREATE TABLE " + sr_file + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT,DATE_MAILED TEXT)")
    s_cols = ""
    co = open(ed_path + "201_reported.txt", "r")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "PROCESS":
            continue
        elif row[0] != "paym_dupl_1":
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "','" + row[8] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the impoted data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "201_reported.txt (" + sr_file + ")")

# ADD PREVIOUS FINDINGS
sr_file = "X001ad_paym_addprev"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0:
    print("Join previously reported to current findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
      FIND.*,
      'paym_dupl_1' AS PROCESS,
      '%TODAY%' AS DATE_REPORTED,
      '%TODAYPLUS%' AS DATE_RETEST,
      PREV.PROCESS AS PREV_PROCESS,
      PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
      PREV.DATE_RETEST AS PREV_DATE_RETEST,
      PREV.DATE_MAILED
    FROM
      X001ab_paym_dupl FIND
      LEFT JOIN X001ac_paym_getprev PREV ON PREV.FIELD1 = FIND.EDOC AND
          PREV.FIELD2 = FIND.DUP_EDOC AND
          PREV.DATE_RETEST >= Date('%TODAY%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%",funcdate.today())
    s_sql = s_sql.replace("%TODAYPLUS%",funcdate.today_plusdays(10))
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD LIST TO UPDATE FINDINGS
sr_file = "X001ae_paym_newprev"
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
if i_find > 0:
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      FIND.PROCESS,
      FIND.EDOC AS FIELD1,
      FIND.DUP_EDOC AS FIELD2,
      '' AS FIELD3,
      '' AS FIELD4,
      '' AS FIELD5,
      FIND.DATE_REPORTED,
      FIND.DATE_RETEST,
      FIND.DATE_MAILED
    FROM
      X001ad_paym_addprev FIND
    WHERE
      FIND.PREV_PROCESS IS NULL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
    # Export findings to previous reported file
    i_coun = funcsys.tablerowcount(so_curs,sr_file)
    if i_coun > 0:
        print("*** " +str(i_coun)+ " Finding(s) to report ***")    
        sr_filet = sr_file
        sx_path = ed_path
        sx_file = "201_reported"
        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        # Write the data
        if l_record == True:
            funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
            funcfile.writelog("%t FINDING: "+str(i_coun)+" new finding(s) to export")        
            funcfile.writelog("%t EXPORT DATA: "+sr_file)
    else:
        print("*** No new findings to report ***")
        funcfile.writelog("%t FINDING: No new findings to export")



""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("---------------------------------")
funcfile.writelog("COMPLETED: C200_CREDITOR_TEST_DEV")
