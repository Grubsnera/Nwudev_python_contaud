""" Script to extract STUDENT DEFERMENTS FOR THE CURRENT AND PREVIOUS YEAR *****
Created on: 19 MAR 2018
Author: Albert J v Rensburg (NWU21162395)
*****************************************************************************"""

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
BUILD DEFERMENTS
OBTAIN STUDENTS
OBTAIN STUDENT TRANSACTIONS AND CALCULATE BALANCES
ADD BALANCES TO STUDENTS
CALCULATE DEFERMENT STATUS
END OF SCRIPT
*****************************************************************************"""

""" DEPENDANCIES ***************************************************************
CODEDESCRIPTION table (Vss.sqlite)
ACCDEFERMENT table (Vss_deferment.sqlite)
STUDACC (Vss.sqlite)
SYSTEMUSER (Vss.sqlite)
STUDENTSITE (Vss.sqlite)
**************************************************************************** """

""" SCRIPT DESCRIPTION (and reulting table/view) *******************************
Can be run at any time depending on update status of dependancies listed
01 Build the current year deferment list (X000_DEFERMENTS)
02 Build the previous year deferment list (X001_deferments_prev)
03 Build the current year deferment list (X001_deferments_curr)
04 OBTAIN THE LIST OF CURRENT REGISTERED STUDENTS (X002_Students_curr)
05 OBTAIN A LIST OF CURRENT YEAR OPENING BALANCES (X003_TRAN_BALOPEN_CURR)
06 OBTAIN A LIST OF CURRENT YEAR REGISTRATION FEES (X003_TRAN_FEEREG_CURR)
07 ADD OPEN BALANCE TO REGISTERED STUDENTS (X001aa_Students)
**************************************************************************** """

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# IMPORT PYTHON MODULES
import sys

# ADD OWN MODULE PATH
sys.path.append('S:/_my_modules')

# IMPORT PYTHON OBJECTS
import datetime
import sqlite3

# IMPORT OWN MODULES
import funcdate
import funccsv
import funcfile

# SCRIPT LOG FILE
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: REPORT_VSS_DEFERMENTS")
funcfile.writelog("-----------------------------")
print("---------------------")    
print("REPORT_VSS_DEFERMENTS")
print("---------------------")
ilog_severity = 1

# DECLARE VARIABLES
so_path = "W:/Vss_deferment/" #Source database path
re_path = "R:/Vss/"
so_file = "Vss_deferment.sqlite" #Source database
s_sql = "" #SQL statements
l_export = False
l_mail = False
l_record = False
l_vacuum = True

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN SQLITE SOURCE table
print("Open sqlite database...")
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN SQLITE DATABASE: VSS_DEFERMENT.SQLITE")

# OPEN WEB MYSQL DESTINATION table
"""
print("Open web mysql database...")      
s_database = "Web_ia_nwu"
ms_cnxn = funcmysql.mysql_open(s_database)
ms_curs = ms_cnxn.cursor()
funcfile.writelog("%t OPEN MYSQL DATABASE: " + s_database)
"""

# ATTACH VSS DATABASE
print("Attach vss database...")
so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")

# ATTACH VSS STUDDEB DATABASE
print("Attach vss student debtors database...")      
so_curs.execute("ATTACH DATABASE 'W:/Kfs_vss_studdeb/Kfs_vss_studdeb.sqlite' AS 'TRAN'")
funcfile.writelog("%t ATTACH DATABASE: Kfs_vss_studdeb.sqlite")

""" ****************************************************************************
TEMPORARY AREA
*****************************************************************************"""
print("TEMPORARY AREA")
funcfile.writelog("TEMPORARY AREA")

so_curs.execute("DROP TABLE IF EXISTS X001_DEFERMENTS_CURR")
so_curs.execute("DROP TABLE IF EXISTS X001_DEFERMENTS_PREV")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")      

""" ****************************************************************************
BUILD DEFERMENTS
*****************************************************************************"""
print("BUILD DEFERMENTS")
funcfile.writelog("BUILD DEFERMENTS")      

# 01 BUILD THE DEFERMENT LIST **************************************************
# Extract the current deferments and add lookups
# All deferments starting on 1 Jan of current year
# Join subaccount description language 3 english
# Join deferment type language 3 english
# Join deferment reason  language 3 english
# Join the system user employee number
print("Build deferments...")
sr_file = "X000_Deferments"
s_sql = "CREATE TABLE " + sr_file + " AS" + """
SELECT
  DEFER.KACCDEFERMENTID,
  DEFER.FACCID,
  STUDACC.FBUSENTID,
  DEFER.DATEARRANGED,
  USER.FUSERBUSINESSENTITYID,
  DEFER.STARTDATE,
  DEFER.ENDDATE,
  DEFER.TOTALAMOUNT,
  SUBACC.CODESHORTDESCRIPTION AS SUBACCOUNTTYPE,
  TYPE.CODESHORTDESCRIPTION AS DEFERMENTTYPE,
  REAS.CODESHORTDESCRIPTION AS DEFERMENTREASON,
  DEFER.NOTE,
  DEFER.FAUDITUSERCODE,
  DEFER.AUDITDATETIME,
  DEFER.FAUDITSYSTEMFUNCTIONID
FROM
  ACCDEFERMENT DEFER
  LEFT JOIN VSS.CODEDESCRIPTION SUBACC ON SUBACC.KCODEDESCID = DEFER.FSUBACCTYPECODEID
  LEFT JOIN VSS.CODEDESCRIPTION TYPE ON TYPE.KCODEDESCID = DEFER.FDEFERMENTTYPECODEID
  LEFT JOIN VSS.CODEDESCRIPTION REAS ON REAS.KCODEDESCID = DEFER.FDEFERMENTREASONCODEID
  LEFT JOIN VSS.STUDACC STUDACC ON STUDACC.KACCID = DEFER.FACCID
  LEFT JOIN VSS.SYSTEMUSER USER ON USER.KUSERCODE = DEFER.FAUDITUSERCODE
WHERE
  SUBACC.KSYSTEMLANGUAGECODEID = 3 AND
  TYPE.KSYSTEMLANGUAGECODEID = 3 AND
  REAS.KSYSTEMLANGUAGECODEID = 3
ORDER BY
  STUDACC.FBUSENTID,
  DEFER.AUDITDATETIME
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# 02 BUILD THE PREVIOUS YEAR DEFERMENT LIST ************************************
print("Build the previous deferments...")
sr_file = "X000_Deferments_prev"
s_sql = "CREATE TABLE " + sr_file + " AS" + """
SELECT
  DEFER.KACCDEFERMENTID,
  DEFER.FBUSENTID AS 'STUDENT',
  SITE.FDEBTCOLLECTIONSITE AS 'CAMPUS',
  DEFER.DATEARRANGED,
  DEFER.FUSERBUSINESSENTITYID AS 'EMPLOYEE',
  DEFER.STARTDATE AS 'DATESTART',
  DEFER.ENDDATE AS 'DATEEND',
  DEFER.TOTALAMOUNT,
  DEFER.SUBACCOUNTTYPE,
  DEFER.DEFERMENTTYPE,
  DEFER.DEFERMENTREASON,
  DEFER.NOTE,
  DEFER.FAUDITUSERCODE,
  DEFER.AUDITDATETIME,
  DEFER.FAUDITSYSTEMFUNCTIONID,
  SITE.FADMISSIONSITE,
  SITE.FMAINQUALSITE
FROM
  X000_DEFERMENTS DEFER
  LEFT JOIN VSS.STUDENTSITE SITE ON SITE.KSTUDENTBUSENTID = DEFER.FBUSENTID
WHERE
  SITE.KSTARTDATETIME <= DEFER.DATEARRANGED AND
  SITE.ENDDATETIME > DEFER.DATEARRANGED AND
  DEFER.STARTDATE >= Date('%PYEARB%') AND
  DEFER.ENDDATE <= Date('%PYEARE%')
"""
s_sql = s_sql.replace("%PYEARB%",funcdate.prev_yearbegin())
s_sql = s_sql.replace("%PYEARE%",funcdate.prev_yearend())
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
# Export the declaration data
if l_export == True:      
    sr_filet = sr_file
    sx_path = "R:/Debtorstud/" + funcdate.prev_year() + "/"
    sx_file = "Deferment_000_deferment_"
    sx_filet = sx_file + funcdate.today_file() #Today
    print("Export data..." + sx_path + sx_filet)
    # Read the header data
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    # Write the data
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# 03 BUILD THE CURRENT DEFERMENT LIST ******************************************
print("Build the current deferments...")
sr_file = "X000_Deferments_curr"
s_sql = "CREATE TABLE " + sr_file + " AS" + """
SELECT
  DEFER.KACCDEFERMENTID,
  DEFER.FBUSENTID AS 'STUDENT',
  SITE.FDEBTCOLLECTIONSITE AS 'CAMPUS',
  DEFER.DATEARRANGED,
  DEFER.FUSERBUSINESSENTITYID AS 'EMPLOYEE',
  DEFER.STARTDATE AS 'DATESTART',
  DEFER.ENDDATE AS 'DATEEND',
  DEFER.TOTALAMOUNT,
  DEFER.SUBACCOUNTTYPE,
  DEFER.DEFERMENTTYPE,
  DEFER.DEFERMENTREASON,
  DEFER.NOTE,
  DEFER.FAUDITUSERCODE,
  DEFER.AUDITDATETIME,
  DEFER.FAUDITSYSTEMFUNCTIONID,
  SITE.FADMISSIONSITE,
  SITE.FMAINQUALSITE
FROM
  X000_DEFERMENTS DEFER
  LEFT JOIN STUDENTSITE SITE ON SITE.KSTUDENTBUSENTID = DEFER.FBUSENTID
WHERE
  SITE.KSTARTDATETIME <= DEFER.DATEARRANGED AND
  SITE.ENDDATETIME > DEFER.DATEARRANGED AND
  DEFER.STARTDATE >= Date('%CYEARB%') AND
  DEFER.ENDDATE <= Date('%CYEARE%')
"""
s_sql = s_sql.replace("%CYEARB%",funcdate.cur_yearbegin())
s_sql = s_sql.replace("%CYEARE%",funcdate.cur_yearend())
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
# Export the declaration data
if l_export == True:      
    sr_filet = sr_file
    sx_path = "R:/Debtorstud/" + funcdate.cur_year() + "/"
    sx_file = "Deferment_000_deferment_"
    sx_filet = sx_file + funcdate.today_file() #Today
    print("Export data..." + sx_path + sx_filet)
    # Read the header data
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    # Write the data
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
    funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

""" ****************************************************************************
OBTAIN STUDENTS
*****************************************************************************"""
print("OBTAIN STUDENTS")
funcfile.writelog("OBTAIN STUDENTS")      

# 04 OBTAIN THE LIST OF CURRENT REGISTERED STUDENTS ****************************
# Exclude all short courses (QUAL_TYPE Not Like '%Short Course%')
# Only main qualifications (ISMAINQUALLEVEL = 1)
# Only include active students (ACTIVE_IND = 'Active')
print("Obtain the current registered students...")
sr_file = "X000_Students_curr"
s_sql = "CREATE TABLE " + sr_file+ " AS" + """
SELECT
  STUD.*,
  CASE
      WHEN DATEENROL < STARTDATE THEN STARTDATE
      ELSE DATEENROL
  END AS DATEENROL_CALC,
  CASE
      WHEN FENTRYLEVELCODEID = 4407 THEN 0
      WHEN FENTRYLEVELCODEID = 4410 THEN 0
      WHEN FENTRYLEVELCODEID = 4406 THEN 1
      WHEN FENTRYLEVELCODEID = 4409 THEN 1
      ELSE 9
  END AS ENTRY_LEVEL_CALC
FROM
  VSS.X001cx_Stud_qual_curr STUD
WHERE
  UPPER(STUD.QUAL_TYPE) Not Like '%SHORT COURSE%' AND
  STUD.ISMAINQUALLEVEL = 1 AND
  UPPER(STUD.ACTIVE_IND) = 'ACTIVE'
"""
# STUD.PRESENT_CAT = 'Contact' AND
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
"""
2019-02-04
FENTRYLEVELCODEID	ENTRY_LEVEL	Count_KSTUDBUSENTID	
4406	First-time Entering Student Undergraduate (f)	10838
4407	Non-entering Student Undergraduate (n)	29593
4408	Transfer Student Undergraduate (t)	857
4409	First-time Entering Student Postgraduate (f)	2191
4410	Non-entering Student Postgraduate (n)	4283
4411	Transfer Student Postgraduate (t)	303
4412	Embarking On Different Qualification Undergraduate (e)	2148
4413	Embarking On Different Qualification Postgraduate (e)	504

"""

""" ****************************************************************************
OBTAIN STUDENT TRANSACTIONS AND CALCULATE BALANCES
*****************************************************************************"""
print("OBTAIN STUDENT TRANSACTIONS")
funcfile.writelog("OBTAIN STUDENT TRANSACTIONS")      

# OBTAIN A COPY OF VSS TRANSACTIONS TO DATE (CURRENT YEAR STUDENT ACCOUNT ******
print("Import current year student transactions...")
sr_file = "X000_Transaction_curr"
s_sql = "CREATE TABLE " + sr_file+ " AS" + """
SELECT
  TRAN.STUDENT_VSS,
  TRAN.CAMPUS_VSS,
  TRAN.TRANSCODE_VSS,
  TRAN.MONTH_VSS,
  TRAN.TRANSDATE_VSS,
  TRAN.DESCRIPTION_E,
  TRAN.TRANUSER,
  TRAN.AMOUNT_VSS,
  TRAN.AMOUNT_DT,
  TRAN.AMOUNT_CR,
  CASE
    WHEN TRANSCODE_VSS = "001" THEN 1
    WHEN TRANSCODE_VSS = "031" THEN 1
    WHEN TRANSCODE_VSS = "061" THEN 1
    ELSE 0
  END As IS_OPEN_BAL
FROM
  TRAN.X002ab_vss_transort TRAN
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# CALCULATE THE STUDENT ACCOUNT OPENING BALANCE ********************************
print("Calculate the account opening balance...")
sr_file = "X001aa_Trans_balopen"
s_sql = "CREATE VIEW " + sr_file+ " AS" + """
SELECT
  X000_Transaction_curr.STUDENT_VSS,
  CAST(TOTAL(X000_Transaction_curr.AMOUNT_VSS) AS REAL) AS BAL_OPEN
FROM
  X000_Transaction_curr
WHERE
  X000_Transaction_curr.IS_OPEN_BAL = 1
GROUP BY
  X000_Transaction_curr.STUDENT_VSS
"""
so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD VIEW: " + sr_file)

# CALCULATE THE REGISTRATION FEES LEVIED ***************************************
print("Calculate the registration fee transactions...")
sr_file = "X001ab_Trans_feereg"
s_sql = "CREATE VIEW " + sr_file+ " AS" + """
SELECT
  X000_Transaction_curr.STUDENT_VSS,
  CAST(TOTAL(X000_Transaction_curr.AMOUNT_VSS) AS REAL) AS FEE_REG
FROM
  X000_Transaction_curr
WHERE
  X000_Transaction_curr.TRANSCODE_VSS = "002" Or
  X000_Transaction_curr.TRANSCODE_VSS = "095"
GROUP BY
  X000_Transaction_curr.STUDENT_VSS
"""
so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD VIEW: " + sr_file)

# ADD THE REGISTRATION DATE TO THE LIST OF TRANSACTIONS ************************
print("Add the registration date to the list of transactions...")
sr_file = "X001ac_Trans_addreg"
s_sql = "CREATE VIEW " + sr_file+ " AS" + """
SELECT
  X000_Transaction_curr.*,
  X000_Students_curr.DATEENROL_CALC
FROM
  X000_Transaction_curr
  INNER JOIN X000_Students_curr ON X000_Students_curr.KSTUDBUSENTID = X000_Transaction_curr.STUDENT_VSS
"""
so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD VIEW: " + sr_file)

# CALCULATE THE STUDENT ACCOUNT BALANCE ON REGISTRATION DATE *******************
print("Calculate the account balance on registration date...")
sr_file = "X001ad_Trans_balreg"
s_sql = "CREATE VIEW " + sr_file+ " AS" + """
SELECT
  X001ac_Trans_addreg.STUDENT_VSS,
  CAST(TOTAL(X001ac_Trans_addreg.AMOUNT_VSS) AS REAL) AS BAL_REG
FROM
  X001ac_Trans_addreg
WHERE
  X001ac_Trans_addreg.TRANSDATE_VSS <= X001ac_Trans_addreg.DATEENROL_CALC
GROUP BY
  X001ac_Trans_addreg.STUDENT_VSS
"""
so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD VIEW: " + sr_file)

# CALCULATE THE STUDENT ACCOUNT CREDIT TRANSACTIONS BEFORE REGISTRATION *********
print("Calculate the credits after registration date...")
sr_file = "X001ae_Trans_crebefreg"
s_sql = "CREATE VIEW " + sr_file+ " AS" + """
SELECT
  X001ac_Trans_addreg.STUDENT_VSS,
  CAST(TOTAL(X001ac_Trans_addreg.AMOUNT_CR) AS REAL) AS CRE_REG_BEFORE
FROM
  X001ac_Trans_addreg
WHERE
  X001ac_Trans_addreg.IS_OPEN_BAL = 0 AND
  X001ac_Trans_addreg.TRANSDATE_VSS <= X001ac_Trans_addreg.DATEENROL_CALC
GROUP BY
  X001ac_Trans_addreg.STUDENT_VSS
"""
so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
so_curs.execute("DROP VIEW IF EXISTS X001ae_Trans_crereg")
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD VIEW: " + sr_file)

# CALCULATE THE STUDENT ACCOUNT CREDIT TRANSACTIONS AFTER REGISTRATION *********
print("Calculate the credits after registration date...")
sr_file = "X001af_Trans_creaftreg"
s_sql = "CREATE VIEW " + sr_file+ " AS" + """
SELECT
  X001ac_Trans_addreg.STUDENT_VSS,
  CAST(TOTAL(X001ac_Trans_addreg.AMOUNT_CR) AS REAL) AS CRE_REG_AFTER
FROM
  X001ac_Trans_addreg
WHERE
  X001ac_Trans_addreg.IS_OPEN_BAL = 0 AND
  X001ac_Trans_addreg.TRANSDATE_VSS > X001ac_Trans_addreg.DATEENROL_CALC
GROUP BY
  X001ac_Trans_addreg.STUDENT_VSS
"""
so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
so_curs.execute("DROP VIEW IF EXISTS X001ae_Trans_crereg")
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD VIEW: " + sr_file)

# CALCULATE THE STUDENT ACCOUNT BALANCE ****************************************
print("Calculate the account balance...")
sr_file = "X001ag_Trans_balance"
s_sql = "CREATE VIEW " + sr_file+ " AS" + """
SELECT
  X000_Transaction_curr.STUDENT_VSS,
  CAST(TOTAL(X000_Transaction_curr.AMOUNT_VSS) AS REAL) AS BAL_CUR
FROM
  X000_Transaction_curr
GROUP BY
  X000_Transaction_curr.STUDENT_VSS
"""
so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD VIEW: " + sr_file)

# CALCULATE THE DEFERMENT DATE
print("Calculate the deferment date per student...")
sr_file = "X002aa_Defer_date"
s_sql = "CREATE VIEW " + sr_file+ " AS" + """
Select
    DEFER.STUDENT,
    DEFER.DATEEND
From
    X000_Deferments_curr DEFER
Group By
    DEFER.STUDENT
Order By
    DEFER.DATEEND
"""
so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD VIEW: " + sr_file)

# CALCULATE THE STUDENT ACCOUNT CREDIT TRANSACTIONS BEFORE DEFERMENT DATE *********
print("Calculate the credits up to deferment date...")
sr_file = "X002ab_Trans_crebefdef"
s_sql = "CREATE VIEW " + sr_file+ " AS" + """
Select
    TRAN.STUDENT_VSS,
    Sum(TRAN.AMOUNT_VSS) As Sum_AMOUNT_VSS
From
    X000_Transaction_curr TRAN Inner Join
    X002aa_Defer_date DDATE On DDATE.STUDENT = TRAN.STUDENT_VSS
Where
    TRAN.IS_OPEN_BAL = 0 And
    TRAN.AMOUNT_VSS <= 0 And
    TRAN.TRANSDATE_VSS <= DDATE.DATEEND
Group By
    TRAN.STUDENT_VSS
"""
so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
so_curs.execute("DROP VIEW IF EXISTS X001ae_Trans_crereg")
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD VIEW: " + sr_file)

""" ****************************************************************************
ADD BALANCES TO STUDENTS
*****************************************************************************"""
print("ADD BALANCES TO STUDENTS")
funcfile.writelog("ADD BALANCES TO STUDENTS")      

# ADD THE BALANCES TO THE LIST OF REGISTERED STUDENTS **************************
print("Add the calculated balances to the students list...")
sr_file = "X001aa_Students"
s_sql = "CREATE TABLE " + sr_file+ " AS" + """
SELECT
  X000_Students_curr.*,
  X001aa_Trans_balopen.BAL_OPEN,
  X001ae_Trans_crebefreg.CRE_REG_BEFORE,
  CAST(0 AS REAL) AS BAL_REG_CALC,
  X001ad_Trans_balreg.BAL_REG,
  X001af_Trans_creaftreg.CRE_REG_AFTER,
  CAST(0 AS REAL) AS BAL_CRE_CALC,
  X001ag_Trans_balance.BAL_CUR,
  X001ab_Trans_feereg.FEE_REG,
  X002ab_Trans_crebefdef.Sum_Amount_VSS As CRE_DEF_BEFORE,
  CAST(0 AS REAL) AS BAL_DEF_CALC
FROM
  X000_Students_curr
  LEFT JOIN X001ad_Trans_balreg ON X001ad_Trans_balreg.STUDENT_VSS = X000_Students_curr.KSTUDBUSENTID
  LEFT JOIN X001aa_Trans_balopen ON X001aa_Trans_balopen.STUDENT_VSS = X000_Students_curr.KSTUDBUSENTID
  LEFT JOIN X001ae_Trans_crebefreg ON X001ae_Trans_crebefreg.STUDENT_VSS = X000_Students_curr.KSTUDBUSENTID
  LEFT JOIN X001af_Trans_creaftreg ON X001af_Trans_creaftreg.STUDENT_VSS = X000_Students_curr.KSTUDBUSENTID
  LEFT JOIN X001ab_Trans_feereg ON X001ab_Trans_feereg.STUDENT_VSS = X000_Students_curr.KSTUDBUSENTID
  LEFT JOIN X001ag_Trans_balance ON X001ag_Trans_balance.STUDENT_VSS = X000_Students_curr.KSTUDBUSENTID
  LEFT JOIN X002ab_Trans_crebefdef ON X002ab_Trans_crebefdef.STUDENT_VSS = X000_Students_curr.KSTUDBUSENTID  
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
# Calc balance after credits up to registration
print("Add column bal_reg_calc...")
so_curs.execute("UPDATE " + sr_file + """
                SET BAL_REG_CALC =
                CASE
                    WHEN TYPEOF(BAL_OPEN) = "null" AND TYPEOF(CRE_REG_BEFORE) = "null" THEN 0
                    WHEN TYPEOF(BAL_OPEN) = "null" THEN CRE_REG_BEFORE
                    WHEN TYPEOF(CRE_REG_BEFORE) = "null"  THEN BAL_OPEN
                    ELSE BAL_OPEN + CRE_REG_BEFORE
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: bal_reg_calc")
# Calc balance including all credits
print("Add column bal_cre_calc...")
so_curs.execute("UPDATE " + sr_file + """
                SET BAL_CRE_CALC =
                CASE
                    WHEN TYPEOF(CRE_REG_AFTER) = "null"  THEN BAL_REG_CALC
                    ELSE BAL_REG_CALC + CRE_REG_AFTER
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: bal_cre_calc")
# Calc balance after credits up to registration
print("Add column bal_def_calc...")
so_curs.execute("UPDATE " + sr_file + """
                SET BAL_DEF_CALC =
                CASE
                    WHEN TYPEOF(BAL_OPEN) = "null" AND TYPEOF(CRE_DEF_BEFORE) = "null" THEN 0
                    WHEN TYPEOF(BAL_OPEN) = "null" THEN CRE_DEF_BEFORE
                    WHEN TYPEOF(CRE_DEF_BEFORE) = "null"  THEN BAL_OPEN
                    ELSE BAL_OPEN + CRE_DEF_BEFORE
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: bal_def_calc")

# CALCULATE THE STUDENT ACCOUNT CREDIT TRANSACTIONS BEFORE REGISTRATION *********
print("Join students and deferments...")
sr_file = "X001ab_Students_deferment"
s_sql = "CREATE TABLE " + sr_file+ " AS" + """
Select
    X001aa_Students.*,
    X000_Deferments_curr.*
From
    X001aa_Students Left Join
    X000_Deferments_curr On X000_Deferments_curr.STUDENT = X001aa_Students.KSTUDBUSENTID
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD VIEW: " + sr_file)

""" ****************************************************************************
CALCULATE DEFERMENT STATUS
*****************************************************************************"""
print("CALCULATE DEFERMENT STATUS")
funcfile.writelog("CALCULATE DEFERMENT STATUS") 

# CALCULATE THE DEFERMENT TYPE
print("Calculate the deferment type...")
so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN DEFER_TYPE INT;")
so_curs.execute("UPDATE " + sr_file + """
                SET DEFER_TYPE = 
                CASE
                   WHEN BAL_REG_CALC <= 0 THEN 0
                   WHEN BAL_REG_CALC > 0 And BAL_REG_CALC <= 1000 THEN 1
                   WHEN BAL_REG_CALC > 1000 And STUDENT IS NULL THEN 3
                   WHEN BAL_REG_CALC > 1000 And DATEEND = '2019-12-31' THEN 6
                   WHEN BAL_REG_CALC > 1000 And DATEEND >= '2019-04-30' THEN 5
                   WHEN BAL_REG_CALC > 1000 And BAL_DEF_CALC > 0 THEN 4
                   WHEN BAL_REG_CALC > 1000 THEN 2
                   ELSE 7
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: DEFER_TYPE")

# CALCULATE THE DEFERMENT TYPE
print("Calculate the deferment type description...")
so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN DEFER_TYPE_DESC TEXT;")
so_curs.execute("UPDATE " + sr_file + """
                SET DEFER_TYPE_DESC = 
                CASE
                   WHEN DEFER_TYPE = 0 THEN "CREDIT ACCOUNT WITH REGISTRATION"
                   WHEN DEFER_TYPE = 1 THEN "ACCOUNT LESS THAN R1000 WITH REGISTRATION"
                   WHEN DEFER_TYPE = 2 THEN "ACCOUNT SETTLED ON DEFERMENT DATE"
                   WHEN DEFER_TYPE = 3 THEN "REGISTERED WITHOUT AGREEMENT"
                   WHEN DEFER_TYPE = 4 THEN "ACCOUNT IN ARREAR ON AGREEMENT DATE"
                   WHEN DEFER_TYPE = 5 THEN "FUTURE AGREEMENT DATE"
                   WHEN DEFER_TYPE = 6 THEN "FULL YEAR DEFERMENT"
                   ELSE "OTHER"
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: DEFER_TYPE_DESC")

# SUMMARIZE
print("Summarize registrations with accounts...")
sr_file = "X001ac_Students_deferment_summ"
s_sql = "CREATE TABLE " + sr_file+ " AS" + """
Select
    X001ab_Students_deferment.FSITEORGUNITNUMBER,
    X001ab_Students_deferment.DEFER_TYPE,
    X001ab_Students_deferment.DEFER_TYPE_DESC,
    Count(X001ab_Students_deferment.KSTUDBUSENTID) As STUD_COUNT,
    Sum(X001ab_Students_deferment.BAL_REG_CALC) As BAL_REG_DATE,
    Sum(X001ab_Students_deferment.BAL_DEF_CALC) As BAL_DEF_DATE,
    Sum(X001ab_Students_deferment.BAL_CUR) As Sum_BAL_CUR
From
    X001ab_Students_deferment
Group By
    X001ab_Students_deferment.DEFER_TYPE,
    X001ab_Students_deferment.DEFER_TYPE_DESC,
    X001ab_Students_deferment.FSITEORGUNITNUMBER
Order By
    X001ab_Students_deferment.FSITEORGUNITNUMBER,
    X001ab_Students_deferment.DEFER_TYPE
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD VIEW: " + sr_file)

""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
if l_vacuum == True:
    print("Vacuum the database...")
    so_conn.commit()
    so_conn.execute('VACUUM')
    funcfile.writelog("%t DATABASE: Vacuum Vss_deferment")    
so_conn.commit()
so_conn.close()

# CLOSE THE LOG WRITER *********************************************************
funcfile.writelog("--------------------------------")
funcfile.writelog("COMPLETED: REPORT_VSS_DEFERMENTS")
