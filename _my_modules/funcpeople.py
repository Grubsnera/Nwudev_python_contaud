""" FUNCPEOPLE.PY **************************************************************
Script for re-useable people functions
Copyright (c) AB Janse van Rensburg 25 May 2018
"""

# Import python objects
import csv
import pyodbc
import sqlite3
import datetime
import sys

# Add own module path
sys.path.append('S:/_my_modules')

# Import own functions
import funcdate
import funccsv
import funcfile

# 17 Build current assignment round 1 ******************************************

def Assign01(so_conn,s_table,s_from,s_to,s_on,s_mess):

    """ Function to build the X000_Assignment table with different date periods
    Parameters: s_table = Table name to create
                s_from = Period start date
                s_to = Period end date
                s_on = Date in period
    """
    
    print(s_mess)
    so_curs = so_conn.cursor()

    s_sql = "CREATE TABLE " + s_table + " AS" + """
    SELECT
      X000_PER_ALL_ASSIGNMENTS.ASS_ID,
      X000_PER_ALL_ASSIGNMENTS.PERSON_ID,
      X000_PER_ALL_ASSIGNMENTS.ASSIGNMENT_NUMBER As ASS_NUMB,
      X000_PER_ALL_ASSIGNMENTS.SERVICE_DATE_START As EMP_START,
      X000_PER_ALL_ASSIGNMENTS.EFFECTIVE_START_DATE As ASS_START,
      X000_PER_ALL_ASSIGNMENTS.EFFECTIVE_END_DATE As ASS_END,
      X000_PER_ALL_ASSIGNMENTS.SERVICE_DATE_ACTUAL_TERMINATION As EMP_END,
      X000_PER_ALL_ASSIGNMENTS.LEAVING_REASON,
      X000_PER_ALL_ASSIGNMENTS.LEAVE_REASON_DESCRIP,
      X000_PER_ALL_ASSIGNMENTS.LOCATION_DESCRIPTION,  
      X000_PER_ALL_ASSIGNMENTS.ORG_TYPE_DESC,
      X000_PER_ALL_ASSIGNMENTS.OE_CODE,
      X000_PER_ALL_ASSIGNMENTS.ORG_NAME,  
      X000_PER_ALL_ASSIGNMENTS.FACULTY,
      X000_PER_ALL_ASSIGNMENTS.DIVISION,      
      X000_PER_ALL_ASSIGNMENTS.GRADE,  
      X000_PER_ALL_ASSIGNMENTS.GRADE_NAME,
      X000_PER_ALL_ASSIGNMENTS.GRADE_CALC,
      X000_PER_ALL_ASSIGNMENTS.POSITION_ID,
      X000_PER_ALL_ASSIGNMENTS.POSITION,
      X000_PER_ALL_ASSIGNMENTS.POSITION_NAME,
      X000_PER_ALL_ASSIGNMENTS.JOB_NAME,
      X000_PER_ALL_ASSIGNMENTS.JOB_SEGMENT_NAME,
      X000_PER_ALL_ASSIGNMENTS.ACAD_SUPP,
      X000_PER_ALL_ASSIGNMENTS.EMPLOYMENT_CATEGORY,
      X000_PER_ALL_ASSIGNMENTS.LEAVE_CODE,
      X000_PER_ALL_ASSIGNMENTS.SUPERVISOR_ID,  
      X000_PER_ALL_ASSIGNMENTS.ASS_WEEK_LEN,
      X000_PER_ALL_ASSIGNMENTS.ASS_ATTRIBUTE2,
      X000_PER_ALL_ASSIGNMENTS.PRIMARY_FLAG,
      X000_PER_ALL_ASSIGNMENTS.MAILTO      
    FROM
      X000_PER_ALL_ASSIGNMENTS
    WHERE
      (X000_PER_ALL_ASSIGNMENTS.EFFECTIVE_END_DATE >= Date('%FROM%') AND
      X000_PER_ALL_ASSIGNMENTS.EFFECTIVE_END_DATE <= Date('%TO%')) OR
      (X000_PER_ALL_ASSIGNMENTS.EFFECTIVE_START_DATE >= Date('%FROM%') AND
      X000_PER_ALL_ASSIGNMENTS.EFFECTIVE_START_DATE <= Date('%TO%')) OR
      (X000_PER_ALL_ASSIGNMENTS.EFFECTIVE_END_DATE >= Date('%FROM%') AND
      X000_PER_ALL_ASSIGNMENTS.EFFECTIVE_START_DATE <= Date('%TO%'))
    ORDER BY
      X000_PER_ALL_ASSIGNMENTS.ASSIGNMENT_NUMBER,
      X000_PER_ALL_ASSIGNMENTS.EFFECTIVE_START_DATE
    """
    s_sql = s_sql.replace("%FROM%",s_from)
    s_sql = s_sql.replace("%TO%",s_to)
    so_curs.execute("DROP TABLE IF EXISTS " + s_table)
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: " + s_table)

    if "DATE_ASS_LOOKUP" not in funccsv.get_colnames_sqlite(so_curs,s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN DATE_ASS_LOOKUP TEXT;")
        s_sql = "UPDATE " + s_table + """
                        SET DATE_ASS_LOOKUP = 
                        CASE
                           WHEN ASS_END > Date('%TO%') THEN Date('%TO%')
                           ELSE ASS_END
                        END
                        ;"""
        s_sql = s_sql.replace("%TO%",s_on)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: DATE_ASS_LOOKUP")

    if "ASS_ACTIVE" not in funccsv.get_colnames_sqlite(so_curs,s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN ASS_ACTIVE TEXT;")
        s_sql = "UPDATE " + s_table + """
                        SET ASS_ACTIVE = 
                        CASE
                           WHEN ORG_TYPE_DESC = 'Parent Organisation' THEN 'O'
                           WHEN EMP_START = EMP_END AND LEAVING_REASON = '' THEN 'Y'
                           WHEN EMP_END >= Date('%FR%') THEN 'Y'
                           WHEN INSTR(POSITION_NAME,'Pensioner') > 0 THEN 'P'                           
                           ELSE 'N'
                        END
                        ;"""
        s_sql = s_sql.replace("%FR%",s_from)    
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: ASS_ACTIVE")

    if "DATE_EMP_LOOKUP" not in funccsv.get_colnames_sqlite(so_curs,s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN DATE_EMP_LOOKUP TEXT;")
        s_sql = "UPDATE " + s_table + """
                        SET DATE_EMP_LOOKUP = 
                        CASE
                           WHEN EMP_START = EMP_END AND LEAVING_REASON = '' THEN Date('%TO%')
                           WHEN EMP_END > Date('%TO%') THEN Date('%TO%')
                           ELSE EMP_END
                        END
                        ;"""
        s_sql = s_sql.replace("%TO%",s_on)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: DATE_EMP_LOOKUP")

    if "EMP_ACTIVE" not in funccsv.get_colnames_sqlite(so_curs,s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN EMP_ACTIVE TEXT;")
        s_sql = "UPDATE " + s_table + """
                        SET EMP_ACTIVE = 
                        CASE
                           WHEN ASS_ACTIVE = 'O' AND EMP_START <= Date('%ON%') AND DATE_EMP_LOOKUP >= Date('%ON%') THEN 'O'
                           WHEN ASS_ACTIVE = 'P' AND EMP_START <= Date('%ON%') AND DATE_EMP_LOOKUP >= Date('%ON%') THEN 'P'
                           WHEN ASS_ACTIVE = 'Y' AND EMP_START <= Date('%ON%') AND DATE_EMP_LOOKUP >= Date('%ON%') THEN 'Y'
                           ELSE 'N'
                        END
                        ;"""
        s_sql = s_sql.replace("%ON%",s_on)    
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: EMP_ACTIVE")    

    return;

# 18 Build current assignment round 2 ******************************************

def Assign02(so_conn,s_table,s_source,s_mess):

    """ Function to build the X000_Assignment table with different date periods
    Parameters: s_table = Table name to create
                s_source = source table
                s_mess = Print and log message
    """
    
    print(s_mess)
    so_curs = so_conn.cursor()

    s_sql = "CREATE TABLE " + s_table + " AS" + """
    SELECT
      X000_PER_ALL_PEOPLE.EMPLOYEE_NUMBER,
      %SOURCET%.ASS_ID,
      %SOURCET%.PERSON_ID,
      %SOURCET%.ASS_NUMB,
      X000_PER_ALL_PEOPLE.FULL_NAME,
      X000_PER_ALL_PEOPLE.KNOWN_NAME,
      X000_PER_ALL_PEOPLE.DATE_OF_BIRTH,
      %SOURCET%.EMP_START,
      %SOURCET%.ASS_START,
      %SOURCET%.ASS_END,
      %SOURCET%.EMP_END,
      %SOURCET%.LEAVING_REASON,
      %SOURCET%.LEAVE_REASON_DESCRIP,
      %SOURCET%.LOCATION_DESCRIPTION,
      %SOURCET%.ORG_TYPE_DESC,
      %SOURCET%.OE_CODE,
      %SOURCET%.GRADE,
      %SOURCET%.GRADE_NAME,
      %SOURCET%.GRADE_CALC,
      %SOURCET%.POSITION_ID,
      %SOURCET%.POSITION,
      %SOURCET%.POSITION_NAME,
      %SOURCET%.ORG_NAME,
      %SOURCET%.JOB_NAME,
      %SOURCET%.JOB_SEGMENT_NAME,
      %SOURCET%.ACAD_SUPP,
      %SOURCET%.FACULTY,
      %SOURCET%.DIVISION,      
      %SOURCET%.EMPLOYMENT_CATEGORY,
      %SOURCET%.LEAVE_CODE,
      %SOURCET%.SUPERVISOR_ID,
      X000_PER_ALL_PEOPLE1.EMPLOYEE_NUMBER As SUPERVISOR,
      %SOURCET%.ASS_WEEK_LEN,
      %SOURCET%.ASS_ATTRIBUTE2,
      X000_PER_ALL_PEOPLE.NATIONALITY,
      X000_PER_ALL_PEOPLE.NATIONALITY_NAME,
      X000_PER_ALL_PEOPLE.USER_PERSON_TYPE,
      %SOURCET%.PRIMARY_FLAG,
      X000_PER_ALL_PEOPLE.CURRENT_EMPLOYEE_FLAG,
      %SOURCET%.DATE_ASS_LOOKUP,
      %SOURCET%.ASS_ACTIVE,
      %SOURCET%.DATE_EMP_LOOKUP,
      %SOURCET%.EMP_ACTIVE,
      X000_COUNTS.COUNT_ASS,
      X000_COUNTS.COUNT_PEO,
      X000_COUNTS.COUNT_POS,
      %SOURCET%.MAILTO,
      X000_PAY_ACCOUNTS.ACC_TYPE,
      X000_PAY_ACCOUNTS.ACC_BRANCH,
      X000_PAY_ACCOUNTS.ACC_NUMBER,
      X000_PAY_ACCOUNTS.ACC_RELATION
    FROM
      %SOURCET%
      LEFT JOIN X000_PER_ALL_PEOPLE ON X000_PER_ALL_PEOPLE.PERSON_ID = %SOURCET%.PERSON_ID AND
        X000_PER_ALL_PEOPLE.EFFECTIVE_START_DATE <= %SOURCET%.DATE_ASS_LOOKUP AND
        X000_PER_ALL_PEOPLE.EFFECTIVE_END_DATE >= %SOURCET%.DATE_ASS_LOOKUP
      LEFT JOIN X000_PER_ALL_PEOPLE X000_PER_ALL_PEOPLE1 ON X000_PER_ALL_PEOPLE1.PERSON_ID = %SOURCET%.SUPERVISOR_ID AND
        X000_PER_ALL_PEOPLE1.EFFECTIVE_START_DATE <= %SOURCET%.DATE_ASS_LOOKUP AND
        X000_PER_ALL_PEOPLE1.EFFECTIVE_END_DATE >= %SOURCET%.DATE_ASS_LOOKUP
      LEFT JOIN X000_COUNTS ON X000_COUNTS.PERSON_ID = %SOURCET%.PERSON_ID
      LEFT JOIN X000_PAY_ACCOUNTS ON X000_PAY_ACCOUNTS.ASSIGNMENT_ID = %SOURCET%.ASS_ID AND
        X000_PAY_ACCOUNTS.EFFECTIVE_START_DATE <= %SOURCET%.DATE_ASS_LOOKUP AND
        X000_PAY_ACCOUNTS.EFFECTIVE_END_DATE >= %SOURCET%.DATE_ASS_LOOKUP
    ORDER BY
      X000_PER_ALL_PEOPLE.EMPLOYEE_NUMBER,
      %SOURCET%.EMP_START
    """
    so_curs.execute("DROP TABLE IF EXISTS " + s_table )
    s_sql = s_sql.replace("%SOURCET%",s_source)
    so_curs.execute(s_sql)
    so_conn.commit()
    so_curs.execute("DROP TABLE IF EXISTS " + s_source )
    funcfile.writelog("%t BUILD TABLE: " + s_table)
    return;

# 19 Build PEOPLE CURRENT ******************************************************

def People01(so_conn,s_table,s_source,s_peri,s_mess,s_acti):

    """ Function to build the X002_People table from different assignments
    Parameters: s_table = Table name to create
                s_source = source table
                s_mess = Print and log message
                s_acti = Should list include only active people = Y (or active assignments = N)
    """
    
    print(s_mess)
    so_curs = so_conn.cursor()
    s_wher = ""

    if s_acti == "Y":
       s_wher = "%SOURCET%.EMP_ACTIVE = 'Y'"
    else:
       s_wher = "%SOURCET%.ASS_ACTIVE = 'Y'"
    
    s_sql = "CREATE TABLE " + s_table + " As " + """
    Select
      %SOURCET%.EMPLOYEE_NUMBER,
      %SOURCET%.ASS_ID,
      %SOURCET%.PERSON_ID,
      %SOURCET%.ASS_NUMB,
      X000_PER_ALL_PEOPLE.PARTY_ID,
      Upper(%SOURCET%.FULL_NAME) As FULL_NAME,
      '' As NAME_LIST,
      '' As NAME_ADDR,
      Upper(%SOURCET%.KNOWN_NAME) As KNOWN_NAME,
      CASE
         WHEN ORG_NAME IS NULL THEN OE_CODE||': '||POSITION_NAME
         ELSE ORG_NAME||': '||POSITION_NAME
      END AS POSITION_FULL,
      %SOURCET%.DATE_OF_BIRTH,
      Upper(X000_PER_ALL_PEOPLE.NATIONALITY) As NATIONALITY,
      Upper(X000_PER_ALL_PEOPLE.NATIONALITY_NAME) As NATIONALITY_NAME,
      X000_PER_ALL_PEOPLE.NATIONAL_IDENTIFIER As IDNO,
      X000_PER_ALL_PEOPLE.PER_INFORMATION2 As PASSPORT,
      X000_PER_ALL_PEOPLE.PER_INFORMATION3 As PERMIT,
      X000_PER_ALL_PEOPLE.TAX_NUMBER,
      Case
          When X000_PER_ALL_PEOPLE.SEX = 'F' Then 'FEMALE'
          When X000_PER_ALL_PEOPLE.SEX = 'M' Then 'MALE'
          Else 'OTHER'
      End As SEX,
      X000_PER_ALL_PEOPLE.MARITAL_STATUS,
      X000_PER_ALL_PEOPLE.REGISTERED_DISABLED_FLAG As DISABLED,
      X000_PER_ALL_PEOPLE.RACE_CODE,
      Upper(X000_PER_ALL_PEOPLE.RACE_DESC) As RACE_DESC,
      X000_PER_ALL_PEOPLE.LANG_CODE,
      Upper(X000_PER_ALL_PEOPLE.LANG_DESC) As LANG_DESC,
      X000_PER_ALL_PEOPLE.INT_MAIL,
      Lower(X000_PER_ALL_PEOPLE.EMAIL_ADDRESS) As EMAIL_ADDRESS,
      X000_PER_ALL_PEOPLE.CURRENT_EMPLOYEE_FLAG As CURR_EMPL_FLAG,
      X000_PER_ALL_PEOPLE.USER_PERSON_TYPE,
      %SOURCET%.ASS_START,
      %SOURCET%.ASS_END,
      %SOURCET%.EMP_START,
      %SOURCET%.EMP_END,
      %SOURCET%.LEAVING_REASON,
      Upper(%SOURCET%.LEAVE_REASON_DESCRIP) As LEAVE_REASON_DESCRIP,
      Upper(%SOURCET%.LOCATION_DESCRIPTION) As LOCATION_DESCRIPTION,
      Upper(%SOURCET%.ORG_TYPE_DESC) As ORG_TYPE_DESC,
      Upper(%SOURCET%.OE_CODE) As OE_CODE,
      Upper(%SOURCET%.ORG_NAME) As ORG_NAME,
      %SOURCET%.PRIMARY_FLAG,
      Upper(%SOURCET%.ACAD_SUPP) As ACAD_SUPP,
      Upper(%SOURCET%.FACULTY) As FACULTY,
      Upper(%SOURCET%.DIVISION) As DIVISION,
      Case
          When EMPLOYMENT_CATEGORY = 'P' Then 'PERMANENT'
          When EMPLOYMENT_CATEGORY = 'T' Then 'TEMPORARY'
          Else 'OTHER'
      End As EMPLOYMENT_CATEGORY,
      %SOURCET%.ASS_WEEK_LEN,
      %SOURCET%.LEAVE_CODE,
      %SOURCET%.GRADE,
      Upper(%SOURCET%.GRADE_NAME) As GRADE_NAME,
      %SOURCET%.GRADE_CALC,
      %SOURCET%.POSITION,
      Upper(%SOURCET%.POSITION_NAME) As POSITION_NAME,
      Upper(%SOURCET%.JOB_NAME) As JOB_NAME,
      Upper(%SOURCET%.JOB_SEGMENT_NAME) As JOB_SEGMENT_NAME,
      %SOURCET%.SUPERVISOR,
      X000_PER_ALL_PEOPLE.TITLE_FULL,
      X000_PER_ALL_PEOPLE.FIRST_NAME,
      X000_PER_ALL_PEOPLE.MIDDLE_NAMES,
      X000_PER_ALL_PEOPLE.LAST_NAME,
      X000_PHONE_WORK_%PERIOD%_LIST.PHONE_WORK,
      X000_PHONE_MOBI_%PERIOD%_LIST.PHONE_MOBI,
      X000_PHONE_HOME_%PERIOD%_LIST.PHONE_HOME,
      X000_ADDRESS_SARS.ADDRESS_SARS,
      X000_ADDRESS_POST.ADDRESS_POST,
      X000_ADDRESS_HOME.ADDRESS_HOME,
      X000_ADDRESS_OTHE.ADDRESS_OTHE,
      %SOURCET%.COUNT_POS,
      %SOURCET%.COUNT_ASS,
      %SOURCET%.COUNT_PEO,
      %SOURCET%.DATE_ASS_LOOKUP,
      %SOURCET%.ASS_ACTIVE,
      %SOURCET%.DATE_EMP_LOOKUP,
      %SOURCET%.EMP_ACTIVE,      
      %SOURCET%.MAILTO,
      PER_PAY_PROPOSALS.PROPOSED_SALARY_N,
      Upper(X000_PER_PEOPLE_TYPES.USER_PERSON_TYPE) As PERSON_TYPE,
      Upper(%SOURCET%.ACC_TYPE) As ACC_TYPE,
      Upper(%SOURCET%.ACC_BRANCH) As ACC_BRANCH,
      %SOURCET%.ACC_NUMBER,
      Upper(%SOURCET%.ACC_RELATION) As ACC_RELATION
    FROM
      %SOURCET%
      LEFT JOIN X000_PER_ALL_PEOPLE ON X000_PER_ALL_PEOPLE.PERSON_ID = %SOURCET%.PERSON_ID AND
        X000_PER_ALL_PEOPLE.EFFECTIVE_START_DATE <= %SOURCET%.DATE_EMP_LOOKUP AND
        X000_PER_ALL_PEOPLE.EFFECTIVE_END_DATE >= %SOURCET%.DATE_EMP_LOOKUP
      LEFT JOIN X000_PHONE_WORK_%PERIOD%_LIST ON X000_PHONE_WORK_%PERIOD%_LIST.PERSON_ID = %SOURCET%.PERSON_ID
      LEFT JOIN X000_PHONE_MOBI_%PERIOD%_LIST ON X000_PHONE_MOBI_%PERIOD%_LIST.PERSON_ID = %SOURCET%.PERSON_ID
      LEFT JOIN X000_PHONE_HOME_%PERIOD%_LIST ON X000_PHONE_HOME_%PERIOD%_LIST.PERSON_ID = %SOURCET%.PERSON_ID
      LEFT JOIN X000_ADDRESS_SARS ON X000_ADDRESS_SARS.PERSON_ID = %SOURCET%.PERSON_ID AND
        X000_ADDRESS_SARS.DATE_FROM <= %SOURCET%.DATE_EMP_LOOKUP AND X000_ADDRESS_SARS.DATE_TO >=
        %SOURCET%.DATE_EMP_LOOKUP
      LEFT JOIN X000_ADDRESS_POST ON X000_ADDRESS_POST.PERSON_ID = %SOURCET%.PERSON_ID AND
        X000_ADDRESS_POST.DATE_FROM <= %SOURCET%.DATE_EMP_LOOKUP AND X000_ADDRESS_POST.DATE_TO >=
        %SOURCET%.DATE_EMP_LOOKUP
      LEFT JOIN X000_ADDRESS_HOME ON X000_ADDRESS_HOME.PERSON_ID = %SOURCET%.PERSON_ID AND
        X000_ADDRESS_HOME.DATE_FROM <= %SOURCET%.DATE_EMP_LOOKUP AND X000_ADDRESS_HOME.DATE_TO >=
        %SOURCET%.DATE_EMP_LOOKUP
      LEFT JOIN X000_ADDRESS_OTHE ON X000_ADDRESS_OTHE.PERSON_ID = %SOURCET%.PERSON_ID AND
        X000_ADDRESS_OTHE.DATE_FROM <= %SOURCET%.DATE_EMP_LOOKUP AND
        X000_ADDRESS_OTHE.DATE_TO >= %SOURCET%.DATE_EMP_LOOKUP
      LEFT JOIN PER_PAY_PROPOSALS ON PER_PAY_PROPOSALS.ASSIGNMENT_ID = %SOURCET%.ASS_ID AND
        PER_PAY_PROPOSALS.CHANGE_DATE <= %SOURCET%.DATE_EMP_LOOKUP AND
        PER_PAY_PROPOSALS.DATE_TO >= %SOURCET%.DATE_EMP_LOOKUP
      LEFT JOIN X000_PER_PEOPLE_TYPES ON X000_PER_PEOPLE_TYPES.PERSON_ID = %SOURCET%.PERSON_ID AND
        X000_PER_PEOPLE_TYPES.EFFECTIVE_START_DATE <= %SOURCET%.DATE_EMP_LOOKUP AND
        X000_PER_PEOPLE_TYPES.EFFECTIVE_END_DATE >= %SOURCET%.DATE_EMP_LOOKUP
    WHERE
    """ + s_wher + """
    GROUP BY
      %SOURCET%.EMPLOYEE_NUMBER
    """
    so_curs.execute("DROP TABLE IF EXISTS " + s_table )
    s_sql = s_sql.replace("%SOURCET%",s_source)
    s_sql = s_sql.replace("%PERIOD%",s_peri)
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: " + s_table)

    if "INITIALS" not in funccsv.get_colnames_sqlite(so_curs,s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN INITIALS TEXT;")
        s_sql = "UPDATE " + s_table + """
                        SET INITIALS = 
                        CASE
                           WHEN INSTR(MIDDLE_NAMES,' ') > 1 THEN SUBSTR(FIRST_NAME,1,1) || SUBSTR(MIDDLE_NAMES,1,1) || TRIM(SUBSTR(MIDDLE_NAMES,INSTR(MIDDLE_NAMES,' '),2))
                           WHEN LENGTH(MIDDLE_NAMES) > 0 THEN SUBSTR(FIRST_NAME,1,1) || SUBSTR(MIDDLE_NAMES,1,1)
                           ELSE SUBSTR(FIRST_NAME,1,1)
                        END
                        ;"""
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: INITIALS")

    so_curs.execute("UPDATE "+s_table+" SET NAME_LIST = LAST_NAME||' '||TITLE_FULL||' '||INITIALS;")
    so_conn.commit()
    so_curs.execute("UPDATE "+s_table+" SET NAME_ADDR = TITLE_FULL||' '||INITIALS||' '||LAST_NAME;")
    so_conn.commit()

    if "AGE" not in funccsv.get_colnames_sqlite(so_curs,s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN AGE INT;")
        s_sql = "UPDATE " + s_table + """
                        SET AGE = cast( (strftime('%Y', 'now') - strftime('%Y', DATE_OF_BIRTH)) - (strftime('%m-%d', 'now') < strftime('%m-%d', DATE_OF_BIRTH)) As int)
                        ;"""
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: AGE") 

    if "MONTH" not in funccsv.get_colnames_sqlite(so_curs,s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN MONTH INT;")
        s_sql = "UPDATE " + s_table + """
                        SET MONTH = cast(strftime('%m', DATE_OF_BIRTH) As int)
                        ;"""
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: MONTH") 

    if "DAY" not in funccsv.get_colnames_sqlite(so_curs,s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN DAY INT;")
        s_sql = "UPDATE " + s_table + """
                        SET DAY = cast(strftime('%d', DATE_OF_BIRTH) As int)
                        ;"""
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: DAY") 



    return;


