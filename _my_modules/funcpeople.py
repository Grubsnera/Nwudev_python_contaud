""" FUNCPEOPLE.PY **************************************************************
Script for re-useable people functions
Copyright (c) AB Janse van Rensburg 25 May 2018
"""

# Import python objects
import sys

# Add own module path
sys.path.append('S:/_my_modules')

# Import own functions
import funccsv
import funcfile


def assign01(so_conn, s_table, s_from, s_to, s_on, s_mess):
    """
    Function to build ASSIGNMENTS (X000_Assignment) for different date periods
    :param so_conn: Connection string
    :param s_table: Table name to create
    :param s_from: Period start date
    :param s_to: Period end date
    :param s_on: On which date
    :param s_mess: Print message
    :return:
    """

    # Print and connect
    print(s_mess)
    so_curs = so_conn.cursor()

    # Build the table
    s_sql = "CREATE TABLE " + s_table + " AS" + """
    SELECT
      ASSI.ASS_ID,
      ASSI.PERSON_ID,
      ASSI.ASSIGNMENT_NUMBER As ASS_NUMB,
      ASSI.SERVICE_DATE_START As EMP_START,
      ASSI.EFFECTIVE_START_DATE As ASS_START,
      ASSI.EFFECTIVE_END_DATE As ASS_END,
      ASSI.SERVICE_DATE_ACTUAL_TERMINATION As EMP_END,
      ASSI.LEAVING_REASON,
      ASSI.LEAVE_REASON_DESCRIP,
      ASSI.LOCATION_DESCRIPTION,  
      ASSI.ORG_TYPE_DESC,
      ASSI.OE_CODE,
      ASSI.ORG_NAME,  
      ASSI.FACULTY,
      ASSI.DIVISION,      
      ASSI.GRADE,  
      ASSI.GRADE_NAME,
      ASSI.GRADE_CALC,
      ASSI.POSITION_ID,
      ASSI.POSITION,
      ASSI.POSITION_NAME,
      ASSI.JOB_NAME,
      ASSI.JOB_SEGMENT_NAME,
      ASSI.ACAD_SUPP,
      ASSI.EMPLOYMENT_CATEGORY,
      ASSI.LEAVE_CODE,
      ASSI.SUPERVISOR_ID,  
      ASSI.ASS_WEEK_LEN,
      ASSI.ASS_ATTRIBUTE2,
      ASSI.PRIMARY_FLAG,
      ASSI.MAILTO      
    FROM
      X000_PER_ALL_ASSIGNMENTS ASSI
    WHERE
      (ASSI.EFFECTIVE_END_DATE >= Date('%FROM%') AND
      ASSI.EFFECTIVE_END_DATE <= Date('%TO%')) OR
      (ASSI.EFFECTIVE_START_DATE >= Date('%FROM%') AND
      ASSI.EFFECTIVE_START_DATE <= Date('%TO%')) OR
      (ASSI.EFFECTIVE_END_DATE >= Date('%FROM%') AND
      ASSI.EFFECTIVE_START_DATE <= Date('%TO%'))
    ORDER BY
      ASSI.ASSIGNMENT_NUMBER,
      ASSI.EFFECTIVE_START_DATE
    """
    s_sql = s_sql.replace("%FROM%", s_from)
    s_sql = s_sql.replace("%TO%", s_to)
    so_curs.execute("DROP TABLE IF EXISTS " + s_table)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + s_table)

    # Add column assignment lookup date
    if "DATE_ASS_LOOKUP" not in funccsv.get_colnames_sqlite(so_curs, s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN DATE_ASS_LOOKUP TEXT;")
        s_sql = "UPDATE " + s_table + """
                        SET DATE_ASS_LOOKUP = 
                        CASE
                           WHEN ASS_END > Date('%TO%') THEN Date('%TO%')
                           ELSE ASS_END
                        END
                        ;"""
        s_sql = s_sql.replace("%TO%", s_on)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: DATE_ASS_LOOKUP")

    # Add column is assignment active
    if "ASS_ACTIVE" not in funccsv.get_colnames_sqlite(so_curs, s_table):
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
        s_sql = s_sql.replace("%FR%", s_from)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: ASS_ACTIVE")

    # Add column people lookup
    if "DATE_EMP_LOOKUP" not in funccsv.get_colnames_sqlite(so_curs, s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN DATE_EMP_LOOKUP TEXT;")
        s_sql = "UPDATE " + s_table + """
                        SET DATE_EMP_LOOKUP = 
                        CASE
                           WHEN EMP_START = EMP_END AND LEAVING_REASON = '' THEN Date('%TO%')
                           WHEN EMP_END > Date('%TO%') THEN Date('%TO%')
                           ELSE EMP_END
                        END
                        ;"""
        s_sql = s_sql.replace("%TO%", s_on)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: DATE_EMP_LOOKUP")

    # Add column is people active
    if "EMP_ACTIVE" not in funccsv.get_colnames_sqlite(so_curs, s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN EMP_ACTIVE TEXT;")
        s_sql = "UPDATE " + s_table + """
            SET EMP_ACTIVE = 
            CASE
                WHEN ASS_ACTIVE = 'O' AND EMP_START <= Date('%ON%') AND DATE_EMP_LOOKUP >= Date('%ON%')
                    THEN 'O'
                WHEN ASS_ACTIVE = 'P' AND EMP_START <= Date('%ON%') AND DATE_EMP_LOOKUP >= Date('%ON%')
                    THEN 'P'
                WHEN ASS_ACTIVE = 'Y' AND EMP_START <= Date('%ON%') AND DATE_EMP_LOOKUP >= Date('%ON%')
                    THEN 'Y'
                ELSE 'N'
            END
            ;"""
        s_sql = s_sql.replace("%ON%", s_on)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: EMP_ACTIVE")

    return


# 18 Build current assignment round 2 ******************************************

def assign02(so_conn, s_table, s_source, s_mess):
    """
    Function to build ASSIGNMENT for different date periods
    :param so_conn: Connection string
    :param s_table: Table name to create
    :param s_source: Source table
    :param s_mess: Print message
    :return: Nothing
    """

    # Print and connect
    print(s_mess)
    so_curs = so_conn.cursor()

    # Build the table
    s_sql = "CREATE TABLE " + s_table + " AS" + """
    SELECT
      X000_PER_ALL_PEOPLE.EMPLOYEE_NUMBER,
      ASSI.ASS_ID,
      ASSI.PERSON_ID,
      ASSI.ASS_NUMB,
      X000_PER_ALL_PEOPLE.FULL_NAME,
      X000_PER_ALL_PEOPLE.KNOWN_NAME,
      X000_PER_ALL_PEOPLE.DATE_OF_BIRTH,
      ASSI.EMP_START,
      ASSI.ASS_START,
      ASSI.ASS_END,
      ASSI.EMP_END,
      ASSI.LEAVING_REASON,
      ASSI.LEAVE_REASON_DESCRIP,
      ASSI.LOCATION_DESCRIPTION,
      ASSI.ORG_TYPE_DESC,
      ASSI.OE_CODE,
      ASSI.GRADE,
      ASSI.GRADE_NAME,
      ASSI.GRADE_CALC,
      ASSI.POSITION_ID,
      ASSI.POSITION,
      ASSI.POSITION_NAME,
      ASSI.ORG_NAME,
      ASSI.JOB_NAME,
      ASSI.JOB_SEGMENT_NAME,
      ASSI.ACAD_SUPP,
      ASSI.FACULTY,
      ASSI.DIVISION,      
      ASSI.EMPLOYMENT_CATEGORY,
      ASSI.LEAVE_CODE,
      ASSI.SUPERVISOR_ID,
      X000_PER_ALL_PEOPLE1.EMPLOYEE_NUMBER As SUPERVISOR,
      ASSI.ASS_WEEK_LEN,
      ASSI.ASS_ATTRIBUTE2,
      X000_PER_ALL_PEOPLE.NATIONALITY,
      X000_PER_ALL_PEOPLE.NATIONALITY_NAME,
      X000_PER_ALL_PEOPLE.USER_PERSON_TYPE,
      ASSI.PRIMARY_FLAG,
      X000_PER_ALL_PEOPLE.CURRENT_EMPLOYEE_FLAG,
      ASSI.DATE_ASS_LOOKUP,
      ASSI.ASS_ACTIVE,
      ASSI.DATE_EMP_LOOKUP,
      ASSI.EMP_ACTIVE,
      X000_COUNTS.COUNT_ASS,
      X000_COUNTS.COUNT_PEO,
      X000_COUNTS.COUNT_POS,
      ASSI.MAILTO,
      BANK.ACC_TYPE,
      BANK.ACC_BRANCH,
      BANK.ACC_NUMBER,
      BANK.ACC_RELATION,
      BANK.PPM_INFORMATION1 As ACC_SARS
    FROM
      %SOURCET% ASSI Left Join
      X000_PER_ALL_PEOPLE ON X000_PER_ALL_PEOPLE.PERSON_ID = ASSI.PERSON_ID AND
        X000_PER_ALL_PEOPLE.EFFECTIVE_START_DATE <= ASSI.DATE_ASS_LOOKUP AND
        X000_PER_ALL_PEOPLE.EFFECTIVE_END_DATE >= ASSI.DATE_ASS_LOOKUP Left Join
      X000_PER_ALL_PEOPLE X000_PER_ALL_PEOPLE1 ON X000_PER_ALL_PEOPLE1.PERSON_ID = ASSI.SUPERVISOR_ID AND
        X000_PER_ALL_PEOPLE1.EFFECTIVE_START_DATE <= ASSI.DATE_ASS_LOOKUP AND
        X000_PER_ALL_PEOPLE1.EFFECTIVE_END_DATE >= ASSI.DATE_ASS_LOOKUP Left Join
      X000_COUNTS ON X000_COUNTS.PERSON_ID = ASSI.PERSON_ID Left Join
      X000_PAY_ACCOUNTS BANK ON BANK.ASSIGNMENT_ID = ASSI.ASS_ID AND
        BANK.ACC_TYPE <> 'BOND' AND
        BANK.EFFECTIVE_START_DATE <= ASSI.DATE_ASS_LOOKUP AND
        BANK.EFFECTIVE_END_DATE >= ASSI.DATE_ASS_LOOKUP
    ORDER BY
      X000_PER_ALL_PEOPLE.EMPLOYEE_NUMBER,
      ASSI.EMP_START
    """
    so_curs.execute("DROP TABLE IF EXISTS " + s_table)
    s_sql = s_sql.replace("%SOURCET%", s_source)
    so_curs.execute(s_sql)
    so_conn.commit()
    so_curs.execute("DROP TABLE IF EXISTS " + s_source)
    funcfile.writelog("%t BUILD TABLE: " + s_table)
    return


def people01(so_conn, s_table, s_source, s_peri, s_mess, s_acti):
    """
    Function to build PEOPLE table from different assignments
    :param so_conn: Connection string
    :param s_table: Table name to create
    :param s_source: Table source
    :param s_peri: For which period
    :param s_mess: Print and log message
    :param s_acti: Should list include only active people = Y (or active assignments = N)
    :return: Nothing
    """

    # Print and connect
    print(s_mess)
    so_curs = so_conn.cursor()

    # Use assignment or people date
    if s_acti == "Y":
        s_wher = "ASSI.EMP_ACTIVE = 'Y'"
    else:
        s_wher = "ASSI.ASS_ACTIVE = 'Y'"

    # Create the people table
    s_sql = "CREATE TABLE " + s_table + " As " + """
    Select
      ASSI.EMPLOYEE_NUMBER,
      ASSI.ASS_ID,
      ASSI.PERSON_ID,
      ASSI.ASS_NUMB,
      X000_PER_ALL_PEOPLE.PARTY_ID,
      Upper(ASSI.FULL_NAME) As FULL_NAME,
      '' As NAME_LIST,
      '' As NAME_ADDR,
      Upper(ASSI.KNOWN_NAME) As KNOWN_NAME,
      CASE
         WHEN ORG_NAME IS NULL THEN OE_CODE||': '||POSITION_NAME
         ELSE ORG_NAME||': '||POSITION_NAME
      END AS POSITION_FULL,
      ASSI.DATE_OF_BIRTH,
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
      ASSI.ASS_START,
      ASSI.ASS_END,
      ASSI.EMP_START,
      ASSI.EMP_END,
      ASSI.LEAVING_REASON,
      Upper(ASSI.LEAVE_REASON_DESCRIP) As LEAVE_REASON_DESCRIP,
      Upper(ASSI.LOCATION_DESCRIPTION) As LOCATION_DESCRIPTION,
      Upper(ASSI.ORG_TYPE_DESC) As ORG_TYPE_DESC,
      Upper(ASSI.OE_CODE) As OE_CODE,
      Upper(ASSI.ORG_NAME) As ORG_NAME,
      ASSI.PRIMARY_FLAG,
      Upper(ASSI.ACAD_SUPP) As ACAD_SUPP,
      Upper(ASSI.FACULTY) As FACULTY,
      Upper(ASSI.DIVISION) As DIVISION,
      Case
          When EMPLOYMENT_CATEGORY = 'P' Then 'PERMANENT'
          When EMPLOYMENT_CATEGORY = 'T' Then 'TEMPORARY'
          Else 'OTHER'
      End As EMPLOYMENT_CATEGORY,
      ASSI.ASS_WEEK_LEN,
      ASSI.LEAVE_CODE,
      ASSI.GRADE,
      Upper(ASSI.GRADE_NAME) As GRADE_NAME,
      ASSI.GRADE_CALC,
      ASSI.POSITION,
      Upper(ASSI.POSITION_NAME) As POSITION_NAME,
      Upper(ASSI.JOB_NAME) As JOB_NAME,
      Upper(ASSI.JOB_SEGMENT_NAME) As JOB_SEGMENT_NAME,
      ASSI.SUPERVISOR,
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
      ASSI.COUNT_POS,
      ASSI.COUNT_ASS,
      ASSI.COUNT_PEO,
      ASSI.DATE_ASS_LOOKUP,
      ASSI.ASS_ACTIVE,
      ASSI.DATE_EMP_LOOKUP,
      ASSI.EMP_ACTIVE,      
      ASSI.MAILTO,
      PER_PAY_PROPOSALS.PROPOSED_SALARY_N,
      Upper(X000_PER_PEOPLE_TYPES.USER_PERSON_TYPE) As PERSON_TYPE,
      Upper(ASSI.ACC_TYPE) As ACC_TYPE,
      Upper(ASSI.ACC_BRANCH) As ACC_BRANCH,
      ASSI.ACC_NUMBER,
      Upper(ASSI.ACC_RELATION) As ACC_RELATION,
      ASSI.ACC_SARS
    FROM
      %SOURCET% ASSI
      LEFT JOIN X000_PER_ALL_PEOPLE ON X000_PER_ALL_PEOPLE.PERSON_ID = ASSI.PERSON_ID AND
        X000_PER_ALL_PEOPLE.EFFECTIVE_START_DATE <= ASSI.DATE_EMP_LOOKUP AND
        X000_PER_ALL_PEOPLE.EFFECTIVE_END_DATE >= ASSI.DATE_EMP_LOOKUP
      LEFT JOIN X000_PHONE_WORK_%PERIOD%_LIST ON X000_PHONE_WORK_%PERIOD%_LIST.PERSON_ID = ASSI.PERSON_ID
      LEFT JOIN X000_PHONE_MOBI_%PERIOD%_LIST ON X000_PHONE_MOBI_%PERIOD%_LIST.PERSON_ID = ASSI.PERSON_ID
      LEFT JOIN X000_PHONE_HOME_%PERIOD%_LIST ON X000_PHONE_HOME_%PERIOD%_LIST.PERSON_ID = ASSI.PERSON_ID
      LEFT JOIN X000_ADDRESS_SARS ON X000_ADDRESS_SARS.PERSON_ID = ASSI.PERSON_ID AND
        X000_ADDRESS_SARS.DATE_FROM <= ASSI.DATE_EMP_LOOKUP AND X000_ADDRESS_SARS.DATE_TO >=
        ASSI.DATE_EMP_LOOKUP
      LEFT JOIN X000_ADDRESS_POST ON X000_ADDRESS_POST.PERSON_ID = ASSI.PERSON_ID AND
        X000_ADDRESS_POST.DATE_FROM <= ASSI.DATE_EMP_LOOKUP AND X000_ADDRESS_POST.DATE_TO >=
        ASSI.DATE_EMP_LOOKUP
      LEFT JOIN X000_ADDRESS_HOME ON X000_ADDRESS_HOME.PERSON_ID = ASSI.PERSON_ID AND
        X000_ADDRESS_HOME.DATE_FROM <= ASSI.DATE_EMP_LOOKUP AND X000_ADDRESS_HOME.DATE_TO >=
        ASSI.DATE_EMP_LOOKUP
      LEFT JOIN X000_ADDRESS_OTHE ON X000_ADDRESS_OTHE.PERSON_ID = ASSI.PERSON_ID AND
        X000_ADDRESS_OTHE.DATE_FROM <= ASSI.DATE_EMP_LOOKUP AND
        X000_ADDRESS_OTHE.DATE_TO >= ASSI.DATE_EMP_LOOKUP
      LEFT JOIN PER_PAY_PROPOSALS ON PER_PAY_PROPOSALS.ASSIGNMENT_ID = ASSI.ASS_ID AND
        PER_PAY_PROPOSALS.CHANGE_DATE <= ASSI.DATE_EMP_LOOKUP AND
        PER_PAY_PROPOSALS.DATE_TO >= ASSI.DATE_EMP_LOOKUP
      LEFT JOIN X000_PER_PEOPLE_TYPES ON X000_PER_PEOPLE_TYPES.PERSON_ID = ASSI.PERSON_ID AND
        X000_PER_PEOPLE_TYPES.EFFECTIVE_START_DATE <= ASSI.DATE_EMP_LOOKUP AND
        X000_PER_PEOPLE_TYPES.EFFECTIVE_END_DATE >= ASSI.DATE_EMP_LOOKUP
    WHERE
    """ + s_wher + """
    GROUP BY
      ASSI.EMPLOYEE_NUMBER
    """
    so_curs.execute("DROP TABLE IF EXISTS " + s_table)
    s_sql = s_sql.replace("%SOURCET%", s_source)
    s_sql = s_sql.replace("%PERIOD%", s_peri)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + s_table)

    # Add column initials
    if "INITIALS" not in funccsv.get_colnames_sqlite(so_curs, s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN INITIALS TEXT;")
        s_sql = "UPDATE " + s_table + """
        SET INITIALS = 
        CASE
            WHEN INSTR(MIDDLE_NAMES,' ') > 1
                THEN SUBSTR(FIRST_NAME,1,1) || SUBSTR(MIDDLE_NAMES,1,1) || TRIM(SUBSTR(MIDDLE_NAMES,INSTR(MIDDLE_NAMES,' '),2))
            WHEN LENGTH(MIDDLE_NAMES) > 0 THEN
                SUBSTR(FIRST_NAME,1,1) || SUBSTR(MIDDLE_NAMES,1,1)
            ELSE SUBSTR(FIRST_NAME,1,1)
        END
        ;"""
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: INITIALS")

    so_curs.execute("UPDATE " + s_table + " SET NAME_LIST = LAST_NAME||' '||TITLE_FULL||' '||INITIALS;")
    so_conn.commit()
    so_curs.execute("UPDATE " + s_table + " SET NAME_ADDR = TITLE_FULL||' '||INITIALS||' '||LAST_NAME;")
    so_conn.commit()

    # Add column age
    if "AGE" not in funccsv.get_colnames_sqlite(so_curs, s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN AGE INT;")
        s_sql = "UPDATE " + s_table + """
                        SET AGE = cast( (strftime('%Y', 'now') - strftime('%Y', DATE_OF_BIRTH)) - (strftime('%m-%d', 'now') < strftime('%m-%d', DATE_OF_BIRTH)) As int)
                        ;"""
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: AGE")

    # Add column month
    if "MONTH" not in funccsv.get_colnames_sqlite(so_curs, s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN MONTH INT;")
        s_sql = "UPDATE " + s_table + """
                        SET MONTH = cast(strftime('%m', DATE_OF_BIRTH) As int)
                        ;"""
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: MONTH")

    # Add column day
    if "DAY" not in funccsv.get_colnames_sqlite(so_curs, s_table):
        so_curs.execute("ALTER TABLE " + s_table + " ADD COLUMN DAY INT;")
        s_sql = "UPDATE " + s_table + """
                        SET DAY = cast(strftime('%d', DATE_OF_BIRTH) As int)
                        ;"""
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: DAY")

    return
