""" Script to build standard PEOPLE lists
Created on: 12 Apr 2018
Author: Albert J v Rensburg (NWU21162395)
"""

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
BUILD LOOKUPS
BUILD GRADES
BUILD POSITIONS
BUILD JOBS
BUILD ORGANIZATION
BUILD BANK ACCOUNTS
BUILD PERIODS OF SERVICE
BUILD ASSIGNMENTS
BUILD ALL PEOPLE
BUILD PERSON TYPES
COUNT RECORDS
BUILD ADDRESSES AND PHONES
BUILD ASSIGNMENTS AND PEOPLE
PEOPLE ORGANIZATION STRUCTURE REF (Employee numbers of structure)
BUILD CURRENT SYSTEM USERS (X000_USER_CURR)
BUILD PEOPLE LEAVE
*****************************************************************************"""

""" DEPENDANCIES ***************************************************************
# 01 PER_GRADES table (Vss.sqlite)
     PER_GRADE_DEFINITIONS (Vss.sqlite)
# 02 PER_ALL_POSITIONS (Vss.sqlite)
     PER_POSITION_DEFINITIONS (Vss.sqlite)
# 03 PER_JOBS (Vss.sqlite)
     PER_JOB_DEFINITIONS (Vss.sqlite)
# 04 HR_ALL_ORGANIZATION_UNITS (Vss.sqlite)
     HR_ORGANIZATION_INFORMATION (Vss.sqlite)
# 05 PER_PERIODS_OF_SERVICE (Vss.sqlite)
     HR_LOOKUPS (Vss.sqlite)
# 06 PER_ALL_ASSIGNMENTS_F (Vss.sqlite)
     HR_LOCATIONS_ALL (Vss.sqlite)
     PAY_PEOPLE_GROUPS (Vss.sqlite)
     (01 - 05) (Vss.sqlite)
# 07 PER_ALL_PEOPLE_F (Vss.sqlite)
     PER_PERSON_TYPES (Vss.sqlite)
     HR_LOOKUPS (Vss.sqlite)
# 08 PER_ALL_ASSIGNMENTS_F (Vss.sqlite)
# 09 PER_ALL_PEOPLE_F (Vss.sqlite)
# 10 PER_PERIODS_OF_SERVICE (Vss.sqlite)
# 11 X000_COUNT_ASSIGNMENTS (Vss.sqlite)
     X000_COUNT_PEOPLE (Vss.sqlite)
#    X000_COUNT_PERIODOS (Vss.sqlite)
# 12 X000_PER_ALL_ASSIGNMENTS (Vss.sqlite view)
# 13 X000_PER_ALL_PEOPLE
     X001_ASSIGNMENT_CURR_01
     X000_COUNTS
**************************************************************************** """

def People_lists():

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # Import python modules
    import sys

    # Add own module path
    sys.path.append('S:/_my_modules')

    # Import python objects
    import csv
    import pyodbc
    import datetime
    import sqlite3    

    # Import own modules
    import funcdate
    import funccsv
    import funcfile
    import funcpeople
    import funcmail
    import funcmysql
    import funcstr

    # Script log file
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: B001_PEOPLE_LISTS")
    funcfile.writelog("-------------------------")
    print("-----------------")    
    print("B001_PEOPLE_LISTS")
    print("-----------------")
    ilog_severity = 1
    
    # SQLITE Declare variables 
    so_path = "W:/People/" #Source database path
    re_path = "R:/People/"
    so_file = "People.sqlite" #Source database
    s_sql = "" #SQL statements
    l_export = True
    l_mail = True
    l_vacuum = False

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # Open the SQLITE SOURCE file
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("%t OPEN SQLITE DATABASE: PEOPLE.SQLITE")
    
    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """ ****************************************************************************
    BUILD LOOKUPS
    *****************************************************************************"""
    print("BUILD LOOKUPS")
    funcfile.writelog("BUILD LOOKUPS")
    
    # Import the X000_OWN_HR_LOOKUPS table *****************************************
    print("Import own lookups...")
    ed_path = "S:/_external_data/"
    tb_name = "X000_OWN_HR_LOOKUPS"
    so_curs.execute("DROP TABLE IF EXISTS " + tb_name)
    so_curs.execute("CREATE TABLE " + tb_name + "(LOOKUP TEXT,LOOKUP_CODE TEXT,LOOKUP_DESCRIPTION TEXT)")
    s_cols = ""
    co = open(ed_path + "001_own_hr_lookups.csv", newline=None)
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

    """ ****************************************************************************
    BUILD GRADES
    *****************************************************************************"""
    print("BUILD GRADES")
    funcfile.writelog("BUILD GRADES")

    # BUILD GRADES
    print("Build grades...")
    s_sql = "CREATE TABLE X000_GRADES AS " + """
    SELECT
      GRADES.GRADE_ID,
      SUBSTR(NAME,INSTR(NAME,'~')+1,60) As GRADE_NAME,
      GRADES.DATE_FROM,
      GRADES.DATE_TO,
      GRADES.GRADE_DEFINITION_ID,
      DEF.SEGMENT1 AS GRADE,
      GRADES.NAME AS GRADE_COMB,
      GRADES.CREATED_BY,
      GRADES.CREATION_DATE,
      GRADES.LAST_UPDATED_BY,
      GRADES.LAST_UPDATE_LOGIN,
      GRADES.SEQUENCE,
      DEF.SEGMENT2 AS GRADE_SEGMENT2,
      DEF.ID_FLEX_NUM,
      GRADES.BUSINESS_GROUP_ID,
      LOOKUP.LOOKUP_DESCRIPTION AS GRADE_CALC
    FROM
      PER_GRADES GRADES
      LEFT JOIN PER_GRADE_DEFINITIONS DEF ON DEF.GRADE_DEFINITION_ID = GRADES.GRADE_DEFINITION_ID
      LEFT JOIN X000_OWN_HR_LOOKUPS LOOKUP ON LOOKUP.LOOKUP_CODE = DEF.SEGMENT1 AND
        LOOKUP.LOOKUP = 'GRADE'  
    ORDER BY
      GRADES.GRADE_ID
    """
    so_curs.execute("DROP TABLE IF EXISTS X000_GRADES")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: X000_GRADES")

    """
    # CALC GRADE NAME FIELD
    if "GRADE_NAME" not in funccsv.get_colnames_sqlite(so_curs,"X000_GRADES"):
        so_curs.execute("ALTER TABLE X000_GRADES ADD GRADE_NAME TEXT;")
        so_curs.execute("UPDATE X000_GRADES SET GRADE_NAME = SUBSTR(GRADE_COMB,INSTR(GRADE_COMB,'~')+1,60);")
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMNS: GRADE_NAME")
    """

    """ ****************************************************************************
    BUILD POSITIONS
    *****************************************************************************"""
    print("BUILD POSITIONS")
    funcfile.writelog("BUILD POSITIONS")

    print("Build positions...")
    s_sql = "CREATE TABLE X000_POSITIONS AS " + """
    SELECT
      PER_ALL_POSITIONS.POSITION_ID,
      PER_ALL_POSITIONS.DATE_EFFECTIVE,
      PER_ALL_POSITIONS.DATE_END,
      PER_ALL_POSITIONS.POSITION_DEFINITION_ID,
      PER_POSITION_DEFINITIONS.SEGMENT1 AS POSITION,
      PER_POSITION_DEFINITIONS.SEGMENT2 AS POSITION_NAME,
      PER_ALL_POSITIONS.BUSINESS_GROUP_ID,
      PER_ALL_POSITIONS.JOB_ID,
      PER_ALL_POSITIONS.ORGANIZATION_ID,
      PER_ALL_POSITIONS.LOCATION_ID,
      PER_ALL_POSITIONS.PROBATION_PERIOD,
      PER_ALL_POSITIONS.PROBATION_PERIOD_UNITS,
      PER_ALL_POSITIONS.WORKING_HOURS,
      PER_ALL_POSITIONS.STATUS,
      PER_ALL_POSITIONS.ATTRIBUTE1,
      PER_ALL_POSITIONS.ATTRIBUTE2,
      PER_ALL_POSITIONS.ATTRIBUTE3,
      PER_ALL_POSITIONS.ATTRIBUTE4,
      PER_ALL_POSITIONS.ATTRIBUTE5,
      PER_ALL_POSITIONS.ATTRIBUTE6,
      PER_ALL_POSITIONS.ATTRIBUTE7,
      PER_ALL_POSITIONS.ATTRIBUTE8,
      PER_ALL_POSITIONS.ATTRIBUTE9,
      PER_ALL_POSITIONS.ATTRIBUTE10,
      PER_ALL_POSITIONS.ATTRIBUTE11,
      PER_ALL_POSITIONS.ATTRIBUTE12,
      PER_ALL_POSITIONS.ATTRIBUTE13,
      PER_ALL_POSITIONS.ATTRIBUTE14,
      PER_ALL_POSITIONS.ATTRIBUTE15,
      PER_ALL_POSITIONS.ATTRIBUTE16,
      PER_ALL_POSITIONS.ATTRIBUTE17,
      PER_ALL_POSITIONS.ATTRIBUTE18,
      PER_ALL_POSITIONS.ATTRIBUTE19,
      PER_ALL_POSITIONS.ATTRIBUTE20,
      PER_ALL_POSITIONS.ATTRIBUTE21,
      PER_ALL_POSITIONS.ATTRIBUTE22,
      PER_ALL_POSITIONS.ATTRIBUTE23,
      PER_ALL_POSITIONS.ATTRIBUTE24,
      PER_ALL_POSITIONS.ATTRIBUTE25,
      PER_ALL_POSITIONS.ATTRIBUTE26,
      PER_ALL_POSITIONS.ATTRIBUTE27,
      PER_ALL_POSITIONS.ATTRIBUTE28,
      PER_ALL_POSITIONS.ATTRIBUTE29,
      PER_ALL_POSITIONS.ATTRIBUTE30,
      PER_ALL_POSITIONS.CREATION_DATE,
      PER_ALL_POSITIONS.CREATED_BY,
      PER_ALL_POSITIONS.LAST_UPDATE_DATE,
      PER_ALL_POSITIONS.LAST_UPDATED_BY,
      PER_ALL_POSITIONS.LAST_UPDATE_LOGIN,
      PER_POSITION_DEFINITIONS.ID_FLEX_NUM,
      PER_POSITION_DEFINITIONS.SUMMARY_FLAG,
      PER_POSITION_DEFINITIONS.ENABLED_FLAG,
      PER_POSITION_DEFINITIONS.SEGMENT3 AS POS_DEF_SEGMENT3,
      PER_POSITION_DEFINITIONS.SEGMENT4 AS POS_DEF_SEGMENT4
    FROM
      PER_ALL_POSITIONS
      LEFT JOIN PER_POSITION_DEFINITIONS ON PER_POSITION_DEFINITIONS.POSITION_DEFINITION_ID =
        PER_ALL_POSITIONS.POSITION_DEFINITION_ID
    """
    so_curs.execute("DROP TABLE IF EXISTS X000_POSITIONS")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: X000_POSITIONS")

    if "ACAD_SUPP" not in funccsv.get_colnames_sqlite(so_curs,"X000_POSITIONS"):
        so_curs.execute("ALTER TABLE X000_POSITIONS ADD COLUMN ACAD_SUPP TEXT;")
        so_curs.execute("UPDATE X000_POSITIONS " + """
                        SET ACAD_SUPP = 
                        CASE
                           WHEN POS_DEF_SEGMENT4 = "1" THEN "Academic"
                           ELSE "Support"
                        END
                        ;""")
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMNS: ACAD_SUPP")

    """ ****************************************************************************
    BUILD JOBS
    *****************************************************************************"""
    print("BUILD JOBS")
    funcfile.writelog("BUILD JOBS")

    print("Build jobs...")
    s_sql = "CREATE TABLE X000_JOBS AS " + """
    SELECT
      PER_JOBS.JOB_ID,
      PER_JOBS.DATE_FROM,
      PER_JOBS.DATE_TO,
      PER_JOBS.JOB_DEFINITION_ID,
      PER_JOB_DEFINITIONS.SEGMENT1 AS JOB_NAME,
      PER_JOBS.NAME AS JOB_COMB,
      PER_JOB_DEFINITIONS.SEGMENT2 AS JOB_SEGMENT,
      PER_JOBS.ATTRIBUTE1,
      PER_JOBS.CREATION_DATE,
      PER_JOBS.CREATED_BY,
      PER_JOBS.LAST_UPDATE_DATE,
      PER_JOBS.LAST_UPDATED_BY,
      PER_JOBS.LAST_UPDATE_LOGIN,
      PER_JOBS.JOB_GROUP_ID,
      PER_JOB_DEFINITIONS.ID_FLEX_NUM
    FROM
      PER_JOBS
      LEFT JOIN PER_JOB_DEFINITIONS ON PER_JOB_DEFINITIONS.JOB_DEFINITION_ID = PER_JOBS.JOB_DEFINITION_ID
    ORDER BY
      PER_JOBS.JOB_ID  
    """
    so_curs.execute("DROP TABLE IF EXISTS X000_JOBS")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: X000_JOBS")

    # Calc grade name field
    if "JOB_SEGMENT_NAME" not in funccsv.get_colnames_sqlite(so_curs,"X000_JOBS"):
        so_curs.execute("ALTER TABLE X000_JOBS ADD JOB_SEGMENT_NAME TEXT;")
        so_curs.execute("UPDATE X000_JOBS SET JOB_SEGMENT_NAME = SUBSTR(JOB_COMB,INSTR(JOB_COMB,'~')+1,60);")
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMNS: JOB_SEGMENT_NAME")

    """ ****************************************************************************
    BUILD ORGANIZATION
    *****************************************************************************"""
    print("BUILD ORGANIZATION")
    funcfile.writelog("BUILD ORGANIZATION")

    print("Build organization...")
    s_sql = "CREATE TABLE X000_ORGANIZATION AS " + """
    SELECT
      HR_ALL_ORGANIZATION_UNITS.ORGANIZATION_ID,
      HR_ALL_ORGANIZATION_UNITS.LOCATION_ID,
      HR_ALL_ORGANIZATION_UNITS.DATE_FROM,
      HR_ALL_ORGANIZATION_UNITS.DATE_TO,
      HR_ALL_ORGANIZATION_UNITS.TYPE AS ORG_TYPE,
      HR_ALL_ORGANIZATION_UNITS.ATTRIBUTE_CATEGORY AS ORG_TYPE_DESC,
      HR_ALL_ORGANIZATION_UNITS.ATTRIBUTE1,
      HR_ALL_ORGANIZATION_UNITS.ATTRIBUTE2 AS ORG_NAAM,
      HR_ALL_ORGANIZATION_UNITS.NAME AS OE_CODE,
      HR_ORGANIZATION_INFORMATION.ORG_INFORMATION1 AS ORG_NAME,
      HR_ALL_ORGANIZATION_UNITS.CREATION_DATE,
      HR_ALL_ORGANIZATION_UNITS.CREATED_BY,
      HR_ALL_ORGANIZATION_UNITS.LAST_UPDATE_DATE,
      HR_ALL_ORGANIZATION_UNITS.LAST_UPDATED_BY,
      HR_ALL_ORGANIZATION_UNITS.LAST_UPDATE_LOGIN,
      HR_ALL_ORGANIZATION_UNITS.BUSINESS_GROUP_ID,
      HR_ALL_ORGANIZATION_UNITS.COST_ALLOCATION_KEYFLEX_ID,
      HR_ALL_ORGANIZATION_UNITS.SOFT_CODING_KEYFLEX_ID,
      OWN_HR_LOOKUPS_MAILTO.LOOKUP_DESCRIPTION AS MAILTO
    FROM
      HR_ALL_ORGANIZATION_UNITS
      LEFT JOIN HR_ORGANIZATION_INFORMATION ON HR_ORGANIZATION_INFORMATION.ORGANIZATION_ID = HR_ALL_ORGANIZATION_UNITS.ORGANIZATION_ID
        AND HR_ORGANIZATION_INFORMATION.ORG_INFORMATION_CONTEXT = 'NWU_ORG_INFO'
      LEFT JOIN X000_OWN_HR_LOOKUPS OWN_HR_LOOKUPS_MAILTO ON OWN_HR_LOOKUPS_MAILTO.LOOKUP_CODE = HR_ALL_ORGANIZATION_UNITS.NAME
        AND OWN_HR_LOOKUPS_MAILTO.LOOKUP = 'CHECK_OFFICER'
    """
    so_curs.execute("DROP TABLE IF EXISTS X000_ORGANIZATION")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: X000_ORGANIZATION")

    # Build organization structure step 1*******************************************
    print("Build organization structure step 1...")
    sr_file = "X000_ORG_STRUCT_1"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      PER_ORG_STRUCTURE_ELEMENTS.ORG_STRUCTURE_ELEMENT_ID,
      PER_ORG_STRUCTURE_ELEMENTS.BUSINESS_GROUP_ID,
      PER_ORG_STRUCTURE_ELEMENTS.ORG_STRUCTURE_VERSION_ID,
      PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_CHILD AS ORG1,
      PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_PARENT AS ORG2
    FROM
      PER_ORG_STRUCTURE_ELEMENTS
    WHERE
      PER_ORG_STRUCTURE_ELEMENTS.ORG_STRUCTURE_VERSION_ID = 61
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)

    # Build organization structure step 2*******************************************
    print("Build organization structure step 2...")
    sr_file = "X000_ORG_STRUCT_2"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      X000_ORG_STRUCT_1.ORG_STRUCTURE_ELEMENT_ID,
      X000_ORG_STRUCT_1.BUSINESS_GROUP_ID,
      X000_ORG_STRUCT_1.ORG_STRUCTURE_VERSION_ID,
      X000_ORG_STRUCT_1.ORG1,
      X000_ORG_STRUCT_1.ORG2,
      PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_PARENT AS ORG3
    FROM
      X000_ORG_STRUCT_1
      LEFT JOIN PER_ORG_STRUCTURE_ELEMENTS ON PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_CHILD = X000_ORG_STRUCT_1.ORG2
    GROUP BY
      X000_ORG_STRUCT_1.ORG1
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)

    # Build organization structure step 3*******************************************
    print("Build organization structure step 3...")
    sr_file = "X000_ORG_STRUCT_3"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      X000_ORG_STRUCT_2.ORG_STRUCTURE_ELEMENT_ID,
      X000_ORG_STRUCT_2.BUSINESS_GROUP_ID,
      X000_ORG_STRUCT_2.ORG_STRUCTURE_VERSION_ID,
      X000_ORG_STRUCT_2.ORG1,
      X000_ORG_STRUCT_2.ORG2,
      X000_ORG_STRUCT_2.ORG3,
      PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_PARENT AS ORG4
    FROM
      X000_ORG_STRUCT_2
      LEFT JOIN PER_ORG_STRUCTURE_ELEMENTS ON PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_CHILD = X000_ORG_STRUCT_2.ORG3
    GROUP BY
      X000_ORG_STRUCT_2.ORG1  
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)

    # Build organization structure step 4*******************************************
    print("Build organization structure step 4...")
    sr_file = "X000_ORG_STRUCT_4"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      X000_ORG_STRUCT_3.ORG_STRUCTURE_ELEMENT_ID,
      X000_ORG_STRUCT_3.BUSINESS_GROUP_ID,
      X000_ORG_STRUCT_3.ORG_STRUCTURE_VERSION_ID,
      X000_ORG_STRUCT_3.ORG1,
      X000_ORG_STRUCT_3.ORG2,
      X000_ORG_STRUCT_3.ORG3,
      X000_ORG_STRUCT_3.ORG4,
      PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_PARENT AS ORG5
    FROM
      X000_ORG_STRUCT_3
      LEFT JOIN PER_ORG_STRUCTURE_ELEMENTS ON PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_CHILD = X000_ORG_STRUCT_3.ORG4
    GROUP BY
      X000_ORG_STRUCT_3.ORG1  
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)

    # Build organization structure step 5*******************************************
    print("Build organization structure step 5...")
    sr_file = "X000_ORG_STRUCT_5"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      X000_ORG_STRUCT_4.ORG_STRUCTURE_ELEMENT_ID,
      X000_ORG_STRUCT_4.BUSINESS_GROUP_ID,
      X000_ORG_STRUCT_4.ORG_STRUCTURE_VERSION_ID,
      X000_ORG_STRUCT_4.ORG1,
      X000_ORG_STRUCT_4.ORG2,
      X000_ORG_STRUCT_4.ORG3,
      X000_ORG_STRUCT_4.ORG4,
      X000_ORG_STRUCT_4.ORG5,  
      PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_PARENT AS ORG6
    FROM
      X000_ORG_STRUCT_4
      LEFT JOIN PER_ORG_STRUCTURE_ELEMENTS ON PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_CHILD = X000_ORG_STRUCT_4.ORG5
    GROUP BY
      X000_ORG_STRUCT_4.ORG1  
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)

    # Build organization structure step 6*******************************************
    print("Build organization structure step 6...")
    sr_file = "X000_ORG_STRUCT_6"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      X000_ORG_STRUCT_5.ORG_STRUCTURE_ELEMENT_ID,
      X000_ORG_STRUCT_5.BUSINESS_GROUP_ID,
      X000_ORG_STRUCT_5.ORG_STRUCTURE_VERSION_ID,
      X000_ORG_STRUCT_5.ORG1,
      X000_ORG_STRUCT_5.ORG2,
      X000_ORG_STRUCT_5.ORG3,
      X000_ORG_STRUCT_5.ORG4,
      X000_ORG_STRUCT_5.ORG5,
      X000_ORG_STRUCT_5.ORG6,    
      PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_PARENT AS ORG7
    FROM
      X000_ORG_STRUCT_5
      LEFT JOIN PER_ORG_STRUCTURE_ELEMENTS ON PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_CHILD = X000_ORG_STRUCT_5.ORG6
    GROUP BY
      X000_ORG_STRUCT_5.ORG1  
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)

    # Build organization structure step 7*******************************************
    print("Build organization structure step 7...")
    sr_file = "X000_ORG_STRUCT_7"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      X000_ORG_STRUCT_6.ORG_STRUCTURE_ELEMENT_ID,
      X000_ORG_STRUCT_6.BUSINESS_GROUP_ID,
      X000_ORG_STRUCT_6.ORG_STRUCTURE_VERSION_ID,
      X000_ORG_STRUCT_6.ORG1,
      X000_ORG_STRUCT_6.ORG2,
      X000_ORG_STRUCT_6.ORG3,
      X000_ORG_STRUCT_6.ORG4,
      X000_ORG_STRUCT_6.ORG5,  
      X000_ORG_STRUCT_6.ORG6,
      X000_ORG_STRUCT_6.ORG7,    
      PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_PARENT AS ORG8
    FROM
      X000_ORG_STRUCT_6
      LEFT JOIN PER_ORG_STRUCTURE_ELEMENTS ON PER_ORG_STRUCTURE_ELEMENTS.ORGANIZATION_ID_CHILD = X000_ORG_STRUCT_6.ORG7
    GROUP BY
      X000_ORG_STRUCT_6.ORG1  
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)

    # Build organization structure *************************************************
    print("Build organization structure...")
    sr_file = "X000_ORGANIZATION_STRUCT"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X000_ORG_STRUCT_7.ORG_STRUCTURE_ELEMENT_ID,
      X000_ORG_STRUCT_7.BUSINESS_GROUP_ID,
      X000_ORG_STRUCT_7.ORG_STRUCTURE_VERSION_ID,
      X000_ORG_STRUCT_7.ORG1,
      ORG1.ORG_TYPE AS ORG1_TYPE,
      ORG1.ORG_TYPE_DESC AS ORG1_TYPE_DESC,
      ORG1.OE_CODE AS ORG1_OE_CODE,
      ORG1.ORG_NAME AS ORG1_NAME,
      ORG1.ORG_NAAM AS ORG1_NAAM,
      X000_ORG_STRUCT_7.ORG2,
      ORG2.ORG_TYPE AS ORG2_TYPE,
      ORG2.ORG_TYPE_DESC AS ORG2_TYPE_DESC,
      ORG2.OE_CODE AS ORG2_OE_CODE,
      ORG2.ORG_NAME AS ORG2_NAME,
      ORG2.ORG_NAAM AS ORG2_NAAM,
      X000_ORG_STRUCT_7.ORG3,
      ORG3.ORG_TYPE AS ORG3_TYPE,
      ORG3.ORG_TYPE_DESC AS ORG3_TYPE_DESC,
      ORG3.OE_CODE AS ORG3_OE_CODE,
      ORG3.ORG_NAME AS ORG3_NAME,
      ORG3.ORG_NAAM AS ORG3_NAAM,
      X000_ORG_STRUCT_7.ORG4,
      ORG4.ORG_TYPE AS ORG4_TYPE,
      ORG4.ORG_TYPE_DESC AS ORG4_TYPE_DESC,
      ORG4.OE_CODE AS ORG4_OE_CODE,
      ORG4.ORG_NAME AS ORG4_NAME,
      ORG4.ORG_NAAM AS ORG4_NAAM,
      X000_ORG_STRUCT_7.ORG5,
      ORG5.ORG_TYPE AS ORG5_TYPE,
      ORG5.ORG_TYPE_DESC AS ORG5_TYPE_DESC,
      ORG5.OE_CODE AS ORG5_OE_CODE,
      ORG5.ORG_NAME AS ORG5_NAME,
      ORG5.ORG_NAAM AS ORG5_NAAM,
      X000_ORG_STRUCT_7.ORG6,
      ORG6.ORG_TYPE AS ORG6_TYPE,
      ORG6.ORG_TYPE_DESC AS ORG6_TYPE_DESC,
      ORG6.OE_CODE AS ORG6_OE_CODE,
      ORG6.ORG_NAME AS ORG6_NAME,
      ORG6.ORG_NAAM AS ORG6_NAAM,
      X000_ORG_STRUCT_7.ORG7,
      ORG7.ORG_TYPE AS ORG7_TYPE,
      ORG7.ORG_TYPE_DESC AS ORG7_TYPE_DESC,
      ORG7.OE_CODE AS ORG7_OE_CODE,
      ORG7.ORG_NAME AS ORG7_NAME,
      ORG7.ORG_NAAM AS ORG7_NAAM,
      X000_ORG_STRUCT_7.ORG8,
      ORG8.ORG_TYPE AS ORG8_TYPE,
      ORG8.ORG_TYPE_DESC AS ORG8_TYPE_DESC,
      ORG8.OE_CODE AS ORG8_OE_CODE,
      ORG8.ORG_NAME AS ORG8_NAME,
      ORG8.ORG_NAAM AS ORG8_NAAM
    FROM
      X000_ORG_STRUCT_7
      LEFT JOIN X000_ORGANIZATION ORG1 ON ORG1.ORGANIZATION_ID = X000_ORG_STRUCT_7.ORG1
      LEFT JOIN X000_ORGANIZATION ORG2 ON ORG2.ORGANIZATION_ID = X000_ORG_STRUCT_7.ORG2
      LEFT JOIN X000_ORGANIZATION ORG3 ON ORG3.ORGANIZATION_ID = X000_ORG_STRUCT_7.ORG3
      LEFT JOIN X000_ORGANIZATION ORG4 ON ORG4.ORGANIZATION_ID = X000_ORG_STRUCT_7.ORG4
      LEFT JOIN X000_ORGANIZATION ORG5 ON ORG5.ORGANIZATION_ID = X000_ORG_STRUCT_7.ORG5
      LEFT JOIN X000_ORGANIZATION ORG6 ON ORG6.ORGANIZATION_ID = X000_ORG_STRUCT_7.ORG6
      LEFT JOIN X000_ORGANIZATION ORG7 ON ORG7.ORGANIZATION_ID = X000_ORG_STRUCT_7.ORG7
      LEFT JOIN X000_ORGANIZATION ORG8 ON ORG8.ORGANIZATION_ID = X000_ORG_STRUCT_7.ORG8
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    if "FACULTY" not in funccsv.get_colnames_sqlite(so_curs,sr_file):
        so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN FACULTY TEXT;")
        so_curs.execute("UPDATE " + sr_file + """
                        SET FACULTY = 
                        CASE
                           WHEN ORG1_TYPE = "FAC" THEN ORG1_NAME
                           WHEN ORG2_TYPE = "FAC" THEN ORG2_NAME
                           WHEN ORG3_TYPE = "FAC" THEN ORG3_NAME
                           WHEN ORG4_TYPE = "FAC" THEN ORG4_NAME
                           WHEN ORG5_TYPE = "FAC" THEN ORG5_NAME
                           WHEN ORG6_TYPE = "FAC" THEN ORG6_NAME
                           WHEN ORG7_TYPE = "FAC" THEN ORG7_NAME
                           WHEN ORG8_TYPE = "FAC" THEN ORG8_NAME
                           ELSE ""
                        END
                        ;""")
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMNS: FACULTY")

    if "DIVISION" not in funccsv.get_colnames_sqlite(so_curs,sr_file):
        so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN DIVISION TEXT;")
        so_curs.execute("UPDATE " + sr_file + """
                        SET DIVISION = 
                        CASE
                           WHEN ORG1_TYPE = "DIV" THEN ORG1_NAME
                           WHEN ORG2_TYPE = "DIV" THEN ORG2_NAME
                           WHEN ORG3_TYPE = "DIV" THEN ORG3_NAME
                           WHEN ORG4_TYPE = "DIV" THEN ORG4_NAME
                           WHEN ORG5_TYPE = "DIV" THEN ORG5_NAME
                           WHEN ORG6_TYPE = "DIV" THEN ORG6_NAME
                           WHEN ORG7_TYPE = "DIV" THEN ORG7_NAME
                           WHEN ORG8_TYPE = "DIV" THEN ORG8_NAME
                           ELSE ""
                        END
                        ;""")
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMNS: DIVISION")

    """ ****************************************************************************
    BUILD BANK ACCOUNTS
    *****************************************************************************"""
    print("BUILD BANK ACCOUNTS")
    funcfile.writelog("BUILD BANK ACCOUNTS")

    # BUILD PERSONAL PAY BANK ACCOUNT LIST
    print("Build personal pay bank account number list...")
    sr_file = "X000_PAY_ACCOUNTS"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    Select
        PAYM.ASSIGNMENT_ID,
        PAYM.EFFECTIVE_START_DATE,
        PAYM.EFFECTIVE_END_DATE,
        PAYM.PERSONAL_PAYMENT_METHOD_ID,
        PAYM.BUSINESS_GROUP_ID,
        PAYM.ORG_PAYMENT_METHOD_ID,
        PAYM.PPM_INFORMATION_CATEGORY,
        PAYM.PPM_INFORMATION1,
        PAYM.CREATED_BY,
        PAYM.CREATION_DATE,
        PAYM.LAST_UPDATE_DATE,
        PAYM.LAST_UPDATED_BY,
        PAYM.EXTERNAL_ACCOUNT_ID,
        EXTA.TERRITORY_CODE,
        EXTA.SEGMENT1 As ACC_BRANCH,
        EXTA.SEGMENT2 As ACC_TYPE_CODE,
        HRTY.MEANING As ACC_TYPE,
        EXTA.SEGMENT3 As ACC_NUMBER,
        EXTA.SEGMENT4 As ACC_HOLDER,
        EXTA.SEGMENT5 As ACC_UNKNOWN,
        EXTA.SEGMENT6 As ACC_RELATION_CODE,
        HRRE.MEANING As ACC_RELATION
    From
        PAY_PERSONAL_PAYMENT_METHODS_F PAYM Inner Join
        PAY_EXTERNAL_ACCOUNTS EXTA On EXTA.EXTERNAL_ACCOUNT_ID = PAYM.EXTERNAL_ACCOUNT_ID Left Join
        HR_LOOKUPS HRTY On HRTY.LOOKUP_TYPE = 'ZA_ACCOUNT_TYPE' And HRTY.LOOKUP_CODE = EXTA.SEGMENT2 Left Join
        HR_LOOKUPS HRRE On HRRE.LOOKUP_TYPE = 'ZA_ACCOUNT_HOLDER_RELATION' And HRRE.LOOKUP_CODE = EXTA.SEGMENT6
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)    

    """ ****************************************************************************
    BUILD PERIODS OF SERVICE
    *****************************************************************************"""
    print("BUILD PERIODS OF SERVICE")
    funcfile.writelog("BUILD PERIODS OF SERVICE")

    # PER PERIODS OF SERVICE
    print("Build per periods of service...")
    s_sql = "CREATE VIEW X000_PER_PERIODS_OF_SERVICE AS " + """
    SELECT
      PER_PERIODS_OF_SERVICE.PERIOD_OF_SERVICE_ID,
      PER_PERIODS_OF_SERVICE.BUSINESS_GROUP_ID,
      PER_PERIODS_OF_SERVICE.PERSON_ID,
      PER_PERIODS_OF_SERVICE.DATE_START,
      PER_PERIODS_OF_SERVICE.ACCEPTED_TERMINATION_DATE,
      PER_PERIODS_OF_SERVICE.ACTUAL_TERMINATION_DATE,
      PER_PERIODS_OF_SERVICE.FINAL_PROCESS_DATE,
      PER_PERIODS_OF_SERVICE.LAST_STANDARD_PROCESS_DATE,
      PER_PERIODS_OF_SERVICE.LEAVING_REASON,
      HR_LOOKUPS.MEANING AS LEAVE_REASON_DESCRIP,
      PER_PERIODS_OF_SERVICE.LAST_UPDATE_DATE,
      PER_PERIODS_OF_SERVICE.LAST_UPDATED_BY,
      PER_PERIODS_OF_SERVICE.LAST_UPDATE_LOGIN,
      PER_PERIODS_OF_SERVICE.CREATED_BY,
      PER_PERIODS_OF_SERVICE.CREATION_DATE,
      PER_PERIODS_OF_SERVICE.COMMENTS
    FROM
      PER_PERIODS_OF_SERVICE
      LEFT JOIN HR_LOOKUPS ON HR_LOOKUPS.LOOKUP_CODE = PER_PERIODS_OF_SERVICE.LEAVING_REASON AND
       HR_LOOKUPS.LOOKUP_TYPE = 'LEAV_REAS'  
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_PER_PERIODS_OF_SERVICE")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_PER_PERIODS_OF_SERVICE")

    """ ****************************************************************************
    BUILD ASSIGNMENTS
    *****************************************************************************"""
    print("BUILD ASSIGNMENTS")
    funcfile.writelog("BUILD ASSIGNMENTS")

    # BUILD ASSIGNMENTS
    print("Build assignments...")
    s_sql = "CREATE VIEW X000_PER_ALL_ASSIGNMENTS AS " + """
    SELECT
      PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_ID AS ASS_ID,
      PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_NUMBER,
      PER_ALL_ASSIGNMENTS_F.PERSON_ID,
      PER_ALL_ASSIGNMENTS_F.SUPERVISOR_ID,
      PER_ALL_ASSIGNMENTS_F.EMPLOYMENT_CATEGORY,
      PER_ALL_ASSIGNMENTS_F.PRIMARY_FLAG,
      PER_ALL_ASSIGNMENTS_F.EFFECTIVE_START_DATE,
      PER_ALL_ASSIGNMENTS_F.EFFECTIVE_END_DATE,
      PER_ALL_ASSIGNMENTS_F.PERIOD_OF_SERVICE_ID,
      X000_PER_PERIODS_OF_SERVICE.DATE_START AS SERVICE_DATE_START,
      X000_PER_PERIODS_OF_SERVICE.ACTUAL_TERMINATION_DATE AS SERVICE_DATE_ACTUAL_TERMINATION,
      X000_PER_PERIODS_OF_SERVICE.LAST_STANDARD_PROCESS_DATE AS SERVICE_DATE_STD_PROCESS,
      X000_PER_PERIODS_OF_SERVICE.FINAL_PROCESS_DATE AS SERVICE_FINAL_PROCESS,
      X000_PER_PERIODS_OF_SERVICE.LEAVING_REASON,
      X000_PER_PERIODS_OF_SERVICE.LEAVE_REASON_DESCRIP,
      PER_ALL_ASSIGNMENTS_F.GRADE_ID,
      X000_GRADES.GRADE,
      X000_GRADES.GRADE_NAME,
      X000_GRADES.GRADE_CALC,  
      PER_ALL_ASSIGNMENTS_F.POSITION_ID,
      X000_POSITIONS.POSITION,
      X000_POSITIONS.POSITION_NAME,
      X000_POSITIONS.ACAD_SUPP,
      PER_ALL_ASSIGNMENTS_F.JOB_ID,
      X000_JOBS.JOB_NAME,
      X000_JOBS.JOB_SEGMENT_NAME,
      PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_STATUS_TYPE_ID,
      PER_ALL_ASSIGNMENTS_F.LOCATION_ID,
      HR_LOCATIONS_ALL.LOCATION_CODE,
      HR_LOCATIONS_ALL.DESCRIPTION AS LOCATION_DESCRIPTION,
      PER_ALL_ASSIGNMENTS_F.ORGANIZATION_ID,
      X000_ORGANIZATION.ORG_TYPE,
      X000_ORGANIZATION.ORG_TYPE_DESC,
      X000_ORGANIZATION.OE_CODE,
      X000_ORGANIZATION.ORG_NAME,
      X000_ORGANIZATION.MAILTO,
      X000_ORGANIZATION_STRUCT.FACULTY,
      X000_ORGANIZATION_STRUCT.DIVISION,      
      PER_ALL_ASSIGNMENTS_F.ASS_ATTRIBUTE1 AS ASS_WEEK_LEN,
      PER_ALL_ASSIGNMENTS_F.ASS_ATTRIBUTE2,
      PER_ALL_ASSIGNMENTS_F.PEOPLE_GROUP_ID,
      PAY_PEOPLE_GROUPS.GROUP_NAME AS "LEAVE_CODE"
    FROM
      PER_ALL_ASSIGNMENTS_F
      LEFT JOIN X000_PER_PERIODS_OF_SERVICE ON X000_PER_PERIODS_OF_SERVICE.PERIOD_OF_SERVICE_ID =
        PER_ALL_ASSIGNMENTS_F.PERIOD_OF_SERVICE_ID
      LEFT JOIN X000_GRADES ON X000_GRADES.GRADE_ID = PER_ALL_ASSIGNMENTS_F.GRADE_ID
      LEFT JOIN X000_POSITIONS ON X000_POSITIONS.POSITION_ID = PER_ALL_ASSIGNMENTS_F.POSITION_ID
      LEFT JOIN X000_JOBS ON X000_JOBS.JOB_ID = PER_ALL_ASSIGNMENTS_F.JOB_ID
      LEFT JOIN HR_LOCATIONS_ALL ON HR_LOCATIONS_ALL.LOCATION_ID = PER_ALL_ASSIGNMENTS_F.LOCATION_ID
      LEFT JOIN X000_ORGANIZATION ON X000_ORGANIZATION.ORGANIZATION_ID = PER_ALL_ASSIGNMENTS_F.ORGANIZATION_ID
      LEFT JOIN X000_ORGANIZATION_STRUCT ON X000_ORGANIZATION_STRUCT.ORG1 = PER_ALL_ASSIGNMENTS_F.ORGANIZATION_ID
      LEFT JOIN PAY_PEOPLE_GROUPS ON PAY_PEOPLE_GROUPS.PEOPLE_GROUP_ID = PER_ALL_ASSIGNMENTS_F.PEOPLE_GROUP_ID
    ORDER BY
      ASSIGNMENT_NUMBER,
      PER_ALL_ASSIGNMENTS_F.EFFECTIVE_START_DATE
    """

    """
    WHERE
      (PER_ALL_ASSIGNMENTS_F.EFFECTIVE_END_DATE >= Date('%CYEARB%') AND
      PER_ALL_ASSIGNMENTS_F.EFFECTIVE_END_DATE <= Date('%CYEARE%')) OR
      (PER_ALL_ASSIGNMENTS_F.EFFECTIVE_START_DATE >= Date('%CYEARB%') AND
      PER_ALL_ASSIGNMENTS_F.EFFECTIVE_START_DATE <= Date('%CYEARE%')) OR
      (PER_ALL_ASSIGNMENTS_F.EFFECTIVE_END_DATE >= Date('%CYEARE%') AND
      PER_ALL_ASSIGNMENTS_F.EFFECTIVE_START_DATE <= Date('%CYEARB%'))
    """

    so_curs.execute("DROP VIEW IF EXISTS X000_ASSIGNMENTS")
    s_sql = s_sql.replace("%CYEARB%",funcdate.cur_yearbegin())
    s_sql = s_sql.replace("%CYEARE%",funcdate.cur_yearend())
    so_curs.execute("DROP VIEW IF EXISTS X000_PER_ALL_ASSIGNMENTS")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_PER_ALL_ASSIGNMENTS")

    """ ****************************************************************************
    BUILD ALL PEOPLE
    *****************************************************************************"""
    print("BUILD ALL PEOPLE")
    funcfile.writelog("BUILD ALL PEOPLE")

    # PER ALL PEOPLE
    print("Build per all people...")
    s_sql = "CREATE VIEW X000_PER_ALL_PEOPLE AS " + """
    SELECT
      PER_ALL_PEOPLE_F.PERSON_ID,
      PER_ALL_PEOPLE_F.PARTY_ID,
      PER_ALL_PEOPLE_F.EMPLOYEE_NUMBER,
      PER_ALL_PEOPLE_F.FULL_NAME,
      PER_ALL_PEOPLE_F.DATE_OF_BIRTH,
      PER_ALL_PEOPLE_F.SEX,
      PER_ALL_PEOPLE_F.NATIONAL_IDENTIFIER,
      PER_ALL_PEOPLE_F.EFFECTIVE_START_DATE,
      PER_ALL_PEOPLE_F.EFFECTIVE_END_DATE,
      PER_ALL_PEOPLE_F.EMAIL_ADDRESS,
      PER_ALL_PEOPLE_F.TITLE,
      HR_LOOKUPS_TITLE.MEANING AS TITLE_FULL,
      PER_ALL_PEOPLE_F.FIRST_NAME,
      PER_ALL_PEOPLE_F.MIDDLE_NAMES,
      PER_ALL_PEOPLE_F.LAST_NAME,
      PER_ALL_PEOPLE_F.PERSON_TYPE_ID,
      PER_PERSON_TYPES.USER_PERSON_TYPE,
      PER_ALL_PEOPLE_F.MARITAL_STATUS,
      PER_ALL_PEOPLE_F.NATIONALITY,
      HR_LOOKUPS_NATIONALITY.MEANING AS NATIONALITY_NAME,
      PER_ALL_PEOPLE_F.ATTRIBUTE1,
      PER_ALL_PEOPLE_F.ATTRIBUTE2 AS INT_MAIL,
      PER_ALL_PEOPLE_F.ATTRIBUTE3,
      PER_ALL_PEOPLE_F.ATTRIBUTE4 AS KNOWN_NAME,
      PER_ALL_PEOPLE_F.ATTRIBUTE5,
      PER_ALL_PEOPLE_F.ATTRIBUTE6,
      PER_ALL_PEOPLE_F.ATTRIBUTE7,
      PER_ALL_PEOPLE_F.PER_INFORMATION_CATEGORY,
      PER_ALL_PEOPLE_F.PER_INFORMATION1 AS TAX_NUMBER,
      PER_ALL_PEOPLE_F.PER_INFORMATION2,
      PER_ALL_PEOPLE_F.PER_INFORMATION3,
      PER_ALL_PEOPLE_F.PER_INFORMATION4 AS RACE_CODE,
      HR_LOOKUPS_RACE.MEANING AS RACE_DESC,  
      PER_ALL_PEOPLE_F.PER_INFORMATION5,
      PER_ALL_PEOPLE_F.PER_INFORMATION6 AS LANG_CODE,
      HR_LOOKUPS_LANG.MEANING AS LANG_DESC,  
      PER_ALL_PEOPLE_F.PER_INFORMATION7,
      PER_ALL_PEOPLE_F.PER_INFORMATION8,
      PER_ALL_PEOPLE_F.PER_INFORMATION9,
      PER_ALL_PEOPLE_F.PER_INFORMATION10,
      PER_ALL_PEOPLE_F.PER_INFORMATION11,
      PER_ALL_PEOPLE_F.PER_INFORMATION12,
      PER_ALL_PEOPLE_F.PER_INFORMATION13,
      PER_ALL_PEOPLE_F.PER_INFORMATION14,
      PER_ALL_PEOPLE_F.CREATED_BY,
      PER_ALL_PEOPLE_F.CREATION_DATE,
      PER_ALL_PEOPLE_F.LAST_UPDATE_DATE,
      PER_ALL_PEOPLE_F.LAST_UPDATED_BY,
      PER_ALL_PEOPLE_F.LAST_UPDATE_LOGIN,
      PER_ALL_PEOPLE_F.ORIGINAL_DATE_OF_HIRE,
      PER_ALL_PEOPLE_F.START_DATE,
      PER_ALL_PEOPLE_F.DATE_OF_DEATH,
      PER_ALL_PEOPLE_F.RECEIPT_OF_DEATH_CERT_DATE,
      PER_ALL_PEOPLE_F.OBJECT_VERSION_NUMBER,
      PER_ALL_PEOPLE_F.BUSINESS_GROUP_ID,
      PER_ALL_PEOPLE_F.CURRENT_EMP_OR_APL_FLAG,
      PER_ALL_PEOPLE_F.CURRENT_EMPLOYEE_FLAG,
      PER_ALL_PEOPLE_F.DATE_EMPLOYEE_DATA_VERIFIED,
      PER_ALL_PEOPLE_F.RESUME_EXISTS,
      PER_ALL_PEOPLE_F.RESUME_LAST_UPDATED,
      PER_ALL_PEOPLE_F.REGISTERED_DISABLED_FLAG,
      PER_ALL_PEOPLE_F.SECOND_PASSPORT_EXISTS,
      PER_ALL_PEOPLE_F.PREVIOUS_LAST_NAME
    FROM
      PER_ALL_PEOPLE_F
      LEFT JOIN PER_PERSON_TYPES ON PER_PERSON_TYPES.PERSON_TYPE_ID = PER_ALL_PEOPLE_F.PERSON_TYPE_ID
      LEFT JOIN HR_LOOKUPS HR_LOOKUPS_NATIONALITY ON HR_LOOKUPS_NATIONALITY.LOOKUP_CODE = PER_ALL_PEOPLE_F.NATIONALITY AND
        HR_LOOKUPS_NATIONALITY.LOOKUP_TYPE = 'NATIONALITY'  
      LEFT JOIN HR_LOOKUPS HR_LOOKUPS_TITLE ON HR_LOOKUPS_TITLE.LOOKUP_CODE = PER_ALL_PEOPLE_F.TITLE AND
        HR_LOOKUPS_TITLE.LOOKUP_TYPE = 'TITLE'
      LEFT JOIN HR_LOOKUPS HR_LOOKUPS_RACE ON HR_LOOKUPS_RACE.LOOKUP_CODE = PER_ALL_PEOPLE_F.PER_INFORMATION4 AND
        HR_LOOKUPS_RACE.LOOKUP_TYPE = 'ZA_RACE'
      LEFT JOIN HR_LOOKUPS HR_LOOKUPS_LANG ON HR_LOOKUPS_LANG.LOOKUP_CODE = PER_ALL_PEOPLE_F.PER_INFORMATION6 AND
        HR_LOOKUPS_LANG.LOOKUP_TYPE = 'ZA_LANG_PREF'
    ORDER BY
      PER_ALL_PEOPLE_F.EMPLOYEE_NUMBER,
      PER_ALL_PEOPLE_F.EFFECTIVE_START_DATE
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_PER_ALL_PEOPLE")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_PER_ALL_PEOPLE")

    """ ****************************************************************************
    BUILD PERSON TYPES
    *****************************************************************************"""
    print("BUILD PERSON TYPES")
    funcfile.writelog("BUILD PERSON TYPES")

    # BUILD PERSON TYPES ***********************************************************

    print("Build person types...")
    sr_file = "X000_PER_PEOPLE_TYPES"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    SELECT
      PER_PERSON_TYPE_USAGES_F.PERSON_TYPE_USAGE_ID,
      PER_PERSON_TYPE_USAGES_F.PERSON_ID,
      PER_PERSON_TYPE_USAGES_F.PERSON_TYPE_ID,
      PER_PERSON_TYPE_USAGES_F.EFFECTIVE_START_DATE,
      PER_PERSON_TYPE_USAGES_F.EFFECTIVE_END_DATE,
      PER_PERSON_TYPES.ACTIVE_FLAG,
      PER_PERSON_TYPES.DEFAULT_FLAG,
      PER_PERSON_TYPES.SYSTEM_PERSON_TYPE,
      PER_PERSON_TYPES.USER_PERSON_TYPE
    FROM
      PER_PERSON_TYPE_USAGES_F
      LEFT JOIN PER_PERSON_TYPES ON PER_PERSON_TYPES.PERSON_TYPE_ID = PER_PERSON_TYPE_USAGES_F.PERSON_TYPE_ID
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)

    """ ****************************************************************************
    COUNT RECORDS
    *****************************************************************************"""
    print("COUNT RECORDS")
    funcfile.writelog("COUNT RECORDS")

    # ASSIGNMENTS
    print("Count assignments...")
    s_sql = "CREATE VIEW X000_COUNT_ASSIGNMENTS AS" + """
    SELECT
      PER_ALL_ASSIGNMENTS_F.PERSON_ID,
      Count(PER_ALL_ASSIGNMENTS_F.PRIMARY_FLAG) AS COUNT_ASS
    FROM
      PER_ALL_ASSIGNMENTS_F
    GROUP BY
      PER_ALL_ASSIGNMENTS_F.PERSON_ID
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_COUNT_ASSIGNMENTS")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_COUNT_ASSIGNMENTS")

    # PEOPLE
    print("Count people...")
    s_sql = "CREATE VIEW X000_COUNT_PEOPLE AS" + """
    SELECT
      PER_ALL_PEOPLE_F.PERSON_ID,
      Count(PER_ALL_PEOPLE_F.PARTY_ID) AS COUNT_PEO
    FROM
      PER_ALL_PEOPLE_F
    GROUP BY
      PER_ALL_PEOPLE_F.PERSON_ID
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_COUNT_PEOPLE")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_COUNT_PEOPLE")

    # PERIOD OF SERVICE
    print("Count periods of service...")
    s_sql = "CREATE VIEW X000_COUNT_PERIODOS AS" + """
    SELECT
      PER_PERIODS_OF_SERVICE.PERSON_ID,
      Count(PER_PERIODS_OF_SERVICE.BUSINESS_GROUP_ID) AS COUNT_POS
    FROM
      PER_PERIODS_OF_SERVICE
    GROUP BY
      PER_PERIODS_OF_SERVICE.PERSON_ID
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_COUNT_PERIODOS")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_COUNT_PERIODOS")

    # BUILD COUNTS
    print("Build counts...")
    s_sql = "CREATE VIEW X000_COUNTS AS" + """
    SELECT
      X000_COUNT_PEOPLE.PERSON_ID,
      X000_COUNT_PEOPLE.COUNT_PEO,
      X000_COUNT_PERIODOS.COUNT_POS,
      X000_COUNT_ASSIGNMENTS.COUNT_ASS
    FROM
      X000_COUNT_PEOPLE
      LEFT JOIN X000_COUNT_ASSIGNMENTS ON X000_COUNT_PEOPLE.PERSON_ID = X000_COUNT_ASSIGNMENTS.PERSON_ID
      LEFT JOIN X000_COUNT_PERIODOS ON X000_COUNT_PERIODOS.PERSON_ID = X000_COUNT_PEOPLE.PERSON_ID
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_COUNTS")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: X000_COUNTS")

    """
    so_curs.execute("DROP VIEW IF EXISTS X000_COUNT_ASSIGNMENTS")
    funcfile.writelog("%t DROP TABLE: X000_COUNT_ASSIGNMENTS")
    so_curs.execute("DROP VIEW IF EXISTS X000_COUNT_PEOPLE")
    funcfile.writelog("%t DROP TABLE: X000_COUNT_PEOPLE")
    so_curs.execute("DROP VIEW IF EXISTS X000_COUNT_PERIODOS")
    funcfile.writelog("%t DROP TABLE: X000_COUNT_PERIODOS")
    """

    """ ****************************************************************************
    BUILD ADDRESSES AND PHONES
    *****************************************************************************"""
    print("BUILD ADDRESSES")
    funcfile.writelog("BUILD ADDRESSES")

    # 12 BUILD ADDRESSES
    print("Build adresses...")
    s_sql = "CREATE TABLE X000_ADDRESSES AS " + """
    SELECT
      PER_ADDRESSES.ADDRESS_ID,
      PER_ADDRESSES.PERSON_ID,
      PER_ADDRESSES.DATE_FROM,
      PER_ADDRESSES.DATE_TO,
      PER_ADDRESSES.STYLE,
      PER_ADDRESSES.ADDRESS_TYPE,
      PER_ADDRESSES.PRIMARY_FLAG,
      PER_ADDRESSES.ADDRESS_LINE1,
      PER_ADDRESSES.ADDRESS_LINE2,
      PER_ADDRESSES.ADDRESS_LINE3,
      PER_ADDRESSES.POSTAL_CODE,
      PER_ADDRESSES.TOWN_OR_CITY,
      PER_ADDRESSES.COUNTRY AS COUNTRY_CODE,
      HR_LOOKUPS.MEANING AS COUNTRY_LOOKUP,
      PER_ADDRESSES.REGION_1,
      PER_ADDRESSES.REGION_2,
      PER_ADDRESSES.REGION_3,
      PER_ADDRESSES.ADD_INFORMATION14,
      PER_ADDRESSES.ADD_INFORMATION15,
      PER_ADDRESSES.ADD_INFORMATION16,
      PER_ADDRESSES.ADD_INFORMATION17,
      PER_ADDRESSES.ADD_INFORMATION18,
      PER_ADDRESSES.CREATION_DATE,
      PER_ADDRESSES.CREATED_BY,
      PER_ADDRESSES.LAST_UPDATED_BY,
      PER_ADDRESSES.LAST_UPDATE_DATE,
      PER_ADDRESSES.PARTY_ID
    FROM
      PER_ADDRESSES
      LEFT JOIN HR_LOOKUPS ON HR_LOOKUPS.LOOKUP_CODE = PER_ADDRESSES.COUNTRY AND HR_LOOKUPS.LOOKUP_TYPE =
        'GHR_US_POSTAL_COUNTRY_CODE'
    """
    so_curs.execute("DROP TABLE IF EXISTS X000_ADDRESSES")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: X000_ADDRESSES")

    # Calc COUNTRY NAME field
    if "COUNTRY_NAME" not in funccsv.get_colnames_sqlite(so_curs,"X000_ADDRESSES"):
        so_curs.execute("ALTER TABLE X000_ADDRESSES ADD COUNTRY_NAME TEXT;")
        so_curs.execute("UPDATE X000_ADDRESSES " + """
                        SET COUNTRY_NAME = 
                        CASE
                           WHEN LENGTH(COUNTRY_LOOKUP) <> 0 THEN UPPER(COUNTRY_LOOKUP)
                           WHEN LENGTH(COUNTRY_CODE) > 0 AND TYPEOF(COUNTRY_LOOKUP)='null' THEN UPPER(COUNTRY_CODE)
                           ELSE "SOUTH AFRICA"
                        END
                        ;""")
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: COUNTRY_NAME")

    # Calc ADDRESS_STYLE style field
    if "ADDRESS_STYLE" not in funccsv.get_colnames_sqlite(so_curs,"X000_ADDRESSES"):
        so_curs.execute("ALTER TABLE X000_ADDRESSES ADD ADDRESS_STYLE TEXT;")
        so_curs.execute("UPDATE X000_ADDRESSES " + """
                        SET ADDRESS_STYLE = 
                        CASE
                           WHEN STYLE = "ZA_SARS" THEN        UPPER( PRIMARY_FLAG ||'~'|| TRIM(ADDRESS_LINE1||' '||ADDRESS_LINE2)         ||'~'|| TRIM(ADDRESS_LINE3||' '||REGION_1)          ||'~'|| REGION_2      ||'~'|| TOWN_OR_CITY  ||'~'|| POSTAL_CODE ||'~'|| COUNTRY_NAME )
                           WHEN STYLE = "ZA_POST_STREET" THEN UPPER( PRIMARY_FLAG ||'~'|| TRIM(ADD_INFORMATION15||' '||ADD_INFORMATION16) ||'~'|| TRIM(ADD_INFORMATION17||' '||ADDRESS_LINE1) ||'~'|| ADDRESS_LINE2 ||'~'|| ADDRESS_LINE3 ||'~'|| POSTAL_CODE ||'~'|| COUNTRY_NAME )
                           WHEN STYLE = "ZA_POST_POBOX"  THEN UPPER( PRIMARY_FLAG ||'~'||                                                   '~'|| TRIM('PO BOX '||ADDRESS_LINE2)              ||'~'                 ||'~'|| ADDRESS_LINE3 ||'~'|| POSTAL_CODE ||'~'|| COUNTRY_NAME )
                           WHEN STYLE = "ZA_POST_PBAG"   THEN UPPER( PRIMARY_FLAG ||'~'||                                                   '~'|| TRIM('P BAG '||ADDRESS_LINE2)               ||'~'                 ||'~'|| ADDRESS_LINE3 ||'~'|| POSTAL_CODE ||'~'|| COUNTRY_NAME )
                           WHEN STYLE = "ZA_POST_SSERV"  THEN UPPER( PRIMARY_FLAG ||'~'|| REGION_3                                        ||'~'|| TRIM(ADDRESS_LINE1||' '||ADDRESS_LINE2)     ||'~'                 ||'~'|| ADDRESS_LINE3 ||'~'|| POSTAL_CODE ||'~'|| COUNTRY_NAME )
                           ELSE                               UPPER( PRIMARY_FLAG ||'~'||                                                   '~'|| ADDRESS_LINE1                               ||'~'|| ADDRESS_LINE2 ||'~'|| ADDRESS_LINE3 ||'~'|| POSTAL_CODE ||'~'|| COUNTRY_NAME )
                        END
                        ;""")
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: ADDRESS_STYLE")

    # Calc ADDRESS_SARS field
    if "ADDRESS_SARS" not in funccsv.get_colnames_sqlite(so_curs,"X000_ADDRESSES"):
        so_curs.execute("ALTER TABLE X000_ADDRESSES ADD ADDRESS_SARS TEXT;")
        so_curs.execute("UPDATE X000_ADDRESSES " + """
                        SET ADDRESS_SARS = 
                        CASE
                           WHEN STYLE = "ZA_SARS" THEN ADDRESS_STYLE
                           ELSE ''
                        END
                        ;""")
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: ADDRESS_SARS")
        
    # Calc ADDRESS_HOME field
    if "ADDRESS_HOME" not in funccsv.get_colnames_sqlite(so_curs,"X000_ADDRESSES"):
        so_curs.execute("ALTER TABLE X000_ADDRESSES ADD ADDRESS_HOME TEXT;")
        so_curs.execute("UPDATE X000_ADDRESSES " + """
                        SET ADDRESS_HOME = 
                        CASE
                           WHEN STYLE = "ZA_SARS" THEN ''
                           WHEN ADDRESS_TYPE = "H" THEN ADDRESS_STYLE
                           ELSE ''
                        END
                        ;""")
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: ADDRESS_HOME")

    # Calc ADDRESS_POST field
    if "ADDRESS_POST" not in funccsv.get_colnames_sqlite(so_curs,"X000_ADDRESSES"):
        so_curs.execute("ALTER TABLE X000_ADDRESSES ADD ADDRESS_POST TEXT;")
        so_curs.execute("UPDATE X000_ADDRESSES " + """
                        SET ADDRESS_POST = 
                        CASE
                           WHEN STYLE = "ZA_SARS" THEN ''
                           WHEN ADDRESS_TYPE = "P" THEN ADDRESS_STYLE
                           ELSE ''
                        END
                        ;""")
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: ADDRESS_POST")

    # Calc ADDRESS_OTHE field
    if "ADDRESS_OTHE" not in funccsv.get_colnames_sqlite(so_curs,"X000_ADDRESSES"):
        so_curs.execute("ALTER TABLE X000_ADDRESSES ADD ADDRESS_OTHE TEXT;")
        so_curs.execute("UPDATE X000_ADDRESSES " + """
                        SET ADDRESS_OTHE = 
                        CASE
                           WHEN STYLE = "ZA_SARS" THEN ''
                           WHEN ADDRESS_TYPE = "H" THEN ''
                           WHEN ADDRESS_TYPE = "P" THEN ''
                           ELSE ADDRESS_STYLE
                        END
                        ;""")
        so_conn.commit()
        funcfile.writelog("%t ADD COLUMN: ADDRESS_OTHE")

    # 13 Build ADDRESS SARS ********************************************************

    print("Build sars adresses...")

    s_sql = "CREATE VIEW X000_ADDRESS_SARS AS " + """
    SELECT
      X000_ADDRESSES.ADDRESS_ID,
      X000_ADDRESSES.PERSON_ID,
      X000_ADDRESSES.DATE_FROM,
      X000_ADDRESSES.DATE_TO,
      X000_ADDRESSES.ADDRESS_SARS
    FROM
      X000_ADDRESSES
    WHERE
      LENGTH(X000_ADDRESSES.ADDRESS_SARS) > 0
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_ADDRESS_SARS")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_ADDRESS_SARS")

    # 14 Build ADDRESS POST ********************************************************

    print("Build post adresses...")

    s_sql = "CREATE VIEW X000_ADDRESS_POST AS " + """
    SELECT
      X000_ADDRESSES.ADDRESS_ID,
      X000_ADDRESSES.PERSON_ID,
      X000_ADDRESSES.DATE_FROM,
      X000_ADDRESSES.DATE_TO,
      X000_ADDRESSES.ADDRESS_POST
    FROM
      X000_ADDRESSES
    WHERE
      LENGTH(X000_ADDRESSES.ADDRESS_POST) > 0
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_ADDRESS_POST")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_ADDRESS_POST")

    # 15 Build ADDRESS HOME ********************************************************

    print("Build home adresses...")

    s_sql = "CREATE VIEW X000_ADDRESS_HOME AS " + """
    SELECT
      X000_ADDRESSES.ADDRESS_ID,
      X000_ADDRESSES.PERSON_ID,
      X000_ADDRESSES.DATE_FROM,
      X000_ADDRESSES.DATE_TO,
      X000_ADDRESSES.ADDRESS_HOME
    FROM
      X000_ADDRESSES
    WHERE
      LENGTH(X000_ADDRESSES.ADDRESS_HOME) > 0
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_ADDRESS_HOME")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_ADDRESS_HOME")

    # 16 Build ADDRESS OTHE ********************************************************

    print("Build other adresses...")

    s_sql = "CREATE VIEW X000_ADDRESS_OTHE AS " + """
    SELECT
      X000_ADDRESSES.ADDRESS_ID,
      X000_ADDRESSES.PERSON_ID,
      X000_ADDRESSES.DATE_FROM,
      X000_ADDRESSES.DATE_TO,
      X000_ADDRESSES.ADDRESS_OTHE
    FROM
      X000_ADDRESSES
    WHERE
      LENGTH(X000_ADDRESSES.ADDRESS_OTHE) > 0
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_ADDRESS_OTHE")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_ADDRESS_OTHE")

    """
    so_curs.execute("DROP TABLE IF EXISTS X000_ADDRESSES")
    funcfile.writelog("%t DROP TABLE: X000_ADDRESSES")
    """

    # 17 Build PHONE MOBILE CURR ***************************************************

    print("Build current mobile phones...")

    s_sql = "CREATE VIEW X000_PHONE_MOBI_CURR AS " + """
    SELECT
      X001_ASSIGNMENT_CURR.PERSON_ID,
      X001_ASSIGNMENT_CURR.DATE_EMP_LOOKUP,
      PER_PHONES.PHONE_ID,
      PER_PHONES.DATE_FROM,
      PER_PHONES.DATE_TO,
      PER_PHONES.PHONE_TYPE,
      PER_PHONES.PHONE_NUMBER AS PHONE_MOBI
    FROM
      PER_PHONES
      INNER JOIN X001_ASSIGNMENT_CURR ON PER_PHONES.PARENT_ID = X001_ASSIGNMENT_CURR.PERSON_ID AND PER_PHONES.DATE_FROM <=
        X001_ASSIGNMENT_CURR.DATE_EMP_LOOKUP AND PER_PHONES.DATE_TO >= X001_ASSIGNMENT_CURR.DATE_EMP_LOOKUP
    WHERE    
         PER_PHONES.PHONE_TYPE = 'M'
    ORDER BY
      X001_ASSIGNMENT_CURR.PERSON_ID,
      PER_PHONES.DATE_FROM
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_MOBI_CURR")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_PHONE_MOBI_CURR")

    # 18 Build PHONE MOBILE CURR LIST **********************************************

    print("Build current mobile phone list...")

    s_sql = "CREATE VIEW X000_PHONE_MOBI_CURR_LIST AS " + """
    SELECT
      X000_PHONE_MOBI_CURR.PERSON_ID,
      X000_PHONE_MOBI_CURR.DATE_EMP_LOOKUP,
      X000_PHONE_MOBI_CURR.PHONE_ID,
      X000_PHONE_MOBI_CURR.DATE_FROM,
      X000_PHONE_MOBI_CURR.DATE_TO,
      X000_PHONE_MOBI_CURR.PHONE_TYPE,
      X000_PHONE_MOBI_CURR.PHONE_MOBI
    FROM
      X000_PHONE_MOBI_CURR
    GROUP BY
      X000_PHONE_MOBI_CURR.PERSON_ID
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_MOBI_CURR_LIST")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_PHONE_MOBI_CURR_LIST")

    # 19 Build PHONE WORK CURR ***************************************************

    print("Build current work phones...")

    s_sql = "CREATE VIEW X000_PHONE_WORK_CURR AS " + """
    SELECT
      X001_ASSIGNMENT_CURR.PERSON_ID,
      X001_ASSIGNMENT_CURR.DATE_EMP_LOOKUP,
      PER_PHONES.PHONE_ID,
      PER_PHONES.DATE_FROM,
      PER_PHONES.DATE_TO,
      PER_PHONES.PHONE_TYPE,
      PER_PHONES.PHONE_NUMBER AS PHONE_WORK
    FROM
      PER_PHONES
      INNER JOIN X001_ASSIGNMENT_CURR ON PER_PHONES.PARENT_ID = X001_ASSIGNMENT_CURR.PERSON_ID AND PER_PHONES.DATE_FROM <=
        X001_ASSIGNMENT_CURR.DATE_EMP_LOOKUP AND PER_PHONES.DATE_TO >= X001_ASSIGNMENT_CURR.DATE_EMP_LOOKUP
    WHERE    
         PER_PHONES.PHONE_TYPE = 'W1'
    ORDER BY
      X001_ASSIGNMENT_CURR.PERSON_ID,
      PER_PHONES.DATE_FROM
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_WORK_CURR")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_PHONE_WORK_CURR")

    # 20 Build PHONE WORK CURR LIST **********************************************

    print("Build current work phone list...")

    s_sql = "CREATE VIEW X000_PHONE_WORK_CURR_LIST AS " + """
    SELECT
      X000_PHONE_WORK_CURR.PERSON_ID,
      X000_PHONE_WORK_CURR.DATE_EMP_LOOKUP,
      X000_PHONE_WORK_CURR.PHONE_ID,
      X000_PHONE_WORK_CURR.DATE_FROM,
      X000_PHONE_WORK_CURR.DATE_TO,
      X000_PHONE_WORK_CURR.PHONE_TYPE,
      X000_PHONE_WORK_CURR.PHONE_WORK
    FROM
      X000_PHONE_WORK_CURR
    GROUP BY
      X000_PHONE_WORK_CURR.PERSON_ID
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_WORK_CURR_LIST")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_PHONE_WORK_CURR_LIST")

    # 21 Build PHONE HOME CURR ***************************************************

    print("Build current home phones...")

    s_sql = "CREATE VIEW X000_PHONE_HOME_CURR AS " + """
    SELECT
      X001_ASSIGNMENT_CURR.PERSON_ID,
      X001_ASSIGNMENT_CURR.DATE_EMP_LOOKUP,
      PER_PHONES.PHONE_ID,
      PER_PHONES.DATE_FROM,
      PER_PHONES.DATE_TO,
      PER_PHONES.PHONE_TYPE,
      PER_PHONES.PHONE_NUMBER AS PHONE_HOME
    FROM
      PER_PHONES
      INNER JOIN X001_ASSIGNMENT_CURR ON PER_PHONES.PARENT_ID = X001_ASSIGNMENT_CURR.PERSON_ID AND PER_PHONES.DATE_FROM <=
        X001_ASSIGNMENT_CURR.DATE_EMP_LOOKUP AND PER_PHONES.DATE_TO >= X001_ASSIGNMENT_CURR.DATE_EMP_LOOKUP
    WHERE    
         PER_PHONES.PHONE_TYPE = 'H1'
    ORDER BY
      X001_ASSIGNMENT_CURR.PERSON_ID,
      PER_PHONES.DATE_FROM
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_HOME_CURR")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_PHONE_HOME_CURR")

    # 22 Build PHONE HOME CURR LIST **********************************************

    print("Build current home phone list...")

    s_sql = "CREATE VIEW X000_PHONE_HOME_CURR_LIST AS " + """
    SELECT
      X000_PHONE_HOME_CURR.PERSON_ID,
      X000_PHONE_HOME_CURR.DATE_EMP_LOOKUP,
      X000_PHONE_HOME_CURR.PHONE_ID,
      X000_PHONE_HOME_CURR.DATE_FROM,
      X000_PHONE_HOME_CURR.DATE_TO,
      X000_PHONE_HOME_CURR.PHONE_TYPE,
      X000_PHONE_HOME_CURR.PHONE_HOME
    FROM
      X000_PHONE_HOME_CURR
    GROUP BY
      X000_PHONE_HOME_CURR.PERSON_ID
    """
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_HOME_CURR_LIST")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD VIEW: X000_PHONE_HOME_CURR_LIST")

    """ ****************************************************************************
    BUILD ASSIGNMENTS AND PEOPLE
    *****************************************************************************"""
    print("BUILD ASSIGNMENTS AND PEOPLE")
    funcfile.writelog("BUILD ASSIGNMENTS AND PEOPLE")    

    # Build current year assignment round 1 ******************************************
    funcpeople.Assign01(so_conn,"X001_ASSIGNMENT_CURR_01",funcdate.cur_yearbegin(),funcdate.cur_yearend(),funcdate.today(),"Build current year assignments 1...")
    # Build current year assignment round 2 ******************************************
    funcpeople.Assign02(so_conn,"X001_ASSIGNMENT_CURR","X001_ASSIGNMENT_CURR_01","Build current year assignments 2...")
    if l_export == True:
        # Data export
        sr_file = "X001_ASSIGNMENT_CURR"
        sr_filet = sr_file
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "Assignment_001_all_"
        sx_filet = sx_file + funcdate.cur_year()
        print("Export current year assignments..." + sx_path + sx_filet)
        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        # Write the data
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_filet)

    # Build previous year assignment round 1 ******************************************
    funcpeople.Assign01(so_conn,"X001_ASSIGNMENT_PREV_01",funcdate.prev_yearbegin(),funcdate.prev_yearend(),funcdate.prev_yearend(),"Build previous year assignments 1...")
    # Build previous year assignment round 2 ******************************************
    funcpeople.Assign02(so_conn,"X001_ASSIGNMENT_PREV","X001_ASSIGNMENT_PREV_01","Build previous year assignments 2...")

    # Build PEOPLE CURRENT ******************************************************
    funcpeople.People01(so_conn,"X002_PEOPLE_CURR","X001_ASSIGNMENT_CURR","CURR","Build current people...","Y")
    if l_export == True:
        # Data export
        sr_file = "X002_PEOPLE_CURR"
        sr_filet = sr_file
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "People_002_all_"
        sx_filet = sx_file + funcdate.cur_year()
        print("Export current year people..." + sx_path + sx_filet)
        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        # Write the data
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_filet)

    # Build PEOPLE CURRENT ******************************************************
    funcpeople.People01(so_conn,"X002_PEOPLE_CURR_YEAR","X001_ASSIGNMENT_CURR","CURR","Build current year people ...","N")

    # Build PEOPLE PREVIOUS YEAR ************************************************
    funcpeople.People01(so_conn,"X002_PEOPLE_PREV_YEAR","X001_ASSIGNMENT_PREV","CURR","Build previous year people...","N")

    # Build PEOPLE ORGANIZATION STRUCTURE REF **********************************
    print("Build reference people organogram...")
    s_sql = "CREATE TABLE X003_PEOPLE_ORGA_REF AS " + """
    SELECT
      X002_PEOPLE_CURR.EMPLOYEE_NUMBER AS employee_one,
      X002_PEOPLE_CURR.NAME_LIST AS name_list_one,
      X002_PEOPLE_CURR.KNOWN_NAME AS known_name_one,
      X002_PEOPLE_CURR.POSITION_FULL AS position_full_one,
      X002_PEOPLE_CURR.LOCATION_DESCRIPTION AS location_description_one,
      X002_PEOPLE_CURR.DIVISION AS division_one,
      X002_PEOPLE_CURR.FACULTY AS faculty_one,
      X002_PEOPLE_CURR.EMAIL_ADDRESS AS email_address_one,
      X002_PEOPLE_CURR.PHONE_WORK AS phone_work_one,
      X002_PEOPLE_CURR.PHONE_MOBI AS phone_mobi_one,
      X002_PEOPLE_CURR.PHONE_HOME AS phone_home_one,
      X002_PEOPLE_CURR.ORG_NAME AS org_name_one,
      X002_PEOPLE_CURR.GRADE_CALC AS grade_calc_one,
      X002_PEOPLE_TWO.EMPLOYEE_NUMBER AS employee_two,
      X002_PEOPLE_TWO.NAME_LIST AS name_list_two,
      X002_PEOPLE_TWO.KNOWN_NAME AS known_name_two,
      X002_PEOPLE_TWO.POSITION_FULL AS position_full_two,
      X002_PEOPLE_TWO.LOCATION_DESCRIPTION AS location_description_two,
      X002_PEOPLE_TWO.DIVISION AS division_two,
      X002_PEOPLE_TWO.FACULTY AS faculty_two,
      X002_PEOPLE_TWO.EMAIL_ADDRESS AS email_address_two,
      X002_PEOPLE_TWO.PHONE_WORK AS phone_work_two,
      X002_PEOPLE_TWO.PHONE_MOBI AS phone_mobi_two,
      X002_PEOPLE_TWO.PHONE_HOME AS phone_home_two,
      X002_PEOPLE_THREE.EMPLOYEE_NUMBER AS employee_three,
      X002_PEOPLE_THREE.NAME_LIST AS name_list_three,
      X002_PEOPLE_THREE.KNOWN_NAME AS known_name_three,
      X002_PEOPLE_THREE.POSITION_FULL AS position_full_three,
      X002_PEOPLE_THREE.LOCATION_DESCRIPTION AS location_description_three,
      X002_PEOPLE_THREE.DIVISION AS division_three,
      X002_PEOPLE_THREE.FACULTY AS faculty_three,
      X002_PEOPLE_THREE.EMAIL_ADDRESS AS email_address_three,
      X002_PEOPLE_THREE.PHONE_WORK AS phone_work_three,
      X002_PEOPLE_THREE.PHONE_MOBI AS phone_mobi_three,
      X002_PEOPLE_THREE.PHONE_HOME AS phone_home_three
    FROM
      X002_PEOPLE_CURR
      LEFT JOIN X002_PEOPLE_CURR X002_PEOPLE_TWO ON X002_PEOPLE_TWO.EMPLOYEE_NUMBER = X002_PEOPLE_CURR.SUPERVISOR
      LEFT JOIN X002_PEOPLE_CURR X002_PEOPLE_THREE ON X002_PEOPLE_THREE.EMPLOYEE_NUMBER = X002_PEOPLE_TWO.SUPERVISOR
    """
    so_curs.execute("DROP TABLE IF EXISTS X003_PEOPLE_ORGA_REF")
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: X003_PEOPLE_ORGA_REF")

    # 29 Build PEOPLE SUMMARY CURRENT **********************************************
    print("Build current people summary...")
    s_sql = "CREATE TABLE X003_PEOPLE_SUMM AS " + """
    SELECT
      X002_PEOPLE_CURR.EMPLOYEE_NUMBER,
      X002_PEOPLE_CURR.FULL_NAME,
      X002_PEOPLE_CURR.KNOWN_NAME,
      X002_PEOPLE_CURR.POSITION_FULL,
      X002_PEOPLE_CURR.SEX,
      X002_PEOPLE_CURR.DISABLED,
      X002_PEOPLE_CURR.PHONE_WORK,
      X002_PEOPLE_CURR.EMAIL_ADDRESS,
      X002_PEOPLE_CURR.INT_MAIL,
      X002_PEOPLE_CURR.LOCATION_DESCRIPTION,
      X002_PEOPLE_CURR.ORG_TYPE_DESC,
      X002_PEOPLE_CURR.OE_CODE,
      X002_PEOPLE_CURR.ORG_NAME,
      X002_PEOPLE_CURR.ACAD_SUPP,
      X002_PEOPLE_CURR.EMPLOYMENT_CATEGORY,
      X002_PEOPLE_CURR.GRADE,
      X002_PEOPLE_CURR.GRADE_CALC,
      X002_PEOPLE_CURR.GRADE_NAME,
      X002_PEOPLE_CURR.POSITION,
      X002_PEOPLE_CURR.POSITION_NAME,
      X002_PEOPLE_CURR.JOB_NAME,
      X002_PEOPLE_CURR.JOB_SEGMENT_NAME,
      X002_PEOPLE_CURR.SUPERVISOR
    FROM
      X002_PEOPLE_CURR
    ORDER BY
      X002_PEOPLE_CURR.ORG_NAME,
      X002_PEOPLE_CURR.GRADE_CALC,
      X002_PEOPLE_CURR.FULL_NAME
    """
    so_curs.execute("DROP TABLE IF EXISTS X003_PEOPLE_SUMM")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: X003_PEOPLE_SUMM")

    if l_export == True:
        
        # Data export
        sr_file = "X003_PEOPLE_SUMM"
        sr_filet = sr_file
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "People_003_summary_"
        sx_filet = sx_file + funcdate.cur_month()

        print("Export current people summary..." + sx_path + sx_filet)

        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

        # Write the data
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_filet)

    if l_mail == True:
        funcmail.Mail("hr_people_summary")    

    # 30 Build PEOPLE ORGANIZATION MONTH *******************************************

    print("Build month people organogram...")

    s_sql = "CREATE TABLE X003_PEOPLE_ORGA AS " + """
    SELECT
      X002_PEOPLE_CURR.ASS_ID AS ass030_OraPosiId,
      X002_PEOPLE_CURR.EMPLOYEE_NUMBER AS per002_EmpNumb,
      X002_PEOPLE_CURR.TITLE_FULL AS per_013_PerTitl,
      X002_PEOPLE_CURR.FIRST_NAME AS per011_PerFirs,
      X002_PEOPLE_CURR.MIDDLE_NAMES AS per012_PerMidd,
      X002_PEOPLE_CURR.LAST_NAME AS per010_PerLast,
      X002_PEOPLE_CURR.PHONE_WORK AS pho021_PhoNumb,
      X002_PEOPLE_CURR.PHONE_MOBI AS pho021_PhoNumb2,
      X002_PEOPLE_CURR.INT_MAIL AS per036_PerBoxx,
      X002_PEOPLE_CURR.EMAIL_ADDRESS AS mai010_Emai,
      X002_PEOPLE_CURR.KNOWN_NAME AS mai011_Namc,
      X002_PEOPLE_CURR.INITIALS AS mai012_Init,
      X002_PEOPLE_CURR.IDNO AS bul005_Comb,
      X002_PEOPLE_CURR.JOB_NAME AS jod010_JodName,
      X002_PEOPLE_CURR.POSITION AS pod010_PodPosi,
      X002_PEOPLE_CURR.POSITION_NAME AS pod011_PodName,
      X002_PEOPLE_CURR.OE_CODE AS pos011_PosCost,
      X002_PEOPLE_CURR.ORG_NAME AS org011_OrgName,
      X002_PEOPLE_TWO.ASS_ID AS ass030_OraPosiId2,
      X002_PEOPLE_TWO.EMPLOYEE_NUMBER AS per002_EmpNumb2,
      X002_PEOPLE_TWO.TITLE_FULL AS per_013_PerTitl2,
      X002_PEOPLE_TWO.FIRST_NAME AS per011_PerFirs2,
      X002_PEOPLE_TWO.MIDDLE_NAMES AS per012_PerMidd2,
      X002_PEOPLE_TWO.LAST_NAME AS per010_PerLast2,
      X002_PEOPLE_TWO.PHONE_WORK AS pho021_PhoNumb3,
      X002_PEOPLE_TWO.PHONE_MOBI AS pho021_PhoNumb22,
      X002_PEOPLE_TWO.INT_MAIL AS per036_PerBoxx2,
      X002_PEOPLE_TWO.EMAIL_ADDRESS AS mai010_Emai2,
      X002_PEOPLE_TWO.KNOWN_NAME AS mai011_Namc2,
      X002_PEOPLE_TWO.INITIALS AS mai012_Init2,
      X002_PEOPLE_TWO.IDNO AS bul005_Comb2,
      X002_PEOPLE_TWO.JOB_NAME AS jod010_JodName2,
      X002_PEOPLE_TWO.POSITION AS pod010_PodPosi2,
      X002_PEOPLE_TWO.POSITION_NAME AS pod011_PodName2,
      X002_PEOPLE_TWO.OE_CODE AS pos011_PosCost2,
      X002_PEOPLE_TWO.ORG_NAME AS org011_OrgName2,
      X002_PEOPLE_THREE.ASS_ID AS ass030_OraPosiId3,
      X002_PEOPLE_THREE.EMPLOYEE_NUMBER AS per002_EmpNumb3,
      X002_PEOPLE_THREE.TITLE_FULL AS per_013_PerTitl3,
      X002_PEOPLE_THREE.FIRST_NAME AS per011_PerFirs3,
      X002_PEOPLE_THREE.MIDDLE_NAMES AS per012_PerMidd3,
      X002_PEOPLE_THREE.LAST_NAME AS per010_PerLast3,
      X002_PEOPLE_THREE.PHONE_WORK AS pho021_PhoNumb4,
      X002_PEOPLE_THREE.PHONE_MOBI AS pho021_PhoNumb23,
      X002_PEOPLE_THREE.INT_MAIL AS per036_PerBoxx3,
      X002_PEOPLE_THREE.EMAIL_ADDRESS AS mai010_Emai3,
      X002_PEOPLE_THREE.KNOWN_NAME AS mai011_Namc3,
      X002_PEOPLE_THREE.INITIALS AS mai012_Init3,
      X002_PEOPLE_THREE.IDNO AS bul005_Comb3,
      X002_PEOPLE_THREE.JOB_NAME AS jod010_JodName3,
      X002_PEOPLE_THREE.POSITION AS pod010_PodPosi3,
      X002_PEOPLE_THREE.POSITION_NAME AS pod011_PodName3,
      X002_PEOPLE_THREE.OE_CODE AS pos011_PosCost3,
      X002_PEOPLE_THREE.ORG_NAME AS org011_OrgName3,
      X002_PEOPLE_CURR.GRADE_CALC AS gra010_GraGras
    FROM
      X002_PEOPLE_CURR
      LEFT JOIN X002_PEOPLE_CURR X002_PEOPLE_TWO ON X002_PEOPLE_TWO.EMPLOYEE_NUMBER = X002_PEOPLE_CURR.SUPERVISOR
      LEFT JOIN X002_PEOPLE_CURR X002_PEOPLE_THREE ON X002_PEOPLE_THREE.EMPLOYEE_NUMBER = X002_PEOPLE_TWO.SUPERVISOR
    """
    so_curs.execute("DROP TABLE IF EXISTS X003_PEOPLE_ORGA")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: X003_PEOPLE_ORGA")

    if l_export == True:
        
        # Data export
        sr_file = "X003_PEOPLE_ORGA"
        sr_filet = sr_file
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "People_003_organogram_"
        sx_filet = sx_file + funcdate.cur_month()

        print("Export month people organogram..." + sx_path + sx_filet)

        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)

        # Write the data
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_filet)

    if l_mail == True:
        funcmail.Mail("hr_people_organogram")

    """ ****************************************************************************
    BUILD CURRENT SYSTEM USERS
    *****************************************************************************"""
    print("SYSTEM USERS")
    funcfile.writelog("SYSTEM USERS")

    # LOOKUP PARTY ID FOR USERS
    print("Lookup party id for users...")
    sr_file = "X000_USER_CURR_PARTY"
    s_sql = "CREATE VIEW "+sr_file+" AS " + """
    Select
        FND_USER.USER_ID,
        FND_USER.USER_NAME,
        FND_USER.PERSON_PARTY_ID,
        X002_PEOPLE_CURR.PARTY_ID
    From
        FND_USER Inner Join
        X002_PEOPLE_CURR On X002_PEOPLE_CURR.EMPLOYEE_NUMBER = FND_USER.USER_NAME
    Where
        FND_USER.PERSON_PARTY_ID = 0 And
        Cast(FND_USER.USER_NAME As Integer) > 0
    ;"""
    so_curs.execute("DROP VIEW IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD VIEW: "+sr_file)

    # LOOKUP PARTY ID FOR USERS
    print("Add party id for users...")
    sr_file = "FND_USER_PARTY"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        FND_USER.*,
        X000_USER_CURR_PARTY.PARTY_ID As PEOPLE_PARTY_ID
    From
        FND_USER Left Join
        X000_USER_CURR_PARTY On X000_USER_CURR_PARTY.USER_ID = FND_USER.USER_ID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file) 

    # CALCULATE USER FIELD
    print("Calculate party column...")
    so_curs.execute("UPDATE FND_USER_PARTY " + """
                    SET CALC_PARTY_ID =
                    CASE
                        WHEN PEOPLE_PARTY_ID <> '' THEN PEOPLE_PARTY_ID
                    ELSE
                        PERSON_PARTY_ID
                    END
                    ;""")
    so_conn.commit()
    funcfile.writelog("%t CALC COLUMNS: CALC_PARTY_ID")

    # BUILD CURRENT USERS
    print("Build current users...")
    sr_file = "X000_USER_CURR"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    Select
        FND_USER_PARTY.USER_ID,
        FND_USER_PARTY.USER_NAME,
        X002_PEOPLE_CURR.EMPLOYEE_NUMBER,
        X002_PEOPLE_CURR.KNOWN_NAME,
        X002_PEOPLE_CURR.NAME_ADDR,
        X002_PEOPLE_CURR.EMAIL_ADDRESS,
        FND_USER_PARTY.LAST_LOGON_DATE
    From
        FND_USER_PARTY Inner Join
        X002_PEOPLE_CURR On X002_PEOPLE_CURR.PARTY_ID = FND_USER_PARTY.CALC_PARTY_ID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)        

    # Delete some unncessary files *************************************************

    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_HOME_CURR")
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_HOME_CURR_LIST")
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_MOBI_CURR")
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_MOBI_CURR_LIST")
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_WORK_CURR")
    so_curs.execute("DROP VIEW IF EXISTS X000_PHONE_WORK_CURR_LIST")

    # Close the connection *********************************************************
    so_conn.close()

    """ ****************************************************************************
    BUILD PEOPLE LEAVE
    *****************************************************************************"""
    print("BUILD PEOPLE LEAVE")
    funcfile.writelog("BUILD PEOPLE LEAVE")

    # PEOPLE_LEAVE Database ********************************************************

    # Declare variables
    so_path = "W:/People_leave/" #Source database path
    so_file = "People_leave.sqlite" #Source database
    s_sql = "" #SQL statements

    # Open the SOURCE file
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("%t OPEN DATABASE: PEOPLE_LEAVE.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/PEOPLE/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

    # Build PER ABS ATTENDANCES ****************************************************

    print("Build per absence attendances...")

    s_sql = "CREATE TABLE X100_PER_ABSENCE_ATTENDANCES AS " + """
    SELECT
      PER_ABSENCE_ATTENDANCES.ABSENCE_ATTENDANCE_ID,
      PER_ABSENCE_ATTENDANCES.BUSINESS_GROUP_ID,
      PER_ABSENCE_ATTENDANCES.ABSENCE_ATTENDANCE_TYPE_ID,
      PER_ABSENCE_ATTENDANCES.ABS_ATTENDANCE_REASON_ID,
      PER_ABSENCE_ATTENDANCES.PERSON_ID,
      PER_ABSENCE_ATTENDANCES.AUTHORISING_PERSON_ID,
      PER_ABSENCE_ATTENDANCES.ABSENCE_DAYS,
      PER_ABSENCE_ATTENDANCES.ABSENCE_HOURS,
      PER_ABSENCE_ATTENDANCES.DATE_END,
      PER_ABSENCE_ATTENDANCES.DATE_NOTIFICATION,
      PER_ABSENCE_ATTENDANCES.DATE_PROJECTED_END,
      PER_ABSENCE_ATTENDANCES.DATE_PROJECTED_START,
      PER_ABSENCE_ATTENDANCES.DATE_START,
      PER_ABSENCE_ATTENDANCES.OCCURRENCE,
      PER_ABSENCE_ATTENDANCES.SSP1_ISSUED,
      PER_ABSENCE_ATTENDANCES.PROGRAM_APPLICATION_ID,
      PER_ABSENCE_ATTENDANCES.ATTRIBUTE1,
      PER_ABSENCE_ATTENDANCES.ATTRIBUTE2,
      PER_ABSENCE_ATTENDANCES.ATTRIBUTE3,
      PER_ABSENCE_ATTENDANCES.LAST_UPDATE_DATE,
      PER_ABSENCE_ATTENDANCES.LAST_UPDATED_BY,
      PER_ABSENCE_ATTENDANCES.LAST_UPDATE_LOGIN,
      PER_ABSENCE_ATTENDANCES.CREATED_BY,
      PER_ABSENCE_ATTENDANCES.CREATION_DATE,
      PER_ABSENCE_ATTENDANCES.REASON_FOR_NOTIFICATION_DELAY,
      PER_ABSENCE_ATTENDANCES.ACCEPT_LATE_NOTIFICATION_FLAG,
      PER_ABSENCE_ATTENDANCES.OBJECT_VERSION_NUMBER,
      PEOPLE.PER_ALL_PEOPLE_F.EMPLOYEE_NUMBER
    FROM
      PER_ABSENCE_ATTENDANCES
      LEFT JOIN PEOPLE.PER_ALL_PEOPLE_F ON PEOPLE.PER_ALL_PEOPLE_F.PERSON_ID = PER_ABSENCE_ATTENDANCES.PERSON_ID
    WHERE
      PER_ABSENCE_ATTENDANCES.DATE_START >= PER_ALL_PEOPLE_F.EFFECTIVE_START_DATE AND
      PER_ABSENCE_ATTENDANCES.DATE_START <= PER_ALL_PEOPLE_F.EFFECTIVE_END_DATE
    ORDER BY
      PEOPLE.PER_ALL_PEOPLE_F.EMPLOYEE_NUMBER,
      PER_ABSENCE_ATTENDANCES.CREATION_DATE
    """
    so_curs.execute("DROP TABLE IF EXISTS X100_PER_ABSENCE_ATTENDANCES")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE: X100_PER_ABSENCE_ATTENDANCES")

    # Build PER ABS ATTENDANCE REASONS *********************************************

    print("Build per abs attendance reasons...")

    s_sql = "CREATE TABLE X101_PER_ABS_ATTENDANCE_REASONS AS " + """
    SELECT
      PER_ABS_ATTENDANCE_REASONS.ABS_ATTENDANCE_REASON_ID,
      PER_ABS_ATTENDANCE_REASONS.BUSINESS_GROUP_ID,
      PER_ABS_ATTENDANCE_REASONS.ABSENCE_ATTENDANCE_TYPE_ID,
      PER_ABS_ATTENDANCE_REASONS.NAME,
      PEOPLE.HR_LOOKUPS.MEANING,
      PER_ABSENCE_ATTENDANCE_TYPES.INPUT_VALUE_ID,
      PER_ABSENCE_ATTENDANCE_TYPES.NAME AS NAME1,
      PER_ABSENCE_ATTENDANCE_TYPES.ABSENCE_CATEGORY,
      PER_ABSENCE_ATTENDANCE_TYPES.HOURS_OR_DAYS,
      PER_ABSENCE_ATTENDANCE_TYPES.INCREASING_OR_DECREASING_FLAG
    FROM
      PER_ABS_ATTENDANCE_REASONS
      LEFT JOIN PER_ABSENCE_ATTENDANCE_TYPES ON PER_ABSENCE_ATTENDANCE_TYPES.ABSENCE_ATTENDANCE_TYPE_ID =
        PER_ABS_ATTENDANCE_REASONS.ABSENCE_ATTENDANCE_TYPE_ID
      LEFT JOIN PEOPLE.HR_LOOKUPS ON PEOPLE.HR_LOOKUPS.LOOKUP_CODE = PER_ABS_ATTENDANCE_REASONS.NAME AND
        PEOPLE.HR_LOOKUPS.LOOKUP_TYPE = 'ABSENCE_REASON'    
    """
    so_curs.execute("DROP TABLE IF EXISTS X101_PER_ABS_ATTENDANCE_REASONS")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE:  X101_PER_ABS_ATTENDANCE_REASONS")

    # Build PER ABSENCE ATTENDANCE TYPES *******************************************

    print("Build absence attendance types...")

    s_sql = "CREATE TABLE X102_PER_ABSENCE_ATTENDANCE_TYPES AS " + """
    SELECT
      PER_ABSENCE_ATTENDANCE_TYPES.ABSENCE_ATTENDANCE_TYPE_ID,
      PER_ABSENCE_ATTENDANCE_TYPES.BUSINESS_GROUP_ID,
      PER_ABSENCE_ATTENDANCE_TYPES.INPUT_VALUE_ID,
      PER_ABSENCE_ATTENDANCE_TYPES.DATE_EFFECTIVE,
      PER_ABSENCE_ATTENDANCE_TYPES.NAME,
      PER_ABSENCE_ATTENDANCE_TYPES.ABSENCE_CATEGORY,
      PER_ABSENCE_ATTENDANCE_TYPES.DATE_END,
      PER_ABSENCE_ATTENDANCE_TYPES.HOURS_OR_DAYS,
      PER_ABSENCE_ATTENDANCE_TYPES.INCREASING_OR_DECREASING_FLAG,
      PER_ABSENCE_ATTENDANCE_TYPES.LAST_UPDATE_DATE,
      PER_ABSENCE_ATTENDANCE_TYPES.LAST_UPDATED_BY,
      PER_ABSENCE_ATTENDANCE_TYPES.LAST_UPDATE_LOGIN,
      PER_ABSENCE_ATTENDANCE_TYPES.CREATED_BY,
      PER_ABSENCE_ATTENDANCE_TYPES.CREATION_DATE,
      PEOPLE.HR_LOOKUPS.MEANING
    FROM
      PER_ABSENCE_ATTENDANCE_TYPES
      LEFT JOIN PEOPLE.HR_LOOKUPS ON PEOPLE.HR_LOOKUPS.LOOKUP_CODE = PER_ABSENCE_ATTENDANCE_TYPES.ABSENCE_CATEGORY AND
        PEOPLE.HR_LOOKUPS.LOOKUP_TYPE = 'ABSENCE_CATEGORY'
    ORDER BY
      PER_ABSENCE_ATTENDANCE_TYPES.ABSENCE_ATTENDANCE_TYPE_ID
    """
    so_curs.execute("DROP TABLE IF EXISTS X102_PER_ABSENCE_ATTENDANCE_TYPES")
    so_curs.execute(s_sql)
    so_conn.commit()

    funcfile.writelog("%t BUILD TABLE:  X102_PER_ABSENCE_ATTENDANCE_TYPES")

    # Close the connection *********************************************************
    if l_vacuum == True:
        print("Vacuum the database...")
        so_conn.commit()
        so_conn.execute('VACUUM')
        funcfile.writelog("%t DATABASE: Vacuum people")
    so_conn.commit()
    so_conn.close()

    # Close the log writer *********************************************************
    funcfile.writelog("----------------------------")
    funcfile.writelog("COMPLETED: B001_PEOPLE_LISTS")

    return
