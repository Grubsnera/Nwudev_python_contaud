""" Script to test PEOPLE master file data development area
Created on: 1 Mar 2019
Author: Albert Janse van Rensburg (NWU21162395)
"""

# IMPORT SYSTEM MODULES
import csv
import sqlite3

# OPEN OWN MODULES
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcsys

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
END OF SCRIPT
*****************************************************************************"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# OPEN THE LOG
print("-------------------------------")
print("C001_PEOPLE_TEST_MASTERFILE_DEV")
print("-------------------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C001_PEOPLE_TEST_MASTERFILE_DEV")
funcfile.writelog("---------------------------------------")

# DECLARE VARIABLES
ed_path = "S:/_external_data/"  # External data path
so_path = "W:/People/"  # Source database path
so_file = "People_test_masterfile.sqlite"  # Source database
re_path = "R:/People/"  # Results path
l_export: bool = False
l_mail: bool = False
l_record: bool = True

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN DATABASE: PEOPLE_TEST_MASTERFILE.SQLITE")

# ATTACH DATA SOURCES
so_curs.execute("ATTACH DATABASE '" + so_path + "People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/People_payroll/People_payroll.sqlite' AS 'PAYROLL'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

""" ****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

""" ****************************************************************************
GRADE LEAVE MASTER FILE
*****************************************************************************"""

# OBTAIN LIST OF LONG SERVICE AWARD DATES
# BUILD THE CURRENT ELEMENT LIST
print("Obtain long service awards...")
sr_file = "X007_long_service_date"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select Distinct
    per.employee_number,
    per.full_name,
    Upper(petf.element_name) As ELEMENT_NAME,
    Upper(pivf.name) As INPUT_VALUE,
    Date(Substr(peevf.screen_entry_value,1,4)||'-'||
        Substr(peevf.screen_entry_value,6,2)||'-'||
        Substr(peevf.screen_entry_value,9,2)) As DATE_LONG_SERVICE
From
    PAYROLL.PAY_ELEMENT_ENTRIES_F_CURR peef,
    PEOPLE.PER_ALL_PEOPLE_F per,
    PEOPLE.PER_ALL_ASSIGNMENTS_F paaf,
    PAYROLL.PAY_ELEMENT_TYPES_F petf,
    PAYROLL.PAY_ELEMENT_ENTRY_VALUES_F_CURR peevf,
    PAYROLL.PAY_INPUT_VALUES_F pivf
Where
    per.person_id = paaf.person_id And
    paaf.assignment_id = peef.assignment_id And
    peef.element_type_id = petf.element_type_id And
    peevf.element_entry_id = peef.element_entry_id And
    pivf.element_type_id = petf.element_type_id And
    pivf.input_value_id = peevf.input_value_id And
    Date('%TODAY%') Between peef.effective_start_date And peef.effective_end_date And
    Date('%TODAY%') Between per.effective_start_date And per.effective_end_date And
    Date('%TODAY%') Between paaf.effective_start_date And paaf.effective_end_date And
    Date('%TODAY%') Between petf.effective_start_date And petf.effective_end_date And
    paaf.primary_flag = 'Y' And
    peevf.screen_entry_value > 0 And
    Upper(petf.element_name) = 'NWU LONG SERVICE AWARD' And
    Upper(pivf.name) = 'LONG SERVICE DATE'
Order By
    per.employee_number    
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = s_sql.replace("%TODAY%", funcdate.today())
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD GRADE AND LEAVE MASTER TABLE
print("Obtain master list of all grades and leave codes...")
sr_file = "X007_grade_leave_master"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    'NWU' As ORG,
    CASE LOCATION_DESCRIPTION
        WHEN 'MAFIKENG CAMPUS' THEN 'MAF'
        WHEN 'POTCHEFSTROOM CAMPUS' THEN 'POT'
        WHEN 'VAAL TRIANGLE CAMPUS' THEN 'VAA'
        ELSE 'NWU'
    END AS LOC,
    PEOPLE.EMPLOYEE_NUMBER,
    PEOPLE.EMP_START,
    LONG.DATE_LONG_SERVICE As SERVICE_START,
    PEOPLE.ACAD_SUPP,
    PEOPLE.EMPLOYMENT_CATEGORY,
    PEOPLE.PERSON_TYPE,
    PEOPLE.ASS_WEEK_LEN,
    PEOPLE.LEAVE_CODE,
    PEOPLE.GRADE,
    PEOPLE.GRADE_CALC,
    PEOPLE.FULL_PART_FLAG,
    CASE
        WHEN PEOPLE.PERSON_TYPE = 'TEMPORARY APPOINTMENT' And FULL_PART_FLAG = 'P' THEN 'TEMPORARY APPOINTMENT(P)'
        WHEN PEOPLE.PERSON_TYPE = 'TEMPORARY APPOINTMENT' THEN 'TEMPORARY APPOINTMENT(F)'
        ELSE PEOPLE.PERSON_TYPE
    END As PERSON_TYPE_LEAVE,
    CASE
        WHEN LONG.DATE_LONG_SERVICE Is Null And PEOPLE.EMP_START < Date('2017-05-01') THEN 'OLD'
        WHEN LONG.DATE_LONG_SERVICE < Date('2017-05-01') THEN 'OLD'
        ELSE '2017'
    END As PERIOD
From
    PEOPLE.X002_PEOPLE_CURR PEOPLE Left Join
    X007_long_service_date LONG On LONG.EMPLOYEE_NUMBER = PEOPLE.EMPLOYEE_NUMBER
Where
    Substr(PEOPLE.PERSON_TYPE,1,6) <> 'AD HOC'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IMPORT LEAVE BENCHMARK
sr_file = "X007_leave_master"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
print("Import leave benchmark...")
so_curs.execute(
    "CREATE TABLE " + sr_file + "(CATEGORY TEXT,ACADSUPP TEXT,PERIOD TEXT,WEEK TEXT, GRADE TEXT, LEAVE TEXT)")
s_cols = ""
co = open(ed_path + "001_employee_leave.csv", "r")
co_reader = csv.reader(co)
# Read the COLUMN database data
for row in co_reader:
    # Populate the column variables
    if row[0] == "CATEGORY":
        continue
    else:
        s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[
            3] + "','" + row[4] + "','" + row[5] + "')"
        so_curs.execute(s_cols)
so_conn.commit()
# Close the impoted data file
co.close()
funcfile.writelog("%t IMPORT TABLE: " + ed_path + "001_employee_leave.csv (" + sr_file + ")")

# IDENTIFY FINDING
print("Identify incorrect data...")
sr_file = "X007da_leave"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    MASTER.ORG,
    MASTER.LOC,
    MASTER.EMPLOYEE_NUMBER,
    MASTER.LEAVE_CODE,
    MASTER.GRADE,
    MASTER.EMPLOYMENT_CATEGORY,
    MASTER.ACAD_SUPP,
    MASTER.PERSON_TYPE_LEAVE,
    MASTER.ASS_WEEK_LEN,
    MASTER.EMP_START,
    MASTER.SERVICE_START,
    MASTER.PERIOD,
    CASE
        WHEN MASTER.EMPLOYMENT_CATEGORY = 'PERMANENT' And Instr(PERM.GRADE,'.'||Trim(MASTER.GRADE)||'~') > 0 And Instr(PERM.LEAVE,'.'||Trim(MASTER.LEAVE_CODE)||'~') > 0 Then 'TRUE'
        WHEN MASTER.EMPLOYMENT_CATEGORY = 'TEMPORARY' And  Instr(TEMP.LEAVE,'.'||Trim(MASTER.LEAVE_CODE)||'~') > 0 Then 'TRUE'
        WHEN MASTER.EMPLOYMENT_CATEGORY = 'PERMANENT' Then 'FALSE'
        WHEN MASTER.EMPLOYMENT_CATEGORY = 'TEMPORARY' Then 'FALSE'
        ELSE 'OTHER'
    END As VALID,
    PERM.LEAVE As LEAVEP,
    PERM.GRADE As GRADEP,
    TEMP.LEAVE As LEAVET,
    TEMP.GRADE As GRADET
From
    X007_grade_leave_master MASTER Left Join
    X007_leave_master PERM On PERM.CATEGORY = MASTER.EMPLOYMENT_CATEGORY And
        PERM.ACADSUPP = MASTER.ACAD_SUPP And
        PERM.PERIOD = MASTER.PERIOD And
        PERM.WEEK = MASTER.ASS_WEEK_LEN And
        Instr(PERM.GRADE,'.'||Trim(MASTER.GRADE)||'~') > 0 And
        PERM.CATEGORY = 'PERMANENT' Left Join
    X007_leave_master TEMP On TEMP.CATEGORY = MASTER.EMPLOYMENT_CATEGORY And
        TEMP.GRADE = MASTER.PERSON_TYPE_LEAVE And
        TEMP.CATEGORY = 'TEMPORARY'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)
# EXPORT TEST DATA
if l_export == True:
    sr_filet = sr_file
    sx_path = re_path + funcdate.cur_year() + "/"
    sx_file = "X007da_leave_"
    sx_filet = sx_file + funcdate.cur_month()
    print("Export people birthday..." + sx_path + sx_filet)
    # Read the header data
    s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
    # Write the data
    funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
    funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)

# ADD DETAILS
print("Add data details...")
sr_file = "X007db_detail"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    FIND.ORG,
    FIND.LOC,
    FIND.EMPLOYEE_NUMBER,
    CASE
        WHEN FIND.LEAVEP Is Null THEN FIND.LEAVET
        ELSE FIND.LEAVEP
    END As LEAVE_PROP,
    FIND.SERVICE_START
From
    X007da_leave FIND
Where
    FIND.VALID = 'FALSE' And
    FIND.PERSON_TYPE_LEAVE <> 'EXTRAORDINARY APPOINTMENT'
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE WORKING DATABASE
so_conn.close()

# CLOSE THE LOG
funcfile.writelog("------------------------------------------")
funcfile.writelog("COMPLETED: C001_PEOPLE_TEST_MASTERFILE_DEV")
