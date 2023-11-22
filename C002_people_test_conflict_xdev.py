"""
Script to test PEOPLE conflict of interest
Created on: 8 Apr 2019
Modified on: 18 May 2021
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3
# from fuzzywuzzy import fuzz

# IMPORT OWN MODULES
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcsms
from _my_modules import funcsys
from _my_modules import functest
from _my_modules import funcstr
from _my_modules import funcstat

# INDEX
"""
ENVIRONMENT
OPEN THE DATABASES
BEGIN OF SCRIPT
END OF SCRIPT
"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# DECLARE VARIABLES
source_database_path: str = "W:/People_conflict/"  # Source database path
source_database_name: str = "People_conflict.sqlite"  # Source database
source_database: str = source_database_path + source_database_name
external_data_path: str = "S:/_external_data/"  # external data path
results_path: str = "R:/Kfs/" + funcdate.cur_year() + "/"  # Results path
s_sql: str = ""  # SQL statements
l_debug: bool = True
l_export: bool = False
l_mess: bool = False
l_mail: bool = False
l_record: bool = False

# OPEN THE SCRIPT LOG FILE
if l_debug:
    print("-----------------------------")
    print("C002_PEOPLE_TEST_CONFLICT_DEV")
    print("-----------------------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C002_PEOPLE_TEST_CONFLICT_DEV")
funcfile.writelog("-------------------------------------")

if l_mess:
    funcsms.send_telegram('', 'administrator', 'Testing employee <b>conflict of interest</b>.')

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
if l_debug:
    print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN THE WORKING DATABASE
with sqlite3.connect(source_database) as sqlite_connection:
    sqlite_cursor = sqlite_connection.cursor()
funcfile.writelog("OPEN DATABASE: " + source_database)

# ATTACH DATA SOURCES
sqlite_cursor.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
sqlite_cursor.execute("ATTACH DATABASE 'W:/People_payroll/People_payroll.sqlite' AS 'PAYROLL'")
funcfile.writelog("%t ATTACH DATABASE: PAYROLL.SQLITE")
sqlite_cursor.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
sqlite_cursor.execute("ATTACH DATABASE 'W:/Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
sqlite_cursor.execute("ATTACH DATABASE 'W:/Vss/Vss_curr.sqlite' AS 'VSSCURR'")
funcfile.writelog("%t ATTACH DATABASE: VSS_CURR.SQLITE")

"""*****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
if l_debug:
    print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

"""*****************************************************************************
TEST CONFLICTING TRANSACTIONS MASTER TABLES
*****************************************************************************"""

# DECLARE TEST VARIABLES
test_file_prefix: str = "X200"

# BUILD A TABLE WITH CLEANED UP VENDOR NAMES AND REGISTRATION NUMBERS
if l_debug:
    print('BUILD A TABLE WITH CLEANED UP VENDOR NAMES AND REGISTRATION NUMBERS')

# Read the list of words to exclude in the vendor names
words_to_remove = funcstat.stat_list(sqlite_cursor,
                                     "KFS.X000_Own_kfs_lookups",
                                     "LOOKUP_CODE",
                                     "LOOKUP='EXCLUDE VENDOR WORD'")
if l_debug:
    # print(words_to_remove)
    pass

# Prepare the table to receive cleaned vendors
table_name = test_file_prefix + "_a_vendor_cleaned"
sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
sqlite_cursor.execute(f"""
    CREATE TABLE {table_name} (
    vendor_id TEXT,
    vendor_name TEXT,
    vendor_regno TEXT
    )
""")

# Prepare the insert statement
insert_stmt = f"INSERT INTO {table_name} (vendor_id, vendor_name, vendor_regno) VALUES (?, ?, ?)"

# Execute the SQL query to fetch the data where vendor_type is 'PO' or 'DV'
sqlite_cursor.execute("""
    SELECT VENDOR_ID,
    PAYEE_NAME,
    REG_NO
    FROM KFSCURR.X002aa_Report_payments_summary
    WHERE VENDOR_TYPE IN ("PO", "DV")
    ;""")
vendors = sqlite_cursor.fetchall()

# Accumulating vendors for bulk insert
modified_vendors = []
for vendor in vendors:
    payee_id, payee_name, vendor_reg_nr = vendor
    modified_payee_name = funcstr.clean_paragraph(payee_name, words_to_remove, 'b')
    modified_regno = funcstr.clean_paragraph(vendor_reg_nr, words_to_remove, 'n')
    modified_vendors.append((payee_id, modified_payee_name, modified_regno))

# Bulk insert using executemany
sqlite_cursor.executemany(insert_stmt, modified_vendors)
sqlite_connection.commit()

# Log the actions performed
funcfile.writelog(f"%t BUILD & POPULATE TABLE: {table_name}")

# VENDOR NAME AND REGISTRATION NUMBER COMPARISON
if l_debug:
    print('VENDOR NAME AND REGISTRATION NUMBER COMPARISON')

# Build table with directorship and vendor name and registration number comparison
if l_debug:
    print("Build table with directorship and vendor name and registration number comparison...")
table_name: str = test_file_prefix + "_b_director_vendor_match"
s_sql = f"CREATE TABLE {table_name} AS " + """
Select
    d.nwu_number,
    d.company_name As company_name,
    d.registration_number As company_registration_number,
    v.vendor_id,
    v.vendor_name,
    Case
      When SubStr(d.registration_number, 1, 4) || SubStr(d.registration_number, 6, 6) = SubStr(v.vendor_regno, 1, 10)
      Then 1
      When d.company_name Like (v.vendor_name || '%')
      Then 3
      Else 2
    End As vendor_ratio,
    SubStr(d.registration_number, 1, 4) || SubStr(d.registration_number, 6, 6) As regno_director,
    SubStr(v.vendor_regno, 1, 10) As regno_vendor,
    Case
      When SubStr(d.registration_number, 1, 4) || SubStr(d.registration_number, 6, 6) = SubStr(v.vendor_regno, 1, 10)
      Then 1
      Else 0
    End As regno_ratio
From
    X004x_searchworks_directors d,
    X200_a_vendor_cleaned v
Where
    (SubStr(d.registration_number, 1, 4) || SubStr(d.registration_number, 6, 6) = SubStr(v.vendor_regno, 1, 10)) Or
    (d.company_name Like (v.vendor_name || '%')) Or
    (v.vendor_name Like (d.company_name || '%'))    
;"""
sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
sqlite_cursor.execute(s_sql)
sqlite_connection.commit()
funcfile.writelog(f"%t BUILD TABLE: {table_name}")

# BUILD A TABLE WITH CLEANED UP EMPLOYEE INTERESTS
if l_debug:
    print('BUILD A TABLE WITH CLEANED UP EMPLOYEE INTERESTS')

# Read the list of words to exclude in the vendor names
words_to_remove = funcstat.stat_list(sqlite_cursor,
                                     "KFS.X000_Own_kfs_lookups",
                                     "LOOKUP_CODE",
                                     "LOOKUP='EXCLUDE VENDOR WORD'")
if l_debug:
    # print(words_to_remove)
    pass

# Create SQLite table to receive cleaned vendors
table_name = test_file_prefix + "_c_interests_cleaned"
sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
sqlite_cursor.execute(f"""
    CREATE TABLE {table_name} (
    declaration_id INT,
    interest_id INT,
    employee_number TEXT,
    entity_name TEXT,
    entity_registration_number TEXT        
    ) """)
funcfile.writelog(f"%t BUILD TABLE: {table_name}")

# Prepare the insert statement
insert_stmt = f"INSERT INTO {table_name} (declaration_id, interest_id, employee_number, entity_name, entity_registration_number) VALUES (?, ?, ?, ?, ?)"

# Execute the SQL query to fetch the data
sqlite_cursor.execute("""
Select
    i.DECLARATION_ID As declaration_id,
    i.INTEREST_ID As interest_id,
    i.EMPLOYEE_NUMBER As employee_number,
    i.ENTITY_NAME As entity_name,
    i.ENTITY_REGISTRATION_NUMBER As entity_registration_number,
    Max(i.DECLARATION_DATE) As declaration_date
From
    X002_interests_curr i
Where
    i.ENTITY_NAME <> '' And
    i.INTEREST_STATUS = 'Accepted'
Group By
    i.DECLARATION_ID,
    i.INTEREST_ID
""")
# Fetch all the rows returned by the query
interests = sqlite_cursor.fetchall()

# Accumulating vendors for bulk insert
modified_interests = []
for interest in interests:
    declaration_id, interest_id, employee_number, entity_name, entity_registration_number, declaration_date = interest
    modified_entity_name = funcstr.clean_paragraph(entity_name, words_to_remove, 'b')
    modified_entity_registration_number = funcstr.clean_paragraph(entity_registration_number, words_to_remove, 'n')
    modified_interests.append((declaration_id, interest_id, employee_number, modified_entity_name, modified_entity_registration_number))

# Bulk insert using executemany
sqlite_cursor.executemany(insert_stmt, modified_interests)
sqlite_connection.commit()

# Build table which compare conflicting transactions with declarations
if l_debug:
    print("Build table which compare conflicting transactions with declarations...")
table_name: str = test_file_prefix + "_d_director_interest_match"
s_sql = f"CREATE TABLE {table_name} As " + """
Select
    v.nwu_number,
    v.company_name,
    v.company_registration_number,
    v.vendor_id,
    v.regno_director,
    Case
        When i.employee_number = v.nwu_number And SubStr(i.entity_registration_number, 1, 10) = v.regno_director
        Then 1
        When i.employee_number = v.nwu_number And i.entity_name Like (v.company_name || '%')
        Then 2
        Else 0        
    End As match_type,
    i.declaration_id,
    i.interest_id,
    i.entity_name,
    i.entity_registration_number
From
    X200_b_director_vendor_match v Left Join
    X200_c_interests_cleaned i On (i.employee_number = v.nwu_number
                And SubStr(i.entity_registration_number, 1, 10) = v.regno_director)
            Or (i.employee_number = v.nwu_number
                And i.entity_name Like (v.company_name || '%'))    
;"""
sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
sqlite_cursor.execute(s_sql)
sqlite_connection.commit()
funcfile.writelog(f"%t BUILD TABLE: {table_name}")

# Add data from the people and vendor master tables
if l_debug:
    print("Add some data needed to build the test parameters...")
table_name: str = test_file_prefix + "_e_master_table"
s_sql = f"CREATE TABLE {table_name} As " + """
Select
    i.nwu_number,
    p.name_address,
    i.company_name,
    i.company_registration_number,
    i.vendor_id,
    v.VNDR_NM As vendor_name,
    v.VNDR_TYP_CD As vendor_type,
    i.match_type,
    i.declaration_id,
    i.interest_id,
    i.entity_name,
    i.regno_director,        
    i.entity_registration_number,
    i.nwu_number || '-' || i.company_registration_number As exclude_combination          
From
    X200_d_director_interest_match i Left Join
    PEOPLE.X000_PEOPLE p On p.employee_number = i.nwu_number Left Join
    KFS.X000_Vendor v On v.VENDOR_ID = i.vendor_id
;"""
sqlite_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
sqlite_cursor.execute(s_sql)
sqlite_connection.commit()
funcfile.writelog(f"%t BUILD TABLE: {table_name}")

"""*****************************************************************************
END OF SCRIPT
*****************************************************************************"""
if l_debug:
    print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
sqlite_connection.close()

# CLOSE THE LOG WRITER
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C002_PEOPLE_TEST_CONFLICT_DEV")
