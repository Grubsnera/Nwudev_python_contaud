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
so_path: str = "W:/People_conflict/"  # Source database path
re_path: str = "R:/Kfs/" + funcdate.cur_year() + "/"  # Results path
ed_path: str = "S:/_external_data/"  # external data path
so_file: str = "People_conflict.sqlite"  # Source database
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
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/People_payroll/People_payroll.sqlite' AS 'PAYROLL'")
funcfile.writelog("%t ATTACH DATABASE: PAYROLL.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss_curr.sqlite' AS 'VSSCURR'")
funcfile.writelog("%t ATTACH DATABASE: VSS_CURR.SQLITE")

"""*****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
if l_debug:
    print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

"""*****************************************************************************
TEST CONFLICTING TRANSACTIONS
*****************************************************************************"""

"""
Test if employees declared conflict of interest.
    Request remediation from employee supervisor as per declaration.
Test exclude:
Created: 21 May 2021 (Albert J v Rensburg NWU:21162395)
"""

""" INDEX
BUILD A CSV FILE WITH CLEANED UP VENDOR NAMES AND REGISTRATION NUMBERS
VENDOR NAME AND REGISTRATION NUMBER COMPARISON METHOD 1 USING THE LEVENSHTEIN METHODS
VENDOR NAME AND REGISTRATION NUMBER COMPARISON METHOD 2 USING NORMAL COMPARISON METHODS
"""

# TABLES NEEDED

# DECLARE TEST VARIABLES
i_finding_after: int = 0
s_description = "Employee conflict transaction"
s_file_name: str = "employee_conflict_transaction"
s_file_prefix: str = "X200a"
s_finding: str = "EMPLOYEE CONFLICT TRANSACTION"
s_report_file: str = "002_reported.txt"

# OBTAIN TEST RUN FLAG
if functest.get_test_flag(so_curs, "HR", "TEST " + s_finding, "RUN") == "FALSE":

    if l_debug:
        print('TEST DISABLED')
    funcfile.writelog("TEST " + s_finding + " DISABLED")

else:

    # OPEN LOG
    if l_debug:
        print("TEST " + s_finding)
    funcfile.writelog("TEST " + s_finding)

    # BUILD A CSV FILE WITH CLEANED UP VENDOR NAMES AND REGISTRATION NUMBERS

    # Read the list of words to exclude in the vendor names
    # Create an empty list to store the lookup codes
    words_to_remove = []
    # Read the csv file
    file_path = ed_path + '001_own_kfs_lookups.csv'
    lookup_column_name = 'LOOKUP'
    lookup_code_column_name = 'LOOKUP_CODE'
    with open(file_path, 'r') as file:
        # Create a csv reader object
        csv_reader = csv.DictReader(file)
        # Iterate over each row in the csv file
        for row in csv_reader:
            # Filter rows based on the LOOKUP column value
            if row[lookup_column_name] == 'EXCLUDE VENDOR WORD':
                # Append the LOOKUP_CODE value to the lookup_codes list
                words_to_remove.append(row[lookup_code_column_name])
    # Print the lookup codes
    if l_debug:
        print(words_to_remove)
    # Build a list of cleaned vendor names and registration numbers
    # Execute the SQL query to fetch the data
    so_curs.execute('Select p.VENDOR_ID, p.PAYEE_NAME, p.REG_NO '
                    'From KFSCURR.X002aa_Report_payments_summary p '
                    'Where p.VENDOR_TYPE In ("PO", "DV")')
    # Fetch all the rows returned by the query
    rows = so_curs.fetchall()
    # Modify the data before writing to CSV
    modified_rows = []
    for row in rows:
        payee_id, payee_name, vendor_reg_nr = row
        # Clean the vendor name
        modified_payee_name = funcstr.clean_paragraph(payee_name, words_to_remove, 'b')
        # Clean the vendor registration number
        modified_regno = funcstr.clean_paragraph(vendor_reg_nr, words_to_remove, 'n')
        modified_rows.append((payee_id, modified_payee_name, modified_regno))
    # Write the data into the CSV file
    output_file = re_path + 'X200a_vendors_cleaned.csv'
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["vendor_id", "vendor_name", "vendor_regno"])  # Write column headers
        writer.writerows(modified_rows)  # Write the data rows

    # Create SQLite table to receive cleaned vendors
    sr_file: str = s_file_prefix + "a_" + s_file_name
    s_sql = "CREATE TABLE " + sr_file + """
    (
    vendor_id TEXT,
    vendor_name TEXT,
    vendor_regno TEXT
    )
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Read csv data previously submitted and populate SQLite table.
    with open(output_file, "r") as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            so_curs.execute(
                "INSERT INTO " + sr_file + " VALUES (:vendor_id,"
                                           " :vendor_name,"
                                           " :vendor_regno)",
                row)
    so_conn.commit()
    funcfile.writelog("%t IMPORT CSV: " + output_file)

    # VENDOR NAME AND REGISTRATION NUMBER COMPARISON

    # Build table with directorship and vendor name and registration number comparison
    print("Build declarations master table...")
    sr_file: str = s_file_prefix + "b_" + s_file_name
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        d.nwu_number,
        d.company_name As company_name,
        d.registration_number As company_registration_number,
        v.vendor_id,
        v.vendor_name,
        Case
          When SubStr(d.registration_number, 1, 4) || SubStr(d.registration_number, 6, 6) = SubStr(v.vendor_regno, 1, 10)
          Then 0
          When d.company_name Like (v.vendor_name || '%')
          Then 2
          Else 1
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
        X200aa_employee_conflict_transaction v
    Where
        (SubStr(d.registration_number, 1, 4) || SubStr(d.registration_number, 6, 6) = SubStr(v.vendor_regno, 1, 10)) Or
        (d.company_name Like (v.vendor_name || '%')) Or
        (v.vendor_name Like (d.company_name || '%'))    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD A CSV FILE WITH CLEANED UP EMPLOYEE INTERESTS

    # Read the list of words to exclude in the vendor names
    # Create an empty list to store the lookup codes
    words_to_remove = []
    # Read the csv file
    file_path = ed_path + '001_own_kfs_lookups.csv'
    lookup_column_name = 'LOOKUP'
    lookup_code_column_name = 'LOOKUP_CODE'
    with open(file_path, 'r') as file:
        # Create a csv reader object
        csv_reader = csv.DictReader(file)
        # Iterate over each row in the csv file
        for row in csv_reader:
            # Filter rows based on the LOOKUP column value
            if row[lookup_column_name] == 'EXCLUDE VENDOR WORD':
                # Append the LOOKUP_CODE value to the lookup_codes list
                words_to_remove.append(row[lookup_code_column_name])
    # Print the lookup codes
    if l_debug:
        print(words_to_remove)
    # Build a list of cleaned interest entity name and registration number
    # Execute the SQL query to fetch the data
    so_curs.execute("""
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
    rows = so_curs.fetchall()
    # Modify the data before writing to CSV
    modified_rows = []
    for row in rows:
        declaration_id, interest_id, employee_number, entity_name, entity_registration_number, declaration_date = row
        # Clean the entity name
        modified_entity_name = funcstr.clean_paragraph(entity_name, words_to_remove, 'b')
        # Clean the vendor registration number
        modified_entity_registration_number = funcstr.clean_paragraph(entity_registration_number, words_to_remove, 'n')
        modified_rows.append((declaration_id, interest_id, employee_number, modified_entity_name, modified_entity_registration_number))
    # Write the data into the CSV file
    output_file = re_path + 'X200b_interests_cleaned.csv'
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["declaration_id", "interest_id", "employee_number", "entity_name", "entity_registration_number"])  # Write column headers
        writer.writerows(modified_rows)  # Write the data rows

    # Create SQLite table to receive cleaned vendors
    sr_file: str = s_file_prefix + "c_" + s_file_name
    s_sql = "CREATE TABLE " + sr_file + """
    (
    declaration_id INT,
    interest_id INT,
    employee_number TEXT,
    entity_name TEXT,
    entity_registration_number TEXT
    )
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # Read csv data previously submitted and populate SQLite table.
    with open(output_file, "r") as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            so_curs.execute(
                "INSERT INTO " + sr_file + " VALUES (:declaration_id,"
                                           " :interest_id,"
                                           " :employee_number,"
                                           " :entity_name,"
                                           " :entity_registration_number)",
                row)
    so_conn.commit()
    funcfile.writelog("%t IMPORT CSV: " + output_file)

    # Build table with directorship and vendor name and registration number comparison
    print("Build declarations master table...")
    sr_file: str = s_file_prefix + "d_" + s_file_name
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
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
        X200ab_employee_conflict_transaction v Left Join
        X200ac_employee_conflict_transaction i On (i.employee_number = v.nwu_number
                    And SubStr(i.entity_registration_number, 1, 10) = v.regno_director)
                Or (i.employee_number = v.nwu_number
                    And i.entity_name Like (v.company_name || '%'))    
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)


    # BUILD EITHER COMPANY MASTER FILE OR TRANSACTION MASTER FILE ???

"""*****************************************************************************
END OF SCRIPT
*****************************************************************************"""
if l_debug:
    print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
so_conn.close()

# CLOSE THE LOG WRITER
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C002_PEOPLE_TEST_CONFLICT_DEV")

"""*****************************************************************************
SAMPLE CODE
*****************************************************************************"""

'''
Words Yolandie identified to be removed for vendor name comparisons 20231109
["LTD", "(PTY)", "(CRED)", "(DV)", "AD HOC NO PO","*", "(SOLE)","ADHOC", "(Pty)", "Ltd", "BPK", "Bpk", "EDMS", "Edms", " CC", " cc", "T/A","NO PO", "()", "AD HOC", " SOC", " CR ", "BIB"]
'''

'''
# SCRIPT TO BUILD A CSV OF WORDS THAT OCCUR IN A SQLITE TABLE COLUMNS LIKE VENDOR NAME
# Execute the SQL query to fetch the ID column from the VENDOR table
so_curs.execute('Select v.VNDR_NM From KFS.X000_Vendor v Where v.VNDR_TYP_CD In ("PO", "DV") And v.DOBJ_MAINT_CD_ACTV_IND = "Y"')
so_curs.execute('Select p.PAYEE_ID, p.PAYEE_NAME, p.VENDOR_REG_NR From KFSCURR.X001aa_Report_payments p Where p.VENDOR_TYPE_CALC In ("PO", "DV")')
# Fetch all the rows returned by the query
rows = so_curs.fetchall()
# Open the csv file in write mode
with open('words.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    for row in rows:
        word_list = funcstr.build_word_list(row[0])
        print(word_list)
        # Write each word in a new line
        for word in word_list:
            writer.writerow([word])
'''
