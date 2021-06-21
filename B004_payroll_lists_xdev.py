"""
Script to build standard VSS lists
Created on: 01 Mar 2018
Copyright: Albert J v Rensburg
"""

# IMPORT PYTHON MODULES
import csv
import datetime
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcdate
from _my_modules import funccsv
from _my_modules import funcfile
from _my_modules import funcpayroll

# OPEN THE LOG FILE
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B004_PAYROLL_LISTS_DEV")
funcfile.writelog("------------------------------")
print("----------------------")
print("B004_PAYROLL_LISTS_DEV")
print("----------------------")

# Declare variables
so_path = "W:/People_payroll/"  # Source database path
so_file = "People_payroll.sqlite"  # Source database
re_path = "R:/People/People/"  # Results
ed_path = "S:/_external_data/"

# OPEN THE SOURCE FILE
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

# ATTACH DATA SOURCES
so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

"""*****************************************************************************
BEGIN
*****************************************************************************"""

# NOTE TO SELF
# The following code used in people test leave code invalid
# It show the relationship between the elements file
# Do not delete, but save somewhere else
# -----------------------------

# BUILD THE CURRENT ELEMENT LIST
print("Testing element list...")
sr_file = "A_test_element_list"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select Distinct
    per.employee_number,
    per.full_name,
    Upper(petf.element_name) As ELEMENT_NAME,
    Upper(pivf.name) As INPUT_VALUE,
    peevf.screen_entry_value
From
    PAY_ELEMENT_ENTRIES_F_CURR peef,
    PEOPLE.PER_ALL_PEOPLE_F per,
    PEOPLE.PER_ALL_ASSIGNMENTS_F paaf,
    PAY_ELEMENT_TYPES_F petf,
    PAY_ELEMENT_ENTRY_VALUES_F_CURR peevf,
    PAY_INPUT_VALUES_F pivf
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
    peevf.screen_entry_value > 0
Order By
    per.employee_number    
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = s_sql.replace("%TODAY%",funcdate.today())
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

"""
Code from: http://josephbijoy.blogspot.com/search?q=peevf.screen_entry_value
Select
    per.employee_number,
    per.full_name,
    petf.element_name,
    peevf.screen_entry_value
From
    pay_element_entries_f peef,
    per_all_people_f per,
    per_all_assignments_f paaf,
    pay_element_types_f petf,
    pay_element_entry_values_f peevf,
    pay_input_values_f pivf
Where
    per.person_id = paaf.person_id And
    paaf.assignment_id = peef.assignment_id And
    peef.element_type_id = petf.element_type_id And
    peevf.element_entry_id = peef.element_entry_id And
    pivf.element_type_id = petf.element_type_id And
    pivf.input_value_id = peevf.input_value_id And
    paaf.primary_flag = 'Y' And
    To_Date('01-AUG-2013') Between peef.effective_start_date And peef.effective_end_date And
    To_Date('01-AUG-2013') Between per.effective_start_date And per.effective_end_date And
    To_Date('01-AUG-2013') Between paaf.effective_start_date And paaf.effective_end_date And
    To_Date('01-AUG-2013') Between petf.effective_start_date And petf.effective_end_date And
    Upper(petf.element_name) = 'NWU Long Service Award' And
    peevf.effective_start_date) = 'N' And
    peevf.screen_entry_value > 0 And
    Upper(pivf.name) = 'AMOUNT'
"""




"""*************************************************************************
END
*************************************************************************"""

# CLOSE THE CONNECTION
so_conn.close()

# CLOSE THE LOG
funcfile.writelog("-----------------------------")
funcfile.writelog("COMPLETED: B003_VSS_LISTS_DEV")
