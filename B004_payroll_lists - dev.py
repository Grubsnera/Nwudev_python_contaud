"""
Script to build standard VSS lists
Created on: 01 Mar 2018
Copyright: Albert J v Rensburg
"""

# Import python modules
import csv
import datetime
import sqlite3
import sys

# Add own module path
sys.path.append('S:/_my_modules')
#print(sys.path)

# Import own modules
import funcdate
import funccsv
import funcfile

# Open the script log file ******************************************************

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B004_PAYROLL_LISTS")
funcfile.writelog("--------------------------")
print("------------------")
print("B004_PAYROLL_LISTS")
print("------------------")
ilog_severity = 1

# Declare variables
so_path = "W:/People_payroll/" #Source database path
so_file = "People_payroll.sqlite" #Source database
re_path = "R:/People/" #Results
ed_path = "S:/_external_data/"
s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: PEOPLE_PAYROLL.SQLITE")

# Attach data sources
so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

"""*****************************************************************************
START
*****************************************************************************"""

# Build the balances list ******************************************************
print("Build the balances list...")
sr_file = "X000aa_balance_list_curr"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  PAY_RUN_BALANCES_CURR.ASSIGNMENT_ID,
  PAY_RUN_BALANCES_CURR.EFFECTIVE_DATE,
  PAY_RUN_BALANCES_CURR.BALANCE_VALUE,
  PAY_RUN_BALANCES_CURR.RUN_BALANCE_ID,
  PAY_RUN_BALANCES_CURR.DEFINED_BALANCE_ID,
  PAY_DEFINED_BALANCES.BALANCE_TYPE_ID,
  PAY_BALANCE_TYPES.BALANCE_NAME,
  PAY_BALANCE_TYPES.REPORTING_NAME,
  PAY_BALANCE_TYPES.BALANCE_UOM,
  PAY_BALANCE_TYPES.BALANCE_CATEGORY_ID
FROM
  PAY_RUN_BALANCES_CURR
  LEFT JOIN PAY_DEFINED_BALANCES ON PAY_DEFINED_BALANCES.DEFINED_BALANCE_ID = PAY_RUN_BALANCES_CURR.DEFINED_BALANCE_ID
  LEFT JOIN PAY_BALANCE_TYPES ON PAY_BALANCE_TYPES.BALANCE_TYPE_ID = PAY_DEFINED_BALANCES.BALANCE_TYPE_ID
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Extract the NWU INCOME PER MONTH balance for export **************************
print("Extract the nwu total income balance...")
sr_file = "X002aa_balance_totalincome_curr"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  X000aa_balance_list_curr.ASSIGNMENT_ID,
  X000aa_balance_list_curr.EFFECTIVE_DATE,
  X000aa_balance_list_curr.DEFINED_BALANCE_ID,
  X000aa_balance_list_curr.BALANCE_VALUE,
  X000aa_balance_list_curr.BALANCE_NAME,
  X000aa_balance_list_curr.REPORTING_NAME,
  SUBSTR(PEOPLE.PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_NUMBER,1,8) AS EMPL_NUMB
FROM
  X000aa_balance_list_curr
  LEFT JOIN PEOPLE.PER_ALL_ASSIGNMENTS_F ON PEOPLE.PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_ID = X000aa_balance_list_curr.ASSIGNMENT_ID AND
    PEOPLE.PER_ALL_ASSIGNMENTS_F.EFFECTIVE_START_DATE <= Date('%PMONTHEND%') AND
    PEOPLE.PER_ALL_ASSIGNMENTS_F.EFFECTIVE_END_DATE >= Date('%PMONTHEND%')
WHERE
  X000aa_balance_list_curr.DEFINED_BALANCE_ID = 16264 AND
  X000aa_balance_list_curr.EFFECTIVE_DATE = Date('%PMONTHEND%')
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = s_sql.replace("%PMONTHEND%",funcdate.prev_monthend())
#print(s_sql)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Build the NWU TOTAL INCOME export file ***************************************
print("Build the previous nwu total income balance export file...")
sr_file = "X002ax_balance_totalincome_curr"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  X002aa_balance_totalincome_curr.EMPL_NUMB,
  X002aa_balance_totalincome_curr.EFFECTIVE_DATE AS DATE,
  CAST(X002aa_balance_totalincome_curr.BALANCE_VALUE AS REAL) AS INCOME
FROM
  X002aa_balance_totalincome_curr
ORDER BY
  X002aa_balance_totalincome_curr.EMPL_NUMB
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)
# Export the data
print("Export incomes...")
sr_filet = sr_file
sx_path = re_path + funcdate.cur_year() + "/"
sx_file = "Payroll_002ax_income_total_"
sx_filet = sx_file + funcdate.prev_monthendfile()
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)

"""*************************************************************************
END
*************************************************************************"""

# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("-------------------------")
funcfile.writelog("COMPLETED: B003_VSS_LISTS")


