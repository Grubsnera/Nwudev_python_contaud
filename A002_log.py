"""
Script to import LOG data
Created on: 9 Sep 2019
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3
import csv

# IMPORT OWN MODULES
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcstat
from _my_modules import funcsys

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
END OF SCRIPT
*****************************************************************************"""

"""*****************************************************************************
ENVIRONMENT
*****************************************************************************"""

# SCRIPT LOG FILE
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: A002_LOG")
funcfile.writelog("----------------")
print("--------")
print("A002_LOG")
print("--------")

# DECLARE VARIABLES
ed_path = "S:/_external_data/"  # External data path
so_path = "W:/Admin/"  # Source database path
so_file = "Admin.sqlite"  # Source database
ld_path = "S:/Logs/"
s_data = ""
s_date: str = funcdate.today()
s_date_file: str = funcdate.today_file()
s_time: str = ""
s_script: str = ""
l_vacuum: bool = False

"""*****************************************************************************
OPEN THE DATABASES
*****************************************************************************"""
print("OPEN THE DATABASES")
funcfile.writelog("OPEN THE DATABASES")

# OPEN SQLITE SOURCE table
print("Open sqlite database...")
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("OPEN DATABASE: " + so_file)

"""*****************************************************************************
TEMPORARY AREA
*****************************************************************************"""
print("TEMPORARY AREA")
funcfile.writelog("TEMPORARY AREA")

"""*****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

# IMPORT THE LOG FILE
sr_file = "X001aa_import_log"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
print("Import yesterday log file...")
so_curs.execute(
    "CREATE TABLE " + sr_file + """
    (LOG TEXT,
    LOG_DATE TEXT,
    LOG_TIME TEXT,
    SCRIPT TEXT)
    """)
co = open(ld_path + "Python_log_" + s_date_file + ".txt", "r")
print(ld_path + "Python_log_" + s_date_file + ".txt")
co_reader = csv.reader(co)
# Read the LOG database data
for row in co_reader:
    # ROW[0] = Log record
    # 1 = Log date s_date
    # 2 = Log time s_time
    # 3 = Script s_script

    s_data = row[0]

    # print(s_data.find(":"))

    if s_data.find(":") == 2:
        s_time = s_data[0:8]

    if s_data.find("SCRIPT:") == 0:
        s_script = s_data[8:64]

    s_cols = "INSERT INTO " + sr_file + " VALUES(" \
             "'" + row[0] + "',"\
             "'" + s_date + "'," \
             "'" + s_time + "'," \
             "'" + s_script + "'" \
             ")"
    so_curs.execute(s_cols)
so_conn.commit()
# Close the imported data file
co.close()
funcfile.writelog("%t IMPORT TABLE: " + ld_path + s_date_file + " (" + sr_file + ")")


"""*****************************************************************************
END OF SCRIPT
*****************************************************************************"""
print("END OF SCRIPT")
funcfile.writelog("END OF SCRIPT")

# CLOSE THE DATABASE CONNECTION
if l_vacuum:
    print("Vacuum the database...")
    so_conn.commit()
    so_conn.execute('VACUUM')
    funcfile.writelog("%t VACUUM DATABASE: " + so_file)
so_conn.commit()
so_conn.close()

# CLOSE THE LOG WRITER *********************************************************
funcfile.writelog("-------------------")
funcfile.writelog("COMPLETED: A002_LOG")
