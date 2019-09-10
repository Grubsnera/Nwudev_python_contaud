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

# DECLARE SCRIPT VARIABLES
l_record: bool = True
s_data = ""
s_date: str = funcdate.today()
s_date_file: str = funcdate.today_file()
s_time: str = ""
s_script: str = ""
s_base: str = ""
s_action: str = ""
s_object: str = ""
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
    SCRIPT TEXT,
    DATABASE TEXT,
    ACTION TEXT,
    OBJECT TEXT)
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
    # 4 = Database s_base
    # 5 = Action s_action
    # 6 = Object s_object

    # UNRAVEL THE LOF RECORD LINE
    s_data = row[0]
    if s_data[0:10] == s_date:
        l_record = False
    elif s_data[0:1] == "-":
        l_record = False
    elif s_data.find(":") == 2:
        s_time = s_data[0:8]
        if s_data.find(":", 9) > 0:
            s_action = s_data[9:s_data.find(":", 9)].upper()
            s_object = s_data[s_data.find(":", 9) + 2:100].upper()
    elif s_data.find("SCRIPT:") == 0:
        if s_data.find(":", 8) > 0:
            s_script = s_data[8:s_data.find(":", 8)].upper()
            s_action = "SCRIPT"
            s_object = s_data[s_data.find(":", 8) + 2:100].upper()
        else:
            s_script = s_data[8:100].upper()
            s_action = "SCRIPT"
            s_object = s_data[8:100]
    elif s_data.find("OPEN DATABASE:") == 0:
        s_base = s_data[15:100].upper()
        s_action = "OPEN DATABASE"
        s_object = s_base
    elif s_data.find(":", 9) > 0:
        s_action = s_data[0:s_data.find(":")].upper()
        s_object = s_data[s_data.find(":") + 2:100].upper()
    else:
        s_action = "HEADER"
        s_object = s_data[0:100].upper()

    # SAVE THE RECORD
    if l_record:
        s_cols = "INSERT INTO " + sr_file + " VALUES("\
                 "'" + row[0] + "',"\
                 "'" + s_date + "'," \
                 "'" + s_time + "'," \
                 "'" + s_script + "'," \
                 "'" + s_base + "'," \
                 "'" + s_action + "'," \
                 "'" + s_object + "'" \
                 ")"
        so_curs.execute(s_cols)

    # RESET VARIABLES
    l_record = True
    s_action = ""
    s_object = ""

# BUILD THE LOG TABLE
print("Build the log table...")
sr_file = "X001ab_sort_log"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    Rowid As ID,
    Rowid + 1 As ID2,
    LOG.LOG_DATE,
    LOG.LOG_TIME,
    LOG.SCRIPT,
    LOG."DATABASE",
    LOG."ACTION",
    LOG.OBJECT
From
    X001aa_import_log LOG
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

so_conn.commit()
# Close the imported data file
co.close()
funcfile.writelog("%t IMPORT TABLE: " + ld_path + s_date_file + " (" + sr_file + ")")

# CALCULATE TIMES
print("Calculate times...")
sr_file = "X001ac_calc_time"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
    a.LOG_DATE,
    a.LOG_TIME,
    Cast(strftime('%s',b.LOG_DATE||' '||b.LOG_TIME) - strftime('%s',a.LOG_DATE||' '||a.LOG_TIME) As INT) As LOG_SECOND,
    time(strftime('%s',b.LOG_DATE||' '||b.LOG_TIME) - strftime('%s',a.LOG_DATE||' '||a.LOG_TIME), 'unixepoch') As
        LOG_ELAPSED,
    a.SCRIPT,
    a.DATABASE,
    a.ACTION,
    a.OBJECT
FROM
    X001ab_sort_log a Left Join
    X001ab_sort_log b On b.ID = a.ID2
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# CALCULATE TIMES
print("Isolate auto times...")
sr_file = "X001ad_auto_time"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
    LOG.*
FROM
    X001ac_calc_time LOG
WHERE
    (LOG.LOG_SECOND <= 3600 And LOG.LOG_TIME >= '18:00:00' And LOG.LOG_TIME <= '23:59:59') Or     
    (LOG.LOG_SECOND <= 3600 And LOG.LOG_TIME >= '02:00:00' And LOG.LOG_TIME <= '06:59:59')     
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

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
