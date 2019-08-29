"""
Script to test STUDENT FEES
Created on: 28 Aug 2019
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funcstat

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
funcfile.writelog("SCRIPT: C302_TEST_STUDENT_FEE")
funcfile.writelog("-----------------------------")
print("---------------------")
print("C302_TEST_STUDENT_FEE")
print("---------------------")

# DECLARE VARIABLES
ed_path = "S:/_external_data/"  # External data path
so_path = "W:/Vss_fee/"  # Source database path
so_file = "Vss_fee.sqlite"  # Source database
re_path = "R:/Vss/"
l_export: bool = True
l_mail: bool = False
l_record: bool = False
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
funcfile.writelog("%t OPEN SQLITE DATABASE: " + so_file)

# ATTACH VSS DATABASE
print("Attach vss database...")
so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")

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

"""*****************************************************************************
CALCULATE STATISTICS
*****************************************************************************"""
print("CALCULATE STATISTICS")
funcfile.writelog("CALCULATE STATISTICS")

# CALCULATE THE REGISTRATION FEE MODE
i_calc = funcstat.stat_mode(so_curs, "X001ab_Trans_feereg", "FEE_REG")
print(i_calc)

# IDENTIFY REGISTRATION FEE AMOUNTS NOT MODE
print("Identify abnormal registration fees...")
sr_file = "X010aa_Abnormal_regfee"
s_sql = "CREATE TABLE " + sr_file + " AS" + """
Select
    X001ab_Trans_feereg.STUDENT,
    X001ab_Trans_feereg.FEE_REG
From
    X001ab_Trans_feereg
Where
    X001ab_Trans_feereg.FEE_REG > 0 And 
    X001ab_Trans_feereg.FEE_REG != %AMOUNT% 
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = s_sql.replace("%AMOUNT%", str(i_calc))
print(s_sql)
so_curs.execute(s_sql)
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
    funcfile.writelog("%t DATABASE: " + so_file)
so_conn.commit()
so_conn.close()

# CLOSE THE LOG WRITER *********************************************************
funcfile.writelog("--------------------------------")
funcfile.writelog("COMPLETED: C302_TEST_STUDENT_FEE")
