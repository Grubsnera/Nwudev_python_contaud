""" Script to extract STUDENT DEFERMENTS
Created on: 19 MAR 2018
Author: Albert J v Rensburg
"""

# IMPORT MODULES ***************************************************************

# Import python modules
import datetime
import sqlite3
import sys

# Add own module path
sys.path.append('S:/_my_modules')

# Import own modules
import funcdate
import funccsv
import funcfile

# OPEN THE SCRIPT LOG FILE *****************************************************

funcfile.writelog()
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: Report_vss_deferments")

# DECLARE MODULE WIDE VARIABLES ************************************************

so_path = "W:/Vss_deferment/" #Source database path
so_file = "Vss_deferment.sqlite" #Source database
s_sql = "" #SQL statements

# OPEN DATA FILES **************************************************************

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("%t OPEN DATABASE: Vss_deferment")
so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")

funcfile.writelog("%t OPEN DATABASE: Kfs_vss_studdeb")
so_curs.execute("ATTACH DATABASE 'W:/Kfs_vss_studdeb/Kfs_vss_studdeb.sqlite' AS 'TRAN'")
funcfile.writelog("%t ATTACH DATABASE: Kfs_vss_studdeb.sqlite")

# ADD THE BALANCES TO THE LIST OF REGISTERED STUDENTS **************************

print("Add the calculated balances to the students list...")
sr_file = "X001aa_Students"
s_sql = "CREATE TABLE " + sr_file+ " AS" + """
SELECT
  X000_Students_curr.*,
  X001aa_Trans_balopen.BAL_OPEN,
  X001ad_Trans_balreg.BAL_REG,
  X001ae_Trans_crebefreg.CRE_REG_BEFORE,
  X001af_Trans_creaftreg.CRE_REG_AFTER,
  X001ab_Trans_feereg.FEE_REG,
  CAST(0 AS REAL) AS BAL_REG_CALC
FROM
  X000_Students_curr
  LEFT JOIN X001ad_Trans_balreg ON X001ad_Trans_balreg.STUDENT_VSS = X000_Students_curr.KSTUDBUSENTID
  LEFT JOIN X001aa_Trans_balopen ON X001aa_Trans_balopen.STUDENT_VSS = X000_Students_curr.KSTUDBUSENTID
  LEFT JOIN X001ae_Trans_crebefreg ON X001ae_Trans_crebefreg.STUDENT_VSS = X000_Students_curr.KSTUDBUSENTID
  LEFT JOIN X001af_Trans_creaftreg ON X001af_Trans_creaftreg.STUDENT_VSS = X000_Students_curr.KSTUDBUSENTID
  LEFT JOIN X001ab_Trans_feereg ON X001ab_Trans_feereg.STUDENT_VSS = X000_Students_curr.KSTUDBUSENTID
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD TABLE: " + sr_file)
# Calc indicator for non-entering and first-time students
print("Add column bal_reg_calc...")
so_curs.execute("UPDATE " + sr_file + """
                SET BAL_REG_CALC =
                CASE
                    WHEN TYPEOF(BAL_OPEN) = "null" AND TYPEOF(CRE_REG_BEFORE) = "null" THEN 0
                    WHEN TYPEOF(BAL_OPEN) = "null" THEN CRE_REG_BEFORE
                    WHEN TYPEOF(CRE_REG_BEFORE) = "null"  THEN BAL_OPEN
                    ELSE BAL_OPEN + CRE_REG_BEFORE
                END
                ;""")
so_conn.commit()
funcfile.writelog("%t ADD COLUMN: entry_level_calc")










# CLOSE THE DATABASE CONNECTION ************************************************
so_conn.close()

# CLOSE THE LOG WRITER *********************************************************
funcfile.writelog("COMPLETED")
