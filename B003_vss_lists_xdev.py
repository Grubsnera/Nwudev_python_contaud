"""
Development area for VSS lists
12 Sep 2019
AB Janse van Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import csv
import sqlite3

# IMPORT OWN MODULES
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcstudent

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
funcfile.writelog("SCRIPT: B003_VSS_LISTS_XDEV")
funcfile.writelog("---------------------------")
print("-------------------")
print("B003_VSS_LISTS_XDEV")
print("-------------------")

# DECLARE VARIABLES
ed_path = "S:/_external_data/"  # External data path
so_path = "W:/Vss/"  # Source database path
so_file = "Vss.sqlite"  # Source database
re_path = "R:/Vss/"
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

# ATTACH VSS DATABASE
# print("Attach vss database...")

"""*****************************************************************************
TEMPORARY AREA
*****************************************************************************"""
print("TEMPORARY AREA")
funcfile.writelog("TEMPORARY AREA")

# IMPORT OWN LOOKUPS
print("Import own lookups...")
sr_file = "X000_OWN_LOOKUPS"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute("CREATE TABLE " + sr_file + "(LOOKUP TEXT,LOOKUP_CODE TEXT,LOOKUP_DESCRIPTION TEXT)")
s_cols = ""
co = open(ed_path + "001_own_vss_lookups.csv", newline=None)
co_reader = csv.reader(co)
for row in co_reader:
    if row[0] == "LOOKUP":
        continue
    else:
        s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "')"
        so_curs.execute(s_cols)
so_conn.commit()
co.close()
funcfile.writelog("%t IMPORT TABLE: " + sr_file)

"""*****************************************************************************
BEGIN OF SCRIPT
*****************************************************************************"""
print("BEGIN OF SCRIPT")
funcfile.writelog("BEGIN OF SCRIPT")

# CALCULATE THE REGISTRATION FEES LEVIED
print("Join presentation and qualification levels...")
sr_file = "X006aa_present_quallevel"
s_sql = "Create view " + sr_file + " AS" + """
Select
    PRES.KENROLMENTPRESENTATIONID,
    PRES.FQUALPRESENTINGOUID,
    PRES.FENROLMENTCATEGORYCODEID,
    PRES.FPRESENTATIONCATEGORYCODEID,
    QUAL.FBUSINESSENTITYID,
    QUAL.FQUALLEVELAPID,
    PRES.STARTDATE As PRES_STARTDATE,
    PRES.ENDDATE As PRES_ENDDATE,
    PRES.AUDITDATETIME As PRES_AUDITDATE,
    PRES.FAUDITUSERCODE As PRES_AUDITUSER,
    QUAL.STARTDATE As QUAL_STARTDATE,
    QUAL.ENDDATE As QUAL_ENDDATE,
    QUAL.AUDITDATETIME As QUAL_AUDITDATE,
    QUAL.FAUDITUSERCODE As QUAL_AUDITUSER
From
    PRESENTOUENROLPRESENTCAT PRES Left Join
    QUALLEVELPRESENTINGOU QUAL On QUAL.KPRESENTINGOUID = PRES.FQUALPRESENTINGOUID
;"""
so_curs.execute("DROP VIEW IF EXISTS " + sr_file)
so_curs.execute(s_sql)
funcfile.writelog("%t BUILD VIEW: " + sr_file)

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
funcfile.writelog("------------------------------")
funcfile.writelog("COMPLETED: B003_VSS_LISTS_XDEV")
