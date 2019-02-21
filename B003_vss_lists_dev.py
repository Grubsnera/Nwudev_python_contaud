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
funcfile.writelog("SCRIPT: B003_VSS_LISTS_DEV")
funcfile.writelog("--------------------------")
print("--------------")
print("B003_VSS_LISTS")
print("--------------")
ilog_severity = 1

# Declare variables
so_path = "W:/Vss/" #Source database path
re_path = "R:/Vss/" #Results
so_file = "Vss.sqlite" #Source database
ed_path = "S:/_external_data/"
s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("%t OPEN DATABASE: VSS.SQLITE")

# Build test code **************************************************************

# BUILD PREVIOUS YEAR TRANSACTIONS *****************************************

print("Build previous year transactions...")
sr_file = "X010_Studytrans_prev"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  STUDYTRANS_PREV.KACCTRANSID,
  STUDYTRANS_PREV.FACCID,
  STUDACC.FBUSENTID,
  STUDYTRANS_PREV.FSERVICESITE,
  STUDYTRANS_PREV.FDEBTCOLLECTIONSITE,
  STUDYTRANS_PREV.TRANSDATE,
  STUDYTRANS_PREV.AMOUNT,
  STUDYTRANS_PREV.FTRANSMASTERID,
  X000_Transmaster.TRANSCODE,
  X000_Transmaster.DESCRIPTION_E,
  X000_Transmaster.DESCRIPTION_A,
  STUDYTRANS_PREV.TRANSDATETIME,
  STUDYTRANS_PREV.MONTHENDDATE,
  STUDYTRANS_PREV.POSTDATEDTRANSDATE,
  STUDYTRANS_PREV.FFINAIDSITEID,
  X004_Bursaries.FINAIDCODE,
  X004_Bursaries.FINAIDNAAM,
  STUDYTRANS_PREV.FRESIDENCELOGID,
  STUDYTRANS_PREV.FLEVYLOGID,
  STUDYTRANS_PREV.FMODAPID,
  STUDYTRANS_PREV.FQUALLEVELAPID,
  STUDYTRANS_PREV.FPROGAPID,
  STUDYTRANS_PREV.FENROLPRESID,
  STUDYTRANS_PREV.FRESIDENCEID,
  STUDYTRANS_PREV.FRECEIPTID,
  STUDYTRANS_PREV.FROOMTYPECODEID,
  STUDYTRANS_PREV.REFERENCENO,
  STUDYTRANS_PREV.FSUBACCTYPECODEID,
  STUDYTRANS_PREV.FDEPOSITCODEID,
  STUDYTRANS_PREV.FDEPOSITTYPECODEID,
  STUDYTRANS_PREV.FVARIABLEAMOUNTTYPECODEID,
  STUDYTRANS_PREV.FDEPOSITTRANSTYPECODEID,
  STUDYTRANS_PREV.RESIDENCETRANSTYPE,
  STUDYTRANS_PREV.FSTUDYTRANSTYPECODEID,
  STUDYTRANS_PREV.ISSHOWN,
  STUDYTRANS_PREV.ISCREATEDMANUALLY,
  STUDYTRANS_PREV.FTRANSINSTID,
  STUDYTRANS_PREV.FMONTHENDORGUNITNO,
  STUDYTRANS_PREV.LOCKSTAMP,
  STUDYTRANS_PREV.AUDITDATETIME,
  STUDYTRANS_PREV.FAUDITSYSTEMFUNCTIONID,
  STUDYTRANS_PREV.FAUDITUSERCODE,
  SYSTEMUSER.FUSERBUSINESSENTITYID,
  STUDYTRANS_PREV.FORIGINSYSTEMFUNCTIONID,
  STUDYTRANS_PREV.FPAYMENTREQUESTID
FROM
  STUDYTRANS_PREV
  LEFT JOIN STUDACC ON STUDACC.KACCID = STUDYTRANS_PREV.FACCID
  LEFT JOIN X000_Transmaster ON X000_Transmaster.KTRANSMASTERID = STUDYTRANS_PREV.FTRANSMASTERID
  LEFT JOIN X004_Bursaries ON X004_Bursaries.KFINAIDSITEID = STUDYTRANS_PREV.FFINAIDSITEID
  LEFT JOIN SYSTEMUSER ON SYSTEMUSER.KUSERCODE = STUDYTRANS_PREV.FAUDITUSERCODE
ORDER BY
  STUDYTRANS_PREV.TRANSDATETIME
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: " + sr_file)










# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("-----------------------------")
funcfile.writelog("COMPLETED: B003_VSS_LISTS_DEV")
