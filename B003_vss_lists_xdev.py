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

"""*****************************************************************************
BEGIN
*****************************************************************************"""

so_curs.execute("DROP TABLE IF EXISTS STUDYTRANS_2017")
so_curs.execute("DROP TABLE IF EXISTS X001ax_Student_curr")
so_curs.execute("DROP TABLE IF EXISTS X001cx_Stud_qual_curr")
so_curs.execute("DROP TABLE IF EXISTS X001cx_Stud_qual_peri")
so_curs.execute("DROP VIEW IF EXISTS X000_Student_qual_result_curr")
so_curs.execute("DROP VIEW IF EXISTS X000_Student_qual_result_peri")
so_curs.execute("DROP VIEW IF EXISTS X001aa_Qualification")
so_curs.execute("DROP VIEW IF EXISTS X001ba_Qualification_level")
so_curs.execute("DROP VIEW IF EXISTS X001ca_Stud_qual_curr")
so_curs.execute("DROP VIEW IF EXISTS X001ca_Stud_qual_peri")
so_curs.execute("DROP VIEW IF EXISTS X001cb_Stud_qual_curr")
so_curs.execute("DROP VIEW IF EXISTS X001cb_Stud_qual_peri")
so_curs.execute("DROP VIEW IF EXISTS X001cc_Stud_qual_curr")
so_curs.execute("DROP VIEW IF EXISTS X001cc_Stud_qual_peri")
so_curs.execute("DROP VIEW IF EXISTS X001cd_Stud_qual_curr")
so_curs.execute("DROP VIEW IF EXISTS X001cd_Stud_qual_peri")


# BUILD PERIOD TRANSACTIONS *****************************************
print("Build period transactions...")
sr_file = "X010_Studytrans_peri"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
  TRAN.KACCTRANSID,
  TRAN.FACCID,
  STUDACC.FBUSENTID,
  TRAN.FSERVICESITE,
  TRAN.FDEBTCOLLECTIONSITE,
  TRAN.TRANSDATE,
  TRAN.AMOUNT,
  TRAN.FTRANSMASTERID,
  TRANMAST.TRANSCODE,
  TRANMAST.DESCRIPTION_E,
  TRANMAST.DESCRIPTION_A,
  TRAN.TRANSDATETIME,
  TRAN.MONTHENDDATE,
  TRAN.POSTDATEDTRANSDATE,
  TRAN.FFINAIDSITEID,
  BURS.FINAIDCODE,
  BURS.FINAIDNAAM,
  TRAN.FRESIDENCELOGID,
  TRAN.FLEVYLOGID,
  TRAN.FMODAPID,
  TRAN.FQUALLEVELAPID,
  TRAN.FPROGAPID,
  TRAN.FENROLPRESID,
  TRAN.FRESIDENCEID,
  TRAN.FRECEIPTID,
  TRAN.FROOMTYPECODEID,
  TRAN.REFERENCENO,
  TRAN.FSUBACCTYPECODEID,
  TRAN.FDEPOSITCODEID,
  TRAN.FDEPOSITTYPECODEID,
  TRAN.FVARIABLEAMOUNTTYPECODEID,
  TRAN.FDEPOSITTRANSTYPECODEID,
  TRAN.RESIDENCETRANSTYPE,
  TRAN.FSTUDYTRANSTYPECODEID,
  TRAN.ISSHOWN,
  TRAN.ISCREATEDMANUALLY,
  TRAN.FTRANSINSTID,
  TRAN.FMONTHENDORGUNITNO,
  TRAN.LOCKSTAMP,
  TRAN.AUDITDATETIME,
  TRAN.FAUDITSYSTEMFUNCTIONID,
  TRAN.FAUDITUSERCODE,
  USER.FUSERBUSINESSENTITYID,
  TRAN.FORIGINSYSTEMFUNCTIONID,
  TRAN.FPAYMENTREQUESTID
FROM
  STUDYTRANS_PERI TRAN
  LEFT JOIN STUDACC ON STUDACC.KACCID = TRAN.FACCID
  LEFT JOIN X000_Transmaster TRANMAST ON TRANMAST.KTRANSMASTERID = TRAN.FTRANSMASTERID
  LEFT JOIN X004_Bursaries BURS ON BURS.KFINAIDSITEID = TRAN.FFINAIDSITEID
  LEFT JOIN SYSTEMUSER USER ON USER.KUSERCODE = TRAN.FAUDITUSERCODE
ORDER BY
  TRAN.TRANSDATETIME
"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD VIEW: " + sr_file)    


"""*****************************************************************************
END
*****************************************************************************"""

# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("-----------------------------")
funcfile.writelog("COMPLETED: B003_VSS_LISTS_DEV")
