"""
Script to build GL Student debtor control account reports
Created on: 13 Mar 2018
"""

# Import python modules
import csv
import datetime
import sqlite3
import sys

# Add own module path
sys.path.append('S:/_my_modules')

# Import own modules
import funcdate
import funccsv
import funcfile
import funcmail
import funcsys

# Open the script log file ******************************************************

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C200_REPORT_STUDDEB_RECON_DEV")
funcfile.writelog("-------------------------------------")
print("-------------------------")
print("C200_REPORT_STUDDEB_RECON")
print("-------------------------")
ilog_severity = 1

# Declare variables
so_path = "W:/Kfs_vss_studdeb/" #Source database path
re_path = "R:/Debtorstud/" #Results
ed_path = "S:/_external_data/" #External data
so_file = "Kfs_vss_studdeb.sqlite" #Source database
s_sql = "" #SQL statements
l_mail = True
l_export = True

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN DATABASE: Kfs_vss_studdeb")


#so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
#funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
#so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
#funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
#so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
#funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")

# Development script ***********************************************************


# BURSARY VSS GL RECON X010cc Group matched vss gl transactions on campus & month

print("Group matched vss and gl bursary transactions on campus and month...")
sr_file = "X010cc_group_vss_gl_matched"    
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X010cb_join_vss_gl_matched.CAMPUS_VSS,
  X010cb_join_vss_gl_matched.MONTH_VSS,
  Total(X010cb_join_vss_gl_matched.AMOUNT_VSS) AS Total_AMOUNT_VSS
FROM
  X010cb_join_vss_gl_matched
GROUP BY
  X010cb_join_vss_gl_matched.CAMPUS_VSS,
  X010cb_join_vss_gl_matched.MONTH_VSS
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)
# Export the data
print("Export grouped matched bursary transactions...")
sr_filet = sr_file
sx_path = re_path + funcdate.cur_year() + "/"
sx_file = "Debtor_010c_burs_matched_summmonth_"
sx_filet = sx_file + funcdate.prev_monthendfile()
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
#funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)





# Close the table connection ***************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C200_REPORT_STUDDEB_RECON_DEV")
