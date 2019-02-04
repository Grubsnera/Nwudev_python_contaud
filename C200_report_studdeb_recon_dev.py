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
import funcmysql


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

# Open the MYSQL DESTINATION table
s_database = "Web_ia_nwu"
ms_cnxn = funcmysql.mysql_open(s_database)
ms_curs = ms_cnxn.cursor()
funcfile.writelog("%t OPEN MYSQL DATABASE: " + s_database)

# Development script ***********************************************************



# Transfer GL transactions to the VSS file *********************************
# Open the SOURCE file to obtain column headings
print("Transfer english gl data to the vss table...")
funcfile.writelog("%t GET COLUMN HEADINGS: X003aa_vss_gl_join_eng")
s_head = funcmysql.get_colnames_sqlite_text(so_curs,"X003aa_vss_gl_join_eng","")
s_head = "(" + s_head.rstrip(", ") + ")"
#print(s_head)
# Open the SOURCE file to obtain the data
print("Insert english gl data into vss table...")
#with sqlite3.connect(so_path+so_file) as rs_conn:
#    rs_conn.row_factory = sqlite3.Row
#rs_curs = rs_conn.cursor()
so_curs.execute("SELECT * FROM X003aa_gl_vss_join_eng")
rows = so_curs.fetchall()
i_tota = 0
i_coun = 0
for row in rows:
    s_data = "("
    for member in row:
        #print(type(member))
        if type(member) == str:
            s_data = s_data + "'" + member + "', "
        elif type(member) == int:
            s_data = s_data + str(member) + ", "
        elif type(member) == float:
            s_data = s_data + str(member) + ", "
        else:
            s_data = s_data + "'', "
    s_data = s_data.rstrip(", ") + ")"
    #print(s_data)
    s_sql = "INSERT INTO `X003aa_vss_gl_join_eng` " + s_head + " VALUES " + s_data + ";"
    so_curs.execute(s_sql)
    i_tota = i_tota + 1
    i_coun = i_coun + 1
    if i_coun == 100:
        so_conn.commit()
        i_coun = 0
so_conn.commit()        
print("Inserted " + str(i_tota) + " rows...")
funcfile.writelog("%t POPULATE TABLE: X003aa_vss_gl_join_eng with " + str(i_tota) + " rows")

# Report on vss and gl transaction type join *******************************
print("Report vss gl join transaction type...")
sr_file = "X003ax_vss_gl_join_eng"
s_sql = "CREATE TABLE "+sr_file+" AS " + """
SELECT
  X003aa_vss_gl_join_eng.CAMPUS_VSS AS CAMPUS,
  X003aa_vss_gl_join_eng.MONTH_VSS AS MONTH,
  X003aa_vss_gl_join_eng.TRANSCODE_VSS AS TRANCODE,
  X003aa_vss_gl_join_eng.TEMP_DESC_E AS VSS_DESCRIPTION,
  CAST(X003aa_vss_gl_join_eng.AMOUNT_VSS AS REAL) AS VSS_AMOUNT,
  X003aa_vss_gl_join_eng.DESC_VSS AS GL_DESCRIPTION,
  CAST(X003aa_vss_gl_join_eng.AMOUNT AS REAL) AS GL_AMOUNT,
  X003aa_vss_gl_join_eng.DIFF,
  X003aa_vss_gl_join_eng.MATCHED,
  X003aa_vss_gl_join_eng.PERIOD
FROM
  X003aa_vss_gl_join_eng
ORDER BY
  CAMPUS,
  MONTH
;"""
so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: "+sr_file)    
print("Export vss gl recon...")
sr_filet = sr_file
sx_path = re_path + funcdate.cur_year() + "/"
sx_file = "Debtor_003_vss_gl_recon_eng_"
sx_filet = sx_file + funcdate.cur_monthendfile()
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)
funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)







# Close the table connection ***************************************************
so_conn.commit()
so_conn.close()
ms_cnxn.commit()
ms_cnxn.close()

# Close the log writer *********************************************************
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C200_REPORT_STUDDEB_RECON_DEV")
