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
so_path = "W:/" #Source database path
re_path = "R:/Vss/" #Results
so_file = "Vss.sqlite" #Source database
ed_path = "S:/_external_data/"
s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

funcfile.writelog("%t OPEN DATABASE: VSS.SQLITE")

# Build test code **************************************************************

# Ask student list on which day ************************************************
print("")
s_date = input("Students on which day? (yyyy-mm-dd) ")
print("")

funcfile.writelog("STUDENT LIST ON " + s_date)

# Build current student qualification results **********************************
print("Build list of current students on specific day...")
sr_file = "X001_Student_qual_curr_day"
s_sql = "CREATE TABLE "+ sr_file +" AS " + """
SELECT
  X001cx_Stud_qual_curr.*
FROM
  X001cx_Stud_qual_curr
WHERE
  (X001cx_Stud_qual_curr.DATEENROL <= Date('%DAY%') AND
  X001cx_Stud_qual_curr.DISCONTINUEDATE >= Date('%DAY%') AND
  Upper(X001cx_Stud_qual_curr.QUAL_TYPE) != 'SHORT COURSE') OR
  (X001cx_Stud_qual_curr.DATEENROL <= Date('%DAY%') AND
  X001cx_Stud_qual_curr.DISCONTINUEDATE IS NULL AND
  Upper(X001cx_Stud_qual_curr.QUAL_TYPE) != 'SHORT COURSE')
"""
s_sql = s_sql.replace("%DAY%",s_date)
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()

funcfile.writelog("%t BUILD TABLE: " + sr_file)

# Export the data
print("Export gl student debtor transactions...")
sr_filet = sr_file
sx_path = re_path + funcdate.cur_year() + "/"
sx_file = "Student_001_all_"
sx_filet = sx_file + funcdate.prev_monthendfile()
s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)    
funcfile.writelog("%t EXPORT DATA: "+sx_path+sx_file)










# Close the connection *********************************************************
so_conn.close()

# Close the log writer *********************************************************
funcfile.writelog("-----------------------------")
funcfile.writelog("COMPLETED: B003_VSS_LISTS_DEV")
