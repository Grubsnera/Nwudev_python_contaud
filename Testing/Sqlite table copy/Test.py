# Import python modules
import funccsv
import sqlite3

# Declare variables
ma_path = "W:/" #Source database path
ma_file = "People.sqlite" #Source database

# Open the SOURCE file
with sqlite3.connect(ma_path+ma_file) as ma_conn:
    ma_curs = ma_conn.cursor()

lcol = funccsv.get_colnames_sqlite(ma_curs,"PER_ALL_PEOPLE_F")    

ma_curs.close()

print(lcol)




# Declare variables

so_path = "W:/" #Source database path
so_file = "People_leave.sqlite" #Source database

s_sql = "" #SQL statements

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()

so_curs.execute("DROP TABLE IF EXISTS PER_ALL_PEOPLE_F")
so_curs.execute("ATTACH DATABASE 'People.sqlite' AS 'MASTER'")
so_curs.execute("SELECT sql FROM MASTER.sqlite_master WHERE type='table' AND name='PER_ALL_PEOPLE_F'")
print(so_curs.fetchone()[0])
so_curs.execute("so_curs.fetchone()")
so_curs.execute("INSERT INTO main.PER_ALL_PEOPLE_F SELECT * FROM MASTER.PER_ALL_PEOPLE_F")


# Close the connection
so_curs.close()
