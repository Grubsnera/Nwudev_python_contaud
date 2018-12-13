""" FUNCMYSQL ******************************************************************
MYSQL Functions
Copyright (c) AB Janse v Rensburg 2018-10-27
**************************************************************************** """

""" Short description of functions *********************************************
mysql_open 		Open a MYSQL database and return the connection string
**************************************************************************** """

# Import the system objects
import pyodbc

# Function open a database via ODBC
def mysql_open(s_database):
    if s_database == "Web_ia_nwu":
        cnxn = pyodbc.connect("DSN=Web_ia_nwu; PWD=+C8+amXnmdo; Use Procedure Bodies=false;")
    elif s_database == "Web_ia_joomla":
        cnxn = pyodbc.connect("DSN=Web_ia_joomla; PWD=WbNpdPtDd0Q36pfIbXSHv8cAETPY0Ohd;")
    return cnxn # Connection

#Function to read table columns from sqlite3 table
def get_colnames_sqlite_text(os_cur,s_table,s_prefix):

    """ Parameter
    os_cur = ODBC Source Cursor
    s_table = Data table name
    s_prefix = Table prefix
    """
    
    s_data = ""
    for row in os_cur.execute("PRAGMA table_info(" + s_table + ")").fetchall():
        s_data = s_data + "`" + s_prefix + row[1].lower() + "`, "

    return s_data #Return column headers in text format

