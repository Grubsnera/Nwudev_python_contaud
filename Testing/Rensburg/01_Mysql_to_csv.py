"""
Script to extract raw data from Oracle HR System
Created on: 02 April 2018
Created by: Albert J v Rensburg (21162395)
Modified on:
"""

import sys

# Add own module path
sys.path.append('S:/_my_modules')

# Import python objects
import csv
import pyodbc

# Define Functions
import funcfile
import funcstr

def write_header(o_odbc, c_whd, c_wht):
    
    # Write a column header file
    
    """
    Parameter
        whf = The name of the table for which to obtain header record
    """
    
    f = open(c_whd + "/COLUMN_NAMES.csv", "w")
    csvf = csv.writer(f, lineterminator='\r', quoting=csv.QUOTE_NONNUMERIC)
    for row in o_odbc.execute("select * from information_schema.columns where table_schema = 'joomla' and table_name = '" + c_wht + "'").fetchall():
        csvf.writerow(row)
    f.close()

def read_header(c_rhd):
    
    # Read the columns from the column header file
    
    header_list = ""
    f = open(c_rhd + "/COLUMN_NAMES.csv", "rU")
    reader = csv.reader(f)
    for row in reader:
        iCoun = 0
        for item in row:
            if iCoun == 3:
                header_list += '"' + item + '",'
            iCoun += 1
    f.close()
    return header_list

def write_data(o_odbc, c_wdd, c_wdo, c_wdt, c_wdh):
    
    # Extract and read the data
    
    """
    Parameter
        wdc = ODBC Cursor
        wdd = Data file destination
        wdf = Data file name
        wdt = Data table name
        wdh = Data table header record
    """
    
    """
    f = open(c_wdd + "/" + c_wdt + ".csv", "w", encoding="utf-8")
    csvf = csv.writer(f, lineterminator='\r', quoting=csv.QUOTE_NONNUMERIC)
    csvf.writerow(c_wdh)
    for row in o_odbc.execute("select * from " + c_wdt ).fetchall():
        csvf.writerow(row)
    f.close()
    """

    funcfile.writelog(c_wdh[:-1],c_wdd+"/",c_wdt+".csv")

    for row in o_odbc.execute("select * from " + c_wdt ).fetchall():
        r = ""
        s = ""
        i = 0
        for item in row:
            r = str(item)
            #r = funcstr.include(r,"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890")
            if i == 4 or i == 5:
                r = funcstr.exclude_markup(r)
                r = funcstr.include(r," abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890,.:/")
            else:
                r = funcstr.include(r," abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890,.:/{}-")
            s += '"' + r + '",'
            i += 1
        funcfile.writelog(s[:-1],c_wdd+"/",c_wdt+".csv")    

# Connect to the oracle database
cnxn = pyodbc.connect("DSN=Web_rensburg;PWD=d1Vv=oNge?g")
curs = cnxn.cursor()

# Declare variables
c_dest = "S:/Testing/Rensburg"

# Read the table info for which data should met extracted

tabcsvf = open("01_Mysql_to_csv.csv", "rU")
readert = csv.reader(tabcsvf)
for row in readert:

    c_owne = row[0]
    c_tabl = row[1]

    print(c_tabl)

    # Create a .csv file with the column names
    write_header(curs, c_dest, c_tabl)

    # Read the column names into a list variable
    c_head = read_header(c_dest)
    #print c_head

    # Write the data .csv with the header first
    write_data(curs, c_dest,c_owne,c_tabl,c_head)
        
tabcsvf.close()
