""" ORACLE_TO_SQLITE ***********************************************************
Script to extract raw data from an ODBC data source
Copyright (c) AB Janse van Rensburg 10 Feb 2018
"""

#from __future__ import generators
#wait = input("PRESS ENTER TO CONTINUE.")

def Oracle_to_sqlite():

    # Import python objects *******************************************************
    import csv
    import datetime
    import pyodbc
    import sqlite3
    import sys
    import time

    # Add own module path
    sys.path.append('S:/_my_modules')

    # Import own modules ***********************************************************
    import funcdate
    import funcfile
    import funcstr
    import funcsys

    # Open the script log file ******************************************************
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: A001_ORACLE_TO_SQLITE")
    funcfile.writelog("-----------------------------")
    ilog_severity = 1

    # Declare the global variables ************************************************

    sl_path = "S:/"

    so_dsn = "" #Source file name
    so_usr = "" #Source file user name
    so_pwd = "" #Source file password

    de_fil = "" #Destination file name
    de_pat = "" #Destination file path

    tb_own = "" #Table owner
    tb_nam = "" #Table name
    tb_whe = "" #Table filter / where clause
    tb_ord = "" #Table sort
    tb_alt = "" #Table alternative name
    tb_sch = "" #Table schedule 

    co_nam = "" #Column name
    co_ana = "" #Column alternate name
    co_typ = "" #Column type
    co_len = 0  #Column width
    co_dec = 0  #Column decimals

    sco_nam = "" #Column string with actual names
    sco_ana = "" #Column string with alternate names
    sco_dro = "" #Column drop table

    ssql_create = "" #Sql to create a table

    if ilog_severity >= 2:
        funcfile.writelog("DECLARED: public variables")

    # DATABASE from text ***********************************************************

    # Read the database parameters from the 01_Database.csv file

    db = open(sl_path + "000a_Database.csv", "rU")
    db_reader = csv.reader(db)
    if ilog_severity >= 2:
        funcfile.writelog("OPENED: 000a_database.csv (DATABASE MASTER LIST)")

    #Read the DATABASE database data

    for row in db_reader:

        #Populate the database variables
        if row[0] == "DESC":
            continue
        else:
            
            funcfile.writelog("OPEN DATABASE: " + row[0])
            print("--------")
            print("DATABASE: " + row[0])
            print("--------")
            so_dsn = row[1]
            so_usr = row[2]
            so_pwd = row[3]
            de_fil = row[4]
            de_pat = row[5]
            de_sch = row[6] #Database schema

        # Open the source ORACLE database
        #print("DSN="+so_dsn+";PWD="+so_pwd)
        with pyodbc.connect("DSN="+so_dsn+";PWD="+so_pwd) as so_con:
            so_cur = so_con.cursor()

        # Open the destination SQLite database
        #print(de_pat+de_fil)
        with sqlite3.connect(de_pat+de_fil) as de_con:
            de_cur = de_con.cursor()

        # TABLE info from text *****************************************************

        # Read the table parameters from the 02_Table.csv file
        tb = open(sl_path + "000b_Table.csv", "rU")
        tb_reader = csv.reader(tb)

        # Read the TABLE database data
        for row in tb_reader:

            tb_own = "" #Table owner
            tb_nam = "" #Table name
            tb_whe = "" #SQL where clause
            tb_ord = "" #SQL sort clause
            tb_alt = "" #Table alternative name
            tb_sch = "" #Table schedule
            tb_extract = False

            # Populate the table variables
            if row[0] == "DESC":
                continue
            else:
                if row[1] != de_fil:
                    # Ignore the header
                    continue
                elif funcstr.isNotBlank(row[7]):
                    if row[7] == "X":
                        # Do not do
                        continue
                    elif row[7] == funcdate.today_dayname():
                        # Do if table schedule = day of week
                        tb_extract = True
                    elif row[7] == funcdate.cur_daystrip():
                        # Do if table schedule = day of month
                        tb_extract = True                    
                    else:
                        continue
                else:
                    tb_extract = True

                if tb_extract == True:
                    tb_own = row[2] #Table owner
                    tb_nam = row[3] #Table name
                    tb_whe = row[4] #SQL where clause
                    tb_ord = row[5] #SQL sort clause
                    tb_alt = row[6] #Table alternative name
                    tb_sch = row[7] #Table schedule
                else:
                    continue

            # COLUMN info from text ************************************************

            # Read the table parameters from the 02_Table.csv file """
            co = open(sl_path + "000c_Column.csv", "rU")
            co_reader = csv.reader(co) 

            # Read the COLUMN database data
            sco_nam = ""
            sco_lst = ""
            lco_lst = []

            sty_nam = ""
            sty_lst = ""
            lty_lst = []

            sma_nam = ""
            sma_lst = ""
            lma_lst = []
            
            sco_ana = ""
            sco_dro = ""


            for row in co_reader:

                # Populate the column variables
                if row[0] == "DESC":
                    continue
                else:
                    if row[1] != so_dsn:
                        continue
                    else:
                        if row[2] != tb_nam:
                            continue
                        else:
                            
                            #print("COLUMN: " + row[4])
                            
                            # Populate variables
                            co_nam = row[4] #Column name
                            co_ana = row[8] #Column alternate name
                            co_typ = row[5] #Column type
                            co_len = row[6] #Column width
                            co_dec = row[7] #Column decimals
                            
                            # Populate column string with actual names
                            sco_nam += ''.join(row[4]) + ", "
                            sco_lst += row[4] + " "

                            # Populate column string with comlumn type
                            sty_nam += ''.join(row[5]) + ", "
                            sty_lst += row[5] + " "

                            # Populate column string with begin end date marker
                            s_startdate = """
                            STARTDATE*
                            STARTDATETIME*
                            KSTARTDATETIME*
                            DATE_FROM*
                            START_DATE_ACTIVE
                            """
                            s_enddate = """
                            ENDDATE*
                            ENDDATETIME*
                            KSTARTDATETIME*
                            DATE_TO*
                            END_DATE_ACTIVE
                            """
                            if row[4] in s_startdate:
                                sma_nam += "B, "
                                sma_lst += "B "
                            elif row[4] in s_enddate:
                                sma_nam += "E, "
                                sma_lst += "E "
                            else:
                                sma_nam += "N, "
                                sma_lst += "N "
                            
                            # Populate column string with alternate names
                            if co_typ == "NUMBER" and co_dec != "0":
                                sco_ana = sco_ana + ''.join(row[8]) + " REAL,"
                            elif co_typ == "NUMBER"                        :
                                sco_ana = sco_ana + ''.join(row[8]) + " INTEGER,"
                            else:
                                sco_ana = sco_ana + ''.join(row[8]) + " TEXT,"
                                
            # Create the sql create table variable
            sco_nam = sco_nam.rstrip(", ")
            lco_lst = sco_lst.split()
            sty_nam = sty_nam.rstrip(", ")
            lty_lst = sty_lst.split()
            sma_nam = sma_nam.rstrip(", ")
            lma_lst = sma_lst.split()

            if tb_alt != "":
                if "%CYEAR%" in tb_alt:
                    tb_alt = tb_alt.replace("%CYEAR%",funcdate.cur_year())
                if "%CMONTH%" in tb_alt:
                    tb_alt = tb_alt.replace("%CMONTH%",funcdate.cur_month())
                if "%PYEAR%" in tb_alt:
                    tb_alt = tb_alt.replace("%PYEAR%",funcdate.prev_year())
                if "%PMONTH%" in tb_alt:
                    tb_alt = tb_alt.replace("%PMONTH%",funcdate.prev_month())
                ssql_dro = "DROP TABLE IF EXISTS " + tb_alt
                ssql_create = "CREATE TABLE " + tb_alt + "(" + sco_ana + ")"
            else:
                ssql_dro = "DROP TABLE IF EXISTS " + tb_nam
                ssql_create = "CREATE TABLE " + tb_nam + "(" + sco_ana + ")"
            ssql_create = ssql_create.replace(",)",")",1)

            #print(ssql_create)

            funcfile.writelog("%t WRITE TABLE: " + tb_nam + "(" + tb_alt + ")")
            print("TABLE: " + tb_nam + "(" + tb_alt + ")")

            co.close()

            # Create the DESTINATION table
            de_cur.execute(ssql_dro)
            de_cur.execute(ssql_create)
              
            # Write the data
            ssql_str = "SELECT "
            ssql_str += sco_nam
            ssql_str += " FROM "
            if tb_own == "":
                ssql_str += tb_nam
            else:
                ssql_str += tb_own + "." + tb_nam
            if tb_whe != "":
                if "%CYEAR%" in tb_whe:
                    tb_whe = tb_whe.replace("%CYEAR%",funcdate.cur_year())
                if "%CYEARB%" in tb_whe:
                    tb_whe = tb_whe.replace("%CYEARB%",funcdate.cur_yearbegin())
                if "%CYEARE%" in tb_whe:
                    tb_whe = tb_whe.replace("%CYEARE%",funcdate.cur_yearend())            
                if "%CMONTHB%" in tb_whe:
                    tb_whe = tb_whe.replace("%CMONTHB%",funcdate.cur_monthbegin())
                if "%CMONTHE%" in tb_whe:
                    tb_whe = tb_whe.replace("%CMONTHE%",funcdate.cur_monthend())
                if "%PYEAR%" in tb_whe:
                    tb_whe = tb_whe.replace("%PYEAR%",funcdate.prev_year())
                if "%PMONTHB%" in tb_whe:
                    tb_whe = tb_whe.replace("%PMONTHB%",funcdate.prev_monthbegin())
                if "%PMONTHE%" in tb_whe:
                    tb_whe = tb_whe.replace("%PMONTHE%",funcdate.prev_monthend())
                if "%PYEARB%" in tb_whe:
                    tb_whe = tb_whe.replace("%PYEARB%",funcdate.prev_yearbegin())
                if "%PYEARE%" in tb_whe:
                    tb_whe = tb_whe.replace("%PYEARE%",funcdate.prev_yearend())            
                    
                ssql_str = ssql_str + " " + tb_whe        
            if tb_ord != "":
                ssql_str = ssql_str + " ORDER BY " + tb_ord
                
            #print(ssql_str)
            #print("Name")    
            #print(lco_lst)
            #print("Type")
            #print(lty_lst)
            #print("Marker")
            #print(lma_lst)

            for result in funcsys.ResultIter(so_cur.execute(ssql_str)):
                c_text = ""
                c_test = ""
                c_data = "("
                i = 0
                for item in result:

                    if lty_lst[i] == "DATE":
                        if str(item) == "None" or str(item) == "":
                            if lma_lst[i] == "B":
                                c_test = "0001-01-01"
                            elif lma_lst[i] == "E":
                                c_test = "4712-12-31"
                        else:
                            c_test = str(item)
                            c_test = c_test[0:10]
                    elif lty_lst[i] == "DATETIME":
                        if str(item) == "None" or str(item) == "":
                            if lma_lst[i] == "B":
                                c_test = "0001-01-01 00:00:00"
                            elif lma_lst[i] == "E":
                                c_test = "4712-12-31 23:59:59"
                        else:
                            c_test = str(item)
                    else:
                        c_test = str(item)
                        c_test = c_test.replace(",","")
                        c_test = c_test.replace("'","")
                        c_test = c_test.replace('"','')
                        c_test = c_test.replace("None","")
                    
                    c_data += "'" + c_test + "',"
                    i += 1
                c_data += ")"
                c_data = c_data.replace(",)",")",1)
                
                if tb_alt != "":
                    c_text = 'INSERT INTO ' + tb_alt + ' VALUES' + c_data
                else:
                    c_text = 'INSERT INTO ' + tb_nam + ' VALUES' + c_data

                #print(c_text)    
                    
                de_cur.execute(c_text)
            de_con.commit()

            # Wait a few seconds for log file to close
            #time.sleep(10)

        # Close 02_Table.csv
        tb.close()

        # Display the number of tables in the destination
        for table in de_cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'"):
            print("TABLE in DESTINATION: ", table[0])

        # Display the number of tables in the destination
        for table in de_cur.execute("SELECT name FROM sqlite_master WHERE type = 'view'"):
            print("VIEW in DESTINATION: ", table[0])        

        # Close the destination
        de_con.close()

        # Close the source
        so_con.close()
       
    # Close 01_Database.csv
    db.close()

    # Close the log writer *********************************************************
    funcfile.writelog("--------------------------------")
    funcfile.writelog("COMPLETED: A001_ORACLE_TO_SQLITE")

    return
