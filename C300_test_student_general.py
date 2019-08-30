""" C300_TEST_STUDENT_GENERAL **************************************************
*** Script to vss student general items
*** Albert J van Rensburg (21162395)
*** 25 Jun 2018
*****************************************************************************"""

def Test_student_general():

    # Import python module
    import csv
    import datetime
    import sqlite3
    import sys

    # Add own module path
    sys.path.append('S:/_my_modules')
    #print(sys.path)

    # Import own modules
    import funccsv
    import funcdate
    import funcfile
    import funcmail
    import funcsys

    # Open the script log file ******************************************************

    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C300_TEST_STUDENT_GENERAL")
    funcfile.writelog("---------------------------------")
    print("-------------------------")
    print("C300_TEST_STUDENT_GENERAL")
    print("-------------------------")
    ilog_severity = 1

    # Declare variables
    so_path = "W:/Vss_general/" #Source database path
    re_path = "R:/Vss/" #Results
    ed_path = "S:/_external_data/"
    so_file = "Vss_general.sqlite" #Source database
    s_sql = "" #SQL statements
    l_mail = True
    l_export = True

    # Open the SOURCE file
    with sqlite3.connect(so_path+so_file) as so_conn:
        so_curs = so_conn.cursor()

    funcfile.writelog("OPEN DATABASE: " + so_file)

    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
    funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")

    """*************************************************************************
    ***
    *** CREATE STUDENT ID NUMBER LISTS
    ***
    *** Import vss transactions from VSS.SQLITE
    *** Import vss party data from from VSS.SQLITE
    *** Import previously reported ID numbers
    *** Join the vss tran and party data
    *** Join the previously reported id number list
    ***   Add prev reported columns
    *** Export the ytd ID list to report
    *** Export the current ID list to report
    *** Add new ID number list to prev reported
    ***
    *************************************************************************"""

    print("---------- STUDENT ID NUMBER LIST ----------")
    funcfile.writelog("%t ---------- STUDENT ID NUMBER LIST ----------")

    # Import vss transactions from VSS.SQLITE *********************************
    print("IDNo list import vss transactions from VSS.SQLITE...")
    sr_file = "X001aa_impo_vsstran"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      TRAN.FBUSENTID AS STUDENT,
      TRAN.FDEBTCOLLECTIONSITE AS CAMPUS,
      SUBSTR(TRAN.TRANSDATE,1,4) AS YEAR
    FROM
      VSS.X010_Studytrans_curr TRAN
    GROUP BY
      TRAN.FBUSENTID
    ORDER BY
      TRAN.FBUSENTID
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # Import previously reported findings ******************************************
    print("IDNo list import previously reported findings...")
    tb_name = "X001ac_impo_reported"
    so_curs.execute("DROP TABLE IF EXISTS " + tb_name)
    so_curs.execute("CREATE TABLE " + tb_name + "(PROCESS TEXT,FIELD1 INT,FIELD2 TEXT,FIELD3 TEXT,FIELD4 TEXT,FIELD5 TEXT,DATE_REPORTED TEXT,DATE_RETEST TEXT)")
    s_cols = ""
    co = open(ed_path + "300_reported.txt", "rU")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "PROCESS":
            continue
        elif row[0] != "idno_list":
            continue
        else:
            s_cols = "INSERT INTO " + tb_name + " VALUES('" + row[0] + "','" + row[1] + "','" + row[2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[7] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the impoted data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "300_reported.txt (" + tb_name + ")" )

    # Join the tran and party data *********************************************
    print("IDNo list join the vss tran and party data...")
    sr_file = "X001ba_join_tran_vss"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      VSS.X000_party.IDNO,
      X001aa_impo_vsstran.STUDENT,
      VSS.X000_Party.FULL_NAME AS NAME,
      X001aa_impo_vsstran.YEAR,
      X001aa_impo_vsstran.CAMPUS,
      Trim(VSS.X000_Party.FIRSTNAMES) AS FIRSTNAME,
      VSS.X000_Party.INITIALS,
      VSS.X000_Party.SURNAME,
      VSS.X000_Party.TITLE,
      VSS.X000_Party.DATEOFBIRTH,
      VSS.X000_Party.GENDER,
      VSS.X000_Party.NATIONALITY,
      VSS.X000_Party.POPULATION,
      VSS.X000_Party.RACE,
      VSS.X000_Party.FAUDITUSERCODE AS PARTY_AUDITDATETIME,
      VSS.X000_Party.AUDITDATETIME AS PARTY_AUDITUSERCODE
    FROM
      X001aa_impo_vsstran
      INNER JOIN VSS.X000_Party ON VSS.X000_Party.KBUSINESSENTITYID = X001aa_impo_vsstran.STUDENT AND
        Length(Trim(VSS.X000_Party.IDNO)) = 13
    ORDER BY
      X001aa_impo_vsstran.STUDENT
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)

    # Add previous reported ID list to current ID list *****************************
    print("IDNo list join the previously reported id number list...")
    sr_file = "X001ca_join_prev_reported"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
      X001ba_join_tran_vss.IDNO,
      X001ba_join_tran_vss.STUDENT,
      X001ba_join_tran_vss.NAME,
      X001ba_join_tran_vss.YEAR,
      X001ba_join_tran_vss.CAMPUS,
      X001ba_join_tran_vss.FIRSTNAME,
      X001ba_join_tran_vss.INITIALS,
      X001ba_join_tran_vss.SURNAME,
      X001ba_join_tran_vss.TITLE,
      X001ba_join_tran_vss.DATEOFBIRTH,
      X001ba_join_tran_vss.GENDER,
      X001ba_join_tran_vss.NATIONALITY,
      X001ba_join_tran_vss.POPULATION,
      X001ba_join_tran_vss.RACE,
      X001ba_join_tran_vss.PARTY_AUDITDATETIME,
      X001ba_join_tran_vss.PARTY_AUDITUSERCODE,
      X001ac_impo_reported.PROCESS AS PREV_PROCESS,
      X001ac_impo_reported.DATE_REPORTED AS PREV_DATE_REPORTED,
      X001ac_impo_reported.DATE_RETEST AS PREV_DATE_RETEST
    FROM
      X001ba_join_tran_vss
      LEFT JOIN X001ac_impo_reported ON X001ac_impo_reported.FIELD1 = X001ba_join_tran_vss.STUDENT AND
        X001ac_impo_reported.DATE_RETEST >= Date('%TODAY%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%",funcdate.today())
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Add columns used to export new ID list
    print("IDNo list add prev reported columns...")
    so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN PROCESS TEXT;")
    so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN FIELD2 TEXT;")
    so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN DATE_REPORTED TEXT;")
    so_curs.execute("ALTER TABLE "+sr_file+" ADD COLUMN DATE_RETEST TEXT;")
    so_curs.execute("UPDATE "+sr_file+" SET PROCESS = 'idno_list'")
    s_sql = "UPDATE "+sr_file+" SET DATE_REPORTED = '%TODAY%'"
    s_sql = s_sql.replace("%TODAY%",funcdate.today())
    so_curs.execute(s_sql)
    s_sql = "UPDATE "+sr_file+" SET DATE_RETEST = '%NYEARB%'"
    s_sql = s_sql.replace("%NYEARB%",funcdate.next_yearbegin())
    so_curs.execute(s_sql)
    so_conn.commit()

    # Build the final ytd ID list report table *****************************************
    print("IDNo list build the ytd ID list to export...")
    sr_file = "X001da_report_idlist"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
      X001ca_join_prev_reported.IDNO,
      X001ca_join_prev_reported.STUDENT,
      X001ca_join_prev_reported.NAME,
      X001ca_join_prev_reported.YEAR,
      X001ca_join_prev_reported.CAMPUS,
      X001ca_join_prev_reported.FIRSTNAME,
      X001ca_join_prev_reported.INITIALS,
      X001ca_join_prev_reported.SURNAME,
      X001ca_join_prev_reported.TITLE,
      X001ca_join_prev_reported.DATEOFBIRTH,
      X001ca_join_prev_reported.GENDER,
      X001ca_join_prev_reported.NATIONALITY,
      X001ca_join_prev_reported.POPULATION,
      X001ca_join_prev_reported.RACE
    FROM
      X001ca_join_prev_reported
    ORDER BY
      X001ca_join_prev_reported.STUDENT
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export the data
    if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
        print("IDNo list export ytd ID list...")
        sr_filet = sr_file
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "Idno_001a_list_ytd_"
        sx_filet = sx_file + funcdate.prev_monthendfile()
        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        # Write the data
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        #funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

    # Build the final ytd ID list report table *****************************************
    print("IDNo list build the current ID list to export...")
    sr_file = "X001da_report_idlist"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
      X001ca_join_prev_reported.IDNO,
      X001ca_join_prev_reported.STUDENT,
      X001ca_join_prev_reported.NAME,
      X001ca_join_prev_reported.YEAR,
      X001ca_join_prev_reported.CAMPUS,
      X001ca_join_prev_reported.FIRSTNAME,
      X001ca_join_prev_reported.INITIALS,
      X001ca_join_prev_reported.SURNAME,
      X001ca_join_prev_reported.TITLE,
      X001ca_join_prev_reported.DATEOFBIRTH,
      X001ca_join_prev_reported.GENDER,
      X001ca_join_prev_reported.NATIONALITY,
      X001ca_join_prev_reported.POPULATION,
      X001ca_join_prev_reported.RACE
    FROM
      X001ca_join_prev_reported
    WHERE
      StrfTime('%m', X001ca_join_prev_reported.PREV_DATE_REPORTED) = StrfTime('%m', 'now') OR
      StrfTime('%m', X001ca_join_prev_reported.DATE_REPORTED) = StrfTime('%m', 'now') AND
        X001ca_join_prev_reported.PREV_PROCESS IS NULL
    ORDER BY
      X001ca_join_prev_reported.STUDENT
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export the data
    if l_export == True and funcsys.tablerowcount(so_curs,sr_file) > 0:
        print("IDNo list export current ID lists...")
        sr_filet = sr_file
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "Idno_001b_list_curr_"
        sx_filet = sx_file + funcdate.cur_month()
        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        # Write the data
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_filet, s_head)

    # Build new ID list file to export to external previous reported file **********
    print("IDNo list add new id number list to previous reported...")
    sr_file = "X001ea_prev_reported"
    s_sql = "CREATE TABLE "+sr_file+" AS " + """
    SELECT
      X001ca_join_prev_reported.PROCESS,
      X001ca_join_prev_reported.STUDENT AS FIELD1,
      X001ca_join_prev_reported.FIELD2,
      X001ca_join_prev_reported.FIELD2 AS FIELD3,
      X001ca_join_prev_reported.FIELD2 AS FIELD4,
      X001ca_join_prev_reported.FIELD2 AS FIELD5,
      X001ca_join_prev_reported.DATE_REPORTED,
      X001ca_join_prev_reported.DATE_RETEST
    FROM
      X001ca_join_prev_reported
    WHERE
      X001ca_join_prev_reported.PREV_PROCESS IS NULL
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS "+sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: "+sr_file)
    # Export the new ID list to previous reported file
    if funcsys.tablerowcount(so_curs,sr_file) > 0:
        print("IDNo list export the new data to previously reported file...")
        sr_filet = sr_file
        sx_path = ed_path
        sx_file = "300_reported"
        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_filet)
        # Write the data
        funccsv.write_data(so_conn, "main", sr_filet, sx_path, sx_file, s_head,"a",".txt")
        funcfile.writelog("%t EXPORT DATA: "+sr_file)
    else:
        print("IDNo list no new data to previously reported file...")
        funcfile.writelog("%t EXPORT DATA: No new data to export")

    if l_mail == True:
        funcmail.Mail("vss_list_idno_ytd")

    if l_mail == True:
        funcmail.Mail("vss_list_idno_curr")

    funcfile.writelog("%t **************************************************") 

    # Close the table connection ***************************************************
    so_conn.close()

    # Close the log writer *********************************************************
    funcfile.writelog("------------------------------------")
    funcfile.writelog("COMPLETED: C300_TEST_STUDENT_GENERAL")

    return
