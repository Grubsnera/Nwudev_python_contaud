"""
Script to build GL Student debtor control account reports
Created on: 13 Mar 2018
"""

# IMPORT PYTHON MODULES
import sqlite3
import csv

# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funcdate
from _my_modules import funcsys
from _my_modules import funccsv
from _my_modules import funcmysql

# OPEN THE LOG
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C200_REPORT_STUDDEB_RECON_DEV")
funcfile.writelog("-------------------------------------")
print("-------------------------")
print("C200_REPORT_STUDDEB_RECON")
print("-------------------------")

# DECLARE VARIABLES
so_path = "W:/Kfs_vss_studdeb/"  # Source database path
so_file = "Kfs_vss_studdeb.sqlite"  # Source database
re_path = "R:/Debtorstud/"  # Results
ed_path = "S:/_external_data/"  # External data
gl_month = '06'
l_mail = True
l_export = True
l_record = True
s_burs_code = '042z052z381z500'

# OPEN THE SOURCE
with sqlite3.connect(so_path + so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN DATABASE: Kfs_vss_studdeb")

# ATTACH DATA FILES
so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs.sqlite' AS 'KFS'")
funcfile.writelog("%t ATTACH DATABASE: KFS.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")
so_curs.execute("ATTACH DATABASE 'W:/Kfs_vss_studdeb/Kfs_vss_studdeb_prev.sqlite' AS 'PREV'")
funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")

"""*****************************************************************************
BEGIN
*****************************************************************************"""

"""*************************************************************************
TEST IN GL NO VSS TRANSACTIONS
*************************************************************************"""
print("IN GL NO VSS TRANSACTIONS")
funcfile.writelog("IN GL NO VSS TRANSACTIONS")

# DECLARE VARIABLES
i_finding_after: int = 0

# OBTAIN TEST DATA
print("Identify transactions in the gl but not in vss...")
sr_file = "X004da_ingl_novss"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
SELECT
    'NWU' AS ORG,
    Upper(TRAN.CAMPUS) As CAMPUS,
    TRAN.MONTH,
    TRAN.GL_DESCRIPTION,
    TRAN.GL_AMOUNT
FROM
    X003ax_vss_gl_join TRAN
WHERE
    TRAN.MATCHED = 'X' AND
    TRAN.TRANCODE = 'X'
ORDER BY
    TRAN.MONTH,
    TRAN.CAMPUS,
    TRAN.TRANCODE
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
s_sql = s_sql.replace("%BURSARY%", s_burs_code)
s_sql = s_sql.replace("%PMONTH%", gl_month)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IDENTIFY FINDINGS
print("Identify findings...")
sr_file = "X004db_findings"
s_sql = "CREATE TABLE " + sr_file + " AS " + """
Select
    *
From
    X004da_ingl_novss CURR
;"""
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
so_curs.execute(s_sql)
so_conn.commit()
funcfile.writelog("%t BUILD TABLE: " + sr_file)

# COUNT THE NUMBER OF FINDINGS
i_finding_before: int = funcsys.tablerowcount(so_curs, sr_file)
print("*** Found " + str(i_finding_before) + " exceptions ***")
funcfile.writelog("%t FINDING: " + str(i_finding_before) + " IN GL NO VSS finding(s)")

# GET PREVIOUS FINDINGS
sr_file = "X004dc_get_previous"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    print("Import previously reported findings...")
    so_curs.execute(
        "CREATE TABLE " + sr_file + """
        (PROCESS TEXT,
        FIELD1 INT,
        FIELD2 TEXT,
        FIELD3 TEXT,
        FIELD4 TEXT,
        FIELD5 TEXT,
        DATE_REPORTED TEXT,
        DATE_RETEST TEXT,
        DATE_MAILED TEXT)
        """)
    s_cols = ""
    co = open(ed_path + "200_reported.txt", "r")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        if row[0] == "PROCESS":
            continue
        elif row[0] != "in gl no vss":
            continue
        else:
            s_cols = "INSERT INTO " + sr_file + " VALUES('" + row[0] + "','" + row[1] + "','" + row[
                     2] + "','" + row[3] + "','" + row[4] + "','" + row[5] + "','" + row[6] + "','" + row[
                     7] + "','" + row[8] + "')"
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the imported data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "200_reported.txt (" + sr_file + ")")

# ADD PREVIOUS FINDINGS
sr_file = "X004dd_add_previous"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    print("Join previously reported to current findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        FIND.*,
        'in gl no vss' AS PROCESS,
        '%TODAY%' AS DATE_REPORTED,
        '%DAYS%' AS DATE_RETEST,
        PREV.PROCESS AS PREV_PROCESS,
        PREV.DATE_REPORTED AS PREV_DATE_REPORTED,
        PREV.DATE_RETEST AS PREV_DATE_RETEST,
        PREV.DATE_MAILED
    From
        X004db_findings FIND Left Join
        X004dc_get_previous PREV ON PREV.FIELD1 = FIND.CAMPUS AND
            PREV.FIELD2 = FIND.MONTH And
            PREV.FIELD3 = FIND.GL_DESCRIPTION And
            PREV.FIELD4 = FIND.GL_AMOUNT And
            PREV.DATE_RETEST >= Date('%TODAY%')
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%TODAY%", funcdate.today())
    s_sql = s_sql.replace("%DAYS%", funcdate.today_plusdays(20000))
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD LIST TO UPDATE FINDINGS
# NOTE ADD CODE
sr_file = "X004de_new_previous"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.PROCESS,
        PREV.CAMPUS AS FIELD1,
        PREV.MONTH AS FIELD2,
        PREV.GL_DESCRIPTION AS FIELD3,
        PREV.GL_AMOUNT AS FIELD4,
        '' AS FIELD5,
        PREV.DATE_REPORTED,
        PREV.DATE_RETEST,
        PREV.DATE_MAILED
    From
        X004dd_add_previous PREV
    Where
        PREV.PREV_PROCESS Is Null
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export findings to previous reported file
    i_finding_after = funcsys.tablerowcount(so_curs, sr_file)
    if i_finding_after > 0:
        print("*** " + str(i_finding_after) + " Finding(s) to report ***")
        sx_path = ed_path
        sx_file = "200_reported"
        # Read the header data
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        # Write the data
        if l_record:
            funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head, "a", ".txt")
            funcfile.writelog("%t FINDING: " + str(i_finding_after) + " new finding(s) to export")
            funcfile.writelog("%t EXPORT DATA: " + sr_file)
    else:
        print("*** No new findings to report ***")
        funcfile.writelog("%t FINDING: No new findings to export")

# IMPORT OFFICERS FOR MAIL REPORTING PURPOSES
sr_file = "X004df_officer"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0:
    if i_finding_after > 0:
        print("Import reporting officers for mail purposes...")
        s_sql = "CREATE TABLE " + sr_file + " AS " + """
        Select
            OFFICER.LOOKUP,
            Upper(OFFICER.LOOKUP_CODE) AS CAMPUS,
            OFFICER.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
            PEOP.NAME_ADDR As NAME,
            PEOP.EMAIL_ADDRESS
        From
            VSS.X000_OWN_LOOKUPS OFFICER Left Join
            PEOPLE.X002_PEOPLE_CURR PEOP ON
                PEOP.EMPLOYEE_NUMBER = OFFICER.LOOKUP_DESCRIPTION
        Where
            OFFICER.LOOKUP = 'stud_debt_recon_test_ingl_novss_officer'
        ;"""
        so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
        so_curs.execute(s_sql)
        so_conn.commit()
        funcfile.writelog("%t BUILD TABLE: " + sr_file)

# IMPORT SUPERVISORS FOR MAIL REPORTING PURPOSES
sr_file = "X004dg_supervisor"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0 and i_finding_after > 0:
    print("Import reporting supervisors for mail purposes...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        SUPERVISOR.LOOKUP,
        Upper(SUPERVISOR.LOOKUP_CODE) AS CAMPUS,
        SUPERVISOR.LOOKUP_DESCRIPTION AS EMPLOYEE_NUMBER,
        PEOP.NAME_ADDR As NAME,
        PEOP.EMAIL_ADDRESS
    From
        VSS.X000_OWN_LOOKUPS SUPERVISOR Left Join
        PEOPLE.X002_PEOPLE_CURR PEOP ON 
            PEOP.EMPLOYEE_NUMBER = SUPERVISOR.LOOKUP_DESCRIPTION
    Where
        SUPERVISOR.LOOKUP = 'stud_debt_recon_test_ingl_novss_supervisor'
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# ADD CONTACT DETAILS TO FINDINGS
sr_file = "X004dh_detail"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
if i_finding_before > 0 and i_finding_after > 0:
    print("Add contact details to findings...")
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        PREV.ORG,
        PREV.CAMPUS,
        PREV.MONTH,
        PREV.GL_DESCRIPTION,
        PREV.GL_AMOUNT,
        CAMP_OFF.EMPLOYEE_NUMBER As CAMP_OFF_NUMB,
        CAMP_OFF.NAME As CAMP_OFF_NAME,
        CASE
            WHEN  CAMP_OFF.EMPLOYEE_NUMBER <> '' THEN CAMP_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
            ELSE CAMP_OFF.EMAIL_ADDRESS
        END As CAMP_OFF_MAIL,
        CAMP_SUP.EMPLOYEE_NUMBER As CAMP_SUP_NUMB,
        CAMP_SUP.NAME As CAMP_SUP_NAME,
        CASE
            WHEN CAMP_SUP.EMPLOYEE_NUMBER <> '' THEN CAMP_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
            ELSE CAMP_SUP.EMAIL_ADDRESS
        END As CAMP_SUP_MAIL,
        ORG_OFF.EMPLOYEE_NUMBER As ORG_OFF_NUMB,
        ORG_OFF.NAME As ORG_OFF_NAME,
        CASE
            WHEN ORG_OFF.EMPLOYEE_NUMBER <> '' THEN ORG_OFF.EMPLOYEE_NUMBER||'@nwu.ac.za'
            ELSE ORG_OFF.EMAIL_ADDRESS
        END As ORG_OFF_MAIL,
        ORG_SUP.EMPLOYEE_NUMBER As ORG_SUP_NUMB,
        ORG_SUP.NAME As ORG_SUP_NAME,
        CASE
            WHEN ORG_SUP.EMPLOYEE_NUMBER <> '' THEN ORG_SUP.EMPLOYEE_NUMBER||'@nwu.ac.za'
            ELSE ORG_SUP.EMAIL_ADDRESS
        END As ORG_SUP_MAIL
    From
        X004dd_add_previous PREV
        Left Join X004df_officer CAMP_OFF On CAMP_OFF.CAMPUS = PREV.CAMPUS
        Left Join X004df_officer ORG_OFF On ORG_OFF.CAMPUS = PREV.ORG
        Left Join X004dg_supervisor CAMP_SUP On CAMP_SUP.CAMPUS = PREV.CAMPUS
        Left Join X004dg_supervisor ORG_SUP On ORG_SUP.CAMPUS = PREV.ORG
    Where
        PREV.PREV_PROCESS IS NULL
    Order By
        PREV.CAMPUS,
        PREV.MONTH,
        PREV.GL_DESCRIPTION
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

# BUILD THE FINAL TABLE FOR EXPORT AND REPORT
sr_file = "X004dx_ingl_novss"
so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
print("Build the final report")
if i_finding_before > 0 and i_finding_after > 0:
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        'IN GL NO VSS' As Audit_finding,
        FIND.ORG As Organization,
        FIND.CAMPUS As Campus,
        FIND.MONTH As Month,
        FIND.GL_DESCRIPTION As Gl_description,
        FIND.GL_AMOUNT As Amount_gl,
        FIND.CAMP_OFF_NAME AS Responsible_Officer,
        FIND.CAMP_OFF_NUMB AS Responsible_Officer_Numb,
        FIND.CAMP_OFF_MAIL AS Responsible_Officer_Mail,
        FIND.CAMP_SUP_NAME AS Supervisor,
        FIND.CAMP_SUP_NUMB AS Supervisor_Numb,
        FIND.CAMP_SUP_MAIL AS Supervisor_Mail,
        FIND.ORG_OFF_NAME AS Org_Officer,
        FIND.ORG_OFF_NUMB AS Org_Officer_Numb,
        FIND.ORG_OFF_MAIL AS Org_Officer_Mail,
        FIND.ORG_SUP_NAME AS Org_Supervisor,
        FIND.ORG_SUP_NUMB AS Org_Supervisor_Numb,
        FIND.ORG_SUP_MAIL AS Org_Supervisor_Mail            
    From
        X004dh_detail FIND
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    # Export findings
    if l_export and funcsys.tablerowcount(so_curs, sr_file) > 0:
        print("Export findings...")
        sx_path = re_path + funcdate.cur_year() + "/"
        sx_file = "Vssgl_test_004dx_ingl_novss_"
        sx_file_dated = sx_file + funcdate.today_file()
        s_head = funccsv.get_colnames_sqlite(so_conn, sr_file)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file, s_head)
        funccsv.write_data(so_conn, "main", sr_file, sx_path, sx_file_dated, s_head)
        funcfile.writelog("%t EXPORT DATA: " + sx_path + sx_file)
else:
    s_sql = "CREATE TABLE " + sr_file + " (" + """
    BLANK TEXT
    );"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

"""*****************************************************************************
END
*****************************************************************************"""

# CLOSE THE CONNECTION
so_conn.commit()
so_conn.close()

# CLOSE THE LOG
funcfile.writelog("----------------------------------------")
funcfile.writelog("COMPLETED: C200_REPORT_STUDDEB_RECON_DEV")
