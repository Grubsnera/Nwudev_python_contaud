"""
Script to test STUDENT FEES
Created on: 28 Aug 2019
Author: Albert J v Rensburg (NWU21162395)
"""

# IMPORT PYTHON MODULES
import sqlite3
import csv

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funccsv
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcstat
from _my_modules import funcsys
from _my_modules import funcsms
from _my_modules import functest

""" INDEX **********************************************************************
ENVIRONMENT
OPEN THE DATABASES
TEMPORARY AREA
BEGIN OF SCRIPT
END OF SCRIPT
*****************************************************************************"""


def student_fee(s_period: str = "curr"):
    """
    Script to test STUDENT FEE INCOME
    :param s_period: str: The financial period
    :return: Nothing
    """

    """*****************************************************************************
    ENVIRONMENT
    *****************************************************************************"""

    # DECLARE VARIABLES
    if s_period == "prev":
        s_year = funcdate.prev_year()
    else:
        s_year = funcdate.cur_year()

    ed_path = "S:/_external_data/"  # External data path
    so_path = "W:/Vss_fee/"  # Source database path
    re_path = "R:/Vss/" + s_year

    if s_period == "2019":
        f_reg_fee = 1830.00
        d_sem1_con = "2019-03-05"
        d_sem1_dis = "2019-03-05"
        d_sem2_con = "2019-08-09"
        d_sem2_dis = "2019-08-09"
        d_test_overcharge = "2019-07-15"  # Only month and day used
        so_file = "Vss_test_fee_2019.sqlite"  # Source database
        s_reg_trancode: str = "095"
        s_qual_trancode: str = "004"
        s_modu_trancode: str = "004"
        s_burs_trancode: str = "042z052z381z500"
        # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded
        s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
        s_mpa: str = "665566z618161z618167z618169"  # Exclude these FQUALLEVELAPID
        s_aud: str = "71839z71840z71841z71842z71820z71821z71822z1085390"  # Exclude these FQUALLEVELAPID
        l_record: bool = False
        l_export: bool = True
    elif s_period == "2020":
        f_reg_fee = 1930.00
        d_sem1_con = "2020-02-21"
        d_sem1_dis = "2020-03-09"
        d_sem2_con = "2020-09-04"
        d_sem2_dis = "2020-09-04"
        d_test_overcharge = "2020-07-15"  # Only month and day used
        so_file = "Vss_test_fee_2020.sqlite"  # Source database
        s_reg_trancode: str = "095"
        s_qual_trancode: str = "004"
        s_modu_trancode: str = "004"
        s_burs_trancode: str = "042z052z381z500"
        # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded
        s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
        s_mpa: str = "665566z618161z618167z618169"  # Exclude these FQUALLEVELAPID
        s_aud: str = "71839z71840z71841z71842z71820z71821z71822z1085390"  # Exclude these FQUALLEVELAPID
        l_record: bool = False
        l_export: bool = True
    elif s_period == "2021":
        f_reg_fee = 2020.00
        d_sem1_con = "2021-04-09"
        d_sem1_dis = "2021-04-09"
        d_sem2_con = "2021-09-04"
        d_sem2_dis = "2021-09-04"
        d_test_overcharge = "2021-07-15"  # Only month and day used
        so_file = "Vss_test_fee_2021.sqlite"  # Source database
        s_reg_trancode: str = "095"
        s_qual_trancode: str = "004"
        s_modu_trancode: str = "004"
        s_burs_trancode: str = "042z052z381z500"
        # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded
        s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
        s_mpa: str = "665566z618161z618167z618169"  # Exclude these FQUALLEVELAPID
        s_aud: str = "71839z71840z71841z71842z71820z71821z71822z1085390"  # Exclude these FQUALLEVELAPID
        l_record: bool = False
        l_export: bool = True
    elif s_period == "prev":
        f_reg_fee = 2110.00
        d_sem1_con = "2022-03-11"
        d_sem1_dis = "2022-03-11"
        d_sem2_con = "2022-08-05"
        d_sem2_dis = "2022-08-05"
        d_test_overcharge = "2022-07-15"  # Only month and day used
        so_file = "Vss_test_fee_prev.sqlite"  # Source database
        s_reg_trancode: str = "095"
        s_qual_trancode: str = "004"
        s_modu_trancode: str = "004"
        s_burs_trancode: str = "042z052z381z500"
        # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded
        s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
        s_mpa: str = "665566z618161z618167z618169"  # Exclude these FQUALLEVELAPID
        s_aud: str = "71839z71840z71841z71842z71820z71821z71822z1085390"  # Exclude these FQUALLEVELAPID
        l_record: bool = False
        l_export: bool = True
    else:
        f_reg_fee = 2220.00
        d_sem1_con = "2023-03-03"
        d_sem1_dis = "2023-03-03"
        d_sem2_con = "2023-08-04"
        d_sem2_dis = "2023-08-04"
        d_test_overcharge = "2023-07-15"  # Only month and day used
        so_file = "Vss_test_fee.sqlite"  # Source database
        s_reg_trancode: str = "095"
        s_qual_trancode: str = "004"
        s_modu_trancode: str = "004"
        s_burs_trancode: str = "042z052z381z500"
        # Find these id's from Sqlite->Sqlite_vss_test_fee->Q021aa_qual_nofee_loaded
        s_mba: str = "71500z2381692z2381690z665559"  # Exclude these FQUALLEVELAPID
        s_mpa: str = "665566z618161z618167z618169"  # Exclude these FQUALLEVELAPID
        s_aud: str = "71839z71840z71841z71842z71820z71821z71822z1085390"  # Exclude these FQUALLEVELAPID
        l_record: bool = True
        l_export: bool = True

    l_debug: bool = True
    # l_mail: bool = funcconf.l_mail_project
    l_mail: bool = False
    # l_mess: bool = funcconf.l_mess_project
    l_mess: bool = False
    s_desc: str = ""

    # SCRIPT LOG FILE
    if l_debug:
        print("---------------------")
        print("C302_TEST_STUDENT_FEE")
        print("---------------------")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C302_TEST_STUDENT_FEE")
    funcfile.writelog("-----------------------------")

    # MESSAGE
    if l_mess:
        funcsms.send_telegram('', 'administrator', '<b>C302 Student fee income tests</b>')

    """*****************************************************************************
    OPEN THE DATABASES
    *****************************************************************************"""
    if l_debug:
        print("OPEN THE DATABASES")
    funcfile.writelog("OPEN THE DATABASES")

    # OPEN SQLITE SOURCE table
    if l_debug:
        print("Open sqlite database...")
    with sqlite3.connect(so_path + so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # ATTACH VSS DATABASE
    if l_debug:
        print("Attach vss database...")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
    funcfile.writelog("%t ATTACH DATABASE: Vss.sqlite")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss_curr.sqlite' AS 'VSSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: Vss_curr.sqlite")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss_prev.sqlite' AS 'VSSPREV'")
    funcfile.writelog("%t ATTACH DATABASE: Vss_prev.sqlite")
    so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: People.sqlite")

    """ ****************************************************************************
    TEMPORARY AREA
    *****************************************************************************"""
    if l_debug:
        print("TEMPORARY AREA")
    funcfile.writelog("TEMPORARY AREA")

    """ ****************************************************************************
    BEGIN OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("BEGIN OF SCRIPT")
    funcfile.writelog("BEGIN OF SCRIPT")

    """*****************************************************************************
    QUALIFICATION FEE MASTER 1
    *****************************************************************************"""
    if l_debug:
        print("QUALIFICATION FEE MASTER")
    funcfile.writelog("QUALIFICATION FEE MASTER")

    # IMPORT QUALIFICATION LEVY LIST
    sr_file = "X020aa_Fiabd007"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Import vss qualification fees...")
    so_curs.execute(
        "Create Table " + sr_file + """
        (ACAD_PROG_FEE_TYPE INT,
        ACAD_PROG_FEE_DESC TEXT,
        APOMOD INT,
        FPRESENTATIONCATEGORYCODEID INT,
        PRESENT_CAT TEXT,
        FENROLMENTCATEGORYCODEID INT,
        ENROL_CATEGORY TEXT,
        FSITEORGUNITNUMBER INT,
        CAMPUS TEXT,
        FQUALLEVELAPID INT,
        QUALIFICATION TEXT,
        QUALIFICATION_NAME TEXT,
        LEVY_CATEGORY TEXT,
        TRANSCODE TEXT,
        UMPT_REGU INT,
        AMOUNT REAL,
        START_DATE TEXT,
        END_DATE TEXT)
        """)
    co = open(ed_path + "302_fiapd007_qual_" + s_period + ".csv", "r", encoding="utf-8")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        # if l_debug:
        #   print(row[0])
        if "Academic Program Fee Type" in row[0]:
            continue
        elif row[0] == "":
            continue
        else:
            s_cols = "Insert Into " + sr_file + " Values(" \
                                                "" + row[0] + "," \
                                                "'" + row[1] + "'," \
                                                "" + row[2] + "," \
                                                "" + row[3] + "," \
                                                "'" + row[4] + "'," \
                                                "" + row[5] + "," \
                                                "'" + row[6] + "'," \
                                                "" + row[7] + "," \
                                                "'" + row[8] + "'," \
                                                "" + row[9] + "," \
                                                "'" + row[10] + "'," \
                                                "'" + row[11] + "'," \
                                                "'" + row[12] + "'," \
                                                "'" + row[13] + "'," \
                                                "" + row[14] + "," \
                                                "" + row[15] + "," \
                                                "'" + row[16] + "'," \
                                                "'" + row[17] + "')"

        # if l_debug:
            #   print(s_cols)
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the imported data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "302_fiapd007_qual_period.csv (" + sr_file + ")")

    """ ****************************************************************************
    OBTAIN STUDENTS
    *****************************************************************************"""
    if l_debug:
        print("OBTAIN STUDENTS")
    funcfile.writelog("OBTAIN STUDENTS")

    # OBTAIN THE LIST STUDENTS
    # EXCLUDE SHORT COURSE STUDENTS
    if l_debug:
        print("Obtain the registered students...")
    sr_file = "X000_Student"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    SELECT
        STUD.*,
        CASE
            WHEN DATEENROL < STARTDATE THEN STARTDATE
            ELSE DATEENROL
        END AS DATEENROL_CALC
    FROM
        %VSS%.X001_Student STUD
    WHERE
        UPPER(STUD.QUAL_TYPE) Not Like '%SHORT COURSE%'
    """
    """
    To exclude some students
    STUD.ISMAINQUALLEVEL = 1 AND
    UPPER(STUD.ACTIVE_IND) = 'ACTIVE'
    """
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    """ ****************************************************************************
    OBTAIN STUDENT TRANSACTIONS
    *****************************************************************************"""
    if l_debug:
        print("OBTAIN STUDENT TRANSACTIONS")
    funcfile.writelog("OBTAIN STUDENT TRANSACTIONS")

    # OBTAIN STUDENT ACCOUNT TRANSACTIONS
    if l_debug:
        print("Import student transactions...")
    sr_file = "X000_Transaction"
    s_sql = "CREATE TABLE " + sr_file + " AS" + """
    Select
        TRAN.FBUSENTID As STUDENT,
        CASE
            WHEN TRAN.FDEBTCOLLECTIONSITE = '-9' THEN 'MAHIKENG'
            WHEN TRAN.FDEBTCOLLECTIONSITE = '-2' THEN 'VANDERBIJLPARK'
            ELSE 'POTCHEFSTROOM'
        END AS CAMPUS,
        TRAN.TRANSDATE,
        TRAN.TRANSDATETIME,
        CASE
            WHEN SUBSTR(TRAN.TRANSDATE,6,5)='01-01' AND INSTR('001z031z061',TRAN.TRANSCODE)>0 THEN '00'
            WHEN strftime('%Y',TRANSDATE)>strftime('%Y',POSTDATEDTRANSDATE) And
             Strftime('%Y',POSTDATEDTRANSDATE) = '%YEAR%' THEN strftime('%m',POSTDATEDTRANSDATE)
            ELSE strftime('%m',TRAN.TRANSDATE)
        END AS MONTH,
        TRAN.TRANSCODE,
        TRAN.AMOUNT,
        CASE
            WHEN TRAN.AMOUNT > 0 THEN TRAN.AMOUNT
            ELSE 0.00
        END AS AMOUNT_DT,
        CASE
            WHEN TRAN.AMOUNT < 0 THEN TRAN.AMOUNT
            ELSE 0.00
        END AS AMOUNT_CR,
        TRAN.DESCRIPTION_E As TRANSDESC,
        TRAN.AUDITDATETIME,
        TRAN.FUSERBUSINESSENTITYID,
        TRAN.FAUDITUSERCODE,
        TRAN.SYSTEM_DESC,
        TRAN.FMODAPID,
        TRAN.ENROL_ID,
        TRAN.ENROL_CATEGORY,        
        TRAN.MODULE,
        TRAN.MODULE_NAME,
        TRAN.FQUALLEVELAPID,
        TRAN.QUALIFICATION,
        TRAN.QUALIFICATION_NAME,
        TRAN.FENROLPRESID
    FROM
        %VSS%.X010_Studytrans TRAN
    WHERE
        TRAN.TRANSCODE <> ''
    ORDER BY
        AUDITDATETIME
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = s_sql.replace("%YEAR%", s_year)
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD THE TRANSACTION USER
    # USE TRANSACTION USER IF POSITION IS STUDENT ACCOUNTS ELSE
    # USE 26019817 VAN TONDER MRS. HC FOR POTCHEFSTROOM
    # USE 13163140 BIERMAN MS. EJ FOR VANDERBIJLPARK
    # USE 16343778 LEBEKO MRS. KV FOR MAHIKENG
    if l_debug:
        print("Add the transaction user...")
    sr_file = "X000_Transaction_user"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        TRAN.*,
        CASE
            WHEN PEOP.POSITION_FULL LIKE("%STUDENT ACCOUNTS%") THEN FUSERBUSINESSENTITYID
            WHEN CAMPUS LIKE("P%") THEN 26019817
            WHEN CAMPUS LIKE("V%") THEN 13163140
            WHEN CAMPUS LIKE("M%") THEN 16343778
            ELSE 0 
        END AS FUSER
    From
        X000_Transaction TRAN Left Join
        PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = CAST(TRAN.FUSERBUSINESSENTITYID AS TEXT)        
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # ADD THE TRANSACTION USER NAME
    if l_debug:
        print("Add the transaction user name...")
    sr_file = "X000_Transaction"
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        TRAN.*,
        PEOP.NAME_ADDR AS FUSERNAME
    From
        X000_Transaction_user TRAN Left Join
        PEOPLE.X002_PEOPLE_CURR PEOP ON PEOP.EMPLOYEE_NUMBER = CAST(TRAN.FUSER AS TEXT)        
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    sr_file = "X000_Transaction_user"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)

    """*****************************************************************************
    MODULE FEE MASTER 1 - PREPARE MODULE MASTER FILES
    *****************************************************************************"""
    if l_debug:
        print("MODULE FEE MASTER")
    funcfile.writelog("MODULE FEE MASTER")

    # IMPORT MODULE LEVY LIST
    sr_file = "X030aa_Fiabd007"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    if l_debug:
        print("Import vss module fees...")
    so_curs.execute(
        "Create Table " + sr_file + """
        (ACAD_PROG_FEE_TYPE INT,
        ACAD_PROG_FEE_DESC TEXT,
        APOMOD INT,
        FPRESENTATIONCATEGORYCODEID INT,
        PRESENT_CAT TEXT,
        FENROLMENTCATEGORYCODEID INT,
        ENROL_CATEGORY TEXT,
        FSITEORGUNITNUMBER INT,
        CAMPUS TEXT,
        OE_CODE INT,
        SCHOOL TEXT,
        FMODAPID INT,
        MODULE TEXT,
        MODULE_NAME TEXT,
        MODULE_TYPE TEXT,
        MODULE_TYPE_NAME TEXT,
        TRANSCODE TEXT,
        AMOUNT REAL,
        START_DATE,
        END_DATE)
        """)
    co = open(ed_path + "302_fiapd007_modu_" + s_period + ".csv", "r", encoding="utf-8")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        # if l_debug:
        #   print(row[0])
        if "Academic Program Fee Type" in row[0]:
            continue
        elif row[0] == "":
            continue
        else:
            s_cols = "Insert Into " + sr_file + " Values(" \
                                                "" + row[0] + "," \
                                                "'" + row[1] + "'," \
                                                "" + row[2] + "," \
                                                "" + row[3] + "," \
                                                "'" + row[4] + "'," \
                                                "" + row[5] + "," \
                                                "'" + row[6] + "'," \
                                                "" + row[7] + "," \
                                                "'" + row[8] + "'," \
                                                "" + row[9] + "," \
                                                "'" + row[10] + "'," \
                                                "" + row[11] + "," \
                                                "'" + row[12] + "'," \
                                                "'" + row[13] + "'," \
                                                "'" + row[14] + "'," \
                                                "'" + row[15] + "'," \
                                                "'" + row[16] + "'," \
                                                "" + row[17] + "," \
                                                "'" + row[18] + "'," \
                                                "'" + row[19] + "')"

            s_cols = s_cols.replace("A'S ", "A ")
            s_cols = s_cols.replace("E'S ", "E ")
            s_cols = s_cols.replace("N'S ", "N ")
            # if l_debug:
            #   print(s_cols)
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the imported data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "302_fiapd007_modu_period.csv (" + sr_file + ")")

    # SUMM FIAB LEVY LIST
    if l_debug:
        print("Build summary of module levy list...")
    sr_file = "X030aa_Fiabd007_summ"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        FIAB.FMODAPID,
        Upper(FIAB.CAMPUS) As CAMPUS,
        FIAB.FPRESENTATIONCATEGORYCODEID,
        Upper(FIAB.PRESENT_CAT) As PRESENT_CAT,
        FIAB.FENROLMENTCATEGORYCODEID,
        Upper(FIAB.ENROL_CATEGORY) As ENROL_CATEGORY,    
        FIAB.AMOUNT,
        Cast(Count(FIAB.ACAD_PROG_FEE_TYPE) As INT) As COUNT,
        FIAB.MODULE,
        FIAB.MODULE_NAME        
    From
        X030aa_Fiabd007 FIAB
    Group By
        FIAB.FMODAPID,
        FIAB.CAMPUS,
        FIAB.FPRESENTATIONCATEGORYCODEID,
        FIAB.FENROLMENTCATEGORYCODEID,
        FIAB.AMOUNT
    ;"""
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD SUMMARY OF ALL MODULES PRESENTED
    if l_debug:
        print("Build list of all modules presented...")
    sr_file = "X030ab_Stud_modu_list"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        STUD.KENROLSTUDID,
        STUD.KSTUDBUSENTID As STUDENT,
        STUD.FMODULEAPID,
        STUD.CAMPUS,
        STUD.FPRESENTATIONCATEGORYCODEID As PRESENT_ID,
        STUD.PRESENT_CATEGORY,    
        STUD.FENROLMENTCATEGORYCODEID As ENROL_ID,
        STUD.ENROL_CATEGORY,
        STUD.MODULE,
        STUD.MODULE_NAME,
        Case
            When STUD.ISCONDITIONALREG = 1 Then '0 CONDITIONAL REGISTRATION'
            When STUD.ENROL_CATEGORY Like('%EXAM ONLY%') Then '0 EXAM ONLY'
            When STUD.ENROL_CATEGORY Like('%ROLL OVER%') Then '0 ROLL OVER'
            Else '1'
        End As FINDING    
    From
        %VSS%.X001_Student_module STUD
    Where
            UPPER(STUD.ENROL_CATEGORY) Not Like '%SHORT COURSE%'
    ;"""
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD SUMMARY OF ALL MODULES PRESENTED
    if l_debug:
        print("Build summary of all modules presented...")
    sr_file = "X030ac_Stud_module_summ"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        STUD.FMODULEAPID,
        STUD.CAMPUS,
        STUD.PRESENT_ID,
        STUD.PRESENT_CATEGORY,    
        STUD.ENROL_ID,
        STUD.ENROL_CATEGORY,
        STUD.MODULE,
        STUD.MODULE_NAME,
        Count(STUD.KENROLSTUDID) As COUNT_STUD
    From
        X030ab_Stud_modu_list STUD
    Where
        STUD.FINDING Like('1%')        
    Group By
        STUD.FMODULEAPID,
        STUD.CAMPUS,
        STUD.PRESENT_ID,
        STUD.ENROL_ID,
        STUD.MODULE
    ;"""
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # BUILD LIST OF MODULES PLUS STATS
    if l_debug:
        print("Build summary of modules levied from transactions...")
    sr_file = "X030bb_Trans_feemodu"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        TRAN.FMODAPID,
        TRAN.ENROL_ID,
        TRAN.ENROL_CATEGORY As ENROL_CAT,
        TRAN.MODULE,
        TRAN.MODULE_NAME,
        TRAN.AMOUNT,
        CAST(COUNT(TRAN.STUDENT) As INT) As TRAN_COUNT_ALL,
        CAST(TOTAL(TRAN.AMOUNT) AS REAL) AS FEE_MODU_ALL
    From
        X000_Transaction TRAN
    Where
        Instr('%TRANCODE%', Trim(TRAN.TRANSCODE)) > 0 And TRAN.FMODAPID != 0
    Group by
        TRAN.FMODAPID,
        TRAN.ENROL_ID
    ;"""
    s_sql = s_sql.replace("%TRANCODE%", s_modu_trancode)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # CALCULATE THE MODULE FEES LEVIED PER STUDENT
    if l_debug:
        print("Calculate the module fees levied per student...")
    sr_file = "X030bb_Trans_feemodu_stud"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        TRAN.STUDENT,
        TRAN.FMODAPID,
        TRAN.ENROL_ID,
        TRAN.ENROL_CATEGORY As ENROL_CAT,
        CAST(COUNT(TRAN.STUDENT) As INT) As TRAN_COUNT,
        CAST(TOTAL(TRAN.AMOUNT) AS REAL) AS FEE_MODU,
        MAX(TRAN.AUDITDATETIME),
        TRAN.MODULE,
        TRAN.MODULE_NAME,
        TRAN.FUSER AS FUSERBUSINESSENTITYID,
        TRAN.FUSERNAME AS NAME_ADDR,
        TRAN.FAUDITUSERCODE,
        TRAN.SYSTEM_DESC
    From
        X000_Transaction TRAN
    Where
        Instr('%TRANCODE%', Trim(TRAN.TRANSCODE)) > 0 And TRAN.FMODAPID != 0
    Group by
        TRAN.STUDENT,
        TRAN.FMODAPID,
        TRAN.ENROL_ID
    ;"""
    s_sql = s_sql.replace("%TRANCODE%", s_modu_trancode)
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # NOTE - Function fully functional. Take long time to complete (30min). Not used at the moment.
    """
    # CALCULATE THE STATISTIC MODE FOR EACH QUALIFICATION
    if l_debug:
        print("Calculate the module transaction statistic mode...")
    i_value: int = 0
    sr_file = "X030bc_Trans_feemodu_mode"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute("CREATE TABLE " + sr_file + " (FMODAPID INT, ENROL_ID INT, AMOUNT REAL)")
    for qual in so_curs.execute("SELECT FMODAPID, ENROL_ID FROM X030ab_Trans_feemodu").fetchall():
            # if l_debug:
            #   print("FMODAPID = " + str(qual[0]) + " And ENROL_CAT = " + str(qual[1]))
        try:
            i_value = funcstat.stat_mode(so_curs,
                                         "X030ab_Trans_feemodu_stud",
                                         "FEE_MODU",
                                         "FMODAPID = " + str(qual[0]) + " And ENROL_ID = " + str(qual[1])
                                         )
            if i_value < 0:
                i_value = 0
        except Exception as e:
            # funcsys.ErrMessage(e) if you want error to log
            if "".join(e.args).find("no unique mode") >= 0:
                i_value = funcstat.stat_highest_value(so_curs,
                                                      "X030ab_Trans_feemodu_stud",
                                                      "FEE_MODU",
                                                      "FMODAPID = " + str(qual[0]) + " And ENROL_ID = " + str(qual[1])
                                                      )
            else:
                i_value = 0
            # if l_debug:
            #   print(i_value)
        s_cols = "INSERT INTO " + sr_file + " VALUES(" + str(qual[0]) + ", " + str(qual[1]) + ", " + str(i_value) + ")"
        so_curs.execute(s_cols)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)
    so_conn.commit()
    """

    # BUILD SUMMARY OF ALL MODULES PRESENTED
    if l_debug:
        print("Build summary of all modules presented...")
    sr_file = "X030bd_Stud_modu_present"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    s_sql = "Create table " + sr_file + " AS" + """
    Select
        STUD.KENROLMENTPRESENTATIONID,
        STUD.FMODULEAPID,
        STUD.FENROLMENTCATEGORYCODEID As ENROL_ID,
        STUD.ENROL_CATEGORY,
        STUD.FPRESENTATIONCATEGORYCODEID As PRESENT_ID,
        STUD.PRESENT_CATEGORY,
        Count(STUD.KENROLSTUDID) As COUNT
    From
        %VSS%.X001_Student_module STUD
    Group By
        STUD.KENROLMENTPRESENTATIONID,
        STUD.FMODULEAPID,
        STUD.FENROLMENTCATEGORYCODEID,
        STUD.FPRESENTATIONCATEGORYCODEID
    ;"""
    if s_period == "prev":
        s_sql = s_sql.replace("%VSS%", "VSSPREV")
    else:
        s_sql = s_sql.replace("%VSS%", "VSSCURR")
    so_curs.execute(s_sql)
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # MESSAGE
    # if l_mess:
    #     funcsms.send_telegram('', 'administrator', '<b>STUDENT FEE</b> income tests end.')

    """ ****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    if l_debug:
        print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # CLOSE THE DATABASE CONNECTION
    so_conn.commit()
    so_conn.close()

    # CLOSE THE LOG WRITER *********************************************************
    funcfile.writelog("--------------------------------")
    funcfile.writelog("COMPLETED: C302_TEST_STUDENT_FEE")

    return


if __name__ == '__main__':
    try:
        student_fee("curr")
    except Exception as e:
        funcsys.ErrMessage(e)
