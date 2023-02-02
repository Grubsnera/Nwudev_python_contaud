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


def student_fee(s_period="curr"):
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
    elif s_period == "2020":
        f_reg_fee = 1930.00
        d_sem1_con = "2020-02-21"
        d_sem1_dis = "2020-03-09"
        d_sem2_con = "2020-09-04"
        d_sem2_dis = "2020-09-04"
        d_test_overcharge = "2020-07-15"  # Only month and day used
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
    elif s_period == "2021":
        f_reg_fee = 2020.00
        d_sem1_con = "2021-04-09"
        d_sem1_dis = "2021-04-09"
        d_sem2_con = "2021-09-04"
        d_sem2_dis = "2021-09-04"
        d_test_overcharge = "2021-07-15"  # Only month and day used
        so_file = "Vss_test_fee.sqlite"  # Source database
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
        so_file = "Vss_test_fee.sqlite"  # Source database
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
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C302_TEST_STUDENT_FEE")
    funcfile.writelog("-----------------------------")
    print("---------------------")
    print("C302_TEST_STUDENT_FEE")
    print("---------------------")

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


    # IMPORT QUALIFICATION LEVY LIST
    sr_file = "X020aa_Fiabd007"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
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
        AMOUNT REAL)
        """)
    co = open(ed_path + "302_fiapd007_qual_" + s_period + ".csv", "r", encoding="utf-8")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        # print(row[0])
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
                                                "" + row[15] + ")"
            # print(s_cols)
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the imported data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "302_fiapd007_qual_period.csv (" + sr_file + ")")

    # IMPORT MODULE LEVY LIST
    sr_file = "X030aa_Fiabd007"
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
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
        AMOUNT REAL)
        """)
    co = open(ed_path + "302_fiapd007_modu_" + s_period + ".csv", "r", encoding="utf-8")
    co_reader = csv.reader(co)
    # Read the COLUMN database data
    for row in co_reader:
        # Populate the column variables
        # print(row[0])
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
                                                "" + row[17] + ")"
            s_cols = s_cols.replace("A'S ", "A ")
            s_cols = s_cols.replace("E'S ", "E ")
            s_cols = s_cols.replace("N'S ", "N ")
            # print(s_cols)
            so_curs.execute(s_cols)
    so_conn.commit()
    # Close the imported data file
    co.close()
    funcfile.writelog("%t IMPORT TABLE: " + ed_path + "302_fiapd007_modu_period.csv (" + sr_file + ")")

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
        student_fee()
    except Exception as e:
        funcsys.ErrMessage(e)
