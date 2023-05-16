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
