""" C200_REPORT_STUDDEB_RECON **************************************************
***
*** Script to compare VSS and GL student transactions
***
*** Albert J van Rensburg (21162395)
*** 26 Jun 2018
*** 7 Jan 2020 Read vss data from period database
***
*****************************************************************************"""

# Import python modules
import csv
import sqlite3

# Import own modules
from _my_modules import funcconf
from _my_modules import funcdate
from _my_modules import funcdatn
from _my_modules import funccsv
from _my_modules import funcfile
from _my_modules import funcsys
from _my_modules import funcmysql
from _my_modules import funcsms
from _my_modules import functest

""" CONTENTS *******************************************************************
ENVIRONMENT
OPEN DATABASES
DETERMINE GL POST MONTH
LIST VSS TRANSACTIONS ROUND 1
LIST GL TRANSACTIONS
LIST VSS TRANSACTIONS ROUND 2
JOIN VSS & GL MONTHLY TOTALS
JOIN VSS & GL TRANSACTIONS
# (V1.1.1) MESSAGING ADDED TO ALL TESTS
TEST MATCHED TRANSACTION TYPES
TEST VSS GL DIFFERENCE TRANSACTION SUMMARY
TEST IN VSS NO GL TRANSACTIONS
TEST IN GL NO VSS TRANSACTIONS
TEST VSS GL BURSARY DIFFERENCE TRANSACTION SUMMARY
BURSARY VSS GL RECON
TEST BURSARY INGL NOVSS (UNCOMPLETE)
TEST BURSARY INVSS NOGL
TEST BURSARY VSS GL DIFFERENT CAMPUS
BALANCE ON MORE THAN ONE CAMPUS
TEST STUDENT BALANCE MULTIPLE CAMPUS (V2.0.2) 
END OF SCRIPT
*****************************************************************************"""


def report_studdeb_recon(dopenmaf: float = 0, dopenpot: float = 0, dopenvaa: float = 0, s_period: str = "curr"):
    """
    STUDENT DEBTOR RECONCILIATIONS
    :param dopenmaf: int: Mafiking Campus opening balance
    :param dopenpot: int: Potchefstroom opening balance
    :param dopenvaa: int: Vaal Campus opening balance
    :param s_period: str: Period indication curr, prev or year
    :return: Null
    """

    """ PARAMETERS *************************************************************
    dopenmaf = GL Opening balances for Mafikeng campus
    dopenpot = GL Opening balances for Potchefstroom campus
    dopenvaa = GL Opening balances for Vaal Triangle campus
    Notes:
    1. When new financial year start, GL does not contain opening balances.
       Opening balances are the inserted manually here, until the are inserted
       into the GL by journal, usually at the end of March. This was the case
       for the 2019 financial year
    *************************************************************************"""

    """*************************************************************************
    ENVIRONMENT
    *************************************************************************"""

    # DECLARE VARIABLES

    l_debug: bool = True
    so_path: str = "W:/Kfs_vss_studdeb/"  # Source database path
    if s_period == "curr":
        s_year: str = funcdatn.get_current_year()
        so_file: str = "Kfs_vss_studdeb.sqlite"  # Source database
        s_kfs: str = "KFSCURR"
        s_vss: str = "VSSCURR"
    elif s_period == "prev":
        s_year = funcdatn.get_previous_year()
        so_file = "Kfs_vss_studdeb_prev.sqlite"  # Source database
        s_kfs = "KFSPREV"
        s_vss = "VSSPREV"
    else:
        s_year = s_period
        so_file = "Kfs_vss_studdeb_" + s_year + ".sqlite"  # Source database
        s_kfs = ""
        s_vss = ""
    re_path = "R:/Debtorstud/"  # Results
    ed_path = "S:/_external_data/"  # External data
    # l_mess: bool = funcconf.l_mess_project
    l_mess: bool = False
    l_export = True
    l_record = True
    s_burs_code = '042z052z381z500'  # Current bursary transaction codes

    # Open the script log file ******************************************************
    print("-------------------------")
    print("C200_REPORT_STUDDEB_RECON")
    print("-------------------------")
    print("ENVIRONMENT")
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: C200_REPORT_STUDDEB_RECON")
    funcfile.writelog("---------------------------------")
    funcfile.writelog("ENVIRONMENT")

    # MESSAGE
    if l_mess:
        funcsms.send_telegram('', 'administrator', '<b>C200 Student debtor reconciliations</b>')

    """*************************************************************************
    OPEN DATABASES
    *************************************************************************"""
    print("OPEN DATABASES")
    funcfile.writelog("OPEN DATABASES")

    # Open the SOURCE file
    with sqlite3.connect(so_path + so_file) as so_conn:
        so_curs = so_conn.cursor()
    funcfile.writelog("OPEN DATABASE: " + so_file)

    # Attach data sources
    so_curs.execute("ATTACH DATABASE 'W:/People/People.sqlite' AS 'PEOPLE'")
    funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs_curr.sqlite' AS 'KFSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: KFS_CURR.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Kfs/Kfs_prev.sqlite' AS 'KFSPREV'")
    funcfile.writelog("%t ATTACH DATABASE: KFS_PREV.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss.sqlite' AS 'VSS'")
    funcfile.writelog("%t ATTACH DATABASE: VSS.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss_curr.sqlite' AS 'VSSCURR'")
    funcfile.writelog("%t ATTACH DATABASE: VSS_CURR.SQLITE")
    so_curs.execute("ATTACH DATABASE 'W:/Vss/Vss_prev.sqlite' AS 'VSSPREV'")
    funcfile.writelog("%t ATTACH DATABASE: VSS_PREV.SQLITE")

    """*************************************************************************"""

    # JOIN PREVIOUS BALANCE AND CURRENT OPENING BALANCE
    print("Join previous balance and current opening balance...")
    sr_file = "X002dc_vss_prevbal_curopen"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        CLOS.STUDENT,
        CLOS.CAMPUS As CAMPUS_CLOS,
        CLOS.BALANCE As BAL_CLOS,
        OPEN.CAMPUS As CAMPUS_OPEN,
        OPEN.BALANCE As BAL_OPEN,
        0.00 AS DIFF_BAL,
        0 AS TYPE
    From
        X002da_vss_student_balance_clos CLOS lEFT Join
        X002da_vss_student_balance_open OPEN ON OPEN.CAMPUS = OPEN.CAMPUS AND
        OPEN.STUDENT = CLOS.STUDENT
    Where
        CLOS.BALANCE <> 0        
    ;"""
    """
            And OPEN.CAMPUS = CLOS.CAMPUS
    """
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # CHANGE BALANCE FROM NULL TO 0.00
    so_curs.execute("UPDATE X002dc_vss_prevbal_curopen " + """
                    SET BAL_OPEN =
                    CASE
                      WHEN BAL_OPEN IS NULL THEN 0.00
                      ELSE BAL_OPEN
                    END
                    ;""")
    so_conn.commit()

    # CALCULATE BALANCE DIFFERENCE
    print("Calculate balance difference...")
    so_curs.execute("UPDATE X002dc_vss_prevbal_curopen " + """
                    SET DIFF_BAL = Cast(round(BAL_OPEN - BAL_CLOS,2) As REAL)
                    ;""")
    so_conn.commit()

    # DETERMINE THE DIFFERENCE TYPE
    # 1 = No difference
    # 2 = Campus the same, but balance differ
    # 3 = Campus differ, but balance the same
    # 4 = Campus differ, and balance differ
    so_curs.execute("UPDATE X002dc_vss_prevbal_curopen " + """
                    SET TYPE =
                    CASE
                      WHEN CAMPUS_OPEN = CAMPUS_CLOS AND DIFF_BAL = 0.00 THEN 1
                      WHEN BAL_CLOS = 0.00 AND CAMPUS_OPEN IS NULL THEN 1
                      WHEN CAMPUS_OPEN = CAMPUS_CLOS AND DIFF_BAL <> 0.00 THEN 4
                      WHEN CAMPUS_OPEN IS NULL AND DIFF_BAL <> 0.00 THEN 2                      
                      WHEN CAMPUS_OPEN <> CAMPUS_CLOS AND DIFF_BAL = 0.00 THEN 3
                      WHEN CAMPUS_OPEN <> CAMPUS_CLOS AND DIFF_BAL <> 0.00 THEN 4
                      ELSE TYPE
                    END
                    ;""")
    so_conn.commit()

    # SUMMARIZE SAME CAMPUS BALANCE DIFFER
    print("Summarize same campus where balance differ (Type=2)...")
    sr_file = "X002dd_vss_campus_same_bal_differ"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        Count(x002vpc.STUDENT) As COUNT,
        x002vpc.CAMPUS_CLOS,
        Total(x002vpc.BAL_CLOS) As BAL_CLOS,
        Total(x002vpc.BAL_OPEN) As BAL_OPEN,
        Total(x002vpc.DIFF_BAL) As DIFF
    From
        X002dc_vss_prevbal_curopen x002vpc
    Where
        x002vpc.TYPE = 2
    Group By
        x002vpc.CAMPUS_CLOS
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # SUMMARIZE SAME DIFFERENT CAMPUS BALANCE SAME
    print("Summarize campus different balance same (Type=3)...")
    sr_file = "X002de_vss_campus_differ_bal_same"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        Count(x002vpc.STUDENT) As COUNT,
        x002vpc.CAMPUS_CLOS,
        Total(x002vpc.BAL_CLOS) As BAL_CLOS,
        x002vpc.CAMPUS_OPEN,
        Total(x002vpc.BAL_OPEN) As BAL_OPEN,
        Total(x002vpc.DIFF_BAL) As DIFF
    From
        X002dc_vss_prevbal_curopen x002vpc
    Where
        x002vpc.TYPE = 3
    Group By
        x002vpc.CAMPUS_CLOS,
        x002vpc.CAMPUS_OPEN
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)

    # SUMMARIZE DIFFERENT CAMPUS DIFFERENT BALANCE
    print("Summarize campus different balance different (Type=4)...")
    sr_file = "X002df_vss_campus_differ_bal_differ"
    s_sql = "CREATE TABLE " + sr_file + " AS " + """
    Select
        Count(x002vpc.STUDENT) As COUNT,
        x002vpc.CAMPUS_CLOS,
        Total(x002vpc.BAL_CLOS) As BAL_CLOS,
        x002vpc.CAMPUS_OPEN,
        Total(x002vpc.BAL_OPEN) As BAL_OPEN,
        Total(x002vpc.DIFF_BAL) As DIFF
    From
        X002dc_vss_prevbal_curopen x002vpc
    Where
        x002vpc.TYPE = 4
    Group By
        x002vpc.CAMPUS_CLOS,
        x002vpc.CAMPUS_OPEN
    ;"""
    so_curs.execute("DROP TABLE IF EXISTS " + sr_file)
    so_curs.execute(s_sql)
    so_conn.commit()
    funcfile.writelog("%t BUILD TABLE: " + sr_file)




    """*****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    print("END OF SCRIPT")
    funcfile.writelog("END OF SCRIPT")

    # COMMIT DATA
    so_conn.commit()

    # CLOSE THE DATABASE CONNECTION
    so_conn.close()

    # CLOSE THE LOG WRITER
    funcfile.writelog("------------------------------------")
    funcfile.writelog("COMPLETED: C200_REPORT_STUDDEB_RECON")

    return


if __name__ == '__main__':
    try:
        # report_studdeb_recon()
        # 2021 balances
        report_studdeb_recon(65676774.13, 61655697.80, 41648563.00, "curr")
        # 2020 balances
        # report_studdeb_recon(48501952.09, -12454680.98, 49976048.39, "curr")
        # 2019 balances
        # report_studdeb_recon(66561452.48,-18340951.06,39482933.18, "prev")
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, "B003_vss_lists", "B003_vss_lists")
