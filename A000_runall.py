# IMPORT SYSTEM MODULES

# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funcmail
from _my_modules import funcdate
from _my_modules import funcsys

""" SCRIPT TO RUN ON A SCHEDULED TIME FOR ALL PYTHON SCRIPTS *******************
Author: Albert J van Rensburg (NWU21162395)
****************************************************************************"""

""" INDEX *********************************************************************
ENVIRONMENT
ORACLE TO SQLITE (Extract data from Oracle system databases to local SQLite)(Run tuesdays to saturdays)
PEOPLE LISTS (Run tuesdays to saturdays)
KFS LISTS (Run tuesdays to saturdays)
VSS LISTS (Run tuesdays to saturdays)
PEOPLE MASTER FILE TESTS (Run only on weekdays)
PEOPLE CONFLICT TESTS (Run only on weekdays)
KFS VSS STUDENT DEBTOR RECONCILIATION AND TESTS
VSS STUDENT MASTER FILE TESTS
****************************************************************************"""

"""****************************************************************************
ENVIRONMENT
****************************************************************************"""

# DECLARE VARIABLES
l_mail = True

# OPEN THE SCRIPT LOG FILE
print("------------")
print("A000_RUN_ALL")
print("------------")
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: A000_RUN_ALL")
funcfile.writelog("--------------------")

"""****************************************************************************
ORACLE TO SQLITE
****************************************************************************"""

if funcdate.today_dayname() in "TueWedThuFriSat":
    import A001_oracle_to_sqlite
    try:
        A001_oracle_to_sqlite.Oracle_to_sqlite()
    except Exception as e:
        funcsys.ErrMessage(e)
else:
    print("ORACLE to SQLITE do not run on Sundays and Mondays")
    funcfile.writelog("SCRIPT: A001_ORACLE_TO_SQLITE: DO NOT RUN ON SUNDAYS AND MONDAYS")

"""****************************************************************************
PEOPLE LISTS
****************************************************************************"""

if funcdate.today_dayname() in "TueWedThuFriSat":
    import B001_people_lists
    try:
        B001_people_lists.people_lists()
    except Exception as e:
        funcsys.ErrMessage(e)
else:
    print("PEOPLE lists do not run on Sundays and Mondays")
    funcfile.writelog("SCRIPT: B001_PEOPLE_LISTS: DO NOT RUN ON SUNDAYS AND MONDAYS")

"""****************************************************************************
KFS LISTS
****************************************************************************"""

if funcdate.today_dayname() in "TueWedThuFriSat":
    import B002_kfs_lists
    try:
        B002_kfs_lists.Kfs_lists()
    except Exception as e:
        funcsys.ErrMessage(e)
else:
    print("KFS lists do not run on Sundays and Mondays")
    funcfile.writelog("SCRIPT: B002_KFS_LISTS: DO NOT RUN ON SUNDAYS AND MONDAYS")

"""****************************************************************************
VSS LISTS
****************************************************************************"""

if funcdate.today_dayname() in "TueWedThuFriSat":
    import B003_vss_lists
    try:
        B003_vss_lists.Vss_lists()
    except Exception as e:
        funcsys.ErrMessage(e)
else:
    print("VSS lists do not run on Sundays and Mondays")
    funcfile.writelog("SCRIPT: B003_VSS_LISTS: DO NOT RUN ON SUNDAYS AND MONDAYS")

"""****************************************************************************
PEOPLE MASTER FILE TESTS
****************************************************************************"""

if funcdate.today_dayname() in "MonTueWedThuFri":
    import C001_people_test_masterfile
    try:
        C001_people_test_masterfile.People_test_masterfile()
    except Exception as e:
        funcsys.ErrMessage(e)
else:
    print("PEOPLE MASTER FILE TESTS do not run on Saturdays and Sundays")
    funcfile.writelog("SCRIPT: C001_PEOPLE_TEST_MASTERFILE: DO NOT RUN ON SATURDAYS AND SUNDAYS")

"""****************************************************************************
PEOPLE CONFLICT TESTS
****************************************************************************"""

if funcdate.today_dayname() in "MonTueWedThuFri":
    import C002_people_test_conflict
    try:
        C002_people_test_conflict.People_test_conflict()
    except Exception as e:
        funcsys.ErrMessage(e)
else:
    print("PEOPLE CONFLICT TESTS do not run on Saturdays and Sundays")
    funcfile.writelog("SCRIPT: C001_PEOPLE_TEST_CONFLICT: DO NOT RUN ON SATURDAYS AND SUNDAYS")

"""****************************************************************************
CREDITOR PAYMENT TESTS
****************************************************************************"""

if funcdate.today_dayname() in "MonTueWedThuFri":
    import C201_creditor_test_payments
    try:
        C201_creditor_test_payments.Creditor_test_payments()
    except Exception as e:
        funcsys.ErrMessage(e)
else:
    print("KFS CREDITOR PAYMENT TESTS do not run on Saturdays and Sundays")
    funcfile.writelog("SCRIPT: C201_CREDITOR_TEST_PAYMENTS: DO NOT RUN ON SATURDAYS AND SUNDAYS")

"""****************************************************************************
KFS VSS STUDENT DEBTOR RECONCILIATION AND TESTS
****************************************************************************"""

if funcdate.today_dayname() in "MonTueWedThuFri":
    import C200_report_studdeb_recon
    try:
        C200_report_studdeb_recon.Report_studdeb_recon()
        # 2019 balances
        # C200_report_studdeb_recon.Report_studdeb_recon('66561452.48','-18340951.06','39482933.18')
    except Exception as e:
        funcsys.ErrMessage(e)
else:
    print("C200_REPORT_STUDDEB_RECON only run on 1-4 and 10th of the month")
    funcfile.writelog("SCRIPT: C200_REPORT_STUDDEB_RECON: ONLY RUN ON 1-4 AND 10TH OF THE MONTH")

"""****************************************************************************
VSS STUDENT MASTER FILE TESTS
****************************************************************************"""

if funcdate.cur_day() in "01z13":
    import C300_test_student_general
    try:
        C300_test_student_general.Test_student_general()
    except Exception as e:
        funcsys.ErrMessage(e)
else:
    print("C300_TEST_STUDENT_GENERAL only run on 1st of the month")
    funcfile.writelog("SCRIPT: C300_TEST_STUDENT_GENERAL: ONLY RUN ON 1ST OF THE MONTH")

"""****************************************************************************
VSS STUDENT DEFERMENT MASTERFILE
****************************************************************************"""

if funcdate.today_dayname() in "MonTueWedThuFri":
    import C301_report_student_deferment
    try:
        C301_report_student_deferment.Studdeb_deferments('curr', funcdate.cur_year())
    except Exception as e:
        funcsys.ErrMessage(e)
else:
    print("VSS STUDENT DEFERMENT MASTER FILE do not run on Saturdays and Sundays")
    funcfile.writelog("SCRIPT: C301_REPORT_STUDENT_DEFERMENT_RUN: DO NOT RUN ON SATURDAYS AND SUNDAYS")

""" ***************************************************************************
PEOPLE LIST MASTERFILE
****************************************************************************"""

if funcdate.today_dayname() in "MonTueWedThuFri":
    import C003_people_list_masterfile
    try:
        C003_people_list_masterfile.people_list_masterfile()
    except Exception as e:
        funcsys.ErrMessage(e)
else:
    print("PEOPLE MASTERFILE LISTS do not run on Saturdays and Sundays")
    funcfile.writelog("SCRIPT: C003_PEOPLE_LIST_MASTERFILE_RUN: DO NOT RUN ON SATURDAYS AND SUNDAYS")

"""****************************************************************************
MYSQL LISTS
****************************************************************************"""

if funcdate.today_dayname() in "TueWedThuFriSat":
    import B005_mysql_lists
    try:
        B005_mysql_lists.mysql_lists()
    except Exception as e:
        funcsys.ErrMessage(e)
else:
    print("MYSQL lists do not run on Sundays and Mondays")
    funcfile.writelog("SCRIPT: B005_MYSQL_LISTS: DO NOT RUN ON SUNDAYS AND MONDAYS")

""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""

# CLOSE THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: A000_RUN_ALL")
funcfile.writelog("-----------------------")

# SEND MAIL TO INDICATE THE SUCCESSFUL COMPLETION OF ALL PYTHON SCRIPTS
if l_mail is True:
    funcmail.Mail("python_log")
