""" SCRIPT TO RUN ON A SCHEDULED TIME FOR ALL PYTHON SCRIPTS *******************
Created: 2018
Author: Albert J van Rensburg (NWU21162395)
****************************************************************************"""

""" INDEX *********************************************************************
ENVIRONMENT
ORACLE TO SQLITE (Extract data from Oracle system databases to local SQLite)
PEOPLE LISTS
KFS LISTS
VSS LISTS
PEOPLE MASTER FILE TESTS (Run only on weekdays)
KFS VSS STUDENT DEBTOR RECONCILIATION AND TESTS
VSS STUDENT MASTERFILE TESTS
****************************************************************************"""

"""****************************************************************************
ENVIRONMENT
****************************************************************************"""

# IMPORT PYTHON MODULES
import sys

# ADD OWN MODULE PATH
sys.path.append('S:/_my_modules')
sys.path.append('S:/')

# IMPORT OWN MODULES
import funcdate
import funcfile
import funcmail
import funcsys

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
        B001_people_lists.People_lists()
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
    print("PEOPLE MASTERFILE TESTS do not run on Saturdays and Sundays")
    funcfile.writelog("SCRIPT: C001_PEOPLE_TEST_MASTERFILE: DO NOT RUN ON SATURDAYS AND SUNDAYS")

"""****************************************************************************
KFS VSS STUDENT DEBTOR RECONCILIATION AND TESTS
****************************************************************************"""

if funcdate.cur_day() in "01z02z03z04z10":
    import C200_report_studdeb_recon
    try:
        C200_report_studdeb_recon.Report_studdeb_recon('66561452.48','-18340951.06','39482933.18')
    except Exception as e:
        funcsys.ErrMessage(e)
else:
    print("C200_REPORT_STUDDEB_RECON only run on 1-4 and 10th of the month")
    funcfile.writelog("SCRIPT: C200_REPORT_STUDDEB_RECON: ONLY RUN ON 1-4 AND 10TH OF THE MONTH")

"""****************************************************************************
VSS STUDENT MASTERFILE TESTS
****************************************************************************"""

if funcdate.cur_day() in "01":
    import C300_test_student_general
    try:
        C300_test_student_general.Test_student_general()
    except Exception as e:
        funcsys.ErrMessage(e)
else:
    print("C300_TEST_STUDENT_GENERAL only run on 1st of the month")
    funcfile.writelog("SCRIPT: C300_TEST_STUDENT_GENERAL: ONLY RUN ON 1ST OF THE MONTH")

""" ****************************************************************************
END OF SCRIPT
*****************************************************************************"""

# CLOSE THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: A000_RUN_ALL")
funcfile.writelog("-----------------------")

# SEND MAIL TO INDICATE THE SUCCESSFULL COMPLETION OF ALL PYTHON SCRIPTS
if l_mail == True:
    funcmail.Mail("python_log")   

