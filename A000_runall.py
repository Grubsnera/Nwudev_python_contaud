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

import A001_oracle_to_sqlite
try:
    A001_oracle_to_sqlite.Oracle_to_sqlite()
except Exception as e:
    funcsys.ErrMessage(e)

"""****************************************************************************
PEOPLE LISTS
****************************************************************************"""

import B001_people_lists
try:
    B001_people_lists.People_lists()
except Exception as e:
    funcsys.ErrMessage(e)

"""****************************************************************************
KFS LISTS
****************************************************************************"""

import B002_kfs_lists
try:
    B002_kfs_lists.Kfs_lists()
except Exception as e:
    funcsys.ErrMessage(e)

"""****************************************************************************
VSS LISTS
****************************************************************************"""

import B003_vss_lists
try:
    B003_vss_lists.Vss_lists()
except Exception as e:
    funcsys.ErrMessage(e)

"""****************************************************************************
PEOPLE MASTER FILE TESTS
****************************************************************************"""

if funcdate.today_dayname() == "Sat":
elif funcdate.today_dayname() == "Sun":
else:
    import C001_people_test_masterfile
    try:
        C001_people_test_masterfile.People_test_masterfile()
    except Exception as e:
        funcsys.ErrMessage(e)

"""****************************************************************************
KFS VSS STUDENT DEBTOR RECONCILIATION AND TESTS
****************************************************************************"""

import C200_report_studdeb_recon
try:
    C200_report_studdeb_recon.Report_studdeb_recon('66561452.48','-18340951.06','39482933.18')
except Exception as e:
    funcsys.ErrMessage(e)

"""****************************************************************************
VSS STUDENT MASTERFILE TESTS
****************************************************************************"""

import C300_test_student_general
try:
    C300_test_student_general.Test_student_general()
except Exception as e:
    funcsys.ErrMessage(e)

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

