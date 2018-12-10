"""
"""

import sys

# Add own module path
sys.path.append('S:/_my_modules')
sys.path.append('S:/')

import funcfile
import funcmail
import funcsys

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: A000_RUN_ALL")
funcfile.writelog("--------------------")

import A001_oracle_to_sqlite
import B001_people_lists
import B002_kfs_lists
import C200_report_studdeb_recon
import C300_test_student_general
import B003_vss_lists

# Environment
l_mail = True

# Extract data from oracle
try:
    A001_oracle_to_sqlite.Oracle_to_sqlite()
except Exception as e:
    funcsys.ErrMessage(e)

# People lists
try:
    B001_people_lists.People_lists()
except Exception as e:
    funcsys.ErrMessage(e)

# Kfs lists
try:
    B002_kfs_lists.Kfs_lists()
except Exception as e:
    funcsys.ErrMessage(e)

# Vss lists
try:
    B003_vss_lists.Vss_lists()
except Exception as e:
    funcsys.ErrMessage(e)

# Kfs vss studdeb report
try:
    C200_report_studdeb_recon.Report_studdeb_recon()
except Exception as e:
    funcsys.ErrMessage(e)

# Vss general tests
try:
    C300_test_student_general.Test_student_general()
except Exception as e:
    funcsys.ErrMessage(e)    

# Close the log writer
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: A000_RUN_ALL")
funcfile.writelog("-----------------------")

# Send mail to indicate successfull completion of all python scripts
if l_mail == True:
    funcmail.Mail("python_log")   

