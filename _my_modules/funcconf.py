"""
Script to define global variables
Test
"""

# IMPORT PYTHON PACKAGES
import time
import datetime

# IMPORT OWN MODULES
from _my_modules import funcdate  # Own modules

# DECLARE GLOBAL VARIABLES
l_mail_project: bool = True  # Flag to indicate project sending emails.
l_mess_project: bool = True  # Flag to indicate project sending text messages.
l_run_project: bool = True  # Flag to indicate project running or stopping.

# Date and time to run the vacuum process
if time.strftime("%R", time.localtime()) <= "15:55":
    d_run_vacuum: datetime = datetime.datetime.strptime(funcdate.today() + " 16:00:00", "%Y-%m-%d %H:%M:%S")
else:
    d_run_vacuum: datetime = datetime.datetime.strptime(funcdate.today() + " 16:00:00", "%Y-%m-%d %H:%M:%S") + \
                             datetime.timedelta(days=1)

# Date and time to run the large data process
if time.strftime("%R", time.localtime()) <= "17:55":
    d_run_large: datetime = datetime.datetime.strptime(funcdate.today() + " 18:00:00", "%Y-%m-%d %H:%M:%S")
else:
    d_run_large: datetime = datetime.datetime.strptime(funcdate.today() + " 18:00:00", "%Y-%m-%d %H:%M:%S") + \
                            datetime.timedelta(days=1)

# Date and time to run the test process
if time.strftime("%R", time.localtime()) <= "23:59":
    d_run_test: datetime = datetime.datetime.strptime(funcdate.today() + " 23:00:00", "%Y-%m-%d %H:%M:%S") + \
                           datetime.timedelta(hours=3)
else:
    d_run_test: datetime = datetime.datetime.strptime(funcdate.today() + " 23:00:00", "%Y-%m-%d %H:%M:%S") + \
                             datetime.timedelta(days=1, hours=3)
