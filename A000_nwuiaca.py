"""
NWU Internal Audit Continuous audit robot
Script to peruse all NWU systems, test data and identify audit findings.
Author: Albert B Janse van Rensburg (NWU21162395)
Admin:  Albert J van Rensburg (NWU21162395)
Developed: 15 Mar 2020
"""

# IMPORT PYTHON PACKAGES
import datetime
import time

# IMPORT OWN MODULES
from _my_modules import funcdate
from _my_modules import funcsms

# DECLARE GLOBAL VARIABLES
l_run_project: bool = True
l_message_email: bool = True  # Enable emails communication for the project
l_message_sms: bool = True  # Enable messaging for the project

# SETUP THE ENVIRONMENT

# SEND OPENING MESSAGES
funcsms.send_whatsapp()
funcsms.send_telegram('Dear', 'administrator', "The <b>server</b> is up and running!")

# DISPLAY A RUNNING CLOCK
while True:
    localtime = time.localtime()
    result = time.strftime("%I:%M:%S %p", localtime)
    print(result)
    time.sleep(1)
