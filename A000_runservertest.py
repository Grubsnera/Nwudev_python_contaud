"""
Script to tests to see if SERVER is up and running, and
    to make all finding tables blank
Author: Albert B Janse van Rensburg (NWU:21162395)
Date create: 10 Mar 2020
"""

# IMPORT PYTHON PACKAGES
import datetime
import time

# IMPORT MODULES
from _my_modules import funcdate
from _my_modules import funcmail
from _my_modules import funcsms
from _my_modules import funcsys

# SETUP THE ENVIRONMENT
l_mail: bool = True

# SEND OPENING MESSAGES
funcsms.send_whatsapp()
funcsms.send_telegram('Dear', 'administrator', "The <b>server</b> is up and running!")

# KEEP BOT IDLE
i_year = int(funcdate.cur_year())
i_month = int(funcdate.cur_month())
i_day = int(funcdate.cur_day())
run_start = datetime.datetime(i_year, i_month, i_day, 16, 10, 0)
while datetime.datetime.now() < run_start:
    time.sleep(1)

# STOPPED SLEEPING
funcsms.send_telegram('', 'administrator', 'The <b>server</b> sleeping no more!')

# VACUUM TEST FINDING TABLES
import A003_table_vacuum
try:
    A003_table_vacuum.table_vacuum()
    if l_mail:
        funcmail.Mail('std_success_gmail', 'NWUIAPython:Success:A003_table_vacuum',
                      'NWUIAPython: Success: A003_table_vacuum')
except Exception as e:
    funcsys.ErrMessage(e, l_mail, 'NWUIAPython:Fail:A003_table_vacuum',
                       'NWUIAPython: Fail: A003_table_vacuum')