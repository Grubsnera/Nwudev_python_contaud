"""
Script to tests to see if SERVER is up and running, and
    to make all finding tables blank
Author: Albert B Janse van Rensburg (NWU:21162395)
Date create: 10 Mar 2020
"""

# IMPORT PYTHON PACKAGES
from _my_modules import funcsms

# IMPORT MODULES
# SETUP THE ENVIRONMENT

# SEND OPENING MESSAGES
funcsms.send_whatsapp()
funcsms.send_telegram("The <b>server</b> is up and running!")
