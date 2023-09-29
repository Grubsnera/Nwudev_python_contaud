"""
MAILER Function to invoke the mailer ***************************************
Copyright (c) Albert J van Rensburg
30 May 2018
*****************************************************************************"""

# Import own modules
from _my_modules import funcmail

# Ask which mail trigger to send
s_trigger = input("Mail trigger: ")

# funcmail.Mail(s_trigger)
funcmail.send_mail(s_trigger)

