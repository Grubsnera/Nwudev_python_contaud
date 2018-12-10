""" MAILER Function to invoke the mailer ***************************************
Copyright (c) Albert J van Rensburg
30 May 2018
*****************************************************************************"""

# Import python objects
import sys

# Add own module path
sys.path.append('S:/_my_modules')

# Import own modules
import funcmail

# Ask which mail trigger to send
s_trigger = input("Mail trigger: ")

funcmail.Mail(s_trigger)


