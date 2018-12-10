""" FUNCMAIL Script to handle mail sending
Copyright (c) Albert J van Rensburg
Develope: 30 May 2018
"""

""" Summary of functions *******************************************************
def Mail Function to read mail parameters from 000m_Mail.csv
         Prerequisite: 000m_Mail.csv
def Send Function to do the actual mail sending
         Currently sending via nwu.internal.audit@gmail.com
*****************************************************************************"""

def Mail(s_trigger):

    """ Parameters
    s_trigger = Name of the record flag or trigger in 000m_Mail.csv
    """

    # Import python objects
    import sys
    import csv

    # Add own module path
    sys.path.append('S:/_my_modules')
    
    # Import own modules
    import funcfile # File functions
    import funcdate # Date functions
    import funcmail # Mailing functions
    import funcstr  # String functions

    # Open the script log file
    funcfile.writelog("%t DEFINITION: Send mail " + s_trigger)

    # Declare the global variables ************************************************
    sl_path = "S:/_external_data/"

    print("Send email " + s_trigger)

    # Read the mail parameters from the 000_Mail.csv file """
    co = open(sl_path + "000_mail.csv", "rU")
    co_reader = csv.reader(co)

    # Read the COLUMN database data

    for row in co_reader:

        # Populate the local variables
        s_result = ""
        to_name = ""
        to_address = ""
        mail_language = ""
        mail_subject = ""
        mail_body = ""
        file_path = ""
        file_name = ""
        send_mail = False

        # Populate the column variables
        if row[0] != s_trigger:
            continue
        elif row[1] == "X":
            continue
        elif funcstr.isNotBlank(row[9]):
            if row[9] == funcdate.cur_daystrip():
                send_mail = True
            elif row[9] == funcdate.today_dayname():
                send_mail = True
        else:
            send_mail = True

        if send_mail == True:    

            # Build the mail parameters from the 000m_Mail.csv file
            to_name = row[2]
            to_address = row[3]
            mail_language = row[4]
            mail_subject = row[5]
            mail_body = row[6]
            file_path = row[7]
            file_path = file_path.replace("%PYEAR%",funcdate.prev_year())
            file_path = file_path.replace("%PMONTH%",funcdate.prev_month())
            file_path = file_path.replace("%CYEAR%",funcdate.cur_year())
            file_path = file_path.replace("%CMONTH%",funcdate.cur_month())
            file_path = file_path.replace("%TODAY%",funcdate.today_file())
            file_name = row[8]
            file_name = file_name.replace("%PYEAR%",funcdate.prev_year())
            file_name = file_name.replace("%PMONTH%",funcdate.prev_month())
            file_name = file_name.replace("%CYEAR%",funcdate.cur_year())
            file_name = file_name.replace("%CMONTH%",funcdate.cur_month())
            file_name = file_name.replace("%TODAY%",funcdate.today_file())

            # Send the mail
            s_result = funcmail.Send(to_name,to_address,mail_language,mail_subject,mail_body,file_path,file_name)

            # Mail result log
            if s_result == "Successfully sent email":
                print("MAIL SUCCESS: " + to_address + " (" + to_name + ")")
                funcfile.writelog("%t MAIL SUCCESS: " + to_address + " (" + to_name + ")")
            else:
                print("MAIL FAIL: " + to_address + " (" + to_name + ")")
                print("FAIL REASON: " + s_result)
                funcfile.writelog("%t MAIL FAIL: " + to_address + " (" + to_name + ")")
                funcfile.writelog("%t REASON FAIL: " + s_result)
            
    # Close the impoted data file
    co.close()

def Send(to_name,to_addr,mail_lang,mail_subject,mail_body,file_path,file_name):

    """ Parameters
    to_name = Name of the recipient
    to_addr = Email address of the recipient
    mail_subject = Email subject
    mail_body = Email body
    """
    
    import smtplib
    #from smtplib import SMTP # Standard connection
    from smtplib import SMTP_SSL as SMTP #SSL connection
    from email.mime.base import MIMEBase
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    from email import encoders

    # Import own modules
    import funcfile
    import funcdate # Date functions

    # Declare variables
    s_return = ""

    from_addr = "nwu.internal.audit@gmail.com"
    #from_addr = "21162395@nwu.ac.za"
    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = to_addr
    msg['Subject'] = mail_subject

    if mail_lang == "A":

        body = "Hallo " + to_name + """

""" + mail_body + """

Vriendelike groete
Albert
"""

    else:

        body = "Hallo " + to_name + """

""" + mail_body + """

Best regards
Albert
"""
        
    msg.attach(MIMEText(body, 'plain'))

    if file_name != "":
        attachment = open(file_path + file_name, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= %s" % file_name)
        msg.attach(part)

    ServerConnect = False
    try:
        
        server = SMTP('smtp.gmail.com','465')
        server.login("nwu.internal.audit@gmail.com", "fWQZgiUEaZIA10coueMj")
        
        #server = SMTP('smtp.gmail.com','568')
        #server.login("nwu.internal.audit@gmail.com", "fWQZgiUEaZIA10coueMj")
        
        #server = SMTP('v-utl-lnx3.nwu.ac.za','25')
        #server.login("21162395", "vsnpj%K0L%")
        
        #server.ehlo()
        #server.starttls()
        #server.ehlo()
        
        ServerConnect = True
    except smtplib.SMTPHeloError as e:
        s_return = "Server did not reply"
    except smtplib.SMTPAuthenticationError as e:
        s_return = "Incorrect username/password combination"
    except smtplib.SMTPException as e:
        s_return = "Authentication failed"

    if ServerConnect == True:
        try:
            server.sendmail(from_addr, to_addr, msg.as_string())
            s_return = "Successfully sent email"
        except smtplib.SMTPException as e:
            s_return = "Error: unable to send email", e
        finally:
            server.close()
    
    return s_return

