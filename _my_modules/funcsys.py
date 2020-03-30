

def ResultIter(cursor, size=10000):
    """
    An iterator to keep memory usage down
    :param size: Array size before commit
    :param cursor: Database cursor
    :return: Result of fetchmany
    """
    while True:
        results = cursor.fetchmany(size)
        if not results:
            break
        for result in results:
            yield result


def ErrMessage(e, l_mail=False, s_subject='', s_body=''):
    """
    Error message handler.
    :param e: Error object
    :param l_mail: Send mail and message
    :param s_subject: Mail subject
    :param s_body: Mail body
    :return: Nothing
    """

    # IMPORT OWN MODULES
    from _my_modules import funcconf
    from _my_modules import funcfile
    from _my_modules import funcmail
    from _my_modules import funcsms

    # DECLARE VARIABLES
    s_mess = str(e).replace("'", "")
    s_mess = s_mess.replace('"', '')
    s_mess = s_mess.replace(':', '')
    s_mess = s_mess.replace('[', '')
    s_mess = s_mess.replace(']', '')
    s_mess = s_mess.replace('(', '')
    s_mess = s_mess.replace(')', '')
    if not funcconf.l_mail_project:
        l_mail = False

    # DISPLAY
    print(s_mess)
    print("E: ", e)
    print("TYPE(E): ", type(e))
    print("TYPE(E)NAME: ", type(e).__name__)
    print("JOIN(E.ARGS: ", e.args)

    # DEFINE THE ERROR TEMPLATE AND MESSAGE
    template = "Exception type {0} occurred. Arguments:\n{1!r}"
    message = template.format(type(e).__name__, e.args)
    print("MESSAGE: ", message)

    # SEND MAIL
    if l_mail and s_subject != '' and s_body != '':
        # s_body1 = s_body + '\n' + type(e).__name__ + '\n' + "".join(e.args)
        s_body1 = s_body + '\n' + s_mess
        funcmail.Mail('std_fail_gmail', s_subject, s_body1)

    # SEND MESSAGE
    if funcconf.l_mess_project:
        # s_body1 = s_body + ' | ' + type(e).__name__ + ' | ' + "".join(e.args)
        s_body1 = s_body + ' | ' + s_mess
        funcsms.send_telegram('', 'administrator', s_body1)

    # WRITE ERROR TO LOG
    funcfile.writelog("%t ERROR: " + type(e).__name__)
    # funcfile.writelog("%t ERROR: " + "".join(e.args))
    funcfile.writelog("%t ERROR: " + s_mess)

    return


def tablerowcount(so_curs, table):
    """
    Count the number of rows in a table.
    :param so_curs: Database cursor
    :param table: Table to count
    :rtype: int
    :return: Number of rows
    """
    so_curs.execute("SELECT COUNT(*) FROM " + table)
    x = so_curs.fetchone()
    return int(x[0])
