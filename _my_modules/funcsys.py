from _my_modules import funcfile


def ResultIter(cursor, arraysize=10000):
    """
    An iterator to keep memory usage down
    :param cursor:
    :param arraysize:
    :return:
    """
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result


def ErrMessage(e):
    print(type(e))
    print(e)
    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
    message = template.format(type(e).__name__, e.args)
    funcfile.writelog("%t ERROR: " + type(e).__name__)
    funcfile.writelog("%t ERROR: " + "".join(e.args))
    return


def tablerowcount(so_curs, table):
    """
    Count the number of rows in a table
    :rtype: int
    """
    so_curs.execute("SELECT COUNT(*) FROM " + table)
    x = so_curs.fetchone()
    y = int(x[0])
    return y
