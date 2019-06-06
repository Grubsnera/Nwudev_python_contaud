from _my_modules import funcfile


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
    :param so_curs: Database cursor
    :param table: Table to count
    :rtype: int
    :return: Number of rows
    """
    so_curs.execute("SELECT COUNT(*) FROM " + table)
    x = so_curs.fetchone()
    return int(x[0])
