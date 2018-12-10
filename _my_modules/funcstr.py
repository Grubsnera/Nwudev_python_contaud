""" FUNCSTR.PY *****************************************************************
Python string functions
Copyright (c) AB Janse van Rensburg
5 Apr 2018
"""

""" Short description of functions *********************************************
exclude_markup(a) Remove all <> pairs from a string, except if <> > 20
include(a,b) Return a with only characters contained in b
"""

# Function EXCLUDE_MARKUP Remove all <> combinations
def exclude_markup(a):

    c = "<"
    while c in a:
        b = a.find("<")
        e = a.find(">")
        if b > 0 and e < 0:
            d = "<"
        elif e - b > 20:
            d = "<"
            a = a.replace(d,"")
            d = ">"
        else:
            d = a[b:e+1]
        a = a.replace(d," ")

    return a
    
# Function INCLUDE Return a with only characters contained in b
def include(a,b=" abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"):
    
    """ Parameters
    a = Source string
    b = Characters to test
    """
    
    c = ""
    for i in range(0, len(a)):
        if a[i] in b:
            c+= a[i]

    return c

# Function to test if string is blank
def isBlank(myString):
    return not (myString and myString.strip())

# Function to test if string is not blank
def isNotBlank(myString):
    return bool(myString and myString.strip())
