""" FUNCSTR.PY *****************************************************************
Python string functions
Copyright (c) AB Janse van Rensburg
5 Apr 2018
"""

""" Short description of functions *********************************************
include(a,b) Return a with only characters contained in b
"""

# Function to Return a with only characters contained in b
def include(a,b):

    c = ""
    for i in range(0, len(b)):
        if b[i] in a:
            c+= b[i]
    
    return c
