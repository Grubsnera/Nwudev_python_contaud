c = "<"
while c in a:
    b = a.find("<")
    e = a.find(">")
    d = a[b:e+1]
    a = a.replace(d," ")

	



