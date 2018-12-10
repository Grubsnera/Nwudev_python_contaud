import datetime
import calendar

#Build current year and month variables
st_year = datetime.date.today().strftime("%Y") #Current year
st_month = str(datetime.date.today().strftime("%m")) #Current month

#Build the current year begin and end dates
st_yearb = st_year + "-01-01"
st_yeare = st_year + "-12-31"

#Build previous year and month
if int(st_month) == 1:
    st_pyear = str( int(st_year) - 1) #Previous year
    st_pmonth = "12"
else:
    st_pyear = st_year #Previous year
    st_pmonth = str( int(st_month) - 1)
if int(st_pmonth) < 10:
    st_pmonth = "0" + st_pmonth

#Build current month begin and end date variables
st_monthb = st_year + "-" + st_month + "-01"
if st_month in "01z03z05z07z08z10z12":
    st_monthe = "31"
elif st_month in "04z06z09z11":
    st_monthe = "30"
else:
    if calendar.isleap(int(st_year)):
        st_monthe = "29"
    else:
        st_monthe = "28"
st_monthe = st_year + "-" + st_month + "-" + st_monthe

#Build previous month begin and end dates
st_pmonthb = st_pyear + "-" + st_pmonth + "-01"
if st_pmonth in "01z03z05z07z08z10z12":
    st_pmonthe = "31"
elif st_pmonth in "04z06z09z11":
    st_pmonthe = "30"
else:
    if calendar.isleap(int(st_pyear)):
        st_pmonthe = "29"
    else:
        st_pmonthe = "28"
st_pmonthe = st_pyear + "-" + st_pmonth + "-" + st_pmonthe

print(st_year)
print(st_yearb)
print(st_yeare)
print(st_pyear)
print(st_month)
print(st_pmonth)
print(st_monthb)
print(st_monthe)
print(st_pmonthb)
print(st_pmonthe)

