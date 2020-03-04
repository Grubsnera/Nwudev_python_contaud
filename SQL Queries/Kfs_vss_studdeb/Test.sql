Select
    X021ax_balance_multiple_campus.Campus,
    Count(X021ax_balance_multiple_campus.Student) As Count_Student
From
    X021ax_balance_multiple_campus
Group By
    X021ax_balance_multiple_campus.Campus