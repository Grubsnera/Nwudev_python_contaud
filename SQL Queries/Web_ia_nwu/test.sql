Select
    ia_assignment.ia_assi_priority,
    Count(ia_assignment.ia_assi_auto) As Count_ia_assi_auto
From
    ia_assignment
Group By
    ia_assignment.ia_assi_priority