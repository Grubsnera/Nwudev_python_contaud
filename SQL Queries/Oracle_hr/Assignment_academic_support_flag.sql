﻿Select
    HR.PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_NUMBER,
    HR.PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_ID,
    HR.PER_ALL_ASSIGNMENTS_F.EFFECTIVE_START_DATE,
    HR.PER_ALL_ASSIGNMENTS_F.EFFECTIVE_END_DATE,
    HR.PER_ALL_ASSIGNMENTS_F.POSITION_ID,
    HR.PER_ALL_POSITIONS.POSITION_DEFINITION_ID,
    HR.PER_ALL_POSITIONS.DATE_EFFECTIVE,
    HR.PER_ALL_POSITIONS.DATE_END,
    HR.PER_ALL_POSITIONS.NAME,
    HR.PER_POSITION_DEFINITIONS.START_DATE_ACTIVE,
    HR.PER_POSITION_DEFINITIONS.END_DATE_ACTIVE,
    HR.PER_POSITION_DEFINITIONS.SEGMENT4,
    HR.PER_POSITION_DEFINITIONS.ID_FLEX_NUM,
    HR.PER_POSITION_DEFINITIONS.SUMMARY_FLAG,
    HR.PER_POSITION_DEFINITIONS.ENABLED_FLAG,
    HR.PER_POSITION_DEFINITIONS.SEGMENT1,
    HR.PER_POSITION_DEFINITIONS.SEGMENT2,
    HR.PER_POSITION_DEFINITIONS.SEGMENT3
From
    HR.PER_ALL_ASSIGNMENTS_F Inner Join
    HR.PER_ALL_POSITIONS On HR.PER_ALL_POSITIONS.POSITION_ID = HR.PER_ALL_ASSIGNMENTS_F.POSITION_ID Inner Join
    HR.PER_POSITION_DEFINITIONS On HR.PER_ALL_POSITIONS.POSITION_DEFINITION_ID =
            HR.PER_POSITION_DEFINITIONS.POSITION_DEFINITION_ID
Where
    HR.PER_ALL_ASSIGNMENTS_F.ASSIGNMENT_NUMBER Like '12692174%'
Order By
    HR.PER_ALL_ASSIGNMENTS_F.EFFECTIVE_START_DATE