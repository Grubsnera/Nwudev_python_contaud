SELECT
  X001aa_element_package_prev.ASSIGNMENT_ID,
  Count(X001aa_element_package_prev.INPUT_VALUE_ID) AS Count_INPUT_VALUE_ID
FROM
  X001aa_element_package_prev
GROUP BY
  X001aa_element_package_prev.ASSIGNMENT_ID
