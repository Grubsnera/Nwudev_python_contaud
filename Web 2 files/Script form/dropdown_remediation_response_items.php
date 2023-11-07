<?php

//<code>

// Get the finding record id
$record_hash = JFactory::getApplication()->input->getString('hash');

// Get the customer id
$customer_id = JFactory::getApplication()->input->getString('customer');

// Obtain the edit record create date
// do only id record number > 0 and an edit
// to display the correct dropdown list for when the record was created
// and open the database
$create_date = date('Y-m-d');
$db = JFactory::getDbo();

if ($record_hash <> '') {

	// Build the query
	$query = "
	Select
		date(find.ia_find_createdate) as create_date
	From
		ia_finding find
	Where
		find.ia_find_token = '".$record_hash."'
	";

	// Query the database
	$db->setQuery($query);
	$results = $db->loadObjectList();

	// Format for RSForm! Pro dropdown format.
	foreach ($results as $result) {
		$create_date = $result->create_date;
	}

}

// Build the query
$query = "
Select
    resp.ia_findresp_auto As value,
    resp.ia_findresp_name As label
From
    ia_finding_response resp
Where
    resp.ia_findresp_active = 1 And
    resp.ia_findresp_customer = ".$customer_id." And
    resp.ia_findresp_from <= '".$create_date."' And
    resp.ia_findresp_to >= '".$create_date."'
Order By
    label
";

// Query the database
$db->setQuery($query);
$results = $db->loadObjectList();

// Format for RSForm! Pro dropdown format.
foreach ($results as $result) {
	$items[] = $result->value.'|'.$result->label;
}

// Now we need to return the value to the field
return $items;

//</code>

?>
