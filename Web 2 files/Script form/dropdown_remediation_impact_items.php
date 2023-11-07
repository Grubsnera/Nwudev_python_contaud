<?php

//<code>

// Get the finding record id
$record_hash = JFactory::getApplication()->input->getString('recordHash');

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
    rate.ia_findrate_auto As value,
    rate.ia_findrate_name As label
From
    ia_finding_rate rate
Where
	rate.ia_findrate_active = 1 And
    rate.ia_findrate_customer = ".$customer_id." And
    rate.ia_findrate_from <= '".$create_date."' And
    rate.ia_findrate_to >= '".$create_date."'
Order By
    rate.ia_findrate_impact,
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
