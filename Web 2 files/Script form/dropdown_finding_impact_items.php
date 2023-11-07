<?php

//<code>

// Script to display a list of impact ratings

// Get the record id
$record_id = JFactory::getApplication()->input->getString('recordId');

// Get the record action
$action = JFactory::getApplication()->input->getString('action');

// Test to see if a super user or a customer
$user = JFactory::getUser();
$user_id = $user->get('id');
$customer_id = $_SESSION['customer_id'];

// Determine if the user is a super user = group = 8
$super_user = 0;
foreach ($user->getAuthorisedGroups() as $group) {
	if ($group == 8) {
		$super_user = 1;
	}
}

// Get the finding create date
$create_date = date('Y-m-d');

// Open the database
$db = JFactory::getDbo();

if ($record_id > 0) {
	
	// Get the finding create date

	// Build the query
	$query = "
	Select
		date(ltab.ia_find_createdate) as create_date
	From
		ia_finding ltab
	Where
		ltab.ia_find_auto = ". $record_id."
	";

	// Query the database
	$db->setQuery($query);
	$results = $db->loadObjectList();

	// Format for RSForm! Pro dropdown format.
	foreach ($results as $result) {
		$create_date = $result->create_date;
	}

}

// Build the dropdown list depending on the action
if ($customer_id == '' or $super_user > 0) {

	// In case of super user or no customer

	// Build the query
	$query = "
	Select
		rate.ia_findrate_auto As value,
		case
		when rate.ia_findrate_active = 1 then concat(cont.name, ' - ', rate.ia_findrate_name, ' (Active) ', date(rate.ia_findrate_from), '/', date(rate.ia_findrate_to))
		else concat(cont.name, ' - ', rate.ia_findrate_name, ' (InActve) ', date(rate.ia_findrate_from), '/', date(rate.ia_findrate_to))
		end as label
	From
		ia_finding_rate rate Inner Join
		jm4_contact_details cont On cont.id = rate.ia_findrate_customer
	Order By
		cont.name,
		rate.ia_findrate_impact,
		label
	";

	// Add a please select
	$items[] = "0|Please Select[c]";

} elseif ($action == 'add' or $action == 'copy') {

	// In case of add or copy

	// Build the query
	$query = "
	Select
		frat.ia_findrate_auto As value,
		frat.ia_findrate_name As label
	From
		ia_finding_rate frat
	Where
		frat.ia_findrate_active = 1 And
		frat.ia_findrate_customer = ".$customer_id." And
		frat.ia_findrate_from <= '".date('Y-m-d')."' And    
		frat.ia_findrate_to >= '".date('Y-m-d')."'
	Order By
		frat.ia_findrate_impact,
		label
	";

	// Add a please select
	$items[] = "0|Please Select[c]";

} elseif ($action == 'edit' or $action == 'delete') {

	// In case of edit or delete
	
	// Build the query
	$query = "
	Select
		frat.ia_findrate_auto As value,
		frat.ia_findrate_name As label
	From
		ia_finding_rate frat
	Where
		frat.ia_findrate_customer = ".$customer_id." And
		frat.ia_findrate_from <= '".$create_date."' And    
		frat.ia_findrate_to >= '".$create_date."'
	Order By
		frat.ia_findrate_impact,
		label
	";	

} else {
	
	// In all other cases

	$items[] = '|Error (login first)[c]';

}

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
