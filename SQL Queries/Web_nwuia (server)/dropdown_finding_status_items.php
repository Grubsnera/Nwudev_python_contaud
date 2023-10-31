<?php

//<code>

// To test this page, use the following variables
// in real time, comment these

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

// Obtain the edit record create date
// do only id record number > 0 and an edit
// to display the correct dropdown list for when the record was created
$create_date = date('Y-m-d');

// Open the database
$db = JFactory::getDbo();

if ($record_id > 0) {

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

// Extract the data depending on the action
if ($customer_id == '' or $super_user > 0) {

	// echo 'No customer, proceed with function';

	// Build the query
	$query = "
	Select
		fsta.ia_findstat_auto As value,
		case
		when fsta.ia_findstat_active = 1 then concat(cont.name, ' - ', fsta.ia_findstat_name, ' (Active) ', date(fsta.ia_findstat_from), '/', date(fsta.ia_findstat_to))
		else concat(cont.name, ' - ', fsta.ia_findstat_name, ' (InActve) ', date(fsta.ia_findstat_from), '/', date(fsta.ia_findstat_to))
		end as label
	From
		ia_finding_status fsta Inner Join
		jm4_contact_details cont On cont.id = fsta.ia_findstat_customer
	Order By
		cont.name,
		fsta.ia_findstat_stat,
		label
	";

	// Add a please select
	$items[] = "|Please Select[c]";

} elseif ($action == 'add' or $action == 'copy') {

	// echo 'Add, proceed with function';

	// Build the query
	$query = "
	Select
		fsta.ia_findstat_auto As value,
		fsta.ia_findstat_name As label
	From
		ia_finding_status fsta
	Where
		fsta.ia_findstat_active = 1 And
		fsta.ia_findstat_customer = ".$customer_id." And
		fsta.ia_findstat_from <= '".date('Y-m-d')."' And    
		fsta.ia_findstat_to >= '".date('Y-m-d')."'
	Order By
		fsta.ia_findstat_stat,
		label
	";

	// Add a please select
	$items[] = "|Please Select[c]";

} elseif ($action == 'edit' or $action == 'delete') {

	// echo 'Customer, proceed with function';

	// Build the query
	$query = "
	Select
		fsta.ia_findstat_auto As value,
		fsta.ia_findstat_name As label
	From
		ia_finding_status fsta
	Where
		fsta.ia_findstat_customer = ".$customer_id." And
		fsta.ia_findstat_from <= '".$create_date."' And
		fsta.ia_findstat_to >= '".$create_date."'
	Order By
		fsta.ia_findstat_stat,
		label
	";

} else {

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
