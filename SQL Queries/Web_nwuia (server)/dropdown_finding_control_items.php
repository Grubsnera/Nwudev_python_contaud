<?php

//<code>

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
	$db = JFactory::getDbo();
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
		fcon.ia_findcont_auto As value,
		case
		when fcon.ia_findcont_active = 1 then concat(cont.name, ' - ', fcon.ia_findcont_name, ' (Active) ', date(fcon.ia_findcont_from), '/', date(fcon.ia_findcont_to))
		else concat(cont.name, ' - ', fcon.ia_findcont_name, ' (InActve) ', date(fcon.ia_findcont_from), '/', date(fcon.ia_findcont_to))
		end as label
	From
		ia_finding_control fcon Inner Join
		jm4_contact_details cont On cont.id = fcon.ia_findcont_customer
	Order By
		cont.name,
		fcon.ia_findcont_value,
		label
	";

	// Add a please select
	$items[] = "0|Please Select[c]";

} elseif ($action == 'add' or $action == 'copy') {

	// echo 'Add, proceed with function';

	// Build the query
	$query = "
	Select
		fcon.ia_findcont_auto As value,
		fcon.ia_findcont_name As label
	From
		ia_finding_control fcon
	Where
		fcon.ia_findcont_active = 1 And
		-- fcon.ia_findcont_customer = ".$customer_id." And
		fcon.ia_findcont_customer = 1 And
		fcon.ia_findcont_from <= '".date('Y-m-d')."' And    
		fcon.ia_findcont_to >= '".date('Y-m-d')."'
	Order By
		fcon.ia_findcont_value,
		label
	";

	// Add a please select
	$items[] = "0|Please Select[c]";

} elseif ($action == 'edit' or $action == 'delete') {

	// echo 'Customer, proceed with function';

	// Build the query
	$query = "
	Select
		fcon.ia_findcont_auto As value,
		fcon.ia_findcont_name As label
	From
		ia_finding_control fcon
	Where
		fcon.ia_findcont_customer = ".$customer_id." And
		fcon.ia_findcont_from <= '".$create_date."' And
		fcon.ia_findcont_to >= '".$create_date."'
	Order By
		fcon.ia_findcont_value,
		label
	";

} else {

	$items[] = '|Error (login first)[c]';

}
// echo 'Query: '.$query.'<br/>';

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
