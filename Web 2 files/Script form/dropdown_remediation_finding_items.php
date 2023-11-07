<?php

//<code>

// Variables
$database_lookup = True;

// Get the record id
$record_id = JFactory::getApplication()->input->getString('recordId');
//$record_id = 0;

// Get the record action
$action = JFactory::getApplication()->input->getString('action');
//$action = 'add';

// Get the assignment record id
$assignment_id = 0;
$assignment_id = JFactory::getApplication()->input->getString('assignment');

// Get the finding record id
$finding_id = 0;
$finding_id = JFactory::getApplication()->input->getString('finding');

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

// Extract the data depending on the action
if ($customer_id == '' or $super_user > 0) {

	// Super user or no customer 

	// No findings to display
	$database_lookup = False;
	
	// Add a please select
	$items[] = "|Please Select[c]";	

} elseif ($action == 'edit' or $action == 'delete' or $action == 'copy' or ($action == 'add' and $finding_id > 0)) {

	// Display the finding for a specific assignment

	// Build the query
	$query = "
	Select
		f.ia_find_auto As value,
		Concat(f.ia_find_name, ' (', f.ia_find_auto, ')') As label
	From
		ia_finding f Inner Join
		ia_assignment a On a.ia_assi_auto = f.ia_assi_auto
	Where
		a.ia_assi_auto = ".$assignment_id." And
		f.ia_find_auto = ".$finding_id."
	";
	
} elseif ($action == 'add') {

	// No findings to display
	$database_lookup = False;
	
	// Add a please select
	$items[] = "|Please Select[c]";	
	
} else {

	$items[] = '|Error (login first)[c]';

}

// Lookup list values in database

if ($database_lookup) {

	// Query the database
	$db = JFactory::getDbo();
	$db->setQuery($query);
	$results = $db->loadObjectList();

	// Format for RSForm! Pro dropdown format.
	foreach ($results as $result) {
		if ($finding_id > 0 and $finding_id == $result->value) {
			$items[] = $result->value.'|'.$result->label.'[c]';
		} else {
			$items[] = $result->value.'|'.$result->label;
		}
	}

}	

// Now we need to return the value to the field
return $items;

//</code>

?>