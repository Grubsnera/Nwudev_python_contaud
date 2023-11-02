<?php

// Script to obtain employee details based on the employee number
// Returning the employee name and email address
// Search on employee number
// 29 May 2022 Albert Janse van Rensburg

define( '_JEXEC', 1 );
define('JPATH_BASE', realpath(dirname(__FILE__) . '/..'));
require_once JPATH_BASE . '/includes/defines.php';
require_once JPATH_BASE . '/includes/framework.php';

// Create the Application
$mainframe = JFactory::getApplication('site');

// Get the assignment search id
$user = JFactory::getUser();
$userId = $user->id;
//$search_for = "";
//$search_for = JFactory::getApplication()->input->getString('param1');

// Report the parameters
//echo $search_for;

// Build the query
$query = "
Select
    reme.ia_findreme_auto,
    assi.ia_assi_auto,
    assi.ia_assi_name,
    find.ia_find_auto,
    find.ia_find_name
From
    ia_finding_remediation reme Inner Join
    ia_finding find On find.ia_find_auto = reme.ia_find_auto Inner Join
    ia_assignment assi On assi.ia_assi_auto = find.ia_assi_auto
Where
    reme.ia_findreme_mail_trigger > 0 And
    reme.ia_findreme_schedule > 0 And
    reme.ia_findreme_date_schedule <= Now() And
    find.ia_user_sysid = '".$userId."'855
";

// Report the result query
//echo $query;

// Open the foreign database
$option = array(); //prevent problems
$option['driver'] = 'mysqli'; // Database driver name
$option['host'] = 'sql23.jnb1.host-h.net'; // Database host name
$option['user'] = 'Ia_nwu_1'; // User for database authentication
$option['password'] = '+C8+amXnmdo'; // Password for database authentication
$option['database'] = 'Ia_nwu'; // Database name
$option['prefix'] = ''; // Database prefix (may be empty)
$iadb = JDatabaseDriver::getInstance( $option );
$iadb->setQuery($query);
$results = $iadb->loadObjectList();
$results_count = count($results);

echo $results_count;
//return $results_count;


?>
