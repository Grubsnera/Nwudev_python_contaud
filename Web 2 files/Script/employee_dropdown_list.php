<?php

// Setup joomla to run an external php
define('_JEXEC', 1);    
use Joomla\CMS\Factory;
define('JPATH_BASE', realpath(dirname(__FILE__) . '/..'));
require_once JPATH_BASE . '/includes/defines.php';
require_once JPATH_BASE . '/includes/framework.php';
error_reporting(E_ALL);
ini_set('display_errors', 1);
$crlf = PHP_EOL;
$app = new Joomla\CMS\Application\SiteApplication();
\Joomla\CMS\Factory::$application = $app;

// Get the assignment search id
$search_for = "Searched for";
$search_for = $app->input->getString('param1');

// Build the query
$query = "
Select
    peop.employee_number,
    Concat(peop.name_address, ' (', peop.preferred_name, ') (', peop.organization, ': ', peop.position_name,
    ')') As employee_name
From
    ia_people peop
Where
    (peop.name_full Like ('%".$search_for."%')) Or
    (peop.employee_number Like ('%".$search_for."%')) Or
    (peop.preferred_name Like ('%".$search_for."%')) Or
    (peop.organization Like ('%".$search_for."%')) Or
    (peop.position_name Like ('%".$search_for."%'))
Order By
    peop.name_list,
    peop.initials;
";

// Query the database
$db = JFactory::getDbo();
$db->setQuery($query);
$results = $db->loadObjectList();
$results_count = count($results);

// Result header
$myXMLData = "";
$myXMLData .= "<?xml version='1.0' encoding='UTF-8'?>\n";
$myXMLData .= "<people>\n";

// Exit if no records found
if ($results_count < 1) {
	$myXMLData .= "<peopid>0</peopid>\n";
	$myXMLData .= "<peopname>No records found! Please retry or enter details manually.</peopname>\n";
} else {
	$myXMLData .= "<peopid>0</peopid>\n";
	$myXMLData .= "<peopname>Please select an addressee! (or enter details by hand)</peopname>\n";
	foreach ($results as $result){
		//echo print_r($result);
		$myXMLData .= "<peopid>".$result->employee_number."</peopid>\n";
		$myXMLData .= "<peopname>".$result->employee_name."</peopname>\n";
	}
}

// Result footer
$myXMLData .= "</people>\n";

// Report the data found
$myXMLData = str_replace("&", "and", $myXMLData);
echo $myXMLData;

?>
