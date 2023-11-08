<?php

// Script to obtain the finding likelihood description
// Returning the likelihood description
// Search on impact rating auto number
// 18 September 2023 Albert Janse van Rensburg

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
$id = JFactory::getApplication()->input->getString('id');

// Build the query
$query = "
Select
    hood.ia_findlike_auto,
    hood.ia_findlike_name,
    hood.ia_findlike_desc,
    hood.ia_findlike_value
From
    ia_finding_likelihood hood
Where
    hood.ia_findlike_auto = '".$id."'
Order By
    hood.ia_findlike_value
";

// Query the database
$db = JFactory::getDbo();
$db->setQuery($query);
$results = $db->loadObjectList();
$results_count = count($results);

// Results header
$myXMLData = "<?xml version='1.0' encoding='UTF-8'?>\n";
$myXMLData .= "<likelihood>\n";

// Keep this if you'd like a "Please select" option, otherwise comment or remove it
//$myXMLData .= "<id>0</id>\n";
//$myXMLData .= "<name>Please select a likelihood</name>\n";

// Now, we need to convert the results into a readable RSForm! Pro format.
// The Items field will accept values in this format:
// value-to-be-stored|value-to-be-shown
foreach ($results as $result) {
	$myXMLData .= "<id>".$result->ia_findlike_auto."</id>\n";
	$myXMLData .= "<name>".$result->ia_findlike_name."</name>\n";
	$myXMLData .= "<description>".$result->ia_findlike_desc."</description>\n";
	$myXMLData .= "<value>".$result->ia_findlike_value."</value>\n";
}

$myXMLData .= "</likelihood>";

// Remove any unwanter characters
$myXMLData = str_replace("&", "and", $myXMLData);

// Display the results
echo $myXMLData;

?>