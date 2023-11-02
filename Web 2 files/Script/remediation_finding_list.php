<?php

// Script to obtain the findings for the specific assignment
// Returning the finding name
// 24 October 2023 Albert Janse van Rensburg

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
$assi = JFactory::getApplication()->input->getString('assi');

// Build the finding list

// Prepare the empty array
$items = array();

// Keep this if you'd like a "Please select" option, otherwise comment or remove it
$items[] = "|Please Select";

// Build the query

$query = "
Select
    find.ia_find_auto,
    Concat(find.ia_find_name, ' (', find.ia_find_auto, ')') As ia_find_name
From
    ia_finding find Inner Join
    ia_finding_status fist On fist.ia_findstat_auto = find.ia_findstat_auto
Where
    (find.ia_assi_auto = '".$assi."' And
    fist.ia_findstat_name Like 'Send for approval') Or
    (find.ia_assi_auto = '".$assi."' And
    fist.ia_findstat_name Like 'Request remediation')
Order By
    find.ia_find_name
";

// Query the database
$db = JFactory::getDbo();
$db->setQuery($query);
$results = $db->loadObjectList();
$results_count = count($results);

$myXMLData = "<?xml version='1.0' encoding='UTF-8'?>\n";
$myXMLData .= "<finding>\n";
$myXMLData .= "<findid>0</findid>\n";
$myXMLData .= "<findname>Please select</findname>\n";

// Now, we need to convert the results into a readable RSForm! Pro format.
// The Items field will accept values in this format:
// value-to-be-stored|value-to-be-shown
foreach ($results as $result) {
	$myXMLData .= "<findid>".$result->ia_find_auto."</findid>\n";
	$myXMLData .= "<findname>".$result->ia_find_name."</findname>\n";
	//$value = $result->ia_find_auto;
	//$label = $result->ia_find_name;
	//$items[] = $value.'|'.$label;
}

$myXMLData .= "</finding>";

// Multiple values are separated by new lines, so we need to do this now
//$items = implode("\n", $items);

// Now we need to return the value to the field
//echo $items;
$myXMLData = str_replace("&", "and", $myXMLData);
echo $myXMLData;
//$xml=simplexml_load_string($myXMLData);
//echo $xml;

?>
