<?php
if (!defined('TYPO3_MODE')) {
	die('Access denied.');
}

if (!isset($_EXTKEY)) {
	$_EXTKEY = 'fal_rackspace_cloudfiles';
}

/** @var $driverRegistry \TYPO3\CMS\Core\Resource\Driver\DriverRegistry */
$driverRegistry = \TYPO3\CMS\Core\Utility\GeneralUtility::makeInstance('TYPO3\CMS\Core\Resource\Driver\DriverRegistry');
$driverRegistry->registerDriverClass(
	'TFE\FalRackspaceCloudfiles\Driver\RackspaceCloudfilesDriver',
	'Rackspace Cloud Files',
	'Rackspace Cloud Files Storage driver',
	'FILE:EXT:fal_rackspace_cloudfiles/Configuration/FlexForm/RackspaceCloudfilesDriverFlexForm.xml'
);

if (!is_array($TYPO3_CONF_VARS['SYS']['caching']['cacheConfigurations']['tx_falrackspacecloudfiles_cache'])) {
	$TYPO3_CONF_VARS['SYS']['caching']['cacheConfigurations']['tx_falrackspacecloudfiles_cache'] = array();
}
