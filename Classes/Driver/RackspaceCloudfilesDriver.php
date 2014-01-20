<?php
namespace TFE\FalRackspaceCloudfiles\Driver;

/***************************************************************
 *  Copyright notice
 *
 *  (c) 2014 Arjen Hoekema <a.hoekema@tfe.nl>, theFactor.e
 *  All rights reserved
 *
 *  This script is part of the TYPO3 project. The TYPO3 project is
 *  free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  The GNU General Public License can be found at
 *  http://www.gnu.org/copyleft/gpl.html.
 *  A copy is found in the textfile GPL.txt and important notices to the license
 *  from the author is found in LICENSE.txt distributed with these scripts.
 *
 *
 *  This script is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  This copyright notice MUST APPEAR in all copies of the script!
 ***************************************************************/

use TYPO3\CMS\Core\Resource\FileInterface;
use TYPO3\CMS\Core\Resource\Folder;
use TYPO3\CMS\Core\Utility\GeneralUtility;

use OpenCloud\Rackspace;

/**
 * FAL Rackspace Cloud Files Driver
 *
 * @author Arjen Hoekema <a.hoekema@tfe.nl>
 * @package TYPO3
 * @subpackage fal_rackspace_cloudfiles
 */
class RackspaceCloudfilesDriver extends \TYPO3\CMS\Core\Resource\Driver\AbstractDriver {

	const ROOT_FOLDER_IDENTIFIER = '/';
	const CONTENT_TYPE_DIRECTORY = 'application/directory';
	const TEMPORARY_URL_EXPIRES = 30;

	const OBJECT_METHOD_GET = 0;
	const OBJECT_METHOD_HEAD = 1;

	const CACHE_PREFIX_PARTIAL = 'partial_';
	const CACHE_PREFIX_LIST = 'list_';
	const CACHE_PREFIX_LIST_RECURSIVE = 'list_r_';

	const OPERATION_MOVE = 0;
	const OPERATION_COPY = 1;

	/**
	 * Default Configuration
	 *
	 * @var array
	 */
	protected $defaultConfiguration = array(
		'endpoint' => RACKSPACE_UK,
		'region' => 'LON',
		'composerPath' => 'Packages/'
	);

	/**
	 * Available Regions
	 *
	 * @var array
	 */
	protected static $availableRegions = array(
		'DFW',
		'ORD',
		'IAD',
		'LON',
		'HKG',
		'SYD'
	);

	/**
	 * Service
	 *
	 * @var \OpenCloud\ObjectStore\Service
	 */
	protected $service;

	/**
	 * Container
	 *
	 * @var \OpenCloud\ObjectStore\Resource\Container
	 */
	protected $container;

	/**
	 * Base URL
	 *
	 * @var string
	 */
	protected $baseUrl;

	/**
	 * Public Base URL
	 *
	 * @var string
	 */
	protected $publicBaseUrl;

	/**
	 * Cache
	 *
	 * @var \TYPO3\CMS\Core\Cache\Frontend\AbstractFrontend
	 */
	protected $cache;

	/**
	 * A list of all supported hash algorithms, written all lower case and
	 * without any dashes etc. (e.g. sha1 instead of SHA-1)
	 * Be sure to set this in inherited classes!
	 *
	 * @var array
	 */
	protected $supportedHashAlgorithms = array('md5', 'sha1');

	/**
	 * Use ETag as md5 hash
	 *
	 * @var boolean
	 */
	protected $useETagAsMd5Hash = TRUE;

	/**
	 * Initializes this object. This is called by the storage after the driver
	 * has been attached.
	 *
	 * @return void
	 */
	public function initialize() {
		$this->capabilities = \TYPO3\CMS\Core\Resource\ResourceStorage::CAPABILITY_BROWSABLE
			+ \TYPO3\CMS\Core\Resource\ResourceStorage::CAPABILITY_WRITABLE;

		$this->initializeCache();
		$this->initializePublicBaseUrl();
	}

	/**
	 * Initialize Cache
	 *
	 * @return void
	 */
	protected function initializeCache() {
		$this->cache = $GLOBALS['typo3CacheManager']->getCache('tx_falrackspacecloudfiles_cache');
	}

	/**
	 * Initialize Object Store Service
	 *
	 * @return void
	 */
	protected function initializeObjectStoreService() {
		try {
			$rackspaceClient = new Rackspace(
				$this->configuration['endpoint'],
				array(
					'username' => $this->configuration['username'],
					'apiKey' => $this->configuration['apiKey']
				)
			);
			$this->service = $rackspaceClient->objectStoreService('cloudFiles', $this->configuration['region']);
		} catch (Exception $e) {

		}
	}

	/**
	 * Initialize Container
	 *
	 * @return void
	 */
	protected function initializeContainer() {
		try {
			$this->container = $this->getService()->getContainer($this->configuration['container']);
		} catch (\Guzzle\Http\Exception\ClientErrorResponseException $e) {
			if ($e->getResponse()->getStatusCode() == 404) {
				$this->getService()->createContainer($this->configuration['container']);
			}
		}

		// Check if CDN is enabled and make the storage public
		if ($this->container->isCdnEnabled()) {
			$this->capabilities += \TYPO3\CMS\Core\Resource\ResourceStorage::CAPABILITY_PUBLIC;
		}
	}

	/**
	 * Initialize Base Url
	 *
	 * @return void
	 */
	protected function initializeBaseUrl() {
		if ($this->getContainer()->isCdnEnabled()) {
			if (\TYPO3\CMS\Core\Utility\GeneralUtility::getIndpEnv('TYPO3_SSL')) {
				$this->baseUrl = $this->getContainer()->getCdn()->getCdnSslUri();
			} else {
				$this->baseUrl = $this->getContainer()->getCdn()->getCdnUri();
			}
		}
		if (!empty($this->baseUrl)) {
			$this->baseUrl = rtrim($this->baseUrl, '/') . '/';
		}
	}

	/**
	 * Initialize Public Base URL
	 *
	 * @return void
	 */
	protected function initializePublicBaseUrl() {
		if (isset($this->configuration['publicBaseUrl']) && $this->configuration['publicBaseUrl'] !== '') {
			$this->publicBaseUrl = \TYPO3\CMS\Core\Utility\GeneralUtility::getIndpEnv('TYPO3_SSL') ? 'https' : 'http' . '://';
			$this->publicBaseUrl .= $this->configuration['publicBaseUrl'];
			$this->publicBaseUrl = rtrim($this->publicBaseUrl, '/') . '/';
			$this->capabilities &= \TYPO3\CMS\Core\Resource\ResourceStorage::CAPABILITY_PUBLIC;
		}
	}

	/**
	 * Checks if a configuration is valid for this driver.
	 * Throws an exception if a configuration will not work.
	 *
	 * @param array $configuration
	 * @return void
	 * @throws \TYPO3\CMS\Core\Resource\Exception\InvalidConfigurationException
	 */
	static public function verifyConfiguration(array $configuration) {
		// Check endpoint
		if (!in_array($configuration['endpoint'], array('us', 'uk'))) {
			throw new \TYPO3\CMS\Core\Resource\Exception\InvalidConfigurationException('Configuration must contain a valid endpoint (uk/us).', 1389704875);
		}
		// Check region
		if (!in_array($configuration['region'], static::$availableRegions)) {
			throw new \TYPO3\CMS\Core\Resource\Exception\InvalidConfigurationException('Configuration must contain a valid region (DFW, ORD, IAD, LON, HKG, SYD).', 1389705064);
		}
		// Check username
		if (trim($configuration['username']) == '') {
			throw new \TYPO3\CMS\Core\Resource\Exception\InvalidConfigurationException('Configuration must contain a valid username.', 1389705488);
		}
		// Check apiKey
		if (!preg_match('/^[a-f0-9]{32}$/', $configuration['apiKey'])) {
			throw new \TYPO3\CMS\Core\Resource\Exception\InvalidConfigurationException('Configuration must contain a valid apiKey.', 1389705590);
		}
		// Check composer path
		if (!empty($configuration['composerPath'])) {
			if (static::getComposerVendorAutoload($configuration['composerPath']) === FALSE) {
				throw new \TYPO3\CMS\Core\Resource\Exception\InvalidConfigurationException('Configuration must contain a valid composer path.', 1389883749);
			}
		}
	}

	/**
	 * Get Composer Vendor Autoload
	 *
	 * @param string $composerPath Composer path
	 * @return string|boolean Composer vendor/autoload.php, false on failure
	 */
	protected static function getComposerVendorAutoload($composerPath) {
		if (!GeneralUtility::isAbsPath($composerPath)) {
			$composerPath = PATH_site . $composerPath;
		}
		$vendorAutoloadPath = rtrim($composerPath, '/') . '/vendor/autoload.php';
		if (GeneralUtility::validPathStr($vendorAutoloadPath) && file_exists($vendorAutoloadPath) && is_readable($vendorAutoloadPath)) {
			return $vendorAutoloadPath;
		}
		return FALSE;
	}

	/**
	 * Processes the configuration, should be overridden by subclasses
	 *
	 * @return void
	 * @throws \TYPO3\CMS\Core\Resource\Exception\InvalidConfigurationException
	 */
	public function processConfiguration() {
		// Merge in default configuration
		$this->configuration = GeneralUtility::array_merge_recursive_overrule(
			$this->defaultConfiguration,
			$this->configuration,
			FALSE,
			FALSE
		);

		// Composer path + vendor autoload
		if (($vendorAutoload = static::getComposerVendorAutoload($this->configuration['composerPath'])) !== FALSE) {
			require_once $vendorAutoload;
		} else {
			throw new \TYPO3\CMS\Core\Resource\Exception\InvalidConfigurationException('Configuration must contain a valid composer path containing a valid "vendor/autoload.php".', 1389883749);
		}

		$this->configuration['region'] = strtoupper($this->configuration['region']);

		// Convert identity endpoints
		switch ($this->configuration['endpoint']) {
			case 'us':
				$this->configuration['endpoint'] = Rackspace::US_IDENTITY_ENDPOINT;
				break;
			case 'uk':
			default:
				$this->configuration['endpoint'] = Rackspace::UK_IDENTITY_ENDPOINT;
		}

		// Strip http(s):// from publicBaseUrl
		if (!empty($this->configuration['publicBaseUrl'])) {
			$this->configuration['publicBaseUrl'] = str_replace(array('http://', 'https://'), '', $this->configuration['publicBaseUrl']);
		}
	}

	/**
	 * Returns the public URL to a file.
	 *
	 * @param \TYPO3\CMS\Core\Resource\ResourceInterface $resource
	 * @param bool $relativeToCurrentScript Determines whether the URL returned should be relative to the current script, in case it is relative at all (only for the LocalDriver)
	 * @return string
	 */
	public function getPublicUrl(\TYPO3\CMS\Core\Resource\ResourceInterface $resource, $relativeToCurrentScript = FALSE) {
		if ($this->storage->isPublic()) {
			// Check Public Base URL
			if (isset($this->publicBaseUrl)) {
				return $this->publicBaseUrl . $this->normalizeIdentifier($resource->getIdentifier());
			}

			// Check Public CDN URL from item
			if ($object = $this->getPartialObject($resource->getIdentifier())) {
				try {
					if (\TYPO3\CMS\Core\Utility\GeneralUtility::getIndpEnv('TYPO3_SSL')) {
						return $object->getPublicUrl(\OpenCloud\ObjectStore\Constants\UrlType::SSL);
					} else {
						return $object->getPublicUrl(\OpenCloud\ObjectStore\Constants\UrlType::CDN);
					}
				} catch (\OpenCloud\Common\Exceptions\CdnNotAvailableError $e) {

				}
			}

			// Check global container CDN baseUrl
			$baseUrl = $this->getBaseUrl();
			if (!empty($baseUrl)) {
				return $baseUrl . $this->normalizeIdentifier($resource->getIdentifier());
			}

			// Fallback to temporary URL
			if ($object = $this->getPartialObject($resource->getIdentifier())) {
				return $object->getTemporaryUrl(self::TEMPORARY_URL_EXPIRES, 'GET');
			}
		}
	}

	/**
	 * Creates a (cryptographic) hash for a file.
	 *
	 * @param string $fileIdentifier
	 * @param string $hashAlgorithm The hash algorithm to use
	 * @return string Hash
	 * @throws \InvalidArgumentException
	 * @throws \RuntimeException
	 */
	public function hash($fileIdentifier, $hashAlgorithm) {
		if (!in_array($hashAlgorithm, $this->getSupportedHashAlgorithms())) {
			throw new \InvalidArgumentException('Hash algorithm "' . $hashAlgorithm . '" is not supported.', 1304964032);
		}
		if ($hashAlgorithm === 'md5' && $this->useETagAsMd5Hash) {
			$object = $this->getPartialObject($fileIdentifier);
			return $object->getEtag();
		}
		$object = $this->getObject($fileIdentifier);
		$object->refresh();
		if (($hash = \Guzzle\Stream\Stream::getHash($object->getContent(), $hashAlgorithm)) !== FALSE) {
			return $hash;
		}
		throw new \RuntimeException('Could not hash file"' . $fileIdentifier . '" with algorithm"' . $hashAlgorithm . '".', 1389774103);
	}

	/**
	 * Creates a new file and returns the matching file object for it.
	 *
	 * @param string $fileName
	 * @param Folder $parentFolder
	 * @return \TYPO3\CMS\Core\Resource\File
	 * @throws \TYPO3\CMS\Core\Resource\Exception\InvalidFileNameException
	 */
	public function createFile($fileName, Folder $parentFolder) {
		if (!$this->isValidFilename($fileName)) {
			throw new \TYPO3\CMS\Core\Resource\Exception\InvalidFileNameException('Invalid characters in fileName "' . $fileName . '"', 1320572272);
		}
		$targetIdentifier = $this->normalizeIdentifier($parentFolder->getIdentifier() . $fileName);

		$this->createObject($targetIdentifier);

		$fileInfo = $this->getFileInfoByIdentifier($targetIdentifier);
		return $this->getFileObject($fileInfo);
	}

	/**
	 * Returns the contents of a file. Beware that this requires to load the
	 * complete file into memory and also may require fetching the file from an
	 * external location. So this might be an expensive operation (both in terms
	 * of processing resources and money) for large files.
	 *
	 * @param FileInterface $file
	 * @return string The file contents
	 */
	public function getFileContents(FileInterface $file) {
		if ($object = $this->getObject($file->getIdentifier())) {
			$object->refresh();
			return $object->getContent();
		}
		return '';
	}

	/**
	 * Sets the contents of a file to the specified value.
	 *
	 * @param FileInterface $file
	 * @param string $contents
	 * @return integer The number of bytes written to the file
	 * @throws \RuntimeException if the operation failed
	 */
	public function setFileContents(FileInterface $file, $contents) {
		if ($object = $this->getObject($file->getIdentifier())) {
			$object->setContent($contents);
			$object->setEtag(NULL);
			$object->update();
			$this->cleanupCacheByIdentifier($file->getIdentifier());
			return $object->getContentLength();
		}
		throw new \RuntimeException();
	}

	/**
	 * Adds a file from the local server hard disk to a given path in TYPO3s virtual file system.
	 *
	 * This assumes that the local file exists, so no further check is done here!
	 *
	 * @param string $localFilePath
	 * @param Folder $targetFolder
	 * @param string $fileName The name to add the file under
	 * @param \TYPO3\CMS\Core\Resource\AbstractFile $updateFileObject Optional file object to update (instead of creating a new object). With this parameter, this function can be used to "populate" a dummy file object with a real file underneath.
	 * @return FileInterface
	 */
	public function addFile($localFilePath, Folder $targetFolder, $fileName, \TYPO3\CMS\Core\Resource\AbstractFile $updateFileObject = NULL) {
		$targetIdentifier = $this->addFileRaw($localFilePath, $targetFolder, $fileName);

		$fileInfo = $this->getFileInfoByIdentifier($targetIdentifier);
		if ($updateFileObject) {
			$updateFileObject->updateProperties($fileInfo);
			return $updateFileObject;
		}
		return $this->getFileObject($fileInfo);
	}

	/**
	 * Checks if a resource exists - does not care for the type (file or folder).
	 *
	 * @param string $identifier Identifier
	 * @return boolean
	 */
	public function resourceExists($identifier) {
		return $this->objectExists($identifier);
	}

	/**
	 * Checks if a file exists.
	 *
	 * @param string $identifier
	 * @return boolean
	 */
	public function fileExists($identifier) {
		if ($identifier === self::ROOT_FOLDER_IDENTIFIER) {
			return FALSE;
		}
		if ($partialObject = $this->getPartialObject($identifier)) {
			return !self::objectIsDirectory($partialObject);
		}
		return FALSE;
	}

	/**
	 * Checks if a file inside a storage folder exists.
	 *
	 * @param string $fileName
	 * @param Folder $folder
	 * @return boolean
	 */
	public function fileExistsInFolder($fileName, Folder $folder) {
		if ($partialObject = $this->getPartialObject($folder->getIdentifier() . $fileName)) {
			return !self::objectIsDirectory($partialObject);
		}
		return FALSE;
	}

	/**
	 * Returns a (local copy of) a file for processing it. When changing the
	 * file, you have to take care of replacing the current version yourself!
	 *
	 * @param FileInterface $file
	 * @param bool $writable Set this to FALSE if you only need the file for read operations. This might speed up things, e.g. by using a cached local version. Never modify the file if you have set this flag!
	 * @return string The path to the file on the local disk
	 */
	public function getFileForLocalProcessing(FileInterface $file, $writable = TRUE) {
		return $this->copyFileToTemporaryPath($file);
	}

	/**
	 * Returns the permissions of a file as an array (keys r, w) of boolean flags
	 *
	 * @param FileInterface $file
	 * @return array
	 */
	public function getFilePermissions(FileInterface $file) {
		return $this->getObjectPermissions($file->getIdentifier());
	}

	/**
	 * Returns the permissions of a folder as an array (keys r, w) of boolean flags
	 *
	 * @param Folder $folder
	 * @return array
	 */
	public function getFolderPermissions(Folder $folder) {
		return $this->getObjectPermissions($folder->getIdentifier());
	}

	/**
	 * Renames a file
	 *
	 * @param FileInterface $file
	 * @param string $newName
	 * @return string The new identifier of the file if the operation succeeds
	 * @throws \RuntimeException if renaming the file failed
	 */
	public function renameFile(FileInterface $file, $newName) {
		$newName = $this->sanitizeFileName($newName);
		$targetIdentifier = $this->normalizeIdentifier($this->getFolderIdentifierForFile($file->getIdentifier()) . $newName);
		$this->renameObject($file->getIdentifier(), $targetIdentifier);

		return $targetIdentifier;
	}

	/**
	 * Replaces the contents (and file-specific metadata) of a file object with a local file.
	 *
	 * @param \TYPO3\CMS\Core\Resource\AbstractFile $file
	 * @param string $localFilePath
	 * @return boolean
	 */
	public function replaceFile(\TYPO3\CMS\Core\Resource\AbstractFile $file, $localFilePath) {
		if ($object = $this->getObject($file->getIdentifier())) {
			$object->setContent(file_get_contents($localFilePath));
			$object->update();

			unlink($localFilePath);
			return TRUE;
		}
	}

	/**
	 * Returns information about a file for a given file identifier.
	 *
	 * @param string $identifier The (relative) path to the file.
	 * @param array $propertiesToExtract Array of properties which should be extracted, if empty all will be extracted
	 * @return array
	 */
	public function getFileInfoByIdentifier($identifier, array $propertiesToExtract = array()) {
		$object = $this->getPartialObject($identifier);
		return array(
			'name' => basename($object->getName()),
			'identifier' => $object->getName(),
			'mtime' => strtotime($object->getLastModified()),
			'size' => (integer) $object->getContentLength(),
			'mimetype' => $object->getContentType(),

			'identifier_hash' => $this->hashIdentifier($object->getName()),
			'folder_hash' => $this->hashIdentifier($this->getFolderIdentifierForFile($identifier)),

			'storage' => $this->storage->getUid()
		);
	}

	/**
	 * Returns a folder within the given folder. Use this method instead of doing your own string manipulation magic
	 * on the identifiers because non-hierarchical storages might fail otherwise.
	 *
	 * @param $name
	 * @param Folder $parentFolder
	 * @return Folder
	 */
	public function getFolderInFolder($name, Folder $parentFolder) {
		$folderIdentifier = $parentFolder->getIdentifier() . $name . '/';
		return $this->getFolder($folderIdentifier);
	}

	/**
	 * Returns a list of files inside the specified path
	 *
	 * @param string $folderIdentifier
	 * @param boolean $recursive
	 * @param array $filenameFilterCallbacks The method callbacks to use for filtering the items
	 * @return array of FileIdentifiers
	 */
	public function getFileIdentifierListInFolder($folderIdentifier, $recursive = FALSE, array $filenameFilterCallbacks = array()) {
		return array_values($this->getDirectoryItemList($folderIdentifier, 0, 0, $filenameFilterCallbacks, 'getFileList_itemCallbackIdentifierOnly', array(), $recursive));
	}

	/**
	 * Copies a file to a temporary path and returns that path.
	 *
	 * @param FileInterface $file
	 * @return string The temporary path
	 */
	public function copyFileToTemporaryPath(FileInterface $file) {
		if ($object = $this->getObject($file->getIdentifier())) {
			$temporaryPath = $this->getTemporaryPathForFile($file);
			$localResource = fopen($temporaryPath, 'wb+');
			$object->refresh();
			$object->getContent()->rewind();
			self::pipeStreams($object->getContent()->getStream(), $localResource);
			fclose($localResource);
		}
		return $temporaryPath;
	}

	/**
	 * Pipe Streams
	 *
	 * @param resource $input Input
	 * @param resource $output Output
	 * @return int Size written
	 */
	protected static function pipeStreams($input, $output) {
		$size = 0;
		while (!feof($input)) {
			$size += fwrite($output, fread($input, 8192));
		}
		return $size;
	}

	/**
	 * Moves a file *within* the current storage.
	 * Note that this is only about an intra-storage move action, where a file is just
	 * moved to another folder in the same storage.
	 *
	 * @param FileInterface $file
	 * @param Folder $targetFolder
	 * @param string $fileName
	 * @return string The new identifier of the file
	 */
	public function moveFileWithinStorage(FileInterface $file, Folder $targetFolder, $fileName) {
		$targetIdentifier = $this->normalizeIdentifier($targetFolder->getIdentifier() . basename($file->getIdentifier()));
		$this->renameObject($file->getIdentifier(), $targetIdentifier);
		return $targetIdentifier;
	}

	/**
	 * Copies a file *within* the current storage.
	 * Note that this is only about an intra-storage copy action, where a file is just
	 * copied to another folder in the same storage.
	 *
	 * @param FileInterface $file
	 * @param Folder $targetFolder
	 * @param string $fileName
	 * @return FileInterface The new (copied) file object.
	 */
	public function copyFileWithinStorage(FileInterface $file, Folder $targetFolder, $fileName) {
		$targetIdentifier = $this->normalizeIdentifier($targetFolder->getIdentifier() . basename($file->getIdentifier()));

		$this->copyObject($file->getIdentifier(), $targetIdentifier);

		$fileInfo = $this->getFileInfoByIdentifier($targetIdentifier);

		return $this->getFileObject($fileInfo);
	}

	/**
	 * Folder equivalent to moveFileWithinStorage().
	 *
	 * @param Folder $folderToMove
	 * @param Folder $targetFolder
	 * @param string $newFolderName
	 * @return array A map of old to new file identifiers
	 */
	public function moveFolderWithinStorage(Folder $folderToMove, Folder $targetFolder, $newFolderName) {
		$targetIdentifier = $this->normalizeIdentifier($targetFolder->getIdentifier() . $newFolderName . '/');
		return $this->moveOrCopyFolderByIdentifier($folderToMove->getIdentifier(), $targetIdentifier);
	}

	/**
	 * Folder equivalent to copyFileWithinStorage().
	 *
	 * @param Folder $folderToCopy
	 * @param Folder $targetFolder
	 * @param string $newFileName
	 * @return boolean
	 */
	public function copyFolderWithinStorage(Folder $folderToCopy, Folder $targetFolder, $newFileName) {
		$targetIdentifier = $this->normalizeIdentifier($targetFolder->getIdentifier() . $newFileName . '/');
		return count($this->moveOrCopyFolderByIdentifier($folderToCopy->getIdentifier(), $targetIdentifier, self::OPERATION_COPY)) > 0;
	}

	/**
	 * Copy a folder from another storage.
	 *
	 * @param Folder $folderToCopy Folder to copy
	 * @param Folder $targetParentFolder Target parent folder
	 * @param string $newFolderName New folder name
	 * @return boolean
	 */
	public function copyFolderBetweenStorages(Folder $folderToCopy, Folder $targetParentFolder, $newFolderName) {
		$targetFolder = $this->createFolder($newFolderName, $targetParentFolder);
		foreach ($folderToCopy->getSubfolders() as $subFolder) {
			$this->copyFolderBetweenStorages($subFolder, $targetFolder, $subFolder->getName());
		}
		foreach ($folderToCopy->getFiles() as $file) {
			$file->copyTo($targetFolder);
		}
		return TRUE;
	}

	/**
	 * Removes a file from this storage. This does not check if the file is
	 * still used or if it is a bad idea to delete it for some other reason
	 * this has to be taken care of in the upper layers (e.g. the Storage)!
	 *
	 * @param FileInterface $file
	 * @return boolean TRUE if deleting the file succeeded
	 */
	public function deleteFile(FileInterface $file) {
		return $this->deleteFileRaw($file->getIdentifier());
	}

	/**
	 * Removes a folder from this storage.
	 *
	 * @param Folder $folder
	 * @param boolean $deleteRecursively
	 * @return boolean
	 */
	public function deleteFolder(Folder $folder, $deleteRecursively = FALSE) {
		if ($deleteRecursively) {
			return $this->deleteObjects($folder->getIdentifier());
		}
		return $this->deleteObject($folder->getIdentifier());
	}

	/**
	 * Adds a file at the specified location. This should only be used internally.
	 * Use streams for transferring contents
	 *
	 * @param string $localFilePath
	 * @param Folder $targetFolder
	 * @param string $targetFileName
	 * @return string The new identifier of the file
	 */
	public function addFileRaw($localFilePath, Folder $targetFolder, $targetFileName) {
		$targetIdentifier = $this->normalizeIdentifier($targetFolder->getIdentifier() . $targetFileName);
		$source = fopen($localFilePath, 'r');
		if ($object = $this->getObject($targetIdentifier)) {
			$object->setContent($source);
			$object->update();
			$this->cleanupCacheByIdentifier($targetIdentifier);
		} else {
			$this->createObject($targetIdentifier, $source);
		}
		fclose($source);
		return $targetIdentifier;
	}

	/**
	 * Deletes a file without access and usage checks.
	 * This should only be used internally.
	 *
	 * This accepts an identifier instead of an object because we might want to
	 * delete files that have no object associated with (or we don't want to
	 * create an object for) them - e.g. when moving a file to another storage.
	 *
	 * @param string $identifier
	 * @return boolean TRUE if removing the file succeeded
	 */
	public function deleteFileRaw($identifier) {
		if ($object = $this->getPartialObject($identifier)) {
			if (!self::objectIsDirectory($object)) {
				return $this->deleteObject($identifier);
			}
		}
		return FALSE;
	}

	/**
	 * Returns the root level folder of the storage.
	 *
	 * @return Folder
	 */
	public function getRootLevelFolder() {
		return $this->getFolder(self::ROOT_FOLDER_IDENTIFIER);
	}

	/**
	 * Returns the default folder new files should be put into.
	 *
	 * @return Folder
	 */
	public function getDefaultFolder() {
		return $this->getRootLevelFolder();
	}

	/**
	 * Creates a folder.
	 *
	 * @param string $newFolderName
	 * @param Folder $parentFolder
	 * @return Folder The new (created) folder object
	 */
	public function createFolder($newFolderName, Folder $parentFolder) {
		$newFolderName = trim($newFolderName, '/');
		$targetIdentifier = $this->normalizeIdentifier($parentFolder->getIdentifier() . $newFolderName . '/');

		$this->createObject(
			$targetIdentifier,
			'',
			array(
				'Content-Type' => self::CONTENT_TYPE_DIRECTORY
			)
		);

		return \TYPO3\CMS\Core\Resource\ResourceFactory::getInstance()->createFolderObject($this->storage, $targetIdentifier, $newFolderName);
	}

	/**
	 * Checks if a folder exists
	 *
	 * @param string $identifier
	 * @return boolean
	 */
	public function folderExists($identifier) {
		if ($identifier === self::ROOT_FOLDER_IDENTIFIER) {
			return TRUE;
		}
		if (substr($identifier, -1) !== '/') {
			return FALSE;
		}
		if ($partialObject = $this->getPartialObject($identifier)) {
			return self::objectIsDirectory($partialObject);
		}
		return FALSE;
	}

	/**
	 * Checks if a file inside a storage folder exists.
	 *
	 * @param string $folderName
	 * @param Folder $folder
	 * @return boolean
	 */
	public function folderExistsInFolder($folderName, Folder $folder) {
		return $this->objectExists($folder->getIdentifier() . $folderName . '/');
	}

	/**
	 * Renames a folder in this storage.
	 *
	 * @param Folder $folder
	 * @param string $newName The target path (including the file name!)
	 * @return array A map of old to new file identifiers
	 * @throws \RuntimeException if renaming the folder failed
	 */
	public function renameFolder(Folder $folder, $newName) {
		$targetIdentifier = $this->normalizeIdentifier($this->getFolderIdentifierForFile($folder->getIdentifier()) . $newName . '/');
		return $this->moveOrCopyFolderByIdentifier($folder->getIdentifier(), $targetIdentifier);
	}

	/**
	 * Checks if a given object or identifier is within a container, e.g. if
	 * a file or folder is within another folder.
	 * This can e.g. be used to check for webmounts.
	 *
	 * @param Folder $container
	 * @param mixed $content An object or an identifier to check
	 * @return boolean TRUE if $content is within $container
	 */
	public function isWithin(Folder $container, $content) {
		if ($container->getStorage() != $this->storage) {
			return FALSE;
		}
		if ($content instanceof FileInterface || $content instanceof Folder) {
			$content = $container->getIdentifier();
		}
		return $this->objectExists($container->getIdentifier() . $content);
	}

	/**
	 * Checks if a folder contains files and (if supported) other folders.
	 *
	 * @param Folder $folder
	 * @return boolean TRUE if there are no files and folders within $folder
	 */
	public function isFolderEmpty(Folder $folder) {
		$subObjects = $this->getSubObjects($folder->getIdentifier());
		return count($subObjects) === 0;
	}

	/**
	 * Makes sure the path given as parameter is valid
	 *
	 * @param string $filePath The file path (most times filePath)
	 * @return string
	 */
	protected function canonicalizeAndCheckFilePath($filePath) {
		return $filePath;
	}

	/**
	 * Makes sure the identifier given as parameter is valid
	 *
	 * @param string $fileIdentifier The file Identifier
	 * @return string
	 * @throws \TYPO3\CMS\Core\Resource\Exception\InvalidPathException
	 */
	protected function canonicalizeAndCheckFileIdentifier($fileIdentifier) {
		return $this->normalizeIdentifier($fileIdentifier);
	}

	/**
	 * Makes sure the identifier given as parameter is valid
	 *
	 * @param string $folderIdentifier The folder identifier
	 * @return string
	 */
	protected function canonicalizeAndCheckFolderIdentifier($folderIdentifier) {
		// TODO: Implement canonicalizeAndCheckFolderIdentifier() method.
		return $this->normalizeIdentifier($folderIdentifier);
	}

	/**
	 * Generic handler method for directory listings - gluing together the
	 * listing items is done
	 *
	 * @param string $basePath
	 * @param integer $start
	 * @param integer $numberOfItems
	 * @param array $filterMethods The filter methods used to filter the directory items
	 * @param string $itemHandlerMethod
	 * @param array $itemRows
	 * @param boolean $recursive
	 * @return array
	 */
	protected function getDirectoryItemList($basePath, $start, $numberOfItems, array $filterMethods, $itemHandlerMethod, $itemRows = array(), $recursive = FALSE) {
		$items = array();

		$objects = $this->getSubObjects($basePath);
		foreach ($objects as $object) {
			/** @var $object \OpenCloud\ObjectStore\Resource\DataObject */
			list($key, $item) = $this->{$itemHandlerMethod}($object);

			if ($this->applyFilterMethodsToDirectoryItem($filterMethods, $item['name'], $item['identifier'], $basePath, array('item' => $item)) === FALSE) {
				continue;
			}

			if (empty($item)) {
				continue;
			}

			$items[$key] = $item;
		}
		uksort(
			$items,
			array('\\TYPO3\\CMS\\Core\\Utility\\ResourceUtility', 'recursiveFileListSortingHelper')
		);

		return $items;
	}

	/**
	 * Callback method that extracts file information from a single entry inside a DAV PROPFIND response. Called by getDirectoryItemList.
	 *
	 * @param \OpenCloud\ObjectStore\Resource\DataObject $object Object
	 * @return array
	 */
	protected function getFileList_itemCallback($object) {
		if (substr($object->getName(), -1) == '/') {
			return array('', array());
		}
		$fileName = basename($object->getName());
		return array($fileName, array(
			'name' => $fileName,
			'identifier' => $object->getName(),
			'size' => (integer) $object->getContentLength(),
			'mtime' => strtotime($object->getLastModified()),
			'mimetype' => $object->getContentType(),
			'storage' => $this->storage->getUid()
		));
	}

	/**
	 * Handler for items in a file list.
	 *
	 * @param \OpenCloud\ObjectStore\Resource\DataObject $object Object
	 * @return array
	 */
	protected function getFileList_itemCallbackIdentifierOnly($object) {
		if (substr($object->getName(), -1) == '/') {
			return array('', array());
		}
		$fileName = basename($object->getName());
		return array($fileName, array(
			'name' => $fileName,
			'identifier' => $object->getName()
		));
	}

	/**
	 * Callback method that extracts folder information from a single entry inside a DAV PROPFIND response. Called by getDirectoryItemList.
	 *
	 * @param \OpenCloud\ObjectStore\Resource\DataObject $object Object
	 * @return array
	 */
	protected function getFolderList_itemCallback($object) {
		if (substr($object->getName(), -1) != '/') {
			return array('', array());
		}
		// TODO add more information
		$folderName = basename(rtrim($object->getName(), '/'));
		return array($folderName, array(
			'name' => $folderName,
			'identifier' => $object->getName(),
			'storage' => $this->storage->getUid()
		));
	}

	/**
	 * Returns the identifier of the folder the file resides in
	 *
	 * @param string $fileIdentifier Folder Identifier
	 *
	 * @return mixed
	 */
	public function getFolderIdentifierForFile($fileIdentifier) {
		$parentIdentifier = $this->getObjectParentIdentifier($fileIdentifier);
		return rtrim($parentIdentifier, '/') . '/';
	}

	/**
	 * Basic implementation of the method that does directly return the
	 * file name as is.
	 *
	 * @param string $fileName Input string, typically the body of a fileName
	 * @param string $charset Charset of the a fileName (defaults to current charset; depending on context)
	 * @return string Output string with any characters not matching [.a-zA-Z0-9_-] is substituted by '_' and trailing dots removed
	 * @throws \TYPO3\CMS\Core\Resource\Exception\InvalidFileNameException
	 */
	public function sanitizeFileName($fileName, $charset = '') {
		// Allow ".", "-", 0-9, a-z, A-Z and everything beyond U+C0 (latin capital letter a with grave)
		$fileName = preg_replace('/[\\x00-\\x2C\\/\\x3A-\\x3F\\x5B-\\x60\\x7B-\\xBF]/u', '_', trim($fileName));

		// Remove trailing dots
		$fileName = preg_replace('/\\.*$/', '', $fileName);
		if (!$fileName) {
			throw new \TYPO3\CMS\Core\Resource\Exception\InvalidFileNameException('File name ' . $fileName . ' is invalid.', 1320288991);
		}
		return $fileName;
	}

	/**
	 * Get Service
	 *
	 * @return \OpenCloud\ObjectStore\Service
	 */
	protected function getService() {
		if (NULL === $this->service) {
			$this->initializeObjectStoreService();
		}
		return $this->service;
	}

	/**
	 * Get Container
	 *
	 * @return \OpenCloud\ObjectStore\Resource\Container
	 */
	protected function getContainer() {
		if (NULL === $this->container) {
			$this->initializeContainer();
		}
		return $this->container;
	}

	/**
	 * Get Base URL
	 *
	 * @return string Base Url
	 */
	protected function getBaseUrl() {
		if (NULL === $this->baseUrl) {
			$this->initializeBaseUrl();
		}
		return $this->baseUrl;
	}

	/**
	 * Normalize Identifier
	 *
	 * @param string $identifier Identifier
	 * @return string
	 */
	protected function normalizeIdentifier($identifier) {
		$identifier = str_replace('//', '/', $identifier);
		if ($identifier !== self::ROOT_FOLDER_IDENTIFIER) {
			return ltrim($identifier, '/');
		}
		return $identifier;
	}

	/**
	 * Get Object Parent Identifier
	 *
	 * @param string $identifier Identifier
	 * @return string Parent Identifier
	 */
	public function getObjectParentIdentifier($identifier) {
		$parentIdentifier = dirname($identifier);
		if ($parentIdentifier === '.') {
			$parentIdentifier = '';
		}
		return $parentIdentifier;
	}

	/**
	 * Get Object Permissions
	 *
	 * @param string $identifier Identifier
	 * @return array Permissions
	 */
	protected function getObjectPermissions($identifier) {
		if ($identifier == self::ROOT_FOLDER_IDENTIFIER) {
			return array(
				'r' => TRUE,
				'w' => TRUE
			);
		}
		// @TODO: Rackspace does not support permissions
		return array(
			'r' => TRUE,
			'w' => TRUE
		);
	}

	/**
	 * Object Exists
	 *
	 * @param string $identifier Identified
	 * @return boolean
	 */
	protected function objectExists($identifier) {
		return ($this->getPartialObject($identifier) !== NULL);
	}

	/**
	 * Get Object
	 * Use the 'method' constant to differentiate between HEAD/GET method
	 *
	 * @param string $identifier Identifier
	 * @param integer $method Method (GET/HEAD)
	 * @return \OpenCloud\ObjectStore\Resource\DataObject Object
	 */
	protected function getObject($identifier, $method = self::OBJECT_METHOD_GET) {
		$identifier = $this->normalizeIdentifier($identifier);
		$cacheIdentifier = static::createCacheIdentifier($identifier, $method === self::OBJECT_METHOD_HEAD ? self::CACHE_PREFIX_PARTIAL : '');
		if ($cachedObject = $this->cache->get($cacheIdentifier)) {
			return $cachedObject;
		}
		try {
			switch ($method) {
				case self::OBJECT_METHOD_HEAD:
					$object = $this->getContainer()->getPartialObject($identifier);
					break;
				case self::OBJECT_METHOD_GET:
				default:
					$object = $this->getContainer()->getObject($identifier);
			}
			$this->cache->set($cacheIdentifier, $object);
			return $object;
		} catch (\Guzzle\Http\Exception\ClientErrorResponseException $e) {

		}
		return NULL;
	}

	/**
	 * Get Partial Object
	 * Use this method for fetching objects without full content
	 *
	 * @param string $identifier Identifier
	 * @return \OpenCloud\ObjectStore\Resource\DataObject
	 */
	protected function getPartialObject($identifier) {
		return $this->getObject($identifier, self::OBJECT_METHOD_HEAD);
	}

	/**
	 * Get Sub Objects
	 *
	 * @param string $path Path
	 * @param boolean $recursive Recursive
	 * @return \OpenCloud\Common\Collection\ResourceIterator Resource Iterator
	 */
	protected function getSubObjects($path = self::ROOT_FOLDER_IDENTIFIER, $recursive = FALSE) {
		$parameters = array();
		if ($recursive) {
			$parameters['prefix'] = $path == self::ROOT_FOLDER_IDENTIFIER ? '' : $path;
			$cacheIdentifier = $this->createCacheIdentifier($path, self::CACHE_PREFIX_LIST_RECURSIVE);
		} else {
			$parameters['path'] = $path == self::ROOT_FOLDER_IDENTIFIER ? '' : $path;
			//$parameters['prefix'] = $path == self::ROOT_FOLDER_IDENTIFIER ? '' : $path;
			//$parameters['delimiter'] = '/';
			$cacheIdentifier = $this->createCacheIdentifier($path, self::CACHE_PREFIX_LIST);
		}

		if (($cachedSubObjects = $this->cache->get($cacheIdentifier)) !== FALSE) {
			return $cachedSubObjects;
		}

		/** @var $objectList \OpenCloud\Common\Collection */
		$objectList = $this->getContainer()->objectList($parameters);
		$objects = array();
		foreach ($objectList as $object) {
			$this->cache->set(static::createCacheIdentifier($object->getName(), self::CACHE_PREFIX_PARTIAL), $object);
			$objects[] = $object;
		}
		$this->cache->set($cacheIdentifier, $objects);
		return $objects;
	}

	/**
	 * Create Object
	 *
	 * @param string $identifier Identifier
	 * @param string $data Data
	 * @param array $headers Headers
	 * @return \OpenCloud\ObjectStore\Resource\DataObject Object
	 */
	protected function createObject($identifier, $data = '', $headers = array()) {
		$this->cleanupListCacheByIdentifier($identifier);
		return $this->getContainer()->uploadObject($identifier, $data, $headers);
	}

	/**
	 * Delete Object
	 *
	 * @param string $identifier Identifier
	 * @return boolean True on success, false on failure
	 * @throws \TYPO3\CMS\Core\Resource\Exception\FileOperationErrorException
	 */
	protected function deleteObject($identifier) {
		try {
			$this->getPartialObject($identifier)->delete();
			$this->cleanupCacheByIdentifier($identifier);
			return TRUE;
		} catch (\Guzzle\Http\Exception\ClientErrorResponseException $e) {

		}
		return FALSE;
	}

	/**
	 * Delete Objects By Path
	 *
	 * @param string $path Path (prefix)
	 * @return boolean True on success, false on failure
	 */
	protected function deleteObjects($path) {
		$subObjects = $this->getSubObjects($path, TRUE);
		$paths = array();
		foreach ($subObjects as $subObject) {
			$paths[] = $subObject->getName();
			$this->cleanupCacheByIdentifier($subObject->getName());
		}
		return $this->bulkDeleteObjects($paths);
	}

	/**
	 * Bulk Delete Objects
	 *
	 * @param array $paths Paths
	 * @return boolean
	 */
	protected function bulkDeleteObjects($paths) {
		foreach ($paths as $key => $objectPath) {
			// Prefix container name
			if (strpos($objectPath, $this->getContainer()->name) !== 0) {
				$paths[$key] = $this->getContainer()->name . '/' . $objectPath;
			}
		}
		$response = $this->getService()->bulkDelete($paths);
		return $response->getStatusCode() === 200;
	}

	/**
	 * Copy Object
	 *
	 * @param string $identifier Identifier
	 * @param string $targetIdentifier Target Identifier
	 * @return \OpenCloud\ObjectStore\Resource\DataObject Copied Object
	 */
	protected function copyObject($identifier, $targetIdentifier) {
		try {
			$object = $this->getPartialObject($identifier);

			$object->copy($this->getContainer()->name . '/' . $targetIdentifier);

			$targetObject = $this->getPartialObject($targetIdentifier);
			$this->cache->set(static::createCacheIdentifier($targetIdentifier, self::CACHE_PREFIX_PARTIAL), $targetObject);
			$this->cleanupListCacheByIdentifier($targetIdentifier);
			return $targetObject;
		} catch (\Exception $e) {

		}
	}

	/**
	 * Rename Object
	 *
	 * @param string $identifier Identifier
	 * @param string $targetIdentifier Target Identifier
	 * @return bool|\OpenCloud\ObjectStore\Resource\DataObject Renamed Object
	 */
	protected function renameObject($identifier, $targetIdentifier) {
		try {
			$object = $this->getPartialObject($identifier);

			$targetObject = $this->copyObject($identifier, $targetIdentifier);

			$object->delete();
			//$object->purge();
			$this->cleanupCacheByIdentifier($identifier);

			return $targetObject;
		} catch(\Exception $e) {

		}
		return FALSE;
	}

	/**
	 * Rename folder by identifier
	 *
	 * @param string $identifier Identifier
	 * @param string $targetIdentifier Target identifier
	 * @param integer $operation Operation
	 * @return array Identity map
	 */
	protected function moveOrCopyFolderByIdentifier($identifier, $targetIdentifier, $operation = self::OPERATION_MOVE) {
		$identityMap = array();

		$subObjects = $this->getSubObjects($identifier, TRUE);
		foreach ($subObjects as $subObject) {
			if ($subObject->getName() !== $identifier) {
				$newIdentifier = $targetIdentifier . substr($subObject->getName(), strlen($identifier));
				$operation === self::OPERATION_MOVE ?
					$this->renameObject($subObject->getName(), $newIdentifier) : $this->copyObject($subObject->getName(), $newIdentifier);
				$identityMap[$subObject->getName()] = $newIdentifier;
			}
		}
		unset($subObjects);

		$operation == self::OPERATION_MOVE ?
			$this->renameObject($identifier, $targetIdentifier) : $this->copyObject($identifier, $targetIdentifier);
		$identityMap[$identifier] = $targetIdentifier;

		return $identityMap;
	}

	/**
	 * Object Is Directory
	 *
	 * @param \OpenCloud\ObjectStore\Resource\DataObject $object
	 * @return boolean
	 */
	protected static function objectIsDirectory(\OpenCloud\ObjectStore\Resource\DataObject $object) {
		return (substr($object->getName(), -1) == '/' || $object->isDirectory() || $object->getContentType() === self::CONTENT_TYPE_DIRECTORY);
	}

	/**
	 * Remove Object From Cache
	 *
	 * @param string $identifier Identifier
	 * @return void
	 */
	protected function cleanupCacheByIdentifier($identifier) {
		$this->cache->remove(static::createCacheIdentifier($identifier, self::CACHE_PREFIX_PARTIAL));
		$this->cache->remove(static::createCacheIdentifier($identifier));
		$this->cleanupListCacheByIdentifier($identifier);
	}

	/**
	 * Cleanup List Cache By Identifier
	 *
	 * @param string $identifier Identifier
	 * @return void
	 */
	protected function cleanupListCacheByIdentifier($identifier) {
		$this->cleanupListCacheByPath($this->getFolderIdentifierForFile($identifier));
	}

	/**
	 * Cleanup List Cache By Path
	 *
	 * @param string $path Path
	 * @return void
	 */
	protected function cleanupListCacheByPath($path) {
		$this->cache->remove(static::createCacheIdentifier($path, self::CACHE_PREFIX_LIST));
		$this->cache->remove(static::createCacheIdentifier($path, self::CACHE_PREFIX_LIST_RECURSIVE));
	}

	/**
	 * Create Cache Identifier
	 * - Add short md5 prefix based on identifier and prefix
	 * - Add stripped identifier
	 *
	 * @param string $identifier Identifier
	 * @param string $prefix
	 * @return string Cache Identifier
	 */
	protected static function createCacheIdentifier($identifier, $prefix = '') {
		$cacheIdentifier = GeneralUtility::shortMD5($identifier . $prefix);
		$cacheIdentifier .= preg_replace('/[^a-zA-Z0-9_%\\-&]/', '_', $identifier);
		return substr($cacheIdentifier, 0, 200);
	}
}