package org.carlspring.cloud.storage.s3fs;

import java.util.concurrent.ConcurrentMap;

/**
 * This is an internal interface to allow for easier and more reliable testing.
 */
interface S3FileSystemRegistry
{
    ConcurrentMap<String, S3FileSystem> getFileSystems();
}
