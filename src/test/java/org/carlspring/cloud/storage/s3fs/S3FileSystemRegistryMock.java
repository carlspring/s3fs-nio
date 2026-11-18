package org.carlspring.cloud.storage.s3fs;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class S3FileSystemRegistryMock implements S3FileSystemRegistry
{

    private final ConcurrentMap<String, S3FileSystem> fileSystems = new ConcurrentHashMap<>();

    private S3FileSystemRegistryMock()
    {
    }

    private static class Holder
    {
        private static final S3FileSystemRegistryMock INSTANCE = new S3FileSystemRegistryMock();
    }

    public static S3FileSystemRegistryMock getInstance()
    {
        return Holder.INSTANCE;
    }

    public ConcurrentMap<String, S3FileSystem> getFileSystems()
    {
        return fileSystems;
    }

}
