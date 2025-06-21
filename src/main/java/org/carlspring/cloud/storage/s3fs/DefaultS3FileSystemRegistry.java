package org.carlspring.cloud.storage.s3fs;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class DefaultS3FileSystemRegistry implements S3FileSystemRegistry
{

    private final ConcurrentMap<String, S3FileSystem> fileSystems = new ConcurrentHashMap<>();

    private DefaultS3FileSystemRegistry() {
    }

    private static class Holder {
        private static final DefaultS3FileSystemRegistry INSTANCE = new DefaultS3FileSystemRegistry();
    }

    public static DefaultS3FileSystemRegistry getInstance() {
        return Holder.INSTANCE;
    }

    public ConcurrentMap<String, S3FileSystem> getFileSystems() {
        return fileSystems;
    }

}
