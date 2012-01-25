package org.weakref.s3fs;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

import static java.lang.String.format;

public class S3FileAttributes
    implements BasicFileAttributes
{
    private final FileTime lastModifiedTime;
    private final long size;
    private final boolean directory;
    private final boolean regularFile;
    private final String key;

    public S3FileAttributes(String key, FileTime lastModifiedTime, long size, boolean isDirectory, boolean isRegularFile)
    {
        this.key = key;
        this.lastModifiedTime = lastModifiedTime;
        this.size = size;
        directory = isDirectory;
        regularFile = isRegularFile;
    }

    @Override
    public FileTime lastModifiedTime()
    {
        return lastModifiedTime;
    }

    @Override
    public FileTime lastAccessTime()
    {
        return lastModifiedTime;
    }

    @Override
    public FileTime creationTime()
    {
        return lastModifiedTime;
    }

    @Override
    public boolean isRegularFile()
    {
        return regularFile;
    }

    @Override
    public boolean isDirectory()
    {
        return directory;
    }

    @Override
    public boolean isSymbolicLink()
    {
        return false;
    }

    @Override
    public boolean isOther()
    {
        return false;
    }

    @Override
    public long size()
    {
        return size;
    }

    @Override
    public Object fileKey()
    {
        return key;
    }

    @Override
    public String toString()
    {
        return format("[%s: lastModified=%s, size=%s, isDirectory=%s, isRegularFile=%s]", key, lastModifiedTime, size, directory, regularFile);
    }
}
