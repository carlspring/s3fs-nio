package org.carlspring.cloud.storage.s3fs.attribute;

import org.carlspring.cloud.storage.s3fs.S3Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.attribute.*;
import java.util.Set;


public class S3PosixFileAttributeView
        implements PosixFileAttributeView
{

    private static final Logger log = LoggerFactory.getLogger(S3PosixFileAttributeView.class);
    private S3Path s3Path;

    private PosixFileAttributes posixFileAttributes;


    public S3PosixFileAttributeView(S3Path s3Path)
    {
        this.s3Path = s3Path;
    }

    @Override
    public String name()
    {
        return "posix";
    }

    @Override
    public PosixFileAttributes readAttributes()
            throws IOException
    {
        return read();
    }

    @Override
    public UserPrincipal getOwner()
            throws IOException
    {
        return read().owner();
    }

    @Override
    public void setOwner(UserPrincipal owner)
    {
        // TODO: Implement
        log.debug(getClass() + "#setOwner() is not supported yet.");
    }

    @Override
    public void setPermissions(Set<PosixFilePermission> perms)
    {
        // TODO: Implement
        log.debug(getClass() + "#setPermissions() is not supported yet.");
    }

    @Override
    public void setGroup(GroupPrincipal group)
    {
        // TODO: Implement
        log.debug(getClass() + "#setGroup() is not supported yet.");
    }

    @Override
    public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime)
    {
        // TODO: Implement
        log.debug(getClass() + "#setTimes() is not supported yet.");
    }

    public PosixFileAttributes read()
            throws IOException
    {
        if (posixFileAttributes == null)
        {
            posixFileAttributes = s3Path.getFileSystem().provider().readAttributes(s3Path, PosixFileAttributes.class);
        }

        return posixFileAttributes;
    }

}
