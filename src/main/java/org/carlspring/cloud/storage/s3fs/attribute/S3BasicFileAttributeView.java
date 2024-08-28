package org.carlspring.cloud.storage.s3fs.attribute;

import org.carlspring.cloud.storage.s3fs.S3Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

public class S3BasicFileAttributeView
        implements BasicFileAttributeView
{

    private static final Logger log = LoggerFactory.getLogger(S3BasicFileAttributeView.class);
    private S3Path s3Path;


    public S3BasicFileAttributeView(S3Path s3Path)
    {
        this.s3Path = s3Path;
    }

    @Override
    public String name()
    {
        return "basic";
    }

    @Override
    public BasicFileAttributes readAttributes()
            throws IOException
    {
        return s3Path.getFileSystem().provider().readAttributes(s3Path, BasicFileAttributes.class);
    }

    @Override
    public void setTimes(FileTime lastModifiedTime,
                         FileTime lastAccessTime,
                         FileTime createTime)
    {
        // TODO: Implement
        log.debug(getClass() + "#setTimes() is not supported yet.");
    }

}
