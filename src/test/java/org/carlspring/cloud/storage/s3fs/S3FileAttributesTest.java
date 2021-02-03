package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.attribute.S3BasicFileAttributes;

import java.nio.file.attribute.FileTime;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class S3FileAttributesTest
{


    @Test
    public void toStringPrintsBasicInfo()
    {
        final String key = "a key";
        final FileTime fileTime = FileTime.from(100, TimeUnit.SECONDS);

        final int size = 10;

        final boolean isDirectory = true;
        final boolean isRegularFile = true;

        S3BasicFileAttributes fileAttributes = new S3BasicFileAttributes(key,
                                                                         fileTime,
                                                                         size,
                                                                         isDirectory,
                                                                         isRegularFile);

        String print = fileAttributes.toString();

        assertTrue(print.contains(isRegularFile + ""));
        assertTrue(print.contains(isDirectory + ""));
        assertTrue(print.contains(size + ""));
        assertTrue(print.contains(fileTime.toString()));
        assertTrue(print.contains(key));
    }

}
