package org.carlspring.cloud.storage.s3fs;

import org.carlspring.cloud.storage.s3fs.attribute.S3BasicFileAttributes;
import org.carlspring.cloud.storage.s3fs.util.AttributesUtils;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import static org.junit.Assert.assertEquals;


public class AttributesUtilsTest {
    /**
     * http://stackoverflow.com/questions/9700179/junit-testing-helper-class-with-only-static-methods
     */
    @Test
    public void just_to_silence_coverage() {
        new AttributesUtils() {
            // ignore this
        };
    }

    @Test
    public void filterAll() {
        final String key = "key";
        final FileTime fileTime = FileTime.from(10L, TimeUnit.DAYS);
        final long size = 10L;
        final boolean isDirectory = true;
        final String[] filters = new String[]{"isDirectory", "isRegularFile", "isOther", "creationTime", "fileKey", "isSymbolicLink", "lastAccessTime", "lastModifiedTime", "size"};
        BasicFileAttributes attrs = new S3BasicFileAttributes(key, fileTime, size, isDirectory, !isDirectory);

        Map<String, Object> map = AttributesUtils.fileAttributeToMap(attrs, filters);

        assertEquals(filters.length, map.size());
        assertEquals(key, map.get("fileKey"));
        assertEquals(fileTime, map.get("creationTime"));
        assertEquals(isDirectory, map.get("isDirectory"));
        assertEquals(false, map.get("isRegularFile"));
        assertEquals(false, map.get("isOther"));
        assertEquals(false, map.get("isSymbolicLink"));
        assertEquals(fileTime, map.get("lastAccessTime"));
        assertEquals(fileTime, map.get("lastModifiedTime"));
        assertEquals(size, map.get("size"));
    }
}
