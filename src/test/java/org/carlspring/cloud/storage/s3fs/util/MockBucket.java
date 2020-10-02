package org.carlspring.cloud.storage.s3fs.util;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;

public class MockBucket
{

    private final S3ClientMock s3ClientMock;

    private final Path mockedPath;

    public MockBucket(final S3ClientMock s3ClientMock,
                      final Path mockedPath)
    {
        this.s3ClientMock = s3ClientMock;
        this.mockedPath = mockedPath;
    }

    public MockBucket file(String... file)
            throws IOException
    {
        for (String string : file)
        {
            s3ClientMock.addFile(mockedPath, string, "sample-content".getBytes());
        }

        return this;
    }

    public MockBucket file(String file,
                           final byte[] content)
            throws IOException
    {
        s3ClientMock.addFile(mockedPath, file, content);

        return this;
    }

    public MockBucket file(String file,
                           final byte[] content,
                           final FileAttribute<?>... attrs)
            throws IOException
    {
        s3ClientMock.addFile(mockedPath, file, content, attrs);

        return this;
    }

    public MockBucket dir(String... dir)
            throws IOException
    {
        for (String string : dir)
        {
            s3ClientMock.addDirectory(mockedPath, string);
        }

        return this;
    }

    public Path resolve(final String file)
    {
        final String encodedFile = file.replaceAll("/", "%2F");
        return mockedPath.resolve(encodedFile);
    }


}
