package org.carlspring.cloud.storage.s3fs;

import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;

public class S3AccessControlList
{

    private final String fileStoreName;

    private final String key;

    public S3AccessControlList(final String fileStoreName,
                               final String key)
    {
        this.fileStoreName = fileStoreName;
        this.key = key;
    }

    public void checkAccess(final AccessMode[] modes)
            throws AccessDeniedException
    {
        for (AccessMode accessMode : modes)
        {
            // Checking the ACL grants is not sufficient to determine access as bucket policy may override ACL.
            // Any permission problems will have to be handled at time of access.
            switch (accessMode)
            {
                case EXECUTE:
                    throw new AccessDeniedException(fileName(), null, "file is not executable");
            }
        }
    }

    private String fileName()
    {
        return fileStoreName + S3Path.PATH_SEPARATOR + key;
    }
}
