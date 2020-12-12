package org.carlspring.cloud.storage.s3fs;

import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;

import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.Owner;

public class S3AccessControlList
{

    private final String fileStoreName;

    private final String key;


    /**
     * Creates a new S3AccessControlList
     *
     * @param fileStoreName
     * @param key
     * @param grants unused
     * @param owner unused
     * @deprecated use {@link #S3AccessControlList(String, String)}
     */
    @Deprecated
    public S3AccessControlList(final String fileStoreName,
                               final String key,
                               final Iterable<Grant> grants, //unused, but keeping to preserve signature
                               final Owner owner //unused, but keeping to preserve signature
                               )
    {
        this.fileStoreName = fileStoreName;
        this.key = key;
    }

    /**
     * Creates a new S3AccessControlList
     *
     * @param fileStoreName
     * @param key
     */
    public S3AccessControlList(final String fileStoreName, final String key)
    {
        this.fileStoreName = fileStoreName;
        this.key = key;
    }

    public String getKey()
    {
        return key;
    }

    public void checkAccess(final AccessMode[] modes)
            throws AccessDeniedException
    {
        for (AccessMode accessMode : modes)
        {
            // Checking the ACL grants is not sufficient to determine access as bucket policy may override ACL.
            // Any permission problems will have to be handled at time of access.
            if (accessMode == AccessMode.EXECUTE)
            {
                throw new AccessDeniedException(fileName(), null, "file is not executable");
            }
        }
    }

    private String fileName()
    {
        return fileStoreName + S3Path.PATH_SEPARATOR + key;
    }
}
