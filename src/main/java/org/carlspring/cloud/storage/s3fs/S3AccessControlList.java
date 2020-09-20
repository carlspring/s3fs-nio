package org.carlspring.cloud.storage.s3fs;

import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.util.EnumSet;
import java.util.stream.StreamSupport;

import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.Owner;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.utils.StringUtils;
import static java.lang.String.format;

public class S3AccessControlList
{

    private final String fileStoreName;

    private final String key;

    private final Iterable<Grant> grants;

    private final Owner owner;

    public S3AccessControlList(final String fileStoreName,
                               final String key,
                               final Iterable<Grant> grants,
                               final Owner owner)
    {
        this.fileStoreName = fileStoreName;
        this.grants = grants;
        this.key = key;
        this.owner = owner;
    }

    public String getKey()
    {
        return key;
    }

    /**
     * have almost one of the permission set in the parameter permissions
     *
     * @param permissions almost one
     * @return
     */
    private boolean hasPermission(final EnumSet<Permission> permissions)
    {
        return StreamSupport.stream(grants.spliterator(), false).anyMatch(
                grant -> StringUtils.equals(grant.grantee().id(), owner.id()) &&
                         permissions.contains(grant.permission()));
    }

    public void checkAccess(final AccessMode[] modes)
            throws AccessDeniedException
    {
        for (AccessMode accessMode : modes)
        {
            switch (accessMode)
            {
                case EXECUTE:
                    throw new AccessDeniedException(fileName(), null, "file is not executable");
                case READ:
                    if (!hasPermission(EnumSet.of(Permission.FULL_CONTROL, Permission.READ)))
                    {
                        throw new AccessDeniedException(fileName(), null, "file is not readable");
                    }
                    break;
                case WRITE:
                    if (!hasPermission(EnumSet.of(Permission.FULL_CONTROL, Permission.WRITE)))
                    {
                        throw new AccessDeniedException(fileName(), null,
                                                        format("bucket '%s' is not writable", fileStoreName));
                    }
                    break;
            }
        }
    }

    private String fileName()
    {
        return fileStoreName + S3Path.PATH_SEPARATOR + key;
    }
}
