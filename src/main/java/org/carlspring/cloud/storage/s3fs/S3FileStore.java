package org.carlspring.cloud.storage.s3fs;

import java.nio.file.FileStore;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.Date;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.Owner;

public class S3FileStore
        extends FileStore
        implements Comparable<S3FileStore>
{

    private final S3FileSystem fileSystem;

    private final String name;

    public S3FileStore(final S3FileSystem s3FileSystem,
                       final String name)
    {
        this.fileSystem = s3FileSystem;
        this.name = name;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public String type()
    {
        return "S3Bucket";
    }

    @Override
    public boolean isReadOnly()
    {
        return false;
    }

    @Override
    public long getTotalSpace()
    {
        return Long.MAX_VALUE;
    }

    @Override
    public long getUsableSpace()
    {
        return Long.MAX_VALUE;
    }

    @Override
    public long getUnallocatedSpace()
    {
        return Long.MAX_VALUE;
    }

    @Override
    public boolean supportsFileAttributeView(final Class<? extends FileAttributeView> type)
    {
        return false;
    }

    @Override
    public boolean supportsFileAttributeView(final String attributeViewName)
    {
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type)
    {
        if (type != S3FileStoreAttributeView.class)
        {
            throw new IllegalArgumentException(
                    "FileStoreAttributeView of type '" + type.getName() + "' is not supported.");
        }

        final Bucket buck = getBucket(name);
        if (buck != null)
        {
            final Owner owner = getClient().getBucketAcl(GetBucketAclRequest.builder().bucket(name).build()).owner();

            return (V) new S3FileStoreAttributeView(Date.from(buck.creationDate()), buck.name(), owner.id(),
                                                    owner.displayName());
        }
        else
        {
            throw NoSuchBucketException.builder().message("Bucket not found: " + name).build();
        }
    }

    @Override
    public Object getAttribute(final String attribute)
    {
        return getFileStoreAttributeView(S3FileStoreAttributeView.class).getAttribute(attribute);
    }

    public S3FileSystem getFileSystem()
    {
        return fileSystem;
    }

    public Bucket getBucket()
    {
        return getBucket(name);
    }

    /**
     * This code requires `s3:ListAllMyBuckets` permission.
     *
     * @param bucketName
     * @return
     */
    private Bucket getBucket(final String bucketName)
    {
        for (Bucket buck : getClient().listBuckets().buckets())
        {
            if (buck.name().equals(bucketName))
            {
                return buck;
            }
        }
        return null;
    }

    private boolean hasBucket(final String bucketName)
    {
        // Originally getBucket was being used to determine presence of a bucket
        //
        // This is incorrect for two reasons:
        // 1. The list bucket operation provides buckets for which you are the owner
        //    It would not, therefore, allow you to work with buckets for which you
        //    have access but are not the owner.
        // 2. The way this information is being used later is to determine the
        //    bucket owner, which by definition, is now "you".
        // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTServiceGET.html
        //
        // However, note that the revised code below now has a different permissions
        // model as HeadBucket is now required
        boolean bucket = false;
        try
        {
            getClient().headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            bucket = true;
        }
        catch (NoSuchBucketException ignored)
        {
            //Do nothing
        }
        return bucket;
    }

    public S3Path getRootDirectory()
    {
        return new S3Path(fileSystem, "/" + this.name());
    }

    private S3Client getClient()
    {
        return fileSystem.getClient();
    }

    public Owner getOwner()
    {
        if (hasBucket(name))
        {
            return getClient().getBucketAcl(GetBucketAclRequest.builder().bucket(name).build()).owner();
        }
        // SDK v1 getS3AccountOwner uses the list buckets call, then extracts
        // the owner field (see: https://github.com/aws/aws-sdk-java/blob/4734de6fb0f80fe5768a6587aad3b9d0eaec388f/aws-java-sdk-s3/src/main/java/com/amazonaws/services/s3/model/transform/Unmarshallers.java#L48
        // and https://github.com/aws/aws-sdk-java/blob/2d15a603a96f98076f5458db49d659f296eab313/aws-java-sdk-s3/src/main/java/com/amazonaws/services/s3/AmazonS3Client.java#L926
        //
        // SDK v2 does not have that, as the SDK is mostly auto-generated based on the model files from the service, so much less custom code and helpers
        // More transparency, but we have to unwind this manually. So, here we go...
        return getClient().listBuckets(ListBucketsRequest.builder().build()).owner();
    }

    @Override
    public int compareTo(S3FileStore o)
    {
        if (this == o)
        {
            return 0;
        }
        return o.name().compareTo(name);
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fileSystem == null) ? 0 : fileSystem.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }


    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (!(obj instanceof S3FileStore))
        {
            return false;
        }
        S3FileStore other = (S3FileStore) obj;

        if (fileSystem == null)
        {
            if (other.fileSystem != null)
            {
                return false;
            }
        }
        else if (!fileSystem.equals(other.fileSystem))
        {
            return false;
        }
        if (name == null)
        {
            return other.name == null;
        }
        else
        {
            return name.equals(other.name);
        }
    }
}
