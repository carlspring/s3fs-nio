package org.carlspring.cloud.storage.s3fs;

import java.io.Serializable;

/**
 * An Immutable S3 object identifier. Used to uniquely identify an S3 object.
 * Can be instantiated via the convenient builder {@link Builder}.
 */
public class S3ObjectId
        implements Serializable
{

    private final String bucket;
    private final String key;
    /**
     * Optional and applicable only for get operation.
     */
    private final String versionId;

    /**
     * @param builder must not be null.
     */
    private S3ObjectId(final Builder builder)
    {
        this.bucket = builder.getBucket();
        this.key = builder.getKey();
        this.versionId = builder.getVersionId();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public Builder cloneBuilder()
    {
        return new Builder(this);
    }

    public String getBucket()
    {
        return bucket;
    }

    public String getKey()
    {
        return key;
    }

    /**
     * Returns the version id which is optionally applicable for S3 get (but not
     * put) operations.
     */
    public String getVersionId()
    {
        return versionId;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final S3ObjectId that = (S3ObjectId) o;

        if (bucket != null ? !bucket.equals(that.bucket) : that.bucket != null)
        {
            return false;
        }
        if (key != null ? !key.equals(that.key) : that.key != null)
        {
            return false;
        }
        return versionId != null ? versionId.equals(that.versionId) : that.versionId == null;
    }

    @Override
    public int hashCode()
    {
        int result = bucket != null ? bucket.hashCode() : 0;
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (versionId != null ? versionId.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "bucket: " + bucket + ", key: " + key + ", versionId: " + versionId;
    }

    static final class Builder
    {

        private String bucket;
        private String key;
        private String versionId;

        public Builder()
        {
            super();
        }

        /**
         * @param src S3 object id, which must not be null.
         */
        public Builder(final S3ObjectId src)
        {
            super();
            this.bucket(src.getBucket());
            this.key(src.getKey());
            this.versionId(src.getVersionId());
        }

        public String getBucket()
        {
            return bucket;
        }

        public String getKey()
        {
            return key;
        }

        public String getVersionId()
        {
            return versionId;
        }

        public void setBucket(final String bucket)
        {
            this.bucket = bucket;
        }

        public void setKey(final String key)
        {
            this.key = key;
        }

        public void setVersionId(final String versionId)
        {
            this.versionId = versionId;
        }

        public Builder bucket(final String bucket)
        {
            this.bucket = bucket;
            return this;
        }

        public Builder key(final String key)
        {
            this.key = key;
            return this;
        }

        public Builder versionId(final String versionId)
        {
            this.versionId = versionId;
            return this;
        }

        public S3ObjectId build()
        {
            return new S3ObjectId(this);
        }

    }
}