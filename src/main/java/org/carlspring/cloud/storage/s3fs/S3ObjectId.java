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
     * @param builder must not be null.
     */
    private S3ObjectId(final Builder builder)
    {
        this.bucket = builder.getBucket();
        this.key = builder.getKey();
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

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        S3ObjectId that = (S3ObjectId) o;

        if (getBucket() != null ? !getBucket().equals(that.getBucket()) : that.getBucket() != null) return false;
        return getKey() != null ? getKey().equals(that.getKey()) : that.getKey() == null;
    }

    @Override
    public int hashCode()
    {
        int result = getBucket() != null ? getBucket().hashCode() : 0;
        result = 31 * result + (getKey() != null ? getKey().hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "bucket: " + bucket + ", key: " + key;
    }

    public static final class Builder
    {

        private String bucket;
        private String key;

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
        }

        public String getBucket()
        {
            return bucket;
        }

        public String getKey()
        {
            return key;
        }

        public void setBucket(final String bucket)
        {
            this.bucket = bucket;
        }

        public void setKey(final String key)
        {
            this.key = key;
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

        public S3ObjectId build()
        {
            return new S3ObjectId(this);
        }

    }
}
