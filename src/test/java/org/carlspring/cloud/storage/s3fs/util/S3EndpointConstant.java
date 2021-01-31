package org.carlspring.cloud.storage.s3fs.util;

import java.net.URI;

public class S3EndpointConstant
{

    public static final URI S3_GLOBAL_URI_TEST = URI.create("s3://s3.test.amazonaws.com/");

    public static final String S3_REGION_URI_TEST = "s3://s3.test.%s.amazonaws.com/";

    public static final URI S3_GLOBAL_URI_IT = URI.create("s3://s3.amazonaws.com/");

    public static final URI MINIO_GLOBAL_URI_IT = URI.create("s3://localhost:9000");

    public static final String S3_REGION_URI_IT = "s3://s3.%s.amazonaws.com/";

}
