package org.carlspring.cloud.storage.s3fs.junit.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.Tag;

/**
 * This annotation should be used for integration tests which are specific to MinIO.
 * It is possible to combine it with {@link S3IntegrationTest} for test cases which are compatible with both (i.e. can
 * run on S3 and MinIO)
 */
@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Tag("it-minio")
public @interface MinioIntegrationTest
{

}
