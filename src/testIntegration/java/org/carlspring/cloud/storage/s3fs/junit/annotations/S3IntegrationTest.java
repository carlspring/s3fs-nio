package org.carlspring.cloud.storage.s3fs.junit.annotations;

import org.junit.jupiter.api.Tag;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation should be used for integration tests which are specific to the official S3 API.
 * It is possible to combine it with {@link MinioIntegrationTest} for test cases which are compatible with both (i.e. can
 * run on S3 and MinIO)
 */
@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Tag("it-s3")
public @interface S3IntegrationTest
{

}
