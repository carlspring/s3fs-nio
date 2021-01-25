package org.carlspring.cloud.storage.s3fs.junit.examples;

import org.carlspring.cloud.storage.s3fs.junit.annotations.MinioIntegrationTest;
import org.carlspring.cloud.storage.s3fs.junit.annotations.S3IntegrationTest;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Class level {@link MinioIntegrationTest} annotation and method level {@link S3IntegrationTest} annotation.
 */
@MinioIntegrationTest
public class CombinedMinioS3IT
{

    private static final Logger logger = LoggerFactory.getLogger(S3MethodAnnotationIT.class);

    @Test
    @S3IntegrationTest
    public void testS3MethodShouldExecuteBecauseOfMethodLevelAnnotation()
    {
        logger.debug(new Exception().getStackTrace()[0].getMethodName());
        assertTrue(true);
    }

    @Test
    public void testMinioMethodShouldExecuteBecauseOfClassLevelAnnotation()
    {
        logger.debug(new Exception().getStackTrace()[0].getMethodName());
        assertTrue(true);
    }

}
