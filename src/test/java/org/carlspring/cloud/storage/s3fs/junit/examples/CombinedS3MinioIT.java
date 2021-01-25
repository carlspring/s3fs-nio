package org.carlspring.cloud.storage.s3fs.junit.examples;

import org.carlspring.cloud.storage.s3fs.junit.annotations.MinioIntegrationTest;
import org.carlspring.cloud.storage.s3fs.junit.annotations.S3IntegrationTest;
import org.carlspring.cloud.storage.s3fs.util.BaseIntegrationTest;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Class level {@link S3IntegrationTest} annotation and method level {@link MinioIntegrationTest} annotation.
 */
@S3IntegrationTest
public class CombinedS3MinioIT extends BaseIntegrationTest
{

    private static final Logger logger = LoggerFactory.getLogger(S3MethodAnnotationIT.class);

    @Test
    public void testS3MethodShouldExecuteBecauseOfClassLevelAnnotation()
    {
        logger.debug(new Exception().getStackTrace()[0].getMethodName());
        assertTrue(true);
    }

    @Test
    @MinioIntegrationTest
    public void testMinioMethodShouldExecuteBecauseOfMethodLevelAnnotation()
    {
        logger.debug(new Exception().getStackTrace()[0].getMethodName());
        assertTrue(true);
    }

}
