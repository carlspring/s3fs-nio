package org.carlspring.cloud.storage.s3fs.junit.examples;

import org.carlspring.cloud.storage.s3fs.junit.annotations.MinioIntegrationTest;
import org.carlspring.cloud.storage.s3fs.util.BaseIntegrationTest;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MinioMethodAnnotationIT extends BaseIntegrationTest
{

    private static final Logger logger = LoggerFactory.getLogger(MinioMethodAnnotationIT.class);

    @Test
    @MinioIntegrationTest
    public void testShouldExecuteBecauseOfMethodAnnotation()
    {
        logger.debug(new Exception().getStackTrace()[0].getMethodName());
        assertTrue(true);
    }
}
