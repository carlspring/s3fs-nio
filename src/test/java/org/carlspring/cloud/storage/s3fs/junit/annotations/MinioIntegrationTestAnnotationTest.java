package org.carlspring.cloud.storage.s3fs.junit.annotations;


import org.carlspring.cloud.storage.s3fs.junit.examples.*;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MinioIntegrationTestAnnotationTest
        extends BaseAnnotationTest
{

    @Test
    public void testClassLevelAnnotation()
    {
        assertTrue(hasMinioAnnotation(MinioClassAnnotationIT.class));
        assertTrue(hasMinioAnnotation(CombinedMinioS3IT.class));

        assertFalse(hasMinioAnnotation(S3ClassAnnotationIT.class));
        assertFalse(hasMinioAnnotation(CombinedS3MinioIT.class));

        assertTrue(hasMinioAnnotation(CombinedIT.class));
    }

    @Test
    public void testMethodLevelAnnotation()
            throws NoSuchMethodException
    {
        assertTrue(hasMinioAnnotation(MinioMethodAnnotationIT.class, "testShouldExecuteBecauseOfMethodAnnotation"));

        assertTrue(hasMinioAnnotation(CombinedIT.class, "testMinioMethodShouldExecuteBecauseOfClassLevelAnnotation"));
        assertTrue(hasMinioAnnotation(CombinedIT.class, "testMinioMethodShouldExecuteBecauseOfClassLevelAnnotation"));

        assertTrue(hasMinioAnnotation(CombinedS3MinioIT.class, "testMinioMethodShouldExecuteBecauseOfMethodLevelAnnotation"));
        assertTrue(hasMinioAnnotation(CombinedMinioS3IT.class, "testMinioMethodShouldExecuteBecauseOfClassLevelAnnotation"));

        assertFalse(hasS3Annotation(MinioMethodAnnotationIT.class, "testShouldExecuteBecauseOfMethodAnnotation"));
    }

}
