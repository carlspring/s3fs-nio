package org.carlspring.cloud.storage.s3fs.junit.annotations;

public abstract class BaseAnnotationTest
{

    /**
     * Check if there is an {@link S3IntegrationTest} annotation at a class level.
     *
     * @param clazz
     * @return
     */
    protected static boolean hasS3Annotation(Class<?> clazz)
    {
        return clazz.getAnnotation(S3IntegrationTest.class) != null;
    }

    /**
     * Check if there is an {@link S3IntegrationTest} annotation at a method level (and pick-up class level)
     *
     * @param clazz
     * @param methodName
     * @return
     * @throws NoSuchMethodException
     */
    protected static boolean hasS3Annotation(Class<?> clazz,
                                             String methodName)
            throws NoSuchMethodException
    {
        return hasS3Annotation(clazz) || clazz.getMethod(methodName).getAnnotation(S3IntegrationTest.class) != null;
    }

    /**
     * Check if there is an {@link MinioIntegrationTest} annotation at a class level.
     *
     * @param clazz
     * @return
     */
    protected static boolean hasMinioAnnotation(Class<?> clazz)
    {
        return clazz.getAnnotation(MinioIntegrationTest.class) != null;
    }

    /**
     * Check if there is an {@link MinioIntegrationTest} annotation at a method level (and pick-up class level)
     *
     * @param clazz
     * @param methodName
     * @return
     * @throws NoSuchMethodException
     */
    protected static boolean hasMinioAnnotation(Class<?> clazz,
                                                String methodName)
            throws NoSuchMethodException
    {
        return hasMinioAnnotation(clazz) || clazz.getMethod(methodName).getAnnotation(MinioIntegrationTest.class) != null;
    }
}
