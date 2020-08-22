package org.carlspring.cloud.storage.s3fs.util;

import java.nio.file.attribute.FileAttribute;

public class FileAttributeBuilder
{


    public static <T> FileAttribute<T> build(final String name, final T value)
    {
        return new FileAttribute<T>()
        {
            @Override
            public String name()
            {
                return name;
            }

            @Override
            public T value()
            {
                return value;
            }
        };
    }

}
