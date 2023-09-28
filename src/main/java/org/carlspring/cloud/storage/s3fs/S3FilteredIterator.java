package org.carlspring.cloud.storage.s3fs;

import java.io.IOException;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class S3FilteredIterator implements Iterator<Path>
{

    private final S3Iterator s3iterator;

    private final Filter<? super Path> filter;

    private Path cursor;

    private boolean cursorIsCurrent;

    public S3FilteredIterator(S3Path s3Path,
                              Filter<? super Path> filter)
    {
        this.filter = filter;
        this.s3iterator = new S3Iterator(s3Path);
        clearCursor();
    }

    @Override
    public boolean hasNext()
    {
        if (!cursorIsCurrent)
        {
            findNextFiltered();
        }
        return cursorIsCurrent;
    }

    @Override
    public Path next()
    {
        if (!hasNext())
        {
            throw new NoSuchElementException();
        }
        Path next = cursor;
        clearCursor();
        return next;
    }

    private void findNextFiltered()
    {
        try
        {
            while (s3iterator.hasNext())
            {
                S3Path next = s3iterator.next();
                if (filter.accept(next))
                {
                    cursor = next;
                    cursorIsCurrent = true;
                    return;
                }
            }
            clearCursor();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void clearCursor()
    {
        cursor = null;
        cursorIsCurrent = false;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

}
