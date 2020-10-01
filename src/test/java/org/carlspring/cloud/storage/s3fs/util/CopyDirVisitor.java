package org.carlspring.cloud.storage.s3fs.util;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public class CopyDirVisitor
        extends SimpleFileVisitor<Path>
{

    private Path fromPath;

    private Path toPath;

    private StandardCopyOption copyOption;


    public CopyDirVisitor(Path fromPath, Path toPath, StandardCopyOption copyOption)
    {
        this.fromPath = fromPath;
        this.toPath = toPath;
        this.copyOption = copyOption;
    }

    public CopyDirVisitor(Path fromPath, Path toPath)
    {
        this(fromPath, toPath, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
            throws IOException
    {
        // we allow to work against different providers
        Path targetPath = appendPath(dir);

        if (!Files.exists(targetPath))
        {
            if (!targetPath.toString().endsWith("/"))
            {
                targetPath = targetPath.getParent().resolve(targetPath.getFileName().toString() + "/");
            }

            Files.createDirectory(targetPath);
        }

        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
            throws IOException
    {
        Path targetPath = appendPath(file);

        Files.copy(file, targetPath, copyOption);

        return FileVisitResult.CONTINUE;
    }

    /**
     * Get the corresponding path in the parameter: {@link #fromPath}, and relativize it
     * to the <code>Path to</code> parameter.
     *
     * @param to Path
     * @return
     */
    private Path appendPath(Path to)
    {
        Path targetPath = toPath;

        // Get the relative path and traverse it to add it to the new path.
        for (Path path : fromPath.relativize(to))
        {
            // If Path is used instead of String, an error is thrown because the paths are different.
            targetPath = targetPath.resolve(path.getFileName().toString());
        }

        return targetPath;
    }

}
