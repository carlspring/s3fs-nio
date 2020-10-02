package org.carlspring.cloud.storage.s3fs.util;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;

public class CopyDirVisitor
        extends SimpleFileVisitor<Path>
{

    private final Path fromPath;

    private final Path toPath;

    private final StandardCopyOption copyOption;


    public CopyDirVisitor(final Path fromPath, final Path toPath, final StandardCopyOption copyOption)
    {
        this.fromPath = fromPath;
        this.toPath = toPath;
        this.copyOption = copyOption;
    }

    public CopyDirVisitor(final Path fromPath, final Path toPath)
    {
        this(fromPath, toPath, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs)
            throws IOException
    {
        // we allow to work against different providers
        Path targetPath = appendPath(dir);

        if (Files.notExists(targetPath))
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
    public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs)
            throws IOException
    {
        final Path targetPath = appendPath(file);

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
    private Path appendPath(final Path to)
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
