package com.upplication.s3fs.util;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Created by IntelliJ IDEA. User: bbejeck Date: 1/23/12 Time: 10:29 PM
 */
public class CopyDirVisitor extends SimpleFileVisitor<Path> {

	private Path fromPath;
	private Path toPath;
	private StandardCopyOption copyOption;

	public CopyDirVisitor(Path fromPath, Path toPath,
			StandardCopyOption copyOption) {
		this.fromPath = fromPath;
		this.toPath = toPath;
		this.copyOption = copyOption;
	}

	public CopyDirVisitor(Path fromPath, Path toPath) {
		this(fromPath, toPath, StandardCopyOption.REPLACE_EXISTING);
	}

	@Override
	public FileVisitResult preVisitDirectory(Path dir,
			BasicFileAttributes attrs) throws IOException {

		// permitimos resolver entre distintos providers
		Path targetPath = appendPath(dir);

		if (!Files.exists(targetPath)) {
			if (!targetPath.getFileName().toString().endsWith("/")){
				targetPath = targetPath.getParent().resolve(targetPath.getFileName().toString() + "/");
			}
			Files.createDirectory(targetPath);
		}
		return FileVisitResult.CONTINUE;
	}

	@Override
	public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
			throws IOException {

		Path targetPath = appendPath(file);

		Files.copy(file, targetPath, copyOption);
		return FileVisitResult.CONTINUE;
	}

	/**
	 * Obtenemos el path que corresponde en el parametro: {@link #fromPath}
	 * relativo al parametro <code>Path to</code>
	 * 
	 * @param to
	 *            Path
	 * @return
	 */
	private Path appendPath(Path to) {
		Path targetPath = toPath;
		// sacamos el path relativo y lo recorremos para
		// añadirlo al nuevo

		for (Path path : fromPath.relativize(to)) {
			// si utilizamos path en vez de string: lanza error por ser
			// distintos paths
			targetPath = targetPath.resolve(path.getFileName().toString());
		}

		return targetPath;
	}
}