package com.upplication.s3fs;

import static java.lang.String.format;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.upplication.s3fs.util.FileTypeDetector;
import com.upplication.s3fs.util.IOUtils;

public class S3SeekableByteChannel implements SeekableByteChannel {
	private final FileTypeDetector fileTypeDetector = new FileTypeDetector();
	private S3Path path;
	private Set<? extends OpenOption> options;
	private S3FileStore fileStore;
	private SeekableByteChannel seekable;
	private Path tempFile;

	public S3SeekableByteChannel(S3Path path, Set<? extends OpenOption> options, S3FileStore fileStore) throws IOException {
		this.path = path;
		this.options = options;
		this.fileStore = fileStore;
		String key = path.getKey();
		tempFile = Files.createTempFile("temp-s3-", key.replaceAll("/", "_"));
		boolean existed = false;
		try (S3Object object = fileStore.getObject(key)) {
			InputStream is = object.getObjectContent();
			Files.write(tempFile, IOUtils.toByteArray(is), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
			existed = true;
		} catch (AmazonS3Exception e) {
			// key doesn't exist on server. That's ok.
		}
		if (existed && options.contains(StandardOpenOption.CREATE_NEW))
			throw new FileAlreadyExistsException(format("target already exists: %s", path));
		Set<OpenOption> opts = new HashSet<>();
		if (options.contains(StandardOpenOption.WRITE))
			opts.add(StandardOpenOption.WRITE);
		if (options.contains(StandardOpenOption.READ))
			opts.add(StandardOpenOption.READ);
		seekable = Files.newByteChannel(tempFile, opts);
	}

	@Override
	public boolean isOpen() {
		return seekable.isOpen();
	}

	@Override
	public void close() throws IOException {
		try {
			if (!seekable.isOpen())
				return;
			seekable.close();
			// upload the content where the seekable ends (close)
			if (Files.exists(tempFile)) {
				ObjectMetadata metadata = new ObjectMetadata();
				metadata.setContentLength(Files.size(tempFile));
				// FIXME: #20 ServiceLoader cant load com.upplication.s3fs.util.FileTypeDetector when this library is used inside a ear :(
				metadata.setContentType(fileTypeDetector.probeContentType(tempFile));

				try (InputStream stream = Files.newInputStream(tempFile)) {
					/*
					 FIXME: if the stream is {@link InputStream#markSupported()} i can reuse the same stream
					 and evict the close and open methods of probeContentType. By this way:
					 metadata.setContentType(new Tika().detect(stream, tempFile.getFileName().toString()));
					*/
					fileStore.putObject(path.getKey(), stream, metadata);
				}
			}
			if (options.contains(StandardOpenOption.DELETE_ON_CLOSE)) {
				path.delete();
			}
		} finally {
			try {
				// and delete the temp dir
				Files.deleteIfExists(tempFile);
			} catch (Throwable t) {
				// is ok.
			}
		}
	}

	@Override
	public int write(ByteBuffer src) throws IOException {
		return seekable.write(src);
	}

	@Override
	public SeekableByteChannel truncate(long size) throws IOException {
		return seekable.truncate(size);
	}

	@Override
	public long size() throws IOException {
		return seekable.size();
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		return seekable.read(dst);
	}

	@Override
	public SeekableByteChannel position(long newPosition) throws IOException {
		return seekable.position(newPosition);
	}

	@Override
	public long position() throws IOException {
		return seekable.position();
	}
}
