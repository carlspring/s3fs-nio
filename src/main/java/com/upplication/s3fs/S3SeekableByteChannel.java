package com.upplication.s3fs;

import static java.lang.String.format;

import java.io.BufferedInputStream;
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

import org.apache.tika.Tika;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.upplication.s3fs.util.IOUtils;

public class S3SeekableByteChannel implements SeekableByteChannel {
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
		boolean existed = fileStore.exists(path);
		if(existed) {
			S3Object object = fileStore.getObject(key);
			InputStream is = object.getObjectContent();
			Files.write(tempFile, IOUtils.toByteArray(is), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
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
			if (options.contains(StandardOpenOption.DELETE_ON_CLOSE)) {
				path.delete();
				return;
			}
			// upload the content where the seekable ends (close)
			InputStream stream = null;
			try {
				stream = new BufferedInputStream(Files.newInputStream(tempFile));
				ObjectMetadata metadata = new ObjectMetadata();
				metadata.setContentLength(Files.size(tempFile));
				metadata.setContentType(new Tika().detect(stream, path.getFileName().toString()));
				fileStore.putObject(path.getKey(), stream, metadata);
			} finally {
				if(stream != null)
					stream.close();
			}
		} finally {
			Files.deleteIfExists(tempFile);
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