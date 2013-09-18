package org.weakref.s3fs.spike;

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.marschall.memoryfilesystem.MemoryFileSystemBuilder;

public class ProviderSpecTest {
	FileSystem fs;

	@Before
	public void setup() throws IOException {
		fs = MemoryFileSystemBuilder.newLinux().build("linux");
	}

	@After
	public void close() throws IOException {
		fs.close();
	}
	
	// FIXME @Test
	public void seekable() throws IOException{
		Path base = Files.createDirectories(fs.getPath("/dir"));
		// in windows throw exception
		try (SeekableByteChannel seekable = Files.newByteChannel(base.resolve("file1.html"), 
				EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE, 
				StandardOpenOption.READ))){
			
			ByteBuffer buffer =  ByteBuffer.wrap("content".getBytes());
			seekable.position(7);
			seekable.write(buffer);

			ByteBuffer bufferRead = ByteBuffer.allocate(7);
			bufferRead.clear();
			seekable.read(bufferRead);
			
			assertArrayEquals(bufferRead.array(), buffer.array());
		}
	}
	
	@Test
	public void seekableRead() throws IOException{
		/*
		Path base = Files.createDirectories(fs.getPath("/dir"));
		Path path = Files.write(base.resolve("file"), "contenido yuhu".getBytes(), StandardOpenOption.CREATE_NEW);
		*/
		Path path = Files.write(Files.createTempFile("asdas", "asdsadad"), "contenido uyuhu".getBytes(), StandardOpenOption.APPEND);
		try (SeekableByteChannel channel = Files.newByteChannel(path)) {
		      
			
			
			//channel = Paths.get("Path to file").newByteChannel(StandardOpenOption.READ);
		    ByteBuffer buffer = ByteBuffer.allocate(4096);

		    System.out.println("File size: " + channel.size());

		    while (channel.read(buffer) > 0) {
		        buffer.rewind();
		        
		        System.out.print(new String(buffer.array(), 0, buffer.remaining()));

		        buffer.flip();

		        System.out.println("Current position : " + channel.position());
		    }
			
			
			
			
			/*
			
			
			
			
			ByteBuffer buffer = ByteBuffer.allocate(1024);

		      sbc.position(4);
		      sbc.read(buffer);
		      for (int i = 0; i < 5; i++) {
		        System.out.print((char) buffer.get(i));
		      }

		      buffer.clear();
		      sbc.position(0);
		      sbc.read(buffer);
		      for (int i = 0; i < 4; i++) {
		        System.out.print((char) buffer.get(i));
		      }
		      sbc.position(0);
		      buffer = ByteBuffer.allocate(1024);
		      String encoding = System.getProperty("file.encoding");
		      int numberOfBytesRead = sbc.read(buffer);
		      System.out.println("Number of bytes read: " + numberOfBytesRead);
		      while (numberOfBytesRead > 0) {
		        buffer.rewind();
		        System.out.print("[" + Charset.forName(encoding).decode(buffer) + "]");
		        buffer.flip();
		        numberOfBytesRead = sbc.read(buffer);
		        System.out.println("\nNumber of bytes read: " + numberOfBytesRead);
		      }
*/
		    }
	}
}
