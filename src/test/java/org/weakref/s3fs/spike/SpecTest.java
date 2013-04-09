package org.weakref.s3fs.spike;

import java.nio.file.FileSystems;
import java.nio.file.Path;

import static org.junit.Assert.*;

import org.junit.Test;

public class SpecTest {
	@Test
	public void parentOfRelativeSinglePathIsNull(){
		Path path = FileSystems.getDefault().getPath("relativo");
		
		assertNull(path.getParent());
	}
}
