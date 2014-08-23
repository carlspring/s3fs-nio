package com.upplication.s3fs;

import com.upplication.s3fs.util.IOUtils;
import org.junit.Test;

/**
 * http://stackoverflow.com/questions/9700179/junit-testing-helper-class-with-only-static-methods
 */
public class IOUtilsTest {

    @Test
    public void just_to_silence_coverage(){
        IOUtils io = new IOUtils(){};
    }
}
