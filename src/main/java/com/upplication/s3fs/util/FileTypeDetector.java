package com.upplication.s3fs.util;

import org.apache.tika.Tika;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * FileTypeDetector extension.
 * Use tika for better mime type detection
 * and operating System agnostic.
 */
public class FileTypeDetector extends java.nio.file.spi.FileTypeDetector {
    private Tika tika;

    public FileTypeDetector(Tika tika) {
        this.tika = tika;
    }
    public FileTypeDetector(){
        this.tika = new Tika();
    }

    @Override
    public String probeContentType(Path path) throws IOException {
        try(InputStream stream = Files.newInputStream(path)){
            return tika.detect(stream, path.getFileName().toString());
        }
    }
}
