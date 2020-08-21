package org.carlspring.cloud.storage.s3fs.util;

import java.nio.file.attribute.FileStoreAttributeView;

public class UnsupportedFileStoreAttributeView implements FileStoreAttributeView {
    @Override
    public String name() {
        return "unsupported";
    }
}
