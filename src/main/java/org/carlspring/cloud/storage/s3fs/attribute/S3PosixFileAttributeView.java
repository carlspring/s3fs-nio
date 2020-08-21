package org.carlspring.cloud.storage.s3fs.attribute;

import org.carlspring.cloud.storage.s3fs.S3Path;

import java.io.IOException;
import java.nio.file.attribute.*;
import java.util.Set;


public class S3PosixFileAttributeView implements PosixFileAttributeView {

    private S3Path s3Path;
    private PosixFileAttributes posixFileAttributes;

    public S3PosixFileAttributeView(S3Path s3Path) {
        this.s3Path = s3Path;
    }

    @Override
    public String name() {
        return "posix";
    }

    @Override
    public PosixFileAttributes readAttributes() throws IOException {
        return read();
    }

    @Override
    public UserPrincipal getOwner() throws IOException {
        return read().owner();
    }

    @Override
    public void setOwner(UserPrincipal owner) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPermissions(Set<PosixFilePermission> perms) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setGroup(GroupPrincipal group) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) {
        // not implemented
    }

    public PosixFileAttributes read() throws IOException {
        if (posixFileAttributes == null) {
            posixFileAttributes = s3Path.getFileSystem()
                    .provider().readAttributes(s3Path, PosixFileAttributes.class);
        }
        return posixFileAttributes;
    }
}
