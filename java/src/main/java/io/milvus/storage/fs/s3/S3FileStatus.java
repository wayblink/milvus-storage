package io.milvus.storage.fs.s3;

import io.milvus.storage.fs.FileStatus;
import io.milvus.storage.fs.FileSystem;

import java.io.File;
import java.nio.file.Path;

public class S3FileStatus implements FileStatus {
    /** The file this file status belongs to. */
    private final org.apache.hadoop.fs.FileStatus fileStatus;

    /** Cached length field, to avoid repeated native/syscalls. */
    private final long len;

    public S3FileStatus(org.apache.hadoop.fs.FileStatus fileStatus, FileSystem fs) {
        this.fileStatus = fileStatus;
        this.len = fileStatus.getLen();
    }

    @Override
    public long getLen() {
        return this.len;
    }

    @Override
    public boolean isDir() {
        return fileStatus.isDirectory();
    }

    @Override
    public String getPath() {
        return fileStatus.getPath().toString();
    }

    @Override
    public String getName() {
        return fileStatus.getPath().getName();
    }
}
