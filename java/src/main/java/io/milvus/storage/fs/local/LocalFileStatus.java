package io.milvus.storage.fs.local;

import io.milvus.storage.fs.FileStatus;
import io.milvus.storage.fs.FileSystem;

import java.io.File;
import java.nio.file.Path;

public class LocalFileStatus implements FileStatus {

    /** The file this file status belongs to. */
    private final File file;

    /** The path of this file status belongs to. */
//    private final Path path;

    /** Cached length field, to avoid repeated native/syscalls. */
    private final long len;

    public LocalFileStatus(File f, FileSystem fs) {
        this.file = f;
//        this.path = new Path(fs.getUri().getScheme() + ":" + f.toURI().getPath());
        this.len = f.length();
    }

    @Override
    public long getLen() {
        return this.len;
    }

    @Override
    public boolean isDir() {
        return this.file.isDirectory();
    }

    @Override
    public String getPath() {
        return this.file.getPath();
    }

    @Override
    public String getName() {
        return this.file.getName();
    }
}
