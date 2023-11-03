package io.milvus.storage.fs.s3;

import io.milvus.storage.fs.File;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class S3File implements File {
    private String path;

    @Override
    public void write(byte[] bytes) {

    }

    @Override
    public void read(byte[] bytes) {

    }

    @Override
    public void close() {

    }
}
