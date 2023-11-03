package io.milvus.storage.fs.local;

import io.milvus.storage.fs.File;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.FileOutputStream;
import java.io.IOException;

public class LocalFile implements File {
    @Getter
    private java.io.File file;

    private FileOutputStream fos;

    public LocalFile(String path) {
        this(new java.io.File(path));
    }

    public LocalFile(java.io.File file) {
        this.file = file;
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        if (fos == null) {
            FileOutputStream fos = new FileOutputStream(file);
            this.fos = fos;
        }
        fos.write(bytes);

    }

    @Override
    public void read(byte[] bytes) {

    }

    @Override
    public void close() throws IOException {
        fos.close();
    }
}
