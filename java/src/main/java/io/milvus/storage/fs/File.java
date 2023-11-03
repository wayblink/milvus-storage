package io.milvus.storage.fs;

import java.io.IOException;

public interface File {
    void write(byte[] bytes) throws IOException;
    void read(byte[] bytes) throws IOException;
    void close() throws IOException;
}
