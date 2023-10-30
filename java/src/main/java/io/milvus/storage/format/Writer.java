package io.milvus.storage.format;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;

public interface Writer {
    void Write(VectorSchemaRoot record) throws IOException;
    long Count();
    void Close() throws IOException;
}
