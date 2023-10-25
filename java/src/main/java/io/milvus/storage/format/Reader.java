package io.milvus.storage.format;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;

public interface Reader {
    // If it Table
    // todo exception type?
    // When the Reader reaches the end of the underlying stream, it returns (nil, io.EOF)
    VectorSchemaRoot Read() throws IOException;
    void Close() throws IOException;
}
