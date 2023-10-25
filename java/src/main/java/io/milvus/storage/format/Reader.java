package io.milvus.storage.format;

import org.apache.arrow.vector.VectorSchemaRoot;

public interface Reader {
    // If it Table
    // todo exception type?
    VectorSchemaRoot Read() throws Exception;
    void Close() throws Exception;
}
