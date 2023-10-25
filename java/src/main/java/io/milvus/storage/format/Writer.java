package io.milvus.storage.format;

import org.apache.arrow.vector.VectorSchemaRoot;

public interface Writer {
    void Write(VectorSchemaRoot record);
    long Count();
    void Close();
}
