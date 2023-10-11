package io.milvus.storage.format;

import org.apache.arrow.vector.table.Table;

public interface Writer {
    void Write(Table record);
    long Count();
    void Close();
}
