package io.milvus.storage.format;

import org.apache.arrow.vector.table.Table;

public interface Reader {
    // If it Table
    // todo exception type?
    Table Read() throws Exception;
    void Close() throws Exception;
}
