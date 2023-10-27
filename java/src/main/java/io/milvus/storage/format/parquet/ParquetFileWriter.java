package io.milvus.storage.format.parquet;

import io.milvus.storage.format.Writer;
import io.milvus.storage.fs.FileSystem;
import lombok.Getter;
import org.apache.arrow.dataset.file.DatasetFileWriter;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ArrowScannerReader;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

// https://github.com/apache/arrow/blob/main/java/dataset/src/test/java/org/apache/arrow/dataset/file/TestDatasetFileWriter.java
public class ParquetFileWriter implements Writer {
    private FileSystem fs;
    private String filePath;
    private Schema schema;
    private volatile Long count;

    private ArrowReader reader;

    public ParquetFileWriter(FileSystem fs, String filePath, Schema schema) {
        this.fs = fs;
        this.filePath = filePath;
        this.schema = schema;
        this.count = 0L;

        this.filePath = filePath;

    }

    @Override
    public void Write(VectorSchemaRoot record) {
    }

    @Override
    public long Count() {
        return 0;
    }

    @Override
    public void Close() {

    }
}
