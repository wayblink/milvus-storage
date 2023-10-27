package io.milvus.storage.format.parquet;

import io.milvus.storage.format.Reader;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.io.IOException;

public class ParquetFileReader implements Reader {
//    private FileSystem fs;
    private String filePath;
    private Long batchSize;
    private ArrowReader reader;

    public ParquetFileReader(String filePath) {
        this(filePath, 32768);
    }

    public ParquetFileReader(String filePath, long batchSize) {
        this.filePath = filePath;
        this.batchSize = batchSize;
        ScanOptions options = new ScanOptions(/*batchSize*/ batchSize);
        BufferAllocator allocator = new RootAllocator();
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(),
                FileFormat.PARQUET, filePath);
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);
        ArrowReader reader = scanner.scanBatches();
        this.reader = reader;
    }

    @Override
    public VectorSchemaRoot Read() throws IOException {
        Boolean hashNextBatch = reader.loadNextBatch();
        if (hashNextBatch) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            return root;
        } else {
            return null;
        }
    }

    @Override
    public void Close() throws IOException {
        this.reader.close();
    }
}
