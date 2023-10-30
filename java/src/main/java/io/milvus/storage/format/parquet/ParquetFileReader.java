package io.milvus.storage.format.parquet;

import io.milvus.storage.format.Reader;
import io.milvus.storage.storage.options.ReadOptions;
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
import java.util.Optional;

public class ParquetFileReader implements Reader {
//    private FileSystem fs;
    private String filePath;
    private ArrowReader reader;
    private ReadOptions options;

    public ParquetFileReader(String filePath) {
        this(filePath, new ReadOptions.Builder().build());
    }

    public ParquetFileReader(String filePath, ReadOptions readOptions) {
        this.filePath = filePath;
        this.options = readOptions;
        this.initReader();
    }

    private void initReader() {
        ScanOptions options;
        if (this.options.getColumns().isEmpty()) {
            options = new ScanOptions(this.options.getBatchSize());
        } else {
            String[] columns = this.options.getColumns().toArray(new String[this.options.getColumns().size()]);
            options = new ScanOptions(
                    /*batchSize*/ this.options.getBatchSize(),
                    /*columns*/ Optional.of(columns));
        }

        // todo filter push down is not support, will support in arrow 14.0.0
        BufferAllocator allocator = new RootAllocator();
        DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(),
                FileFormat.PARQUET, filePath);
        Dataset dataset = datasetFactory.finish();
        Scanner scanner = dataset.newScan(options);

        ArrowReader reader = scanner.scanBatches();
        this.reader = reader;
    }

//    public static final class Builder {
//        private String filePath;
//        private ReadOptions options;
//
//        private Builder() {
//        }
//
//        /**
//         * Sets the filePath. filePath cannot be empty or null.
//         *
//         * @param filePath String
//         * @return <code>Builder</code>
//         */
//        public ParquetFileReader.Builder setFilePath(@NonNull String filePath) {
//            this.filePath = filePath;
//            return this;
//        }
//
//        /**
//         * Sets the ReadOptions.
//         *
//         * @param options ReadOptions
//         * @return <code>Builder</code>
//         */
//        public ParquetFileReader.Builder setReadOptions(@NonNull ReadOptions options) {
//            this.options = options;
//            return this;
//        }
//
//        /**
//         * Creates a new {@link ParquetFileReader} instance.
//         *
//         * @return {@link ParquetFileReader}
//         */
//        public ParquetFileReader build() {
//            return new ParquetFileReader(this.filePath, this.options);
//        }
//    }

    @Override
    public VectorSchemaRoot Read() throws IOException {
        Boolean hashNextBatch = reader.loadNextBatch();
        if (hashNextBatch) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            return root;
        } else {
            return null;
        }

        // https://zhuanlan.zhihu.com/p/408918709
        // 2. 转移读取的 readerVectorSchemaRoot中的记录的 ownership 到结果 vectorSchemaRoot
//        List<FieldVector> fieldVectors = readerVectorSchemaRoot.getFieldVectors().stream().map(fv -> {
//            TransferPair transfer = fv.getTransferPair(fv.getAllocator());
//            transfer.transfer();
//            return (FieldVector)transfer.getTo();
//        }).collect(Collectors.toList());
//        vectorSchemaRoot = new VectorSchemaRoot(readerVectorSchemaRoot.getSchema(), fieldVectors, readerVectorSchemaRoot.getRowCount());
    }

    @Override
    public void Close() throws IOException {
        this.reader.close();
    }
}
