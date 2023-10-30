package io.milvus.storage.format.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;

public class TestReadParquetByAvro {

    @Test
    public void testReadByAvro() throws Exception {
        String uri = "file:" + System.getProperty("user.dir") +  "/data/example.parquet";
        Path path = new Path(uri);
        Configuration conf = new Configuration();
        HadoopInputFile inputFile = HadoopInputFile.fromPath(new Path(uri), conf);
        org.apache.parquet.hadoop.ParquetFileReader hadoopReader = org.apache.parquet.hadoop.ParquetFileReader.open(inputFile);

        ParquetMetadata parquetMetadata = hadoopReader.getFooter();
        FileMetaData fileMetaData = hadoopReader.getFileMetaData();
        MessageType parquetSchema = hadoopReader.getFooter().getFileMetaData().getSchema();
        org.apache.avro.Schema avroSchema = new AvroSchemaConverter(conf).convert(parquetSchema);

        AvroParquetReader<GenericRecord> avroReader = new AvroParquetReader<GenericRecord>(path);
        GenericRecord nextRecord = avroReader.read();

//        BufferAllocator allocator = new RootAllocator();
//        AvroToArrowConfig config = new AvroToArrowConfigBuilder(allocator).build();
//        BinaryDecoder decoder = new DecoderFactory().binaryDecoder(inputFile.newStream(), null);
//        try (AvroToArrowVectorIterator avroToArrowVectorIterator = AvroToArrow.avroToArrowIterator(avroSchema, decoder, config)) {
//            while(avroToArrowVectorIterator.hasNext()) {
//                try (VectorSchemaRoot root = avroToArrowVectorIterator.next()) {
//                    System.out.print(root.contentToTSVString());
//                }
//            }
//        }

        System.out.println();
//        VectorSchemaRoot root = reader.Read();
//        System.out.println(root.contentToTSVString());
    }
}
