package io.milvus.storage.format.parquet;

import io.milvus.storage.format.Writer;
import io.milvus.storage.fs.FileSystem;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.table.Row;
import org.apache.arrow.vector.table.Table;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.util.Iterator;

// https://github.com/apache/arrow/blob/main/java/dataset/src/test/java/org/apache/arrow/dataset/file/TestDatasetFileWriter.java
public class ParquetFileWriter implements Writer {
    private FileSystem fs;
    private String filePath;
    private Schema schema;
    private volatile Long count;

    // avro
    private org.apache.avro.Schema avroSchema;
    private ParquetWriter<GenericRecord> avroParquetWriter;

    public ParquetFileWriter(String filePath, Schema schema) {
//        this.fs = fs;
        this.filePath = filePath;
        this.schema = schema;
        this.count = 0L;
    }

    public ParquetFileWriter(FileSystem fs, String filePath, Schema schema) {
//        this.fs = fs;
        this.filePath = filePath;
        this.schema = schema;
        this.count = 0L;
    }

    @Override
    public void Write(VectorSchemaRoot record) throws IOException {
        if (this.avroParquetWriter == null) {
            this.avroSchema = toAvroSchema(schema);
            this.avroParquetWriter = AvroParquetWriter
                    .<GenericRecord>builder(new Path(this.filePath))
                    .withSchema(avroSchema)
                    .build();
        }

        Table arrowTable = new Table(record);
        Iterator<Row> iter = arrowTable.iterator();
        while (iter.hasNext()) {
            Row row = iter.next();
            this.avroParquetWriter.write(toAvroRecord(row, this.avroSchema, this.schema));
        }
        this.count += record.getRowCount();
    }

    @Override
    public long Count() {
        return this.count;
    }

    @Override
    public void Close() throws IOException {
        if (this.avroParquetWriter != null) {
            this.avroParquetWriter.close();
        }
    }

    private GenericData.Record toAvroRecord(Row arrowRow, org.apache.avro.Schema avroSchema, Schema arrowSchema) {
        GenericRecordBuilder avroRecordBuilder = new GenericRecordBuilder(avroSchema);
        for (Field field:arrowSchema.getFields()) {
            ArrowType fieldType = field.getFieldType().getType();
            switch (fieldType.getTypeID()) {
                case Int:
                    avroRecordBuilder.set(field.getName(), arrowRow.getInt(field.getName()));
                    break;
                case Utf8:
                    avroRecordBuilder.set(field.getName(), arrowRow.getVarChar(field.getName()));
                    break;
                // Add cases for other Arrow data types as needed
                default:
                    throw new IllegalArgumentException("Unsupported Arrow data type: " + fieldType);
            }
        }
        return avroRecordBuilder.build();
    }

    private org.apache.avro.Schema toAvroSchema(org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
        SchemaBuilder.FieldAssembler<org.apache.avro.Schema> avroFields =
                SchemaBuilder.record("nomeaning").fields();
        for (Field field : arrowSchema.getFields()) {
            String fieldName = field.getName();
            FieldType fieldType = field.getFieldType();
            org.apache.avro.Schema avroFieldType;
            switch (fieldType.getType().getTypeID()) {
                case Int:
                    avroFieldType = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT);
                    break;
                case Utf8:
                    avroFieldType = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
                    break;
                // Add cases for other Arrow data types as needed
                default:
                    throw new IllegalArgumentException("Unsupported Arrow data type: " + fieldType);
            }
            avroFields.name(fieldName).type(avroFieldType).noDefault();
        }
        org.apache.avro.Schema avroSchema = avroFields.endRecord();
        return avroSchema;
    }
}
