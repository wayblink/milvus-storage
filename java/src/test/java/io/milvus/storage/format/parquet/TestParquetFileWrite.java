package io.milvus.storage.format.parquet;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.table.Row;
import org.apache.arrow.vector.table.Table;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.util.Arrays.asList;

public class TestParquetFileWrite {

    public static void main(String[] args) throws IOException {
        // schema
        Field a = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), /*children*/ null);
        Field b = new Field("name", FieldType.nullable(new ArrowType.Utf8()), /*children*/ null);
        Map<String, String> metadata = new HashMap<>();
        Schema arrowSchema = new Schema(asList(a, b), metadata);

        SchemaBuilder.FieldAssembler<org.apache.avro.Schema> avroFields =
                SchemaBuilder.record("user").fields();
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

        String path = "file:" + System.getProperty("user.dir") + "/output.parquet";
//        org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("user", "", "org.apache.arrow.dataset", false);

        ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(new Path(path))
                .withSchema(avroSchema)
                .build();


        BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
        IntVector ageVector = (IntVector) root.getVector("age");
        VarCharVector nameVector = (VarCharVector) root.getVector("name");
        root.setRowCount(3);
        ageVector.allocateNew(3);
        ageVector.set(0, 10);
        ageVector.set(1, 20);
        ageVector.set(2, 30);
        nameVector.allocateNew(3);
        nameVector.set(0, "Dave".getBytes(StandardCharsets.UTF_8));
        nameVector.set(1, "Peter".getBytes(StandardCharsets.UTF_8));
        nameVector.set(2, "Mary".getBytes(StandardCharsets.UTF_8));
        System.out.println("VectorSchemaRoot created: \n" + root.contentToTSVString());

        // Create an Arrow table from the record batch
        Table arrowTable = new Table(root);
        Iterator<Row> iter = arrowTable.iterator();
        while (iter.hasNext()) {
            Row row = iter.next();
            GenericRecordBuilder avroRecordBuilder = new GenericRecordBuilder(avroSchema);
            for (Field field:arrowSchema.getFields()) {
                ArrowType fieldType = field.getFieldType().getType();
                switch (fieldType.getTypeID()) {
                    case Int:
                        avroRecordBuilder.set(field.getName(), row.getInt(field.getName()));
                        break;
                    case Utf8:
                        avroRecordBuilder.set(field.getName(), row.getVarChar(field.getName()));
                        break;
                    // Add cases for other Arrow data types as needed
                    default:
                        throw new IllegalArgumentException("Unsupported Arrow data type: " + fieldType);
                }
            }

            writer.write(avroRecordBuilder.build());
        }
        writer.close();
    }
}
