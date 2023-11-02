package io.milvus.storage.space;

import io.milvus.storage.storage.options.WriteOptions;
import io.milvus.storage.storage.schema.Schema;
import io.milvus.storage.storage.schema.SchemaOptions;
import io.milvus.storage.storage.space.Space;
import io.milvus.storage.storage.space.SpaceOptions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestSpace {

    @Test
    public void testSpace() throws Exception {
        org.apache.arrow.vector.types.pojo.Field pkField = new Field("pk_field", FieldType.nullable(new ArrowType.Int(64, true)), null);
        org.apache.arrow.vector.types.pojo.Field vsField = new Field("vs_field", FieldType.nullable(new ArrowType.Int(64, true)), null);
        org.apache.arrow.vector.types.pojo.Field vecField = new Field("vec_field", FieldType.nullable(new ArrowType.FixedSizeBinary(10)), null);

        List<org.apache.arrow.vector.types.pojo.Field> fields = new ArrayList<>();
        fields.add(pkField);
        fields.add(vsField);
        fields.add(vecField);
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = new org.apache.arrow.vector.types.pojo.Schema(fields);

        SchemaOptions schemaOptions = new SchemaOptions.Builder()
                .setPrimaryColumn("pk_field")
                .setVersionColumn("vs_field")
                .setVectorColumn("vec_field")
                .build();
        Schema schema = new Schema(arrowSchema, schemaOptions);
        SpaceOptions spaceOptions = new SpaceOptions.Builder().setVersion(0L).setSchema(schema).build();
        Space space = Space.Open("space", spaceOptions);

        BufferAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
        root.setRowCount(3);

        BigIntVector pkVector = (BigIntVector)root.getVector("pk_field");
        pkVector.allocateNew(3);
        pkVector.set(0, 1);
        pkVector.set(1, 2);
        pkVector.set(2, 3);
        pkVector.setValueCount(3);

        BigIntVector vsVector = (BigIntVector)root.getVector("vs_field");
        vsVector.allocateNew(3);
        vsVector.set(0, 1);
        vsVector.set(1, 2);
        vsVector.set(2, 3);
        vsVector.setValueCount(3);

        FixedSizeBinaryVector vecVector = (FixedSizeBinaryVector)root.getVector("vec_field");
        vecVector.allocateNew(3);
        vecVector.set(0, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        vecVector.set(1, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        vecVector.set(2, new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        vecVector.setValueCount(3);

        WriteOptions writeOptions = WriteOptions.builder().maxRecordPerFile(100L).build();

        space.Write(root, writeOptions);

        System.out.println(space);
    }


}
