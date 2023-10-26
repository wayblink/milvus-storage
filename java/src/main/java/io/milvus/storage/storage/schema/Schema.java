package io.milvus.storage.storage.schema;

import io.milvus.storage.common.utils.Utils;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import schema_proto.SchemaOuterClass;

import java.util.ArrayList;
import java.util.List;

import static io.milvus.storage.common.constant.Constant.OffsetFieldName;
import static io.milvus.storage.storage.schema.SchemaException.ErrPrimaryColumnNotFound;
import static io.milvus.storage.storage.schema.SchemaException.ErrVersionColumnNotFound;

@Getter
@ToString
@NoArgsConstructor
public class Schema {
    private org.apache.arrow.vector.types.pojo.Schema schema;
    private org.apache.arrow.vector.types.pojo.Schema scalarSchema;
    private org.apache.arrow.vector.types.pojo.Schema vectorSchema;
    private org.apache.arrow.vector.types.pojo.Schema deleteSchema;

    private SchemaOptions options;

    public Schema(org.apache.arrow.vector.types.pojo.Schema schema, SchemaOptions options) {
        this.schema = schema;
        this.options = options;
    }

    public void Validate() throws SchemaException {
        this.options.Validate(this.schema);
        this.BuildScalarSchema();
        this.BuildVectorSchema();
        this.BuildDeleteSchema();
    }

    public void BuildScalarSchema() throws SchemaException {
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < this.schema.getFields().size(); i++) {
            if (!this.schema.getFields().get(i).getName().equals(this.options.vectorColumn)) {
                fields.add(this.schema.getFields().get(i));
            }
        }
        Field offsetField = new Field(
                OffsetFieldName,
                new FieldType(true, Types.MinorType.BIGINT.getType(), null),
                null);
        fields.add(offsetField);
        this.scalarSchema = new org.apache.arrow.vector.types.pojo.Schema(fields);
    }

    public void BuildVectorSchema() throws SchemaException {
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < this.schema.getFields().size(); i++) {
            String fieldName = this.schema.getFields().get(i).getName();
            if (fieldName.equals(this.options.getVectorColumn())
                    ||fieldName.equals(this.options.getPrimaryColumn())
                    ||fieldName.equals(this.options.getVersionColumn())) {
                fields.add(this.schema.getFields().get(i));
            }
        }
        this.vectorSchema = new org.apache.arrow.vector.types.pojo.Schema(fields);
    }

    public void BuildDeleteSchema() throws SchemaException {
        Field pkField = null;
        Boolean exist = true;
        try {
            pkField = this.schema.findField(this.options.getPrimaryColumn());
        } catch (IllegalArgumentException e) {
            exist = false;
        }
        if (pkField == null || !exist) {
            throw new SchemaException(ErrPrimaryColumnNotFound);
        }

        Field versionField = null;
        try {
            versionField = this.schema.findField(this.options.getVersionColumn());
        } catch (IllegalArgumentException e) {
            exist = false;
        }
        if (versionField == null || !exist) {
            throw new SchemaException(ErrVersionColumnNotFound);
        }

        List<Field> fields = new ArrayList<>();
        fields.add(pkField);
        fields.add(versionField);
        this.deleteSchema = new org.apache.arrow.vector.types.pojo.Schema(fields);
    }

    public SchemaOuterClass.Schema ToProtobuf() {
        SchemaOuterClass.Schema.Builder builder = SchemaOuterClass.Schema.newBuilder();
        builder.setArrowSchema(Utils.ToProtobufSchema(this.schema));
        builder.setSchemaOptions(SchemaOptions.ToProtobuf(this.options));
        return builder.build();
    }

    public static Schema FromProtobuf(SchemaOuterClass.Schema schema) throws Exception {
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = Utils.FromProtobufSchema(schema.getArrowSchema());
        Schema schema2 = new Schema();
        schema2.schema = arrowSchema;
        schema2.options = SchemaOptions.FromProtobuf(schema.getSchemaOptions());
        schema2.BuildScalarSchema();
        schema2.BuildVectorSchema();
        schema2.BuildDeleteSchema();
        return schema2;
    }

}
