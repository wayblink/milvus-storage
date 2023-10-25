package io.milvus.storage.storage.schema;

import lombok.*;
import schema_proto.SchemaOuterClass;

import static io.milvus.storage.storage.schema.SchemaException.*;
import static org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.*;

@Getter
@ToString
@AllArgsConstructor
public class SchemaOptions {
    String primaryColumn;
    String versionColumn;
    String vectorColumn;

    public SchemaOptions DefaultSchemaOptions() {
        return new SchemaOptions("","","");
    }

    public static SchemaOptions FromProtobuf(SchemaOuterClass.SchemaOptions schemaOptions) {
        return new SchemaOptions(
                schemaOptions.getPrimaryColumn(),
                schemaOptions.getVersionColumn(),
                schemaOptions.getVectorColumn());
    }

    public static SchemaOuterClass.SchemaOptions ToProtobuf(SchemaOptions schemaOptions) {
        return SchemaOuterClass.SchemaOptions.newBuilder()
                .setPrimaryColumn(schemaOptions.getPrimaryColumn())
                .setVersionColumn(schemaOptions.getVersionColumn())
                .setVectorColumn(schemaOptions.getVectorColumn())
                .build();
    }

    public void Validate(org.apache.arrow.vector.types.pojo.Schema schema) throws SchemaException {
        if (this.primaryColumn != "") {
            org.apache.arrow.vector.types.pojo.Field primaryField = schema.findField(this.primaryColumn);
            if (primaryField == null) {
                throw new SchemaException(ErrPrimaryColumnNotFound);
            } else if (primaryField.getType().getTypeID() != Utf8
                    && primaryField.getType().getTypeID() != Int) {
                throw new SchemaException(ErrPrimaryColumnType);
            }
        } else {
            throw new SchemaException(ErrPrimaryColumnEmpty);
        }

        if (this.versionColumn != "") {
            org.apache.arrow.vector.types.pojo.Field versionField = schema.findField(this.versionColumn);
            if (versionField == null) {
                throw new SchemaException(ErrVersionColumnNotFound);
            } else if (versionField.getType().getTypeID() != Int) {
                throw new SchemaException(ErrVersionColumnType);
            }
        }

        if (this.vectorColumn != "") {
            org.apache.arrow.vector.types.pojo.Field vectorField = schema.findField(this.vectorColumn);
            if (vectorField == null) {
                throw new SchemaException(ErrVectorColumnNotFound);
            } else if (vectorField.getType().getTypeID() != FixedSizeBinary
                    && vectorField.getType().getTypeID() != FixedSizeList) {
                throw new SchemaException(ErrVectorColumnType);
            }
        } else {
            throw new SchemaException(ErrVectorColumnEmpty);
        }

        return;
    }

    public Boolean HasVersionColumn() {
        return this.versionColumn != "";
    }
}
