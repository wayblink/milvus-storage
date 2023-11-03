package io.milvus.storage.storage.schema;

import lombok.Getter;
import lombok.ToString;
import schema_proto.SchemaOuterClass;

import static io.milvus.storage.storage.schema.SchemaException.*;
import static org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.*;

@Getter
@ToString
public class SchemaOptions {
    private String primaryColumn;
    private String versionColumn;
    private String vectorColumn;

    public SchemaOptions(String primaryColumn, String versionColumn, String vectorColumn) {
        this.primaryColumn = primaryColumn;
        this.versionColumn = versionColumn;
        this.vectorColumn = vectorColumn;
    }

    public static final class Builder {
        private String primaryColumn;
        private String versionColumn;
        private String vectorColumn;

        public SchemaOptions.Builder setPrimaryColumn(String column) {
            this.primaryColumn = column;
            return this;
        }

        public SchemaOptions.Builder setVersionColumn(String column) {
            this.versionColumn = column;
            return this;
        }

        public SchemaOptions.Builder setVectorColumn(String column) {
            this.vectorColumn = column;
            return this;
        }

        public SchemaOptions build() {
            return new SchemaOptions(primaryColumn, versionColumn, vectorColumn);
        }

    }


    public static SchemaOptions DefaultSchemaOptions() {
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
            org.apache.arrow.vector.types.pojo.Field primaryField = schema.findField(this.getPrimaryColumn());
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
