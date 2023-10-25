package io.milvus.storage.storage.schema;

//
//        func (s *Schema) FromProtobuf(schema *schema_proto.Schema) error {
//        schemaType, err := utils.FromProtobufSchema(schema.ArrowSchema)
//        if err != nil {
//        return err
//        }
//
//        s.schema = schemaType
//        s.options.FromProtobuf(schema.GetSchemaOptions())
//        s.BuildScalarSchema()
//        s.BuildVectorSchema()
//        s.BuildDeleteSchema()
//        return nil
//        }
//
//        func (s *Schema) ToProtobuf() (*schema_proto.Schema, error) {
//        schema := &schema_proto.Schema{}
//        arrowSchema, err := utils.ToProtobufSchema(s.schema)
//        if err != nil {
//        return nil, err
//        }
//        schema.ArrowSchema = arrowSchema
//        schema.SchemaOptions = s.options.ToProtobuf()
//        return schema, nil
//        }
//
//        func (s *Schema) BuildScalarSchema() error {
//        fields := make([]arrow.Field, 0, len(s.schema.Fields()))
//        for _, field := range s.schema.Fields() {
//        if field.Name == s.options.VectorColumn {
//        continue
//        }
//        fields = append(fields, field)
//        }
//        offsetFiled := arrow.Field{Name: constant.OffsetFieldName, Type: arrow.DataType(&arrow.Int64Type{})}
//        fields = append(fields, offsetFiled)
//        s.scalarSchema = arrow.NewSchema(fields, nil)
//
//        return nil
//        }
//
//        func (s *Schema) BuildVectorSchema() error {
//        fields := make([]arrow.Field, 0, len(s.schema.Fields()))
//        for _, field := range s.schema.Fields() {
//        if field.Name == s.options.VectorColumn ||
//        field.Name == s.options.PrimaryColumn ||
//        field.Name == s.options.VersionColumn {
//        fields = append(fields, field)
//        }
//        }
//        s.vectorSchema = arrow.NewSchema(fields, nil)
//
//        return nil
//        }
//
//        func (s *Schema) BuildDeleteSchema() error {
//        pkColumn, ok := s.schema.FieldsByName(s.options.PrimaryColumn)
//        if !ok {
//        return ErrPrimaryColumnNotFound
//        }
//        versionField, ok := s.schema.FieldsByName(s.options.VersionColumn)
//        if !ok {
//        return ErrPrimaryColumnNotFound
//        }
//        fields := make([]arrow.Field, 0, 2)
//        fields = append(fields, pkColumn[0])
//        fields = append(fields, versionField[0])
//        s.deleteSchema = arrow.NewSchema(fields, nil)
//        return nil
//        }

import io.milvus.storage.common.utils.Utils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import schema_proto.SchemaOuterClass;

import javax.rmi.CORBA.Util;
import java.util.ArrayList;
import java.util.List;

import static io.milvus.storage.common.constant.Constant.OffsetFieldName;
import static io.milvus.storage.storage.schema.SchemaException.ErrPrimaryColumnNotFound;
import static io.milvus.storage.storage.schema.SchemaException.ErrVersionColumnNotFound;

@Getter
@ToString
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

    public void FromProtobuf(SchemaOuterClass.Schema schema) throws Exception {
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = Utils.FromProtobufSchema(schema.getArrowSchema());
        this.schema = arrowSchema;
        this.options = SchemaOptions.FromProtobuf(schema.getSchemaOptions());
        this.BuildScalarSchema();
        this.BuildVectorSchema();
        this.BuildDeleteSchema();
        return;
    }

    public SchemaOuterClass.Schema ToProtobuf(Schema schema) throws Exception {
        SchemaOuterClass.Schema.Builder builder = SchemaOuterClass.Schema.newBuilder();
        builder.setArrowSchema(Utils.ToProtobufSchema(this.schema));
        builder.setSchemaOptions(SchemaOptions.ToProtobuf(this.options));
        return builder.build();
    }
}
