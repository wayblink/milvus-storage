package io.milvus.storage.common.utils;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import schema_proto.SchemaOuterClass;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.arrow.vector.types.FloatingPointPrecision.*;

public class Utils {

    // proto:
    // Schema -> ArrowSchema -> fields Field -> name
    //                                       -> nullable Boolean
    //                                       -> dataType DataType -> LogicalType enum
    //                                                            -> children
    //                                                            -> type_related_values
    //                                       -> metadata KeyValueMetadata
    //                       -> metadata KeyValueMetadata
    //        -> SchemaOptions

    /**
     * arrow:
     *  Schema -> Schema -> fields Field -> name String
     *                                   -> fieldType FieldType -> type ArrowType -> ArrowTypeID enum
     *                                                          -> nullable Boolean
     *                                                          -> metadata Map<String,String>
     *                                                          -> dictionary DictionaryEncoding
     *                                   -> children Field
     *                   -> metadata Map<String,String>
     */

    public static SchemaOuterClass.ArrowSchema ToProtobufSchema(Schema schema) throws Exception {
        SchemaOuterClass.ArrowSchema.Builder builder = SchemaOuterClass.ArrowSchema.newBuilder();
        schema.getFields().forEach(field -> builder.addFields(ToProtobufField(field)));

        // todo Arrow.Schema does not have endianness, check it is right
        boolean littleEndian = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
        if (littleEndian) {
            builder.setEndiannessValue(SchemaOuterClass.Endianness.Little_VALUE);
        } else {
            builder.setEndiannessValue(SchemaOuterClass.Endianness.Big_VALUE);
        }

        builder.setMetadata(ToProtobufMetadata(schema.getCustomMetadata()));
        return builder.build();
    }


    public static Field FromProtobufField(SchemaOuterClass.Field field) throws Exception {
        ArrowType arrowType = FromProtobufDataType(field.getDataType());
        FieldType fieldType = new FieldType(field.getNullable(), arrowType, null, FromProtobufMetadata(field.getMetadata()));
        return new Field(field.getName(), fieldType, null);
    }

    public static ArrowType FromProtobufDataType(SchemaOuterClass.DataType dataType) throws Exception {
       switch (dataType.getLogicType()) {
           case NA:
               return new ArrowType.Null();
           case BOOL:
               return new ArrowType.Bool();
           case UINT8:
               return new ArrowType.Int(8, false);
           case INT8:
               return new ArrowType.Int(8, true);
           case UINT16:
               return new ArrowType.Int(16, false);
           case INT16:
               return new ArrowType.Int(16, true);
           case UINT32:
               return new ArrowType.Int(32, false);
           case INT32:
               return new ArrowType.Int(32, true);
           case UINT64:
               return new ArrowType.Int(64, false);
           case INT64:
               return new ArrowType.Int(64, true);
           case HALF_FLOAT:
               return new ArrowType.FloatingPoint(HALF);
           case FLOAT:
               return new ArrowType.FloatingPoint(SINGLE);
           case DOUBLE:
               return new ArrowType.FloatingPoint(DOUBLE);
           case STRING:
               return new ArrowType.Utf8();
           case BINARY:
               return new ArrowType.Binary();
           // todo fix me complex datatype parse
           case FIXED_SIZE_BINARY:
               return new ArrowType.FixedSizeBinary(dataType.getFixedSizeBinaryType().getByteWidth());
           case FIXED_SIZE_LIST:
               return new ArrowType.FixedSizeList(dataType.getFixedSizeListType().getListSize());
           case LIST:
               return new ArrowType.List();
           case STRUCT:
               return new ArrowType.Struct();
           case MAP:
               return new ArrowType.Map(dataType.getMapType().getKeysSorted());
           case DICTIONARY:
           default:
               throw new Exception("Unsupported protobuf datatype: " + dataType.getLogicType().toString());
       }
    }

    public static SchemaOuterClass.Field ToProtobufField(Field field) {
        SchemaOuterClass.Field.Builder builder = SchemaOuterClass.Field.newBuilder();
        /**
         * message Field {
         *   string name = 1;
         *   bool nullable = 2;
         *   DataType data_type = 3;
         *   KeyValueMetadata metadata = 4;
         * }
         */

        // name
        builder.setName(field.getName());
        // nullable
        builder.setNullable(field.getFieldType().isNullable());
        // metadata
        builder.setMetadata(ToProtobufMetadata(field.getMetadata()));

        // data_type
        /**
         * message DataType {
         *   oneof type_related_values {
         *     FixedSizeBinaryType fixed_size_binary_type = 1;
         *     FixedSizeListType fixed_size_list_type = 2;
         *     DictionaryType dictionary_type = 3;
         *     MapType map_type = 4;
         *   }
         *   LogicType logic_type = 100;
         *   repeated Field children = 101;
         * }
         */
        SchemaOuterClass.DataType.Builder dataTypeBuilder = SchemaOuterClass.DataType.newBuilder();
        // logic_type
        dataTypeBuilder.setLogicType(ToProtobufType(field.getFieldType().getType()));
        // children
        field.getChildren().forEach(child -> dataTypeBuilder.addChildren(ToProtobufField(child)));
        // type_related_values
        SetTypeRelatedValues(dataTypeBuilder, field.getFieldType().getType());

        // todo Dictionary support

//        DictionaryEncoding dictionary = field.getFieldType().getDictionary();
//        SchemaOuterClass.DictionaryType.Builder dictionaryTypeBuilder = SchemaOuterClass.DictionaryType.newBuilder().setOrdered(dictionary.isOrdered());
//        dataTypeBuilder.setDictionaryType(dictionaryTypeBuilder);

        SchemaOuterClass.DataType dataType = dataTypeBuilder.build();
        builder.setDataType(dataType);

        return builder.build();
    }

    public static SchemaOuterClass.DataType ToProtobufDataType(ArrowType dataType) throws Exception {
        SchemaOuterClass.DataType.Builder builder = SchemaOuterClass.DataType.newBuilder();
//        builder.setTypeVa
        SetTypeRelatedValues(builder, dataType);
        builder.setLogicType(ToProtobufType(dataType));
        //if len(GetFields(dataType)) > 0 {
        ////		for _, field := range GetFields(dataType) {
        ////			protoField := &schema_proto.Field{}
        ////			protoFieldType, err := ToProtobufField(&field)
        ////			if err != nil {
        ////				return nil, err
        ////			}
        ////			protoField = protoFieldType
        ////			protoType.Children = append(protoType.Children, protoField)
        ////		}
        ////	}
        return null;
    }

    public static SchemaOuterClass.LogicType ToProtobufType(ArrowType dataType) {
       return SchemaOuterClass.LogicType.valueOf(dataType.getTypeID().name());
    }

    public static void SetTypeRelatedValues(SchemaOuterClass.DataType.Builder protoDataTypeBuilder, ArrowType dataType) {
        switch (dataType.getTypeID()) {
            case FixedSizeBinary:
                ArrowType.FixedSizeBinary fixedSizeBinary = (ArrowType.FixedSizeBinary)dataType;
                SchemaOuterClass.FixedSizeBinaryType fixedSizeBinaryType =
                        SchemaOuterClass.FixedSizeBinaryType.newBuilder()
                                .setByteWidth(fixedSizeBinary.getByteWidth())
                                .build();
                protoDataTypeBuilder.setFixedSizeBinaryType(fixedSizeBinaryType);
                break;
            case FixedSizeList:
                ArrowType.FixedSizeList fixedSizeList = (ArrowType.FixedSizeList)dataType;
                SchemaOuterClass.FixedSizeListType fixedSizeListType =
                        SchemaOuterClass.FixedSizeListType.newBuilder()
                                .setListSize(fixedSizeList.getListSize())
                                .build();
                protoDataTypeBuilder.setFixedSizeListType(fixedSizeListType);
                break;
            case Map:
                ArrowType.Map map = (ArrowType.Map)dataType;
                SchemaOuterClass.MapType mapType =
                        SchemaOuterClass.MapType.newBuilder()
                                .setKeysSorted(map.getKeysSorted())
                                .build();
                protoDataTypeBuilder.setMapType(mapType);
                break;
            // case Dictionary：
            default:
                return;
        }
    }

//    public static List<Field> GetFields(ArrowType dataType) {}

    public static SchemaOuterClass.KeyValueMetadata ToProtobufMetadata(Map<String, String> metaData) {
        List<String> keys = new ArrayList<>();
        List<String> values = new ArrayList<>();
        metaData.keySet().forEach(key -> {
            keys.add(key);
            values.add(metaData.get(key));
        });
        return SchemaOuterClass.KeyValueMetadata.newBuilder()
                .addAllKeys(keys)
                .addAllValues(values)
                .build();
    }

    public static Map<String, String> FromProtobufMetadata(SchemaOuterClass.KeyValueMetadata metaData) {
        Map<String, String> map = new HashMap<>();

        for (int i = 0; i < metaData.getKeysList().size(); i++) {
            map.put(metaData.getKeys(i), metaData.getValues(i));
        }
        return map;
    }

    public static Schema FromProtobufSchema(SchemaOuterClass.ArrowSchema schema) throws Exception {
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            fields.add(FromProtobufField(schema.getFields(i)));
        }
        return new Schema(fields, FromProtobufMetadata(schema.getMetadata()));
    }
}
