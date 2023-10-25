package io.milvus.storage.file.fragment;

import lombok.*;
import schema_proto.SchemaOuterClass;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@ToString
@AllArgsConstructor
public class DeleteFragment {
    private Long id;
    private SchemaOuterClass.Schema schema;
    private Map<Object, List<Long>> data;
//    private String fs;
}

