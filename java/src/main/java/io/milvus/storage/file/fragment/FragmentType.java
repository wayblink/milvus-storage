package io.milvus.storage.file.fragment;

enum FragmentType {
    Unknown(0), Data(1), Delete(2);    //    调用构造函数来构造枚举项

    private int value = 0;

    private FragmentType(int value) {    //    必须是private的，否则编译错误
        this.value = value;
    }

    public static FragmentType valueOf(int value) {    //    手写的从int到enum的转换函数
        switch (value) {
            case 0:
                return Unknown;
            case 1:
                return Data;
            case 2:
                return Delete;
            default:
                return null;
        }
    }

    public int value() {
        return this.value;
    }
}
