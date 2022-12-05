/**
 * Autogenerated by Thrift Compiler (0.17.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package team.dsys.dssearch.rpc;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.17.0)", date = "2022-12-04")
public class CommonResponse implements org.apache.thrift.TBase<CommonResponse, CommonResponse._Fields>, java.io.Serializable, Cloneable, Comparable<CommonResponse> {
    private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("CommonResponse");

    private static final org.apache.thrift.protocol.TField SUCCESS_FIELD_DESC = new org.apache.thrift.protocol.TField("success", org.apache.thrift.protocol.TType.BOOL, (short)1);
    private static final org.apache.thrift.protocol.TField MSG_FIELD_DESC = new org.apache.thrift.protocol.TField("msg", org.apache.thrift.protocol.TType.STRING, (short)2);

    private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new CommonResponseStandardSchemeFactory();
    private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new CommonResponseTupleSchemeFactory();

    public boolean success; // required
    public @org.apache.thrift.annotation.Nullable java.lang.String msg; // required

    /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
    public enum _Fields implements org.apache.thrift.TFieldIdEnum {
        SUCCESS((short)1, "success"),
        MSG((short)2, "msg");

        private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

        static {
            for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
                byName.put(field.getFieldName(), field);
            }
        }

        /**
         * Find the _Fields constant that matches fieldId, or null if its not found.
         */
        @org.apache.thrift.annotation.Nullable
        public static _Fields findByThriftId(int fieldId) {
            switch(fieldId) {
                case 1: // SUCCESS
                    return SUCCESS;
                case 2: // MSG
                    return MSG;
                default:
                    return null;
            }
        }

        /**
         * Find the _Fields constant that matches fieldId, throwing an exception
         * if it is not found.
         */
        public static _Fields findByThriftIdOrThrow(int fieldId) {
            _Fields fields = findByThriftId(fieldId);
            if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
            return fields;
        }

        /**
         * Find the _Fields constant that matches name, or null if its not found.
         */
        @org.apache.thrift.annotation.Nullable
        public static _Fields findByName(java.lang.String name) {
            return byName.get(name);
        }

        private final short _thriftId;
        private final java.lang.String _fieldName;

        _Fields(short thriftId, java.lang.String fieldName) {
            _thriftId = thriftId;
            _fieldName = fieldName;
        }

        @Override
        public short getThriftFieldId() {
            return _thriftId;
        }

        @Override
        public java.lang.String getFieldName() {
            return _fieldName;
        }
    }

    // isset id assignments
    private static final int __SUCCESS_ISSET_ID = 0;
    private byte __isset_bitfield = 0;
    public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
    static {
        java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
        tmpMap.put(_Fields.SUCCESS, new org.apache.thrift.meta_data.FieldMetaData("success", org.apache.thrift.TFieldRequirementType.DEFAULT,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
        tmpMap.put(_Fields.MSG, new org.apache.thrift.meta_data.FieldMetaData("msg", org.apache.thrift.TFieldRequirementType.DEFAULT,
                new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
        metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
        org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(CommonResponse.class, metaDataMap);
    }

    public CommonResponse() {
    }

    public CommonResponse(
            boolean success,
            java.lang.String msg)
    {
        this();
        this.success = success;
        setSuccessIsSet(true);
        this.msg = msg;
    }

    /**
     * Performs a deep copy on <i>other</i>.
     */
    public CommonResponse(CommonResponse other) {
        __isset_bitfield = other.__isset_bitfield;
        this.success = other.success;
        if (other.isSetMsg()) {
            this.msg = other.msg;
        }
    }

    @Override
    public CommonResponse deepCopy() {
        return new CommonResponse(this);
    }

    @Override
    public void clear() {
        setSuccessIsSet(false);
        this.success = false;
        this.msg = null;
    }

    public boolean isSuccess() {
        return this.success;
    }

    public CommonResponse setSuccess(boolean success) {
        this.success = success;
        setSuccessIsSet(true);
        return this;
    }

    public void unsetSuccess() {
        __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __SUCCESS_ISSET_ID);
    }

    /** Returns true if field success is set (has been assigned a value) and false otherwise */
    public boolean isSetSuccess() {
        return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __SUCCESS_ISSET_ID);
    }

    public void setSuccessIsSet(boolean value) {
        __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __SUCCESS_ISSET_ID, value);
    }

    @org.apache.thrift.annotation.Nullable
    public java.lang.String getMsg() {
        return this.msg;
    }

    public CommonResponse setMsg(@org.apache.thrift.annotation.Nullable java.lang.String msg) {
        this.msg = msg;
        return this;
    }

    public void unsetMsg() {
        this.msg = null;
    }

    /** Returns true if field msg is set (has been assigned a value) and false otherwise */
    public boolean isSetMsg() {
        return this.msg != null;
    }

    public void setMsgIsSet(boolean value) {
        if (!value) {
            this.msg = null;
        }
    }

    @Override
    public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
        switch (field) {
            case SUCCESS:
                if (value == null) {
                    unsetSuccess();
                } else {
                    setSuccess((java.lang.Boolean)value);
                }
                break;

            case MSG:
                if (value == null) {
                    unsetMsg();
                } else {
                    setMsg((java.lang.String)value);
                }
                break;

        }
    }

    @org.apache.thrift.annotation.Nullable
    @Override
    public java.lang.Object getFieldValue(_Fields field) {
        switch (field) {
            case SUCCESS:
                return isSuccess();

            case MSG:
                return getMsg();

        }
        throw new java.lang.IllegalStateException();
    }

    /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
    @Override
    public boolean isSet(_Fields field) {
        if (field == null) {
            throw new java.lang.IllegalArgumentException();
        }

        switch (field) {
            case SUCCESS:
                return isSetSuccess();
            case MSG:
                return isSetMsg();
        }
        throw new java.lang.IllegalStateException();
    }

    @Override
    public boolean equals(java.lang.Object that) {
        if (that instanceof CommonResponse)
            return this.equals((CommonResponse)that);
        return false;
    }

    public boolean equals(CommonResponse that) {
        if (that == null)
            return false;
        if (this == that)
            return true;

        boolean this_present_success = true;
        boolean that_present_success = true;
        if (this_present_success || that_present_success) {
            if (!(this_present_success && that_present_success))
                return false;
            if (this.success != that.success)
                return false;
        }

        boolean this_present_msg = true && this.isSetMsg();
        boolean that_present_msg = true && that.isSetMsg();
        if (this_present_msg || that_present_msg) {
            if (!(this_present_msg && that_present_msg))
                return false;
            if (!this.msg.equals(that.msg))
                return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int hashCode = 1;

        hashCode = hashCode * 8191 + ((success) ? 131071 : 524287);

        hashCode = hashCode * 8191 + ((isSetMsg()) ? 131071 : 524287);
        if (isSetMsg())
            hashCode = hashCode * 8191 + msg.hashCode();

        return hashCode;
    }

    @Override
    public int compareTo(CommonResponse other) {
        if (!getClass().equals(other.getClass())) {
            return getClass().getName().compareTo(other.getClass().getName());
        }

        int lastComparison = 0;

        lastComparison = java.lang.Boolean.compare(isSetSuccess(), other.isSetSuccess());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetSuccess()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.success, other.success);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        lastComparison = java.lang.Boolean.compare(isSetMsg(), other.isSetMsg());
        if (lastComparison != 0) {
            return lastComparison;
        }
        if (isSetMsg()) {
            lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.msg, other.msg);
            if (lastComparison != 0) {
                return lastComparison;
            }
        }
        return 0;
    }

    @org.apache.thrift.annotation.Nullable
    @Override
    public _Fields fieldForId(int fieldId) {
        return _Fields.findByThriftId(fieldId);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
        scheme(iprot).read(iprot, this);
    }

    @Override
    public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
        scheme(oprot).write(oprot, this);
    }

    @Override
    public java.lang.String toString() {
        java.lang.StringBuilder sb = new java.lang.StringBuilder("CommonResponse(");
        boolean first = true;

        sb.append("success:");
        sb.append(this.success);
        first = false;
        if (!first) sb.append(", ");
        sb.append("msg:");
        if (this.msg == null) {
            sb.append("null");
        } else {
            sb.append(this.msg);
        }
        first = false;
        sb.append(")");
        return sb.toString();
    }

    public void validate() throws org.apache.thrift.TException {
        // check for required fields
        // check for sub-struct validity
    }

    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
        try {
            write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
        } catch (org.apache.thrift.TException te) {
            throw new java.io.IOException(te);
        }
    }

    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
        try {
            // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
            __isset_bitfield = 0;
            read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
        } catch (org.apache.thrift.TException te) {
            throw new java.io.IOException(te);
        }
    }

    private static class CommonResponseStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
        @Override
        public CommonResponseStandardScheme getScheme() {
            return new CommonResponseStandardScheme();
        }
    }

    private static class CommonResponseStandardScheme extends org.apache.thrift.scheme.StandardScheme<CommonResponse> {

        @Override
        public void read(org.apache.thrift.protocol.TProtocol iprot, CommonResponse struct) throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TField schemeField;
            iprot.readStructBegin();
            while (true)
            {
                schemeField = iprot.readFieldBegin();
                if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
                    break;
                }
                switch (schemeField.id) {
                    case 1: // SUCCESS
                        if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
                            struct.success = iprot.readBool();
                            struct.setSuccessIsSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    case 2: // MSG
                        if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
                            struct.msg = iprot.readString();
                            struct.setMsgIsSet(true);
                        } else {
                            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                        }
                        break;
                    default:
                        org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
                }
                iprot.readFieldEnd();
            }
            iprot.readStructEnd();

            // check for required fields of primitive type, which can't be checked in the validate method
            struct.validate();
        }

        @Override
        public void write(org.apache.thrift.protocol.TProtocol oprot, CommonResponse struct) throws org.apache.thrift.TException {
            struct.validate();

            oprot.writeStructBegin(STRUCT_DESC);
            oprot.writeFieldBegin(SUCCESS_FIELD_DESC);
            oprot.writeBool(struct.success);
            oprot.writeFieldEnd();
            if (struct.msg != null) {
                oprot.writeFieldBegin(MSG_FIELD_DESC);
                oprot.writeString(struct.msg);
                oprot.writeFieldEnd();
            }
            oprot.writeFieldStop();
            oprot.writeStructEnd();
        }

    }

    private static class CommonResponseTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
        @Override
        public CommonResponseTupleScheme getScheme() {
            return new CommonResponseTupleScheme();
        }
    }

    private static class CommonResponseTupleScheme extends org.apache.thrift.scheme.TupleScheme<CommonResponse> {

        @Override
        public void write(org.apache.thrift.protocol.TProtocol prot, CommonResponse struct) throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
            java.util.BitSet optionals = new java.util.BitSet();
            if (struct.isSetSuccess()) {
                optionals.set(0);
            }
            if (struct.isSetMsg()) {
                optionals.set(1);
            }
            oprot.writeBitSet(optionals, 2);
            if (struct.isSetSuccess()) {
                oprot.writeBool(struct.success);
            }
            if (struct.isSetMsg()) {
                oprot.writeString(struct.msg);
            }
        }

        @Override
        public void read(org.apache.thrift.protocol.TProtocol prot, CommonResponse struct) throws org.apache.thrift.TException {
            org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
            java.util.BitSet incoming = iprot.readBitSet(2);
            if (incoming.get(0)) {
                struct.success = iprot.readBool();
                struct.setSuccessIsSet(true);
            }
            if (incoming.get(1)) {
                struct.msg = iprot.readString();
                struct.setMsgIsSet(true);
            }
        }
    }

    private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
        return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
    }
}

