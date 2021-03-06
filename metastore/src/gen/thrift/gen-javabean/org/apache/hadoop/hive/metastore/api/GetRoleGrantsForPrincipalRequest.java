/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
@org.apache.hadoop.hive.common.classification.InterfaceAudience.Public @org.apache.hadoop.hive.common.classification.InterfaceStability.Stable public class GetRoleGrantsForPrincipalRequest implements org.apache.thrift.TBase<GetRoleGrantsForPrincipalRequest, GetRoleGrantsForPrincipalRequest._Fields>, java.io.Serializable, Cloneable, Comparable<GetRoleGrantsForPrincipalRequest> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("GetRoleGrantsForPrincipalRequest");

  private static final org.apache.thrift.protocol.TField PRINCIPAL_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("principal_name", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField PRINCIPAL_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField("principal_type", org.apache.thrift.protocol.TType.I32, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new GetRoleGrantsForPrincipalRequestStandardSchemeFactory());
    schemes.put(TupleScheme.class, new GetRoleGrantsForPrincipalRequestTupleSchemeFactory());
  }

  private String principal_name; // required
  private PrincipalType principal_type; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    PRINCIPAL_NAME((short)1, "principal_name"),
    /**
     * 
     * @see PrincipalType
     */
    PRINCIPAL_TYPE((short)2, "principal_type");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // PRINCIPAL_NAME
          return PRINCIPAL_NAME;
        case 2: // PRINCIPAL_TYPE
          return PRINCIPAL_TYPE;
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
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.PRINCIPAL_NAME, new org.apache.thrift.meta_data.FieldMetaData("principal_name", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.PRINCIPAL_TYPE, new org.apache.thrift.meta_data.FieldMetaData("principal_type", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.EnumMetaData(org.apache.thrift.protocol.TType.ENUM, PrincipalType.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(GetRoleGrantsForPrincipalRequest.class, metaDataMap);
  }

  public GetRoleGrantsForPrincipalRequest() {
  }

  public GetRoleGrantsForPrincipalRequest(
    String principal_name,
    PrincipalType principal_type)
  {
    this();
    this.principal_name = principal_name;
    this.principal_type = principal_type;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public GetRoleGrantsForPrincipalRequest(GetRoleGrantsForPrincipalRequest other) {
    if (other.isSetPrincipal_name()) {
      this.principal_name = other.principal_name;
    }
    if (other.isSetPrincipal_type()) {
      this.principal_type = other.principal_type;
    }
  }

  public GetRoleGrantsForPrincipalRequest deepCopy() {
    return new GetRoleGrantsForPrincipalRequest(this);
  }

  @Override
  public void clear() {
    this.principal_name = null;
    this.principal_type = null;
  }

  public String getPrincipal_name() {
    return this.principal_name;
  }

  public void setPrincipal_name(String principal_name) {
    this.principal_name = principal_name;
  }

  public void unsetPrincipal_name() {
    this.principal_name = null;
  }

  /** Returns true if field principal_name is set (has been assigned a value) and false otherwise */
  public boolean isSetPrincipal_name() {
    return this.principal_name != null;
  }

  public void setPrincipal_nameIsSet(boolean value) {
    if (!value) {
      this.principal_name = null;
    }
  }

  /**
   * 
   * @see PrincipalType
   */
  public PrincipalType getPrincipal_type() {
    return this.principal_type;
  }

  /**
   * 
   * @see PrincipalType
   */
  public void setPrincipal_type(PrincipalType principal_type) {
    this.principal_type = principal_type;
  }

  public void unsetPrincipal_type() {
    this.principal_type = null;
  }

  /** Returns true if field principal_type is set (has been assigned a value) and false otherwise */
  public boolean isSetPrincipal_type() {
    return this.principal_type != null;
  }

  public void setPrincipal_typeIsSet(boolean value) {
    if (!value) {
      this.principal_type = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case PRINCIPAL_NAME:
      if (value == null) {
        unsetPrincipal_name();
      } else {
        setPrincipal_name((String)value);
      }
      break;

    case PRINCIPAL_TYPE:
      if (value == null) {
        unsetPrincipal_type();
      } else {
        setPrincipal_type((PrincipalType)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case PRINCIPAL_NAME:
      return getPrincipal_name();

    case PRINCIPAL_TYPE:
      return getPrincipal_type();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case PRINCIPAL_NAME:
      return isSetPrincipal_name();
    case PRINCIPAL_TYPE:
      return isSetPrincipal_type();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof GetRoleGrantsForPrincipalRequest)
      return this.equals((GetRoleGrantsForPrincipalRequest)that);
    return false;
  }

  public boolean equals(GetRoleGrantsForPrincipalRequest that) {
    if (that == null)
      return false;

    boolean this_present_principal_name = true && this.isSetPrincipal_name();
    boolean that_present_principal_name = true && that.isSetPrincipal_name();
    if (this_present_principal_name || that_present_principal_name) {
      if (!(this_present_principal_name && that_present_principal_name))
        return false;
      if (!this.principal_name.equals(that.principal_name))
        return false;
    }

    boolean this_present_principal_type = true && this.isSetPrincipal_type();
    boolean that_present_principal_type = true && that.isSetPrincipal_type();
    if (this_present_principal_type || that_present_principal_type) {
      if (!(this_present_principal_type && that_present_principal_type))
        return false;
      if (!this.principal_type.equals(that.principal_type))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_principal_name = true && (isSetPrincipal_name());
    list.add(present_principal_name);
    if (present_principal_name)
      list.add(principal_name);

    boolean present_principal_type = true && (isSetPrincipal_type());
    list.add(present_principal_type);
    if (present_principal_type)
      list.add(principal_type.getValue());

    return list.hashCode();
  }

  @Override
  public int compareTo(GetRoleGrantsForPrincipalRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetPrincipal_name()).compareTo(other.isSetPrincipal_name());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPrincipal_name()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.principal_name, other.principal_name);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPrincipal_type()).compareTo(other.isSetPrincipal_type());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPrincipal_type()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.principal_type, other.principal_type);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("GetRoleGrantsForPrincipalRequest(");
    boolean first = true;

    sb.append("principal_name:");
    if (this.principal_name == null) {
      sb.append("null");
    } else {
      sb.append(this.principal_name);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("principal_type:");
    if (this.principal_type == null) {
      sb.append("null");
    } else {
      sb.append(this.principal_type);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetPrincipal_name()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'principal_name' is unset! Struct:" + toString());
    }

    if (!isSetPrincipal_type()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'principal_type' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class GetRoleGrantsForPrincipalRequestStandardSchemeFactory implements SchemeFactory {
    public GetRoleGrantsForPrincipalRequestStandardScheme getScheme() {
      return new GetRoleGrantsForPrincipalRequestStandardScheme();
    }
  }

  private static class GetRoleGrantsForPrincipalRequestStandardScheme extends StandardScheme<GetRoleGrantsForPrincipalRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, GetRoleGrantsForPrincipalRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // PRINCIPAL_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.principal_name = iprot.readString();
              struct.setPrincipal_nameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // PRINCIPAL_TYPE
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.principal_type = org.apache.hadoop.hive.metastore.api.PrincipalType.findByValue(iprot.readI32());
              struct.setPrincipal_typeIsSet(true);
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
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, GetRoleGrantsForPrincipalRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.principal_name != null) {
        oprot.writeFieldBegin(PRINCIPAL_NAME_FIELD_DESC);
        oprot.writeString(struct.principal_name);
        oprot.writeFieldEnd();
      }
      if (struct.principal_type != null) {
        oprot.writeFieldBegin(PRINCIPAL_TYPE_FIELD_DESC);
        oprot.writeI32(struct.principal_type.getValue());
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class GetRoleGrantsForPrincipalRequestTupleSchemeFactory implements SchemeFactory {
    public GetRoleGrantsForPrincipalRequestTupleScheme getScheme() {
      return new GetRoleGrantsForPrincipalRequestTupleScheme();
    }
  }

  private static class GetRoleGrantsForPrincipalRequestTupleScheme extends TupleScheme<GetRoleGrantsForPrincipalRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, GetRoleGrantsForPrincipalRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.principal_name);
      oprot.writeI32(struct.principal_type.getValue());
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, GetRoleGrantsForPrincipalRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.principal_name = iprot.readString();
      struct.setPrincipal_nameIsSet(true);
      struct.principal_type = org.apache.hadoop.hive.metastore.api.PrincipalType.findByValue(iprot.readI32());
      struct.setPrincipal_typeIsSet(true);
    }
  }

}

