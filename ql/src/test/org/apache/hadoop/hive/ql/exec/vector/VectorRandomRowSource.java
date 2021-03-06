/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.vector;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.RandomTypeUtil;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveIntervalDayTimeObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveIntervalYearMonthObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hive.common.util.DateUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

import com.google.common.base.Preconditions;
import com.google.common.base.Charsets;

/**
 * Generate object inspector and random row object[].
 */
public class VectorRandomRowSource {

  private Random r;

  private int columnCount;

  private List<String> typeNames;

  private Category[] categories;

  private TypeInfo[] typeInfos;

  private List<ObjectInspector> objectInspectorList;

  // Primitive.

  private PrimitiveCategory[] primitiveCategories;

  private PrimitiveTypeInfo[] primitiveTypeInfos;

  private List<ObjectInspector> primitiveObjectInspectorList;

  private StructObjectInspector rowStructObjectInspector;

  private List<GenerationSpec> generationSpecList;

  private String[] alphabets;

  private boolean allowNull;

  private boolean addEscapables;
  private String needsEscapeStr;

  public static class StringGenerationOption {

    private boolean generateSentences;
    private boolean addPadding;

    public StringGenerationOption(boolean generateSentences, boolean addPadding) {
      this.generateSentences = generateSentences;
      this.addPadding = addPadding;
    }

    public boolean getGenerateSentences() {
      return generateSentences;
    }

    public boolean getAddPadding() {
      return addPadding;
    }
  }

  public static class GenerationSpec {

    public static enum GenerationKind {
      SAME_TYPE,
      OMIT_GENERATION,
      STRING_FAMILY,
      STRING_FAMILY_OTHER_TYPE_VALUE,
      TIMESTAMP_MILLISECONDS
    }

    private final GenerationKind generationKind;
    private final TypeInfo typeInfo;
    private final TypeInfo sourceTypeInfo;
    private final StringGenerationOption stringGenerationOption;

    private GenerationSpec(GenerationKind generationKind, TypeInfo typeInfo,
        TypeInfo sourceTypeInfo, StringGenerationOption stringGenerationOption) {
      this.generationKind = generationKind;
      this.typeInfo = typeInfo;
      this.sourceTypeInfo = sourceTypeInfo;
      this.stringGenerationOption = stringGenerationOption;
    }

    public GenerationKind getGenerationKind() {
      return generationKind;
    }

    public TypeInfo getTypeInfo() {
      return typeInfo;
    }

    public TypeInfo getSourceTypeInfo() {
      return sourceTypeInfo;
    }

    public StringGenerationOption getStringGenerationOption() {
      return stringGenerationOption;
    }

    public static GenerationSpec createSameType(TypeInfo typeInfo) {
      return new GenerationSpec(
          GenerationKind.SAME_TYPE, typeInfo, null, null);
    }

    public static GenerationSpec createOmitGeneration(TypeInfo typeInfo) {
      return new GenerationSpec(
          GenerationKind.OMIT_GENERATION, typeInfo, null, null);
    }

    public static GenerationSpec createStringFamily(TypeInfo typeInfo,
        StringGenerationOption stringGenerationOption) {
      return new GenerationSpec(
          GenerationKind.STRING_FAMILY, typeInfo, null, stringGenerationOption);
    }

    public static GenerationSpec createStringFamilyOtherTypeValue(TypeInfo typeInfo,
        TypeInfo otherTypeTypeInfo) {
      return new GenerationSpec(
          GenerationKind.STRING_FAMILY_OTHER_TYPE_VALUE, typeInfo, otherTypeTypeInfo, null);
    }

    public static GenerationSpec createTimestampMilliseconds(TypeInfo typeInfo) {
      return new GenerationSpec(
          GenerationKind.TIMESTAMP_MILLISECONDS, typeInfo, null, null);
    }
  }

  public List<String> typeNames() {
    return typeNames;
  }

  public Category[] categories() {
    return categories;
  }

  public TypeInfo[] typeInfos() {
    return typeInfos;
  }

  public PrimitiveCategory[] primitiveCategories() {
    return primitiveCategories;
  }

  public PrimitiveTypeInfo[] primitiveTypeInfos() {
    return primitiveTypeInfos;
  }

  public StructObjectInspector rowStructObjectInspector() {
    return rowStructObjectInspector;
  }

  public StructObjectInspector partialRowStructObjectInspector(int partialFieldCount) {
    ArrayList<ObjectInspector> partialObjectInspectorList =
        new ArrayList<ObjectInspector>(partialFieldCount);
    List<String> columnNames = new ArrayList<String>(partialFieldCount);
    for (int i = 0; i < partialFieldCount; i++) {
      columnNames.add(String.format("partial%d", i));
      partialObjectInspectorList.add(getObjectInspector(typeInfos[i]));
    }

    return ObjectInspectorFactory.getStandardStructObjectInspector(
        columnNames, objectInspectorList);
  }

  public enum SupportedTypes {
    ALL, PRIMITIVES, ALL_EXCEPT_MAP
  }

  public void init(Random r) {
    init(r, SupportedTypes.PRIMITIVES, 0, true);
  }

  public void init(Random r, SupportedTypes supportedTypes, int maxComplexDepth) {
    init(r, supportedTypes, maxComplexDepth, true);
  }

  public void init(Random r, SupportedTypes supportedTypes, int maxComplexDepth, boolean allowNull) {
    this.r = r;
    this.allowNull = allowNull;
    chooseSchema(supportedTypes, null, null, maxComplexDepth);
  }

  public void init(Random r, Set<String> allowedTypeNameSet, int maxComplexDepth, boolean allowNull) {
    this.r = r;
    this.allowNull = allowNull;
    chooseSchema(SupportedTypes.ALL, allowedTypeNameSet, null, maxComplexDepth);
  }

  public void initExplicitSchema(Random r, List<String> explicitTypeNameList, int maxComplexDepth,
      boolean allowNull) {
    this.r = r;
    this.allowNull = allowNull;

    List<GenerationSpec> generationSpecList = new ArrayList<GenerationSpec>();
    for (String explicitTypeName : explicitTypeNameList) {
      TypeInfo typeInfo =
          TypeInfoUtils.getTypeInfoFromTypeString(explicitTypeName);
      generationSpecList.add(
          GenerationSpec.createSameType(typeInfo));
    }

    chooseSchema(
        SupportedTypes.ALL, null, generationSpecList,
        maxComplexDepth);
  }

  public void initGenerationSpecSchema(Random r, List<GenerationSpec> generationSpecList, int maxComplexDepth,
      boolean allowNull) {
    this.r = r;
    this.allowNull = allowNull;
    chooseSchema(
        SupportedTypes.ALL, null, generationSpecList,
        maxComplexDepth);
  }

  /*
   * For now, exclude CHAR until we determine why there is a difference (blank padding)
   * serializing with LazyBinarySerializeWrite and the regular SerDe...
   */
  private static String[] possibleHivePrimitiveTypeNames = {
      "boolean",
      "tinyint",
      "smallint",
      "int",
      "bigint",
      "date",
      "float",
      "double",
      "string",
      "char",
      "varchar",
      "binary",
      "date",
      "timestamp",
      "interval_year_month",
      "interval_day_time",
      "decimal"
  };

  private static String[] possibleHiveComplexTypeNames = {
      "array",
      "struct",
      "uniontype",
      "map"
  };

  private static String getRandomTypeName(Random random, SupportedTypes supportedTypes,
      Set<String> allowedTypeNameSet) {

    String typeName = null;
    do {
      if (random.nextInt(10 ) != 0) {
        typeName = possibleHivePrimitiveTypeNames[random.nextInt(possibleHivePrimitiveTypeNames.length)];
      } else {
        switch (supportedTypes) {
        case PRIMITIVES:
          typeName = possibleHivePrimitiveTypeNames[random.nextInt(possibleHivePrimitiveTypeNames.length)];
          break;
        case ALL_EXCEPT_MAP:
          typeName = possibleHiveComplexTypeNames[random.nextInt(possibleHiveComplexTypeNames.length - 1)];
          break;
        case ALL:
          typeName = possibleHiveComplexTypeNames[random.nextInt(possibleHiveComplexTypeNames.length)];
          break;
        }
      }
    } while (allowedTypeNameSet != null && !allowedTypeNameSet.contains(typeName));
    return typeName;
  }

  public static String getDecoratedTypeName(Random random, String typeName) {
    return getDecoratedTypeName(random, typeName, null, null, 0, 1);
  }

  private static String getDecoratedTypeName(Random random, String typeName,
      SupportedTypes supportedTypes, Set<String> allowedTypeNameSet, int depth, int maxDepth) {
    depth++;
    if (depth < maxDepth) {
      supportedTypes = SupportedTypes.PRIMITIVES;
    }
    if (typeName.equals("char")) {
      final int maxLength = 1 + random.nextInt(100);
      typeName = String.format("char(%d)", maxLength);
    } else if (typeName.equals("varchar")) {
      final int maxLength = 1 + random.nextInt(100);
      typeName = String.format("varchar(%d)", maxLength);
    } else if (typeName.equals("decimal")) {
      typeName =
          String.format(
              "decimal(%d,%d)",
              HiveDecimal.SYSTEM_DEFAULT_PRECISION,
              HiveDecimal.SYSTEM_DEFAULT_SCALE);
    } else if (typeName.equals("array")) {
      String elementTypeName = getRandomTypeName(random, supportedTypes, allowedTypeNameSet);
      elementTypeName =
          getDecoratedTypeName(random, elementTypeName, supportedTypes, allowedTypeNameSet, depth, maxDepth);
      typeName = String.format("array<%s>", elementTypeName);
    } else if (typeName.equals("map")) {
      String keyTypeName =
          getRandomTypeName(
              random, SupportedTypes.PRIMITIVES, allowedTypeNameSet);
      keyTypeName =
          getDecoratedTypeName(
              random, keyTypeName, supportedTypes, allowedTypeNameSet, depth, maxDepth);
      String valueTypeName =
          getRandomTypeName(
              random, supportedTypes, allowedTypeNameSet);
      valueTypeName =
          getDecoratedTypeName(
              random, valueTypeName, supportedTypes, allowedTypeNameSet, depth, maxDepth);
      typeName = String.format("map<%s,%s>", keyTypeName, valueTypeName);
    } else if (typeName.equals("struct")) {
      final int fieldCount = 1 + random.nextInt(10);
      final StringBuilder sb = new StringBuilder();
      for (int i = 0; i < fieldCount; i++) {
        String fieldTypeName =
            getRandomTypeName(
                random, supportedTypes, allowedTypeNameSet);
        fieldTypeName =
            getDecoratedTypeName(
                random, fieldTypeName, supportedTypes, allowedTypeNameSet, depth, maxDepth);
        if (i > 0) {
          sb.append(",");
        }
        sb.append("col");
        sb.append(i);
        sb.append(":");
        sb.append(fieldTypeName);
      }
      typeName = String.format("struct<%s>", sb.toString());
    } else if (typeName.equals("struct") ||
        typeName.equals("uniontype")) {
      final int fieldCount = 1 + random.nextInt(10);
      final StringBuilder sb = new StringBuilder();
      for (int i = 0; i < fieldCount; i++) {
        String fieldTypeName =
            getRandomTypeName(
                random, supportedTypes, allowedTypeNameSet);
        fieldTypeName =
            getDecoratedTypeName(
                random, fieldTypeName, supportedTypes, allowedTypeNameSet, depth, maxDepth);
        if (i > 0) {
          sb.append(",");
        }
        sb.append(fieldTypeName);
      }
      typeName = String.format("uniontype<%s>", sb.toString());
    }
    return typeName;
  }

  private String getDecoratedTypeName(String typeName,
      SupportedTypes supportedTypes, Set<String> allowedTypeNameSet, int depth, int maxDepth) {
    return getDecoratedTypeName(r, typeName, supportedTypes, allowedTypeNameSet, depth, maxDepth);
  }

  public static ObjectInspector getObjectInspector(TypeInfo typeInfo) {

    final ObjectInspector objectInspector;
    switch (typeInfo.getCategory()) {
    case PRIMITIVE:
      {
        final PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        objectInspector = PrimitiveObjectInspectorFactory.
            getPrimitiveWritableObjectInspector(primitiveTypeInfo);
      }
      break;
    case MAP:
      {
        final MapTypeInfo mapType = (MapTypeInfo) typeInfo;
        final MapObjectInspector mapInspector =
            ObjectInspectorFactory.getStandardMapObjectInspector(
                getObjectInspector(mapType.getMapKeyTypeInfo()),
                getObjectInspector(mapType.getMapValueTypeInfo()));
        objectInspector = mapInspector;
      }
      break;
    case LIST:
      {
        final ListTypeInfo listType = (ListTypeInfo) typeInfo;
        final ListObjectInspector listInspector =
            ObjectInspectorFactory.getStandardListObjectInspector(
                getObjectInspector(listType.getListElementTypeInfo()));
        objectInspector = listInspector;
      }
      break;
    case STRUCT:
      {
        final StructTypeInfo structType = (StructTypeInfo) typeInfo;
        final List<TypeInfo> fieldTypes = structType.getAllStructFieldTypeInfos();

        final List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
        for (TypeInfo fieldType : fieldTypes) {
          fieldInspectors.add(getObjectInspector(fieldType));
        }

        final StructObjectInspector structInspector =
            ObjectInspectorFactory.getStandardStructObjectInspector(
                structType.getAllStructFieldNames(), fieldInspectors);
        objectInspector = structInspector;
      }
      break;
    case UNION:
      {
        final UnionTypeInfo unionType = (UnionTypeInfo) typeInfo;
        final List<TypeInfo> fieldTypes = unionType.getAllUnionObjectTypeInfos();

        final List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
        for (TypeInfo fieldType : fieldTypes) {
          fieldInspectors.add(getObjectInspector(fieldType));
        }

        final UnionObjectInspector unionInspector =
            ObjectInspectorFactory.getStandardUnionObjectInspector(
                fieldInspectors);
        objectInspector = unionInspector;
      }
      break;
    default:
      throw new RuntimeException("Unexpected category " + typeInfo.getCategory());
    }
    Preconditions.checkState(objectInspector != null);
    return objectInspector;
  }

  private void chooseSchema(SupportedTypes supportedTypes, Set<String> allowedTypeNameSet,
      List<GenerationSpec> generationSpecList,
      int maxComplexDepth) {
    HashSet<Integer> hashSet = null;
    final boolean allTypes;
    final boolean onlyOne;
    if (generationSpecList != null) {
      columnCount = generationSpecList.size();
      allTypes = false;
      onlyOne = false;
    } else if (allowedTypeNameSet != null) {
      columnCount = 1 + r.nextInt(20);
      allTypes = false;
      onlyOne = false;
    } else {
      onlyOne = (r.nextInt(100) == 7);
      if (onlyOne) {
        columnCount = 1;
        allTypes = false;
      } else {
        allTypes = r.nextBoolean();
        if (allTypes) {
          switch (supportedTypes) {
          case ALL:
            columnCount = possibleHivePrimitiveTypeNames.length + possibleHiveComplexTypeNames.length;
            break;
          case ALL_EXCEPT_MAP:
            columnCount = possibleHivePrimitiveTypeNames.length + possibleHiveComplexTypeNames.length - 1;
            break;
          case PRIMITIVES:
            columnCount = possibleHivePrimitiveTypeNames.length;
            break;
          }
          hashSet = new HashSet<Integer>();
        } else {
          columnCount = 1 + r.nextInt(20);
        }
      }
    }
    typeNames = new ArrayList<String>(columnCount);
    categories = new Category[columnCount];
    typeInfos = new TypeInfo[columnCount];
    objectInspectorList = new ArrayList<ObjectInspector>(columnCount);

    primitiveCategories = new PrimitiveCategory[columnCount];
    primitiveTypeInfos = new PrimitiveTypeInfo[columnCount];
    primitiveObjectInspectorList = new ArrayList<ObjectInspector>(columnCount);
    List<String> columnNames = new ArrayList<String>(columnCount);
    for (int c = 0; c < columnCount; c++) {
      columnNames.add(String.format("col%d", c));
      final String typeName;

      if (generationSpecList != null) {
        typeName = generationSpecList.get(c).getTypeInfo().getTypeName();
      } else if (onlyOne || allowedTypeNameSet != null) {
        typeName = getRandomTypeName(r, supportedTypes, allowedTypeNameSet);
      } else {
        int typeNum;
        if (allTypes) {
          int maxTypeNum = 0;
          switch (supportedTypes) {
          case PRIMITIVES:
            maxTypeNum = possibleHivePrimitiveTypeNames.length;
            break;
          case ALL_EXCEPT_MAP:
            maxTypeNum = possibleHivePrimitiveTypeNames.length + possibleHiveComplexTypeNames.length - 1;
            break;
          case ALL:
            maxTypeNum = possibleHivePrimitiveTypeNames.length + possibleHiveComplexTypeNames.length;
            break;
          }
          while (true) {

            typeNum = r.nextInt(maxTypeNum);

            Integer typeNumInteger = new Integer(typeNum);
            if (!hashSet.contains(typeNumInteger)) {
              hashSet.add(typeNumInteger);
              break;
            }
          }
        } else {
          if (supportedTypes == SupportedTypes.PRIMITIVES || r.nextInt(10) != 0) {
            typeNum = r.nextInt(possibleHivePrimitiveTypeNames.length);
          } else {
            typeNum = possibleHivePrimitiveTypeNames.length + r.nextInt(possibleHiveComplexTypeNames.length);
            if (supportedTypes == SupportedTypes.ALL_EXCEPT_MAP) {
              typeNum--;
            }
          }
        }
        if (typeNum < possibleHivePrimitiveTypeNames.length) {
          typeName = possibleHivePrimitiveTypeNames[typeNum];
        } else {
          typeName = possibleHiveComplexTypeNames[typeNum - possibleHivePrimitiveTypeNames.length];
        }

      }

      String decoratedTypeName =
          getDecoratedTypeName(typeName, supportedTypes, allowedTypeNameSet, 0, maxComplexDepth);

      final TypeInfo typeInfo;
      try {
        typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(decoratedTypeName);
      } catch (Exception e) {
        throw new RuntimeException("Cannot convert type name " + decoratedTypeName + " to a type " + e);
      }

      typeInfos[c] = typeInfo;
      final Category category = typeInfo.getCategory();
      categories[c] = category;
      ObjectInspector objectInspector = getObjectInspector(typeInfo);
      switch (category) {
      case PRIMITIVE:
        {
          final PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
          primitiveTypeInfos[c] = primitiveTypeInfo;
          PrimitiveCategory primitiveCategory = primitiveTypeInfo.getPrimitiveCategory();
          primitiveCategories[c] = primitiveCategory;
          primitiveObjectInspectorList.add(objectInspector);
        }
        break;
      case LIST:
      case MAP:
      case STRUCT:
      case UNION:
        primitiveObjectInspectorList.add(null);
        break;
      default:
        throw new RuntimeException("Unexpected catagory " + category);
      }
      objectInspectorList.add(objectInspector);

      if (category == Category.PRIMITIVE) {
      }
      typeNames.add(decoratedTypeName);
    }
    rowStructObjectInspector = ObjectInspectorFactory.
        getStandardStructObjectInspector(columnNames, objectInspectorList);
    alphabets = new String[columnCount];

    this.generationSpecList = generationSpecList;
  }

  private static ThreadLocal<DateFormat> DATE_FORMAT =
      new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
          return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
      };

  private static long MIN_FOUR_DIGIT_YEAR_MILLIS = parseToMillis("0001-01-01 00:00:00");
  private static long MAX_FOUR_DIGIT_YEAR_MILLIS = parseToMillis("9999-01-01 00:00:00");

  private static long parseToMillis(String s) {
    try {
      return DATE_FORMAT.get().parse(s).getTime();
    } catch (ParseException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static String[] randomWords =
      new String[] {
    "groovy",
    "attack",
    "wacky",
    "kiss",
    "to",
    "the",
    "a",
    "thoughtless",
    "blushing",
    "pay",
    "rule",
    "profuse",
    "need",
    "smell",
    "bucket",
    "board",
    "eggs",
    "laughable",
    "idiotic",
    "direful",
    "thoughtful",
    "curious",
    "show",
    "surge",
    "opines",
    "cowl",
    "signal",
    ""};
  private static int randomWordCount = randomWords.length;

  private static Object toStringFamilyObject(TypeInfo typeInfo, String string, boolean isWritable) {

    PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
    PrimitiveCategory primitiveCategory =
        primitiveTypeInfo.getPrimitiveCategory();
    Object object;
    switch (primitiveCategory) {
    case STRING:
      if (isWritable) {
        object = new Text(string);
      } else {
        object = string;
      }
      break;
    case CHAR:
      {
        HiveChar hiveChar =
            new HiveChar(
                string, ((CharTypeInfo) typeInfo).getLength());
        if (isWritable) {
          object = new HiveCharWritable(hiveChar);
        } else {
          object = hiveChar;
        }
      }
      break;
    case VARCHAR:
      {
        HiveVarchar hiveVarchar =
            new HiveVarchar(
                string, ((VarcharTypeInfo) typeInfo).getLength());
        if (isWritable) {
          object = new HiveVarcharWritable(hiveVarchar);
        } else {
          object = hiveVarchar;
        }
      }
      break;
    default:
      throw new RuntimeException("Unexpected string family category " + primitiveCategory);
    }
    return object;
  }

  public static Object randomStringFamilyOtherTypeValue(Random random, TypeInfo typeInfo,
      TypeInfo specialValueTypeInfo, boolean isWritable) {
    String string;
    string =
        VectorRandomRowSource.randomPrimitiveObject(
            random, (PrimitiveTypeInfo) specialValueTypeInfo).toString();
    return toStringFamilyObject(typeInfo, string, isWritable);
  }

  public static Object randomStringFamily(Random random, TypeInfo typeInfo,
      StringGenerationOption stringGenerationOption, boolean isWritable) {

    String string;
    if (stringGenerationOption == null) {
      string =
          VectorRandomRowSource.randomPrimitiveObject(
              random, (PrimitiveTypeInfo) typeInfo).toString();
    } else {
      boolean generateSentences = stringGenerationOption.getGenerateSentences();
      boolean addPadding = stringGenerationOption.getAddPadding();
      StringBuilder sb = new StringBuilder();
      if (addPadding && random.nextBoolean()) {
        sb.append(StringUtils.leftPad("", random.nextInt(5)));
      }
      if (generateSentences) {
        boolean capitalizeFirstWord = random.nextBoolean();
        final int n = random.nextInt(10);
        for (int i = 0; i < n; i++) {
          String randomWord = randomWords[random.nextInt(randomWordCount)];
          if (randomWord.length() > 0 &&
              ((i == 0 && capitalizeFirstWord) || random.nextInt(20) == 0)) {
            randomWord = Character.toUpperCase(randomWord.charAt(0)) + randomWord.substring(1);
          }
          if (i > 0) {
            sb.append(" ");
          }
          sb.append(randomWord);
        }
      } else {
        sb.append(
            VectorRandomRowSource.randomPrimitiveObject(
                random, (PrimitiveTypeInfo) typeInfo).toString());
      }
      if (addPadding && random.nextBoolean()) {
        sb.append(StringUtils.leftPad("", random.nextInt(5)));
      }
      string = sb.toString();
    }
    return toStringFamilyObject(typeInfo, string, isWritable);
  }

  public Object[][] randomRows(int n) {

    final Object[][] result = new Object[n][];
    for (int i = 0; i < n; i++) {
      result[i] = randomRow();
    }
    return result;
  }

  public Object[] randomRow() {

    final Object row[] = new Object[columnCount];

    if (generationSpecList == null) {
      for (int c = 0; c < columnCount; c++) {
        row[c] = randomWritable(c);
      }
    } else {
      for (int c = 0; c < columnCount; c++) {
        GenerationSpec generationSpec = generationSpecList.get(c);
        GenerationSpec.GenerationKind generationKind = generationSpec.getGenerationKind();
        Object object;
        switch (generationKind) {
        case SAME_TYPE:
          object = randomWritable(c);
          break;
        case OMIT_GENERATION:
          object = null;
          break;
        case STRING_FAMILY:
        {
          TypeInfo typeInfo = generationSpec.getTypeInfo();
          StringGenerationOption stringGenerationOption =
              generationSpec.getStringGenerationOption();
          object = randomStringFamily(
              r, typeInfo, stringGenerationOption, true);
        }
        break;
        case STRING_FAMILY_OTHER_TYPE_VALUE:
          {
            TypeInfo typeInfo = generationSpec.getTypeInfo();
            TypeInfo otherTypeTypeInfo = generationSpec.getSourceTypeInfo();
            object = randomStringFamilyOtherTypeValue(
                r, typeInfo, otherTypeTypeInfo, true);
          }
          break;
        case TIMESTAMP_MILLISECONDS:
          {
            LongWritable longWritable = (LongWritable) randomWritable(c);
            if (longWritable != null) {

              while (true) {
                long longValue = longWritable.get();
                if (longValue >= MIN_FOUR_DIGIT_YEAR_MILLIS &&
                    longValue <= MAX_FOUR_DIGIT_YEAR_MILLIS) {
                  break;
                }
                longWritable.set(
                    (Long) VectorRandomRowSource.randomPrimitiveObject(
                        r, (PrimitiveTypeInfo) TypeInfoFactory.longTypeInfo));
              }
            }
            object = longWritable;
          }
          break;
        default:
          throw new RuntimeException("Unexpected generationKind " + generationKind);
        }
        row[c] = object;
      }
    }
    return row;
  }

  public Object[] randomRow(boolean allowNull) {

    final Object row[] = new Object[columnCount];
    for (int c = 0; c < columnCount; c++) {
      row[c] = randomWritable(typeInfos[c], objectInspectorList.get(c), allowNull);
    }
    return row;
  }

  public Object[] randomPrimitiveRow(int columnCount) {
    return randomPrimitiveRow(columnCount, r, primitiveTypeInfos);
  }

  public static Object[] randomPrimitiveRow(int columnCount, Random r,
      PrimitiveTypeInfo[] primitiveTypeInfos) {

    final Object row[] = new Object[columnCount];
    for (int c = 0; c < columnCount; c++) {
      row[c] = randomPrimitiveObject(r, primitiveTypeInfos[c]);
    }
    return row;
  }

  public static Object[] randomWritablePrimitiveRow(int columnCount, Random r,
      PrimitiveTypeInfo[] primitiveTypeInfos) {

    final Object row[] = new Object[columnCount];
    for (int c = 0; c < columnCount; c++) {
      final PrimitiveTypeInfo primitiveTypeInfo = primitiveTypeInfos[c];
      final ObjectInspector objectInspector =
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(primitiveTypeInfo);
      final Object object = randomPrimitiveObject(r, primitiveTypeInfo);
      row[c] = getWritablePrimitiveObject(primitiveTypeInfo, objectInspector, object);
    }
    return row;
  }

  public void addBinarySortableAlphabets() {

    for (int c = 0; c < columnCount; c++) {
      if (primitiveCategories[c] == null) {
        continue;
      }
      switch (primitiveCategories[c]) {
      case STRING:
      case CHAR:
      case VARCHAR:
        byte[] bytes = new byte[10 + r.nextInt(10)];
        for (int i = 0; i < bytes.length; i++) {
          bytes[i] = (byte) (32 + r.nextInt(96));
        }
        int alwaysIndex = r.nextInt(bytes.length);
        bytes[alwaysIndex] = 0;  // Must be escaped by BinarySortable.
        int alwaysIndex2 = r.nextInt(bytes.length);
        bytes[alwaysIndex2] = 1;  // Must be escaped by BinarySortable.
        alphabets[c] = new String(bytes, Charsets.UTF_8);
        break;
      default:
        // No alphabet needed.
        break;
      }
    }
  }

  public void addEscapables(String needsEscapeStr) {
    addEscapables = true;
    this.needsEscapeStr = needsEscapeStr;
  }

  public static void sort(Object[][] rows, ObjectInspector oi) {
    for (int i = 0; i < rows.length; i++) {
      for (int j = i + 1; j < rows.length; j++) {
        if (ObjectInspectorUtils.compare(rows[i], oi, rows[j], oi) > 0) {
          Object[] t = rows[i];
          rows[i] = rows[j];
          rows[j] = t;
        }
      }
    }
  }

  public void sort(Object[][] rows) {
    VectorRandomRowSource.sort(rows, rowStructObjectInspector);
  }

  public static Object getWritablePrimitiveObject(PrimitiveTypeInfo primitiveTypeInfo,
      ObjectInspector objectInspector, Object object) {

    switch (primitiveTypeInfo.getPrimitiveCategory()) {
    case BOOLEAN:
      return ((WritableBooleanObjectInspector) objectInspector).create((boolean) object);
    case BYTE:
      return ((WritableByteObjectInspector) objectInspector).create((byte) object);
    case SHORT:
      return ((WritableShortObjectInspector) objectInspector).create((short) object);
    case INT:
      return ((WritableIntObjectInspector) objectInspector).create((int) object);
    case LONG:
      return ((WritableLongObjectInspector) objectInspector).create((long) object);
    case DATE:
      return ((WritableDateObjectInspector) objectInspector).create((Date) object);
    case FLOAT:
      return ((WritableFloatObjectInspector) objectInspector).create((float) object);
    case DOUBLE:
      return ((WritableDoubleObjectInspector) objectInspector).create((double) object);
    case STRING:
      return ((WritableStringObjectInspector) objectInspector).create((String) object);
    case CHAR:
      {
        WritableHiveCharObjectInspector writableCharObjectInspector =
                new WritableHiveCharObjectInspector( (CharTypeInfo) primitiveTypeInfo);
        return writableCharObjectInspector.create(new HiveChar(StringUtils.EMPTY, -1));
      }
    case VARCHAR:
      {
        WritableHiveVarcharObjectInspector writableVarcharObjectInspector =
                new WritableHiveVarcharObjectInspector( (VarcharTypeInfo) primitiveTypeInfo);
        return writableVarcharObjectInspector.create(new HiveVarchar(StringUtils.EMPTY, -1));
      }
    case BINARY:
      return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector.create(ArrayUtils.EMPTY_BYTE_ARRAY);
    case TIMESTAMP:
      return ((WritableTimestampObjectInspector) objectInspector).create(new Timestamp(0));
    case INTERVAL_YEAR_MONTH:
      return ((WritableHiveIntervalYearMonthObjectInspector) objectInspector).create(new HiveIntervalYearMonth(0));
    case INTERVAL_DAY_TIME:
      return ((WritableHiveIntervalDayTimeObjectInspector) objectInspector).create(new HiveIntervalDayTime(0, 0));
    case DECIMAL:
      {
        WritableHiveDecimalObjectInspector writableDecimalObjectInspector =
                new WritableHiveDecimalObjectInspector((DecimalTypeInfo) primitiveTypeInfo);
        return writableDecimalObjectInspector.create(HiveDecimal.ZERO);
      }
    default:
      throw new Error("Unknown primitive category " + primitiveTypeInfo.getPrimitiveCategory());
    }
  }
  public Object randomWritable(int column) {
    return randomWritable(typeInfos[column], objectInspectorList.get(column));
  }

  public Object randomWritable(TypeInfo typeInfo, ObjectInspector objectInspector) {
    return randomWritable(typeInfo, objectInspector, allowNull);
  }

  public Object randomWritable(TypeInfo typeInfo, ObjectInspector objectInspector, boolean allowNull) {

    switch (typeInfo.getCategory()) {
    case PRIMITIVE:
    {
      final Object object = randomPrimitiveObject(r, (PrimitiveTypeInfo) typeInfo);
      return getWritablePrimitiveObject((PrimitiveTypeInfo) typeInfo, objectInspector, object);
    }
    case LIST:
    {
      if (allowNull && r.nextInt(20) == 0) {
        return null;
      }
      // Always generate a list with at least 1 value?
      final int elementCount = 1 + r.nextInt(100);
      final StandardListObjectInspector listObjectInspector =
          (StandardListObjectInspector) objectInspector;
      final ObjectInspector elementObjectInspector =
          listObjectInspector.getListElementObjectInspector();
      final TypeInfo elementTypeInfo =
          TypeInfoUtils.getTypeInfoFromObjectInspector(
              elementObjectInspector);
      boolean isStringFamily = false;
      PrimitiveCategory primitiveCategory = null;
      if (elementTypeInfo.getCategory() == Category.PRIMITIVE) {
        primitiveCategory = ((PrimitiveTypeInfo) elementTypeInfo).getPrimitiveCategory();
        if (primitiveCategory == PrimitiveCategory.STRING ||
            primitiveCategory == PrimitiveCategory.BINARY ||
            primitiveCategory == PrimitiveCategory.CHAR ||
            primitiveCategory == PrimitiveCategory.VARCHAR) {
          isStringFamily = true;
        }
      }
      final Object listObj = listObjectInspector.create(elementCount);
      for (int i = 0; i < elementCount; i++) {
        final Object ele = randomWritable(elementTypeInfo, elementObjectInspector, allowNull);
        // UNDONE: For now, a 1-element list with a null element is a null list...
        if (ele == null && elementCount == 1) {
          return null;
        }
        if (isStringFamily && elementCount == 1) {
          switch (primitiveCategory) {
          case STRING:
            if (((Text) ele).getLength() == 0) {
              return null;
            }
            break;
          case BINARY:
            if (((BytesWritable) ele).getLength() == 0) {
              return null;
            }
            break;
          case CHAR:
            if (((HiveCharWritable) ele).getHiveChar().getStrippedValue().isEmpty()) {
              return null;
            }
            break;
          case VARCHAR:
            if (((HiveVarcharWritable) ele).getHiveVarchar().getValue().isEmpty()) {
              return null;
            }
            break;
          default:
            throw new RuntimeException("Unexpected primitive category " + primitiveCategory);
          }
        }
        listObjectInspector.set(listObj, i, ele);
      }
      return listObj;
    }
    case MAP:
    {
      if (allowNull && r.nextInt(20) == 0) {
        return null;
      }
      final int keyPairCount = r.nextInt(100);
      final StandardMapObjectInspector mapObjectInspector =
          (StandardMapObjectInspector) objectInspector;
      final ObjectInspector keyObjectInspector =
          mapObjectInspector.getMapKeyObjectInspector();
      final TypeInfo keyTypeInfo =
          TypeInfoUtils.getTypeInfoFromObjectInspector(
              keyObjectInspector);
      final ObjectInspector valueObjectInspector =
          mapObjectInspector.getMapValueObjectInspector();
      final TypeInfo valueTypeInfo =
          TypeInfoUtils.getTypeInfoFromObjectInspector(
              valueObjectInspector);
      final Object mapObj = mapObjectInspector.create();
      for (int i = 0; i < keyPairCount; i++) {
        Object key = randomWritable(keyTypeInfo, keyObjectInspector);
        Object value = randomWritable(valueTypeInfo, valueObjectInspector);
        mapObjectInspector.put(mapObj, key, value);
      }
      return mapObj;
    }
    case STRUCT:
    {
      if (allowNull && r.nextInt(20) == 0) {
        return null;
      }
      final StandardStructObjectInspector structObjectInspector =
          (StandardStructObjectInspector) objectInspector;
      final List<? extends StructField> fieldRefsList = structObjectInspector.getAllStructFieldRefs();
      final int fieldCount = fieldRefsList.size();
      final Object structObj = structObjectInspector.create();
      for (int i = 0; i < fieldCount; i++) {
        final StructField fieldRef = fieldRefsList.get(i);
        final ObjectInspector fieldObjectInspector =
            fieldRef.getFieldObjectInspector();
        final TypeInfo fieldTypeInfo =
            TypeInfoUtils.getTypeInfoFromObjectInspector(
                fieldObjectInspector);
        final Object fieldObj = randomWritable(fieldTypeInfo, fieldObjectInspector);
        structObjectInspector.setStructFieldData(structObj, fieldRef, fieldObj);
      }
      return structObj;
    }
    case UNION:
    {
      final StandardUnionObjectInspector unionObjectInspector =
          (StandardUnionObjectInspector) objectInspector;
      final List<ObjectInspector> objectInspectorList = unionObjectInspector.getObjectInspectors();
      final int unionCount = objectInspectorList.size();
      final byte tag = (byte) r.nextInt(unionCount);
      final ObjectInspector fieldObjectInspector =
          objectInspectorList.get(tag);
      final TypeInfo fieldTypeInfo =
          TypeInfoUtils.getTypeInfoFromObjectInspector(
              fieldObjectInspector);
      final Object fieldObj = randomWritable(fieldTypeInfo, fieldObjectInspector, false);
      if (fieldObj == null) {
        throw new RuntimeException();
      }
      return new StandardUnion(tag, fieldObj);
    }
    default:
      throw new RuntimeException("Unexpected category " + typeInfo.getCategory());
    }
  }
  public Object randomPrimitiveObject(int column) {
    return randomPrimitiveObject(r, primitiveTypeInfos[column]);
  }

  public static Object randomPrimitiveObject(Random r, PrimitiveTypeInfo primitiveTypeInfo) {

    switch (primitiveTypeInfo.getPrimitiveCategory()) {
    case BOOLEAN:
      return Boolean.valueOf(r.nextBoolean());
    case BYTE:
      return Byte.valueOf((byte) r.nextInt());
    case SHORT:
      return Short.valueOf((short) r.nextInt());
    case INT:
      return Integer.valueOf(r.nextInt());
    case LONG:
      return Long.valueOf(r.nextLong());
    case DATE:
      return RandomTypeUtil.getRandDate(r);
    case FLOAT:
      return Float.valueOf(r.nextFloat() * 10 - 5);
    case DOUBLE:
      return Double.valueOf(r.nextDouble() * 10 - 5);
    case STRING:
      return RandomTypeUtil.getRandString(r);
    case CHAR:
      return getRandHiveChar(r, (CharTypeInfo) primitiveTypeInfo);
    case VARCHAR:
      return getRandHiveVarchar(r, (VarcharTypeInfo) primitiveTypeInfo);
    case BINARY:
      return getRandBinary(r, 1 + r.nextInt(100));
    case TIMESTAMP:
      return RandomTypeUtil.getRandTimestamp(r);
    case INTERVAL_YEAR_MONTH:
      return getRandIntervalYearMonth(r);
    case INTERVAL_DAY_TIME:
      return getRandIntervalDayTime(r);
    case DECIMAL:
    {
      return getRandHiveDecimal(r, (DecimalTypeInfo) primitiveTypeInfo);
    }
    default:
      throw new Error("Unknown primitive category " + primitiveTypeInfo.getCategory());
    }
  }

  public static HiveChar getRandHiveChar(Random r, CharTypeInfo charTypeInfo, String alphabet) {
    int maxLength = 1 + r.nextInt(charTypeInfo.getLength());
    String randomString = RandomTypeUtil.getRandString(r, alphabet, 100);
    HiveChar hiveChar = new HiveChar(randomString, maxLength);
    return hiveChar;
  }

  public static String randomPrimitiveDateStringObject(Random r) {
    Date randomDate = RandomTypeUtil.getRandDate(r);
    String randomDateString = randomDate.toString();
    return randomDateString;
  }

  public static String randomPrimitiveTimestampStringObject(Random r) {
    Timestamp randomTimestamp = RandomTypeUtil.getRandTimestamp(r);
    String randomTimestampString = randomTimestamp.toString();
    return randomTimestampString;
  }

  public static HiveChar getRandHiveChar(Random r, CharTypeInfo charTypeInfo) {
    return getRandHiveChar(r, charTypeInfo, "abcdefghijklmnopqrstuvwxyz");
  }

  public static HiveVarchar getRandHiveVarchar(Random r, VarcharTypeInfo varcharTypeInfo, String alphabet) {
    int maxLength = 1 + r.nextInt(varcharTypeInfo.getLength());
    String randomString = RandomTypeUtil.getRandString(r, alphabet, 100);
    HiveVarchar hiveVarchar = new HiveVarchar(randomString, maxLength);
    return hiveVarchar;
  }

  public static HiveVarchar getRandHiveVarchar(Random r, VarcharTypeInfo varcharTypeInfo) {
    return getRandHiveVarchar(r, varcharTypeInfo, "abcdefghijklmnopqrstuvwxyz");
  }

  public static byte[] getRandBinary(Random r, int len){
    final byte[] bytes = new byte[len];
    for (int j = 0; j < len; j++){
      bytes[j] = Byte.valueOf((byte) r.nextInt());
    }
    return bytes;
  }

  private static final String DECIMAL_CHARS = "0123456789";

  public static HiveDecimal getRandHiveDecimal(Random r, DecimalTypeInfo decimalTypeInfo) {
    while (true) {
      final StringBuilder sb = new StringBuilder();
      final int precision = 1 + r.nextInt(18);
      final int scale = 0 + r.nextInt(precision + 1);

      final int integerDigits = precision - scale;

      if (r.nextBoolean()) {
        sb.append("-");
      }

      if (integerDigits == 0) {
        sb.append("0");
      } else {
        sb.append(RandomTypeUtil.getRandString(r, DECIMAL_CHARS, integerDigits));
      }
      if (scale != 0) {
        sb.append(".");
        sb.append(RandomTypeUtil.getRandString(r, DECIMAL_CHARS, scale));
      }

      HiveDecimal bd = HiveDecimal.create(sb.toString());
      if (bd.scale() > bd.precision()) {
        // Sometimes weird decimals are produced?
        continue;
      }

      return bd;
    }
  }

  public static HiveIntervalYearMonth getRandIntervalYearMonth(Random r) {
    String yearMonthSignStr = r.nextInt(2) == 0 ? "" : "-";
    String intervalYearMonthStr = String.format("%s%d-%d",
        yearMonthSignStr,
        Integer.valueOf(1800 + r.nextInt(500)),  // year
        Integer.valueOf(0 + r.nextInt(12)));     // month
    HiveIntervalYearMonth intervalYearMonthVal = HiveIntervalYearMonth.valueOf(intervalYearMonthStr);
    return intervalYearMonthVal;
  }

  public static HiveIntervalDayTime getRandIntervalDayTime(Random r) {
    String optionalNanos = "";
    if (r.nextInt(2) == 1) {
      optionalNanos = String.format(".%09d",
          Integer.valueOf(0 + r.nextInt(DateUtils.NANOS_PER_SEC)));
    }
    final String yearMonthSignStr = r.nextInt(2) == 0 ? "" : "-";
    final String dayTimeStr = String.format("%s%d %02d:%02d:%02d%s",
        yearMonthSignStr,
        Integer.valueOf(1 + r.nextInt(28)),      // day
        Integer.valueOf(0 + r.nextInt(24)),      // hour
        Integer.valueOf(0 + r.nextInt(60)),      // minute
        Integer.valueOf(0 + r.nextInt(60)),      // second
        optionalNanos);
    HiveIntervalDayTime intervalDayTimeVal = HiveIntervalDayTime.valueOf(dayTimeStr);
    return intervalDayTimeVal;
  }
}
