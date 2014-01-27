package org.apache.hadoop.hive.ql.io.parquet.read;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.parquet.convert.DataWritableRecordConverter;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.util.StringUtils;

import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;
import parquet.schema.Type.Repetition;

/**
 *
 * A MapWritableReadSupport
 *
 * Manages the translation between Hive and Parquet
 *
 */
public class DataWritableReadSupport extends ReadSupport<ArrayWritable> {

  public static final String HIVE_SCHEMA_KEY = "HIVE_TABLE_SCHEMA";
  private static final List<String> virtualColumns;

  static {
    List<String> vcols =  new ArrayList<String>();
    vcols.add("INPUT__FILE__NAME");
    vcols.add("BLOCK__OFFSET__INSIDE__FILE");
    vcols.add("ROW__OFFSET__INSIDE__BLOCK");
    vcols.add("RAW__DATA__SIZE");
    virtualColumns = Collections.unmodifiableList(vcols);
  }

  /**
   * From a string which columns names (including hive column), return a list
   * of string columns
   *
   * @param comma separated list of columns
   * @return list with virtual columns removed
   */
  private static List<String> getColumns(final String columns) {
    final List<String> result = (List<String>) StringUtils.getStringCollection(columns);
    result.removeAll(virtualColumns);
    return result;
  }
  /**
   *
   * It creates the readContext for Parquet side with the requested schema during the init phase.
   *
   * @param configuration needed to get the wanted columns
   * @param keyValueMetaData // unused
   * @param fileSchema parquet file schema
   * @return the parquet ReadContext
   */
  @Override
  public parquet.hadoop.api.ReadSupport.ReadContext init(final Configuration configuration, final Map<String, String> keyValueMetaData, final MessageType fileSchema) {
    final String columns = configuration.get("columns");
    final Map<String, String> contextMetadata = new HashMap<String, String>();
    if (columns != null) {
      final List<String> listColumns = getColumns(columns);

      final List<Type> typeListTable = new ArrayList<Type>();
      for (final String col : listColumns) {
        if (fileSchema.containsField(col)) {
          typeListTable.add(fileSchema.getType(col));
        } else { // dummy type, should not be called
          typeListTable.add(new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, col));
        }
      }
      MessageType tableSchema = new MessageType("table_schema", typeListTable);
      contextMetadata.put(HIVE_SCHEMA_KEY, tableSchema.toString());

      MessageType requestedSchemaByUser = tableSchema;
      final List<Integer> indexColumnsWanted = ColumnProjectionUtils.getReadColumnIDs(configuration);

      final List<Type> typeListWanted = new ArrayList<Type>();
      for (final Integer idx : indexColumnsWanted) {
        typeListWanted.add(tableSchema.getType(listColumns.get(idx)));
      }
      requestedSchemaByUser = new MessageType(fileSchema.getName(), typeListWanted);

      return new ReadContext(requestedSchemaByUser, contextMetadata);
    } else {
      contextMetadata.put(HIVE_SCHEMA_KEY, fileSchema.toString());
      return new ReadContext(fileSchema, contextMetadata);
    }
  }

  /**
   *
   * It creates the hive read support to interpret data from parquet to hive
   *
   * @param configuration // unused
   * @param keyValueMetaData
   * @param fileSchema // unused
   * @param readContext containing the requested schema and the schema of the hive table
   * @return Record Materialize for Hive
   */
  @Override
  public RecordMaterializer<ArrayWritable> prepareForRead(final Configuration configuration, final Map<String, String> keyValueMetaData, final MessageType fileSchema,
          final parquet.hadoop.api.ReadSupport.ReadContext readContext) {
    final Map<String, String> metadata = readContext.getReadSupportMetadata();
    if (metadata == null) {
      throw new RuntimeException("ReadContext not initialized properly. Don't know the Hive Schema.");
    }
    final MessageType tableSchema = MessageTypeParser.parseMessageType(metadata.get(HIVE_SCHEMA_KEY));
    return new DataWritableRecordConverter(readContext.getRequestedSchema(), tableSchema);
  }
}
