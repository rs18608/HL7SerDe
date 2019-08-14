package abaka.serde;

import abaka.hl7.HL7;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import javax.xml.bind.ValidationException;
import java.util.*;

import static java.lang.String.format;


public class HL7SerDe extends AbstractSerDe {

    private static final Log LOG = LogFactory.getLog(HL7SerDe.class.getName());
    private List<Object> row = new ArrayList<Object>();
    protected StructTypeInfo rowTypeInfo;
    private ObjectInspector rowObjectInspector;

    @Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {

        LOG.info("Initialized called.");
        String columnNamesProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
        if ((columnNamesProperty == null) || columnNamesProperty.isEmpty()) {
            throw new SerDeException("table has no columns");
        }

        String columnTypesProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
        if ((columnTypesProperty == null) || columnTypesProperty.isEmpty()) {
            throw new SerDeException("table has no column types");
        }

        List<String> columnNames = Arrays.asList(columnNamesProperty.split(","));
        List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypesProperty);
        if (columnNames.size() != columnTypes.size()) {
            throw new SerDeException(format("columns size (%s) does not match column types size (%s)", columnNames.size(), columnTypes.size()));
        }

        rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
        rowObjectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);

    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
        throw new UnsupportedOperationException("serialization not supported");
    }

    @Override
    public SerDeStats getSerDeStats() {
        return null;
    }

    @Override
    public Object deserialize(Writable hl7Input) throws SerDeException {
        LOG.info("Writable hl7Input to be de-serialize: " + hl7Input.toString());

        if (!(hl7Input instanceof BinaryComparable)) {
            throw new SerDeException("expected BinaryComparable: " + hl7Input.getClass().getName());
        }

        try {
            HL7 hl7 = new HL7(hl7Input, rowTypeInfo.getAllStructFieldNames());

            Object value = null;
            for (String fieldName : rowTypeInfo.getAllStructFieldNames()) {
                try {
                    TypeInfo fieldTypeInfo = rowTypeInfo.getStructFieldTypeInfo(fieldName);
                    value = parseField(hl7.getValue(fieldName), fieldTypeInfo);
                } catch (Exception e) {
                    value = null;
                }
                row.add(value);
            }

        } catch (ValidationException e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
        LOG.info("ROW: " + row.toString());
        return row;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowObjectInspector;
    }

    /**
     * Parses a JSON object according to the Hive column's type.
     *
     * @param field         - The JSON object to parse
     * @param fieldTypeInfo - Metadata about the Hive column
     * @return - The parsed value of the field
     */
    private Object parseField(Object field, TypeInfo fieldTypeInfo) {
        switch (fieldTypeInfo.getCategory()) {
            case PRIMITIVE:
                if (field instanceof String) {
                    field = field.toString().replaceAll("\n", "\\\\n");
                }
                return field;
            case LIST:
                return parseList(field, (ListTypeInfo) fieldTypeInfo);
            case MAP:
                return parseMap(field, (MapTypeInfo) fieldTypeInfo);
            case STRUCT:
                return parseStruct(field, (StructTypeInfo) fieldTypeInfo);
            case UNION:
                // Unsupported by JSON
            default:
                return null;
        }
    }

    /**
     * Parses a JSON object and its fields. The Hive metadata is used to
     * determine how to parse the object fields.
     *
     * @param field         - The JSON object to parse
     * @param fieldTypeInfo - Metadata about the Hive column
     * @return - A map representing the object and its fields
     */
    private Object parseStruct(Object field, StructTypeInfo fieldTypeInfo) {
        Map<Object, Object> map = (Map<Object, Object>) field;
        ArrayList<TypeInfo> structTypes = fieldTypeInfo.getAllStructFieldTypeInfos();
        ArrayList<String> structNames = fieldTypeInfo.getAllStructFieldNames();

        List<Object> structRow = new ArrayList<Object>(structTypes.size());
        if (map != null) {
            for (int i = 0; i < structNames.size(); i++) {
                structRow.add(parseField(map.get(structNames.get(i)), structTypes.get(i)));
            }
        }
        return structRow;
    }

    /**
     * Parse a JSON list and its elements. This uses the Hive metadata for the
     * list elements to determine how to parse the elements.
     *
     * @param field         - The JSON list to parse
     * @param fieldTypeInfo - Metadata about the Hive column
     * @return - A list of the parsed elements
     */
    private Object parseList(Object field, ListTypeInfo fieldTypeInfo) {
        ArrayList<Object> list = (ArrayList<Object>) field;
        TypeInfo elemTypeInfo = fieldTypeInfo.getListElementTypeInfo();
        if (list != null) {
            for (int i = 0; i < list.size(); i++) {
                list.set(i, parseField(list.get(i), elemTypeInfo));
            }
        }
        return list.toArray();
    }

    /**
     * Parse a JSON object as a map. This uses the Hive metadata for the map
     * values to determine how to parse the values. The map is assumed to have
     * a string for a key.
     *
     * @param field         - The JSON list to parse
     * @param fieldTypeInfo - Metadata about the Hive column
     * @return
     */
    private Object parseMap(Object field, MapTypeInfo fieldTypeInfo) {
        Map<Object, Object> map = (Map<Object, Object>) field;
        TypeInfo valueTypeInfo = fieldTypeInfo.getMapValueTypeInfo();
        if (map != null) {
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                map.put(entry.getKey(), parseField(entry.getValue(), valueTypeInfo));
            }
        }
        return map;
    }

}


