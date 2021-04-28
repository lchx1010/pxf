package org.greenplum.pxf.plugins.hdfs.avro;

import com.google.cloud.hadoop.repackaged.gcs.org.apache.commons.codec.DecoderException;
import com.google.cloud.hadoop.repackaged.gcs.org.apache.commons.codec.binary.Hex;
import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.HcfsType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public final class AvroUtilities {

    private static final Logger LOG = LoggerFactory.getLogger(AvroUtilities.class);
    private static final String COMMON_NAMESPACE = "public.avro";

    private String schemaPath;
    private AvroSchemaFileReaderFactory schemaFileReaderFactory;
    private final FileSearcher fileSearcher;

    public interface FileSearcher {
        File searchForFile(String filename);
    }

    /**
     * default constructor
     */
    public AvroUtilities() {
        this(new DefaultFileSearcher());
    }

    // constructor for use in test
    @VisibleForTesting
    AvroUtilities(FileSearcher fileSearcher) {
        this.fileSearcher = fileSearcher;
    }

    @Autowired
    public void setSchemaFileReaderFactory(AvroSchemaFileReaderFactory schemaFileReaderFactory) {
        this.schemaFileReaderFactory = schemaFileReaderFactory;
    }

    /**
     * All-purpose method for obtaining an Avro schema based on the request context and
     * HCFS config.
     *
     * @param context  the context for the request
     * @param hcfsType the type of hadoop-compatible filesystem we are accessing
     * @return the avro schema
     */
    public Schema obtainSchema(RequestContext context, HcfsType hcfsType) {
        Schema schema = (Schema) context.getMetadata();

        if (schema != null) {
            return schema;
        }
        try {
            schemaPath = context.getDataSource();
            schema = readOrGenerateAvroSchema(context, hcfsType);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to obtain Avro schema from '%s'", schemaPath), e);
        }
        context.setMetadata(schema);
        return schema;
    }

    /**
     * Parse a Postgres external format into a given Avro schema
     *
     * Re-used from GPHDFS
     * https://github.com/greenplum-db/gpdb/blob/3b0bfdc169fab7f686276be7eccb024a5e29543c/gpAux/extensions/gphdfs/src/java/1.2/com/emc/greenplum/gpdb/hadoop/formathandler/util/FormatHandlerUtil.java
     * @param schema target Avro schema
     * @param value Postgres external format (the output of function named by typoutput in pg_type) or `null` if null value
     * @param isTopLevel
     * @param delimiter
     * @return
     */
    public Object decodeString(Schema schema, String value, boolean isTopLevel, char delimiter) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("schema={}, value={}, isTopLevel={}", schema, value, isTopLevel);
        }

        Schema.Type fieldType = schema.getType();
        if (fieldType == Schema.Type.ARRAY) {
            if (value == null) {
                return null;
            }

            List<Object> foo = new ArrayList<>();
            String[] splits = getArraySplits(value.toCharArray(), delimiter);
            for (String s : splits) {
                foo.add(decodeString(schema.getElementType(), s, false, delimiter));
            }
            return foo;
        } else {
            LOG.debug("not an array");

            if (fieldType == Schema.Type.UNION) {
                LOG.debug("is a union");
                schema = firstNotNullSchema(schema.getTypes());
                if (schema == null) {
                    // FIXME: `schema` is always null so why include it in the exception message
                    throw new PxfRuntimeException(String.format("%s is a union but only has null type", schema));
                }

                fieldType = schema.getType();
                if (fieldType == Schema.Type.ARRAY) {
                    LOG.debug("first non-null type is array");
                    return decodeString(schema, value, isTopLevel, delimiter);
                }
            }
            if (StringUtils.equals(value, "NULL") && !isTopLevel) {
                return null;
            }

            switch (fieldType) {
                case INT:
                    return Integer.parseInt(value);
                case DOUBLE:
                    return Double.parseDouble(value);
                case STRING:
                case RECORD:
                case ENUM:
                case MAP:
                    // FIXME: return records as strings to maintain old behavior
                    return value;
                case FLOAT:
                    return Float.parseFloat(value);
                case LONG:
                    return Long.parseLong(value);
                case BYTES:
                    // in the case of array if it comes here the string will in the form "\\xDEADBEAF" for hex or
                    // "\\123456" for octal. If the 4th char (index 3) is an x, then we should decode using hex
                    if (value.charAt(3) == 'x') {
                        try {
                            return ByteBuffer.wrap(Hex.decodeHex(value.substring(4, value.length() - 1).toCharArray()));
                        } catch (DecoderException e) {
                            throw new PxfRuntimeException(String.format("bad value " + value), e);
                        }
                    } else {
                        return octString2byteArray(value);
                    }
                case BOOLEAN:
                    return Boolean.parseBoolean(value);
                default:
                    throw new PxfRuntimeException(String.format("type: %s cannot be supported now", fieldType));
            }
        }
    }

    private Schema readOrGenerateAvroSchema(RequestContext context, HcfsType hcfsType) throws IOException {
        // user-provided schema trumps everything
        String userProvidedSchemaFile = context.getOption("SCHEMA");
        if (userProvidedSchemaFile != null) {
            schemaPath = userProvidedSchemaFile;
            AvroSchemaFileReader schemaFileReader = schemaFileReaderFactory.getAvroSchemaFileReader(userProvidedSchemaFile);
            return schemaFileReader.readSchema(context.getConfiguration(), userProvidedSchemaFile, hcfsType, fileSearcher);
        }

        // if we are writing we must generate the schema since there is none to read
        if (context.getRequestType() == RequestContext.RequestType.WRITE_BRIDGE) {
            return generateSchema(context.getTupleDescription());
        }

        // reading from external: get the schema from data source
        return readSchemaFromAvroDataSource(context.getConfiguration(), context.getDataSource());
    }

    private Schema readSchemaFromAvroDataSource(Configuration configuration, String dataSource) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        FsInput inStream = new FsInput(new Path(dataSource), configuration);

        try (DataFileReader<GenericRecord> fileReader = new DataFileReader<>(inStream, datumReader)) {
            return fileReader.getSchema();
        }
    }

    private Schema generateSchema(List<ColumnDescriptor> tupleDescription) throws IOException {
        Schema schema = Schema.createRecord("tableName", "", COMMON_NAMESPACE, false);
        List<Schema.Field> fields = new ArrayList<>();

        for (ColumnDescriptor cd : tupleDescription) {
            fields.add(new Schema.Field(
                    cd.columnName(),
                    getFieldSchema(DataType.get(cd.columnTypeCode()), cd.columnName()),
                    "",
                    null
            ));
        }

        schema.setFields(fields);

        return schema;
    }

    private Schema getFieldSchema(DataType type, String colName) {
        List<Schema> unionList = new ArrayList<>();
        // in this version of gpdb, external table should not set 'notnull' attribute
        // so we should use union between NULL and another type everywhere
        unionList.add(Schema.create(Schema.Type.NULL));

        switch (type) {
            case BOOLEAN:
                unionList.add(Schema.create(Schema.Type.BOOLEAN));
                break;
            case BYTEA:
                unionList.add(Schema.create(Schema.Type.BYTES));
                break;
            case BIGINT:
                unionList.add(Schema.create(Schema.Type.LONG));
                break;
            case SMALLINT:
            case INTEGER:
                unionList.add(Schema.create(Schema.Type.INT));
                break;
            case REAL:
                unionList.add(Schema.create(Schema.Type.FLOAT));
                break;
            case FLOAT8:
                unionList.add(Schema.create(Schema.Type.DOUBLE));
                break;
            case BOOLARRAY:
                unionList.add(createArraySchema(Schema.Type.BOOLEAN));
                break;
            case BYTEAARRAY:
                unionList.add(createArraySchema(Schema.Type.BYTES));
                break;
            case INT2ARRAY:
            case INT4ARRAY:
                unionList.add(createArraySchema(Schema.Type.INT));
                break;
            case INT8ARRAY:
                unionList.add(createArraySchema(Schema.Type.LONG));
                break;
            case FLOAT4ARRAY:
                unionList.add(createArraySchema(Schema.Type.FLOAT));
                break;
            case FLOAT8ARRAY:
                unionList.add(createArraySchema(Schema.Type.DOUBLE));
                break;
            case TEXTARRAY:
                unionList.add(createArraySchema(Schema.Type.STRING));
                break;
            default:
                unionList.add(Schema.create(Schema.Type.STRING));
                break;
        }

        return Schema.createUnion(unionList);
    }

    private Schema createArraySchema(Schema.Type arrayElemType) {
        return Schema.createArray(Schema.createUnion(
                Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(arrayElemType))));
    }

    public String[] getArraySplits(char[] value, char delimiter) {
        List<Integer> posList = new ArrayList<>();
        posList.add(0);

        if (value[0] != '{' || value[value.length - 1] != '}') {
            throw new PxfRuntimeException(String.format("array dim mismatch, rawData: %s", new String(value)));
        }

        int depth = 0;
        boolean inQuoted = false;
        for (int i = 0; i < value.length; i++) {
            if (value[i] == delimiter) {
                if (depth == 1 && !inQuoted) {
                    posList.add(i);
                }

                continue;
            }
            switch (value[i]) {
                case '{':
                    if (!inQuoted) {
                        depth++;
                    }
                    break;
                case '}':
                    if (!inQuoted) {
                        depth--;
                        if (depth == 0) {
                            posList.add(i);
                        }
                    }
                    break;
                case '\"':
                    if (isQuote(value, i)) {
                        inQuoted = !inQuoted;
                    }
                    break;
                default:
                    break;
            }
        }

        String[] subStrings = new String[posList.size() - 1];
        for (int i = 0; i < posList.size() - 1; i++) {
            subStrings[i] = new String(value, posList.get(i) + 1, posList.get(i+1) - posList.get(i) - 1);
        }
        return subStrings;
    }

    private boolean isQuote(char[] value, int index) {
        int num = 0;
        for (int i = index - 1; i >= 0; i--) {
            if (value[i] == '\\') {
                num++;
            } else {
                break;
            }
        }

        return num % 2 == 0;
    }

    public ByteBuffer octString2byteArray(String value) {
        List<Byte> byteList = new ArrayList<>();
        int index = 0, length = value.length();
        if (value.charAt(0) == '"') {
            index = 1;
            length -= 1;
        }

        while (index < length) {
            if (value.charAt(index) == '\\' && value.charAt(index + 1) == '\\') {
                index += 2;
                if (value.charAt(index) == '\\' && value.charAt(index + 1) == '\\') {
                    byteList.add((byte) 0x5C);
                    index += 2;
                }else {
                    byteList.add(octString2Byte(value.substring(index, index + 3)));
                    index += 3;
                }
            }else if (value.charAt(index) == '\\') {
                byteList.add((byte) value.charAt(index+1));
                index += 2;
            }else {
                byteList.add((byte) value.charAt(index));
                index++;
            }
        }

        byte[] ba = new byte[byteList.size()];
        for (int i = 0; i < byteList.size(); i++) {
            ba[i] = byteList.get(i);
        }

        return ByteBuffer.wrap(ba);
    }

    private byte octString2Byte(String string) {
        int num = 0;
        for (int i = 0; i < string.length(); i++) {
            num *= 8;
            num += (string.charAt(i) - '0');
        }
        return (byte) num;
    }

    public Schema firstNotNullSchema(List<Schema> types) {
        for (Schema schema : types) {
            LOG.debug("check if {} is not null", schema);
            if (schema.getType() != Schema.Type.NULL) {
                return schema;
            }
        }

        return null;
    }

    private static class DefaultFileSearcher implements FileSearcher {

        @Override
        public File searchForFile(String schemaName) {
            try {
                File file = new File(schemaName);
                if (!file.exists()) {
                    URL url = AvroUtilities.class.getClassLoader().getResource(schemaName);

                    // Testing that the schema resource exists
                    if (url == null) {
                        return null;
                    }
                    file = new File(URLDecoder.decode(url.getPath(), "UTF-8"));
                }
                return file;
            } catch (UnsupportedEncodingException e) {
                LOG.info(e.toString());
                return null;
            }
        }
    }
}
