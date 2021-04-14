package org.greenplum.pxf.automation.features.avro;

import org.greenplum.pxf.automation.components.cluster.PhdCluster;
import org.greenplum.pxf.automation.datapreparer.CustomAvroPreparer;
import org.greenplum.pxf.automation.features.BaseFeature;
import org.greenplum.pxf.automation.fileformats.IAvroSchema;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.structures.tables.pxf.ReadableExternalTable;
import org.greenplum.pxf.automation.utils.fileformats.FileFormatsUtils;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Collection of Test cases for PXF ability to read AvroFile or Avro serialized
 * files from HDFS. Relates to cases located in "PXF Test Suite" in testrail.
 * See <a href="https://testrail.greenplum.com/index.php?/suites/view/1099">HDFS
 * Readable - Avro</a>
 */
public class HdfsReadableAvroTest extends BaseFeature {

    private String hdfsPath;
    private String resourcePath;

    private static final String SUFFIX_AVRO = ".avro";

    private static final String avroSimpleFileName = "simple";
    private static final String avroTypesFileName = "supported_primitive_types";
    private static final String avroArrayFileName = "array";
    private static final String avroComplexFileName = "complex";
    private static final String avroComplexNullFileName = "complex_null";
    private static final String avroInSequenceArraysFileName = "avro_in_sequence_arrays.tbl";
    private static final String avroInSequenceArraysSchemaFile = "PXFCustomAvro.avsc";
    private static final String avroInSequenceArraysSchemaFileWithSpaces = "PXF Custom Avro1.avsc";
    private static final String complexAvroFile = "complex.avro";
    private String remotePublicStage;

    @Override
    public void beforeClass() throws Exception {
        // path for storing data on HDFS (for processing by PXF)
        hdfsPath = hdfs.getWorkingDirectory() + "/readableAvro/";

        // location of schema and data files
        String absolutePath = getClass().getClassLoader().getResource("data").getPath();
        resourcePath = absolutePath + "/avro/";

        // create and copy data to hdfs
        prepareData();

        // copy schema file to all nodes
        remotePublicStage = "/tmp/publicstage/pxf";
        // copy schema file to cluster nodes, used for avro in sequence cases
        cluster.copyFileToNodes(new File(resourcePath
                        + avroInSequenceArraysSchemaFile).getAbsolutePath(), remotePublicStage,
                true, false);
        cluster.copyFileToNodes(new File(resourcePath
                        + avroInSequenceArraysSchemaFileWithSpaces).getAbsolutePath(), remotePublicStage,
                true, false);
        String avroInSequenceComplexSchemaFile = "PXFComplexAvro.avsc";
        cluster.copyFileToNodes(new File(resourcePath
                        + avroInSequenceComplexSchemaFile).getAbsolutePath(), remotePublicStage,
                true, false);
        cluster.copyFileToNodes(new File(resourcePath
                        + complexAvroFile).getAbsolutePath(), remotePublicStage,
                true, false);
        // add new path to classpath file and restart PXF service
        cluster.addPathToPxfClassPath(remotePublicStage);
        cluster.restart(PhdCluster.EnumClusterServices.pxf);
    }

    /**
     * TestRail: C120828 Read simple avro file using profile.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void avroSimple() throws Exception {
        prepareReadableTable("avrotest_simple", new String[]{"name text", "age int"}, hdfsPath + avroSimpleFileName + SUFFIX_AVRO);
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.simple.runTest");
    }

    /**
     * TestRail: C120829 Read an Avro file that includes all supported primitive
     * data types.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void avroSupportedPrimitives() throws Exception {
        prepareReadableTable("avrotest_supported_primitive_types", new String[]{
                "type_int int",
                "type_double float8",
                "type_string text",
                "type_float real",
                "type_long bigint",
                "type_bytes bytea",
                "type_boolean bool"}, hdfsPath + avroTypesFileName + SUFFIX_AVRO);
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.supported_primitive_types.runTest");
    }

    /**
     * Read an Avro file that includes all supported array types as text.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void avroArraysAsText() throws Exception {
        prepareReadableTable("avrotest_arrays", new String[]{
                "type_int int",
                "type_int_array text",
                "type_double float8",
                "type_double_array text",
                "type_string text",
                "type_string_array text",
                "type_float real",
                "type_float_array text",
                "type_long bigint",
                "type_long_array text",
                "type_bytes bytea",
                "type_bytes_array text",
                "type_boolean bool",
                "type_boolean_array text",}, hdfsPath + avroArrayFileName + SUFFIX_AVRO);
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.array_types.runTest");
    }

    /**
     * Read an Avro file that includes all supported array types as gpdb arrays.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void avroArraysAsGPDBArrays() throws Exception {
        prepareReadableTable("avrotest_arrays_gpdb_arrays", new String[]{
                "type_int int",
                "type_int_array int[]",
                "type_double float8",
                "type_double_array float8[]",
                "type_string text",
                "type_string_array text[]",
                "type_float real",
                "type_float_array real[]",
                "type_long bigint",
                "type_long_array bigint[]",
                "type_bytes bytea",
                "type_bytes_array bytea[]",
                "type_boolean bool",
                "type_boolean_array bool[]",}, hdfsPath + avroArrayFileName + SUFFIX_AVRO);
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.array_types_gpdb.runTest");
    }

    /**
     * Read an Avro file that includes all supported complex types.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void avroComplex() throws Exception {
        prepareReadableTable("avrotest_complex", new String[]{
                "type_long bigint",
                "type_string text",
                "type_array text",
                "type_union text",
                "type_map text",
                "type_record text",
                "type_enum text",
                "type_fixed bytea"}, hdfsPath + avroComplexFileName + SUFFIX_AVRO);
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.complex_types.runTest");
    }

    /**
     * Read an Avro file that includes all supported complex types, reading the schema from another file.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void avroComplexReadSchemaFromHcfs() throws Exception {
        hdfs.copyFromLocal(resourcePath + complexAvroFile, hdfsPath + "schema/" + complexAvroFile);
        prepareReadableTable("avrotest_complex", new String[]{
                "type_long bigint",
                "type_string text",
                "type_array text",
                "type_union text",
                "type_map text",
                "type_record text",
                "type_enum text",
                "type_fixed bytea"}, hdfsPath + avroComplexFileName + SUFFIX_AVRO);
        String schemaPath = "/" + hdfsPath + "schema/" + complexAvroFile;
        exTable.setExternalDataSchema(schemaPath);
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.complex_types.runTest");
    }

    /**
     * Read an Avro file that includes all supported complex types, reading the schema from another file.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void avroComplexReadSchemaFromSegmentHosts() throws Exception {
        prepareReadableTable("avrotest_complex", new String[]{
                "type_long bigint",
                "type_string text",
                "type_array text",
                "type_union text",
                "type_map text",
                "type_record text",
                "type_enum text",
                "type_fixed bytea"}, hdfsPath + avroComplexFileName + SUFFIX_AVRO);
        String schemaPath = remotePublicStage + "/" + complexAvroFile;
        exTable.setExternalDataSchema(schemaPath);
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.complex_types.runTest");
    }

    /**
     * Read an Avro file that includes null values
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void avroNull() throws Exception {
        prepareReadableTable("avrotest_null", new String[]{
                "type_long bigint",
                "type_string text",
                "type_array text",
                "type_union text",
                "type_map text",
                "type_record text",
                "type_enum text",
                "type_fixed bytea"}, hdfsPath + avroComplexFileName + SUFFIX_AVRO);
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.null_values.runTest");
    }

    /**
     * Read an Avro file that includes unions with complex and null default values
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void avroComplexNull() throws Exception {
        prepareReadableTable("avrotest_complex_null", new String[]{
                "sourcetimestamp bigint",
                "sourceagent varchar(200)",
                "sourceip_address varchar(20)",
                "meetmeuser_id bigint",
                "meetmeinterested_uid bigint",
                "meetmevote varchar(4)",
                "meetmeis_match int",
                "meetmenetwork_score bigint",
                "meetmeresponsiveness_score bigint",
                "meetmesession_id varchar(32)",
                "meetmefriends int",
                "meetmeprevious_view varchar(32)",
                "meetmemodel bigint",
                "meetmescore float8",
                "meetmemethod int",
                "meetmecontributions varchar(120)",
                "meetmeclicksource varchar(20)",
                "meetmeclickaction varchar(20)",
                "meetmeprofileview_ts bigint",
                "meetmeplatform varchar(20)",
                "metatopic_name varchar(100)",
                "metarequest_user_agent varchar(200)",
                "metarequest_session_id varchar(50)",
                "metarequest_id text",
                "metakvpairs varchar(1000)",
                "meta_handlers varchar(1000)"}, hdfsPath + avroComplexNullFileName + SUFFIX_AVRO);
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.complex_null_values.runTest");
    }

    /**
     * Read an Avro format stored in sequence file, with separated schema file
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void avroInSequenceFileArrays() throws Exception {
        prepareReadableTable("avro_in_seq_arrays", new String[]{
                "num  text",
                "int1  integer",
                "int2  integer",
                "strings  text",
                "st1    text",
                "dubs    text",
                "dub    double precision",
                "fts   text",
                "ft   real",
                "longs  text",
                "lng   bigint",
                "bls   text",
                "bl   boolean",
                "bts   bytea",}, hdfsPath + avroInSequenceArraysFileName);
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":AvroSequenceFile");
        exTable.setExternalDataSchema(avroInSequenceArraysSchemaFile);
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.avro_in_sequence_arrays.runTest");
    }

    /**
     * Read an Avro format stored in sequence file, with separated schema file
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void avroFileNameWithSpaces() throws Exception {
        prepareReadableTable("avro_in_seq_arrays", new String[]{
                "num  text",
                "int1  integer",
                "int2  integer",
                "strings  text",
                "st1    text",
                "dubs    text",
                "dub    double precision",
                "fts   text",
                "ft   real",
                "longs  text",
                "lng   bigint",
                "bls   text",
                "bl   boolean",
                "bts   bytea",}, hdfsPath + avroInSequenceArraysFileName);
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":AvroSequenceFile");
        exTable.setExternalDataSchema(avroInSequenceArraysSchemaFileWithSpaces);
        exTable.setFormatter("pxfwritable_import");
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.avro_in_sequence_arrays.runTest");
    }


    /**
     * Read an Avro format stored in sequence file, with separated schema file
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void avroFileNameWithSpacesOnHcfs() throws Exception {
        hdfs.copyFromLocal(resourcePath + avroInSequenceArraysSchemaFileWithSpaces, hdfsPath + avroInSequenceArraysSchemaFileWithSpaces);
        prepareReadableTable("avro_in_seq_arrays", new String[]{
                "num  text",
                "int1  integer",
                "int2  integer",
                "strings  text",
                "st1    text",
                "dubs    text",
                "dub    double precision",
                "fts   text",
                "ft   real",
                "longs  text",
                "lng   bigint",
                "bls   text",
                "bl   boolean",
                "bts   bytea",}, hdfsPath + avroInSequenceArraysFileName);
        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":AvroSequenceFile");
        String schemaPath = "/" + hdfsPath + avroInSequenceArraysSchemaFileWithSpaces;
        exTable.setExternalDataSchema(schemaPath);
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.avro_in_sequence_arrays.runTest");
    }

    /**
     * Read multiple Avro files.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void avroMultiFiles() throws Exception {
        String schemaName = resourcePath + avroInSequenceArraysSchemaFile;

        Table dataTable = new Table("dataTable", null);

        Object[] data = FileFormatsUtils.prepareData(new CustomAvroPreparer(
                schemaName), 10000, dataTable);

        /**
         * To be used for Avro Complex types
         */
        String avroMultiBlockDir = "avro_multi/";
        for (int i = 0; i < 5; i++) {
            hdfs.writeAvroFile(hdfsPath + avroMultiBlockDir + "file" + i
                    + SUFFIX_AVRO, schemaName, null, (IAvroSchema[]) data);
        }

        prepareReadableTable("avro_multi", new String[]{
                "num  text",
                "int1  integer",
                "int2  integer",
                "strings  text",
                "st1    text",
                "dubs    text",
                "dub    double precision",
                "fts   text",
                "ft   real",
                "longs  text",
                "lng   bigint",
                "bls   text",
                "bl   boolean",
                "bts   bytea",}, hdfsPath + avroMultiBlockDir);
        gpdb.createTableAndVerify(exTable);

        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.multi_files.runTest");
    }

    /**
     * Read simple Avro file with Snappy and deflate compressions.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void avroCodecs() throws Exception {
        String schemaName = resourcePath + avroInSequenceArraysSchemaFile;
        Table dataTable = new Table("dataTable", null);

        String[] codecs = {"snappy", "deflate"};
        for (String codec : codecs) {
            String fileName = hdfsPath + codec + SUFFIX_AVRO;

            Object[] data = FileFormatsUtils.prepareData(
                    new CustomAvroPreparer(schemaName), 50, dataTable);
            hdfs.writeAvroFile(fileName, schemaName, codec,
                    (IAvroSchema[]) data);
            prepareReadableTable("avro_codec", new String[]{
                    "num  text",
                    "int1  integer",
                    "int2  integer",
                    "strings  text",
                    "st1    text",
                    "dubs    text",
                    "dub    double precision",
                    "fts   text",
                    "ft   real",
                    "longs  text",
                    "lng   bigint",
                    "bls   text",
                    "bl   boolean",
                    "bts   bytea",}, fileName);
            gpdb.createTableAndVerify(exTable);
            // Verify results
            runTincTest("pxf.features.hdfs.readable.avro.codec.runTest");
        }
    }

    /**
     * Try to read Avro file with extra field in the table definition. The
     * expected behaviour is to error out.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void extraField() throws Exception {
        prepareReadableTable("avro_extra_field", new String[]{
                "name text",
                "age int",
                "alive boolean"}, hdfsPath + avroSimpleFileName + SUFFIX_AVRO);
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.errors.extra_field.runTest");
    }

    /**
     * Try to read Avro file with missing field in the table definition. The
     * expected behaviour is to error out.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void missingField() throws Exception {
        prepareReadableTable("avro_missing_field",
                new String[]{"name text"},
                hdfsPath + avroSimpleFileName + SUFFIX_AVRO);
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.errors.missing_field.runTest");
    }

    /**
     * Try to read Avro file with wrong schema file.
     *
     * @throws Exception
     */
    @Test(groups = {"features", "gpdb", "hcfs", "security"})
    public void noSchemaFile() throws Exception {
        prepareReadableTable("avro_in_seq_no_schema", new String[]{
                "num  text",
                "int1  integer",
                "int2  integer",
                "strings  text",
                "st1    text",
                "dubs    text",
                "dub    double precision",
                "fts   text",
                "ft   real",
                "longs  text",
                "long   bigint",
                "bls   text",
                "bl   boolean",
                "bts   bytea",}, hdfsPath + avroInSequenceArraysFileName);

        exTable.setProfile(ProtocolUtils.getProtocol().value() + ":AvroSequenceFile");
        exTable.setExternalDataSchema("i_do_not_exist");
        gpdb.createTableAndVerify(exTable);
        // Verify results
        runTincTest("pxf.features.hdfs.readable.avro.errors.no_schema_file.runTest");
    }

    private void prepareData() throws Exception {

        // FILE_SCHEME is required here for MapR tests. MapR requires their
        // own hadoop-common libraries which implement FileSystem. In MapR
        // automation test, the FileSystem returns a MapRFS object for
        // relative paths instead of a LocalFS, which was causing issues
        // during data preparation.

        // Create Avro files from schema and json files
        String SUFFIX_AVSC = ".avsc";
        String SUFFIX_JSON = ".json";
        String FILE_SCHEME = "file://";
        hdfs.writeAvroFileFromJson(hdfsPath + avroSimpleFileName + SUFFIX_AVRO,
                FILE_SCHEME + resourcePath + avroSimpleFileName + SUFFIX_AVSC,
                FILE_SCHEME + resourcePath + avroSimpleFileName + SUFFIX_JSON, null);

        hdfs.writeAvroFileFromJson(hdfsPath + avroTypesFileName + SUFFIX_AVRO,
                FILE_SCHEME + resourcePath + avroTypesFileName + SUFFIX_AVSC,
                FILE_SCHEME + resourcePath + avroTypesFileName + SUFFIX_JSON, null);

        hdfs.writeAvroFileFromJson(hdfsPath + avroArrayFileName + SUFFIX_AVRO,
                FILE_SCHEME + resourcePath + avroArrayFileName + SUFFIX_AVSC,
                FILE_SCHEME + resourcePath + avroArrayFileName + SUFFIX_JSON, null);

        hdfs.writeAvroFileFromJson(hdfsPath + avroComplexFileName + SUFFIX_AVRO,
                FILE_SCHEME + resourcePath + avroComplexFileName + SUFFIX_AVSC,
                FILE_SCHEME + resourcePath + avroComplexFileName + SUFFIX_JSON, null);

        hdfs.writeAvroFileFromJson(hdfsPath + avroComplexNullFileName + SUFFIX_AVRO,
                FILE_SCHEME + resourcePath + avroComplexNullFileName + SUFFIX_AVSC,
                FILE_SCHEME + resourcePath + avroComplexNullFileName + SUFFIX_JSON, null);

        String schemaName1 = resourcePath + avroInSequenceArraysSchemaFile;
        Table dataTable1 = new Table("dataTable1", null);
        Object[] data1 = FileFormatsUtils.prepareData(new CustomAvroPreparer(
                schemaName1), 20, dataTable1);
        hdfs.writeAvroInSequenceFile(hdfsPath + avroInSequenceArraysFileName,
                schemaName1, (IAvroSchema[]) data1);
    }

    private void prepareReadableTable(String name, String[] fields, String path) {
        ProtocolEnum protocol = ProtocolUtils.getProtocol();
        // default external table with common settings
        exTable = new ReadableExternalTable(name, fields,
                protocol.getExternalTablePath(hdfs.getBasePath(), path), "custom");
        exTable.setHost(pxfHost);
        exTable.setPort(pxfPort);
        exTable.setProfile(protocol.value() + ":avro");
        exTable.setFormatter("pxfwritable_import");
    }
}
