package serde;

import abaka.serde.HL7SerDe;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;
//import org.openx.data.jsonserde.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NestedMessageTest {
    static HL7SerDe instance;

    @Before
    public void setUp() throws Exception {
        initialize();
    }

    static public void initialize() throws Exception {
        instance = new HL7SerDe();
        Configuration conf = null;
        Properties tbl = new Properties();

        tbl.setProperty(serdeConstants.LIST_COLUMNS, "patient_id,patient_name,dob,patient_gender,patient_address,obx");
        tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, ("string,string,string,string,string,ARRAY<STRUCT<notes:STRING," +
                "test_name:STRING,test_units:STRING,test_results:STRING,reference_range:STRING,abnormal_flags:STRING," +
                "probability:STRING,observation_result_status:STRING,date_of_observation:STRING,producter_id:STRING," +
                "responsible_observer:STRING,observation_method:STRING,observation_requested_on:STRING,ordering_provider:STRING," +
                "orderDescription:STRING>>").toLowerCase());

 //        tbl.setProperty("mapping.v_items", "items");
 //        tbl.setProperty("mapping.v_statistics", "statistics");

        instance.initialize(conf, tbl);
    }

    @Test
    public void testDeSerialize() throws Exception {
        // Test that timestamp object can be deserialized
        Writable hl7String = new Text("MSH|^~&\\|BioRefHMODownload-H|BIO-REFERENCE LABS|02/01/2013 -> 02/01/2013|1200239^|201302011340||ORU^R01|20130201134016.1|P|2.5|1||NE|NE\r" +
                "PID|1||107311769|10389866       |MARY^JANE||19231101|F|||367 91 ST^^BROOKLYN^NY^11209||3472604473||||||\r" +
                "OBR|1|10389866       |107691769|0009-1^Lipid Screen(Basic Lipid Profile)||201204180000|201204180000|||||||201204182334||H53870^FARHAT^LAILA^|(718)213-4287|80061||BIOREFERENCE LABORATORIES||201204191358|||F||||\r" +
                "OBX|1|NM|0058-8^Cholesterol^BRLI^2093-3^CHOLESTEROL^LOINC||166|mg/dL|<200||||F||82465|201204191358\r" +
                "OBX|2|NM|0059-6^HDL CHOL., DIRECT^BRLI^2085-9^CHOLESTEROL.IN HDL^LOINC||54||>40||||F||83718|201204191358\r" +
                "OBX|3|NM|0155-2^Triglycerides^BRLI^2571-8^TRIGLYCERIDE^LOINC||178|mg/dL|<150|H|||F||84478|201204191358\r" +
                "OBX|4|NM|1764-0^HDL as % of Cholesterol^BRLI^2095-8^CHOLESTEROL.IN HDL/CHOLESTEROL.TOTAL^LOINC||33|%|>14||||F|||201204191358");

        Object result = instance.deserialize(hl7String);
        StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();

        assertEquals("107311769", soi.getStructFieldData(result, soi.getStructFieldRef("patient_id")));
        assertEquals("MARY", soi.getStructFieldData(result, soi.getStructFieldRef("patient_name")));
        assertEquals("19231101", soi.getStructFieldData(result, soi.getStructFieldRef("dob")));
        assertEquals("F", soi.getStructFieldData(result, soi.getStructFieldRef("patient_gender")));
        assertEquals("367 91 ST BROOKLYN NY 11209", soi.getStructFieldData(result, soi.getStructFieldRef("patient_address")));

        //Field names
        StructField pageInfoSF = soi.getStructFieldRef("obx");
        //values, array of maps
        Object pageInfo = soi.getStructFieldData(result, pageInfoSF);
        assertEquals(4, soi.getStructFieldsDataAsList(pageInfo).size());


        ObjectInspector objectInspector = pageInfoSF.getFieldObjectInspector();

        StructObjectInspector pageInfoOI = (StructObjectInspector) pageInfoSF.getFieldObjectInspector();

        // should have only 2 elements, totalResults and ResultsPerPage
        assertEquals(2, pageInfoOI.getAllStructFieldRefs().size());

        // now, let's check totalResults
        StructField trSF = pageInfoOI.getStructFieldRef("totalresults");
        Object totalResults = pageInfoOI.getStructFieldData(pageInfo, trSF);

        assertTrue(trSF.getFieldObjectInspector().getCategory() == Category.PRIMITIVE);
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector) trSF.getFieldObjectInspector();
        assertTrue(poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT);

        SettableIntObjectInspector sioi = (SettableIntObjectInspector) poi;
        int value = sioi.get(totalResults);


        assertEquals(1, value);

    }
}
