package abaka.serde;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Properties;

public class HL7SerDeTest extends TestCase {

    private String hl7String = "MSH|^~&\\|BioRefHMODownload-H|BIO-REFERENCE LABS|02/01/2013 -> 02/01/2013|1200239^|201302011340||ORU^R01|20130201134016.1|P|2.5|1||NE|NE\r" +
            "PID|1||107311769|10389866       |MARY^JANE||19231101|F|||367 91 ST^^BROOKLYN^NY^11209||3472604473||||||\r" +
            "OBR|1|10389866       |107691769|0009-1^Lipid Screen(Basic Lipid Profile)||201204180000|201204180000|||||||201204182334||H53870^FARHAT^LAILA^|(718)213-4287|80061||BIOREFERENCE LABORATORIES||201204191358|||F||||\r" +
            "OBX|1|NM|0058-8^Cholesterol^BRLI^2093-3^CHOLESTEROL^LOINC||166|mg/dL|<200||||F||82465|201204191358\r" +
            "OBX|2|NM|0059-6^HDL CHOL., DIRECT^BRLI^2085-9^CHOLESTEROL.IN HDL^LOINC||54||>40||||F||83718|201204191358\r" +
            "OBX|3|NM|0155-2^Triglycerides^BRLI^2571-8^TRIGLYCERIDE^LOINC||178|mg/dL|<150|H|||F||84478|201204191358\r" +
            "OBX|4|NM|1764-0^HDL as % of Cholesterol^BRLI^2095-8^CHOLESTEROL.IN HDL/CHOLESTEROL.TOTAL^LOINC||33|%|>14||||F|||201204191358";


    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
    }

    public void initialize(HL7SerDe serde) throws Exception {
        Configuration conf = null;
        Properties tbl = new Properties();
        tbl.setProperty(serdeConstants.LIST_COLUMNS, "one,two,three,four");
        tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, "string,string,string,string");
        serde.initialize(conf, tbl);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSerialize() throws Exception {

        HL7SerDe serde = new HL7SerDe();
        initialize(serde);

        try {
            serde.serialize(new Object(), serde.getObjectInspector());
            fail("serialize method is not implemented.");
        } catch (UnsupportedOperationException e) {
        }
    }

    @Test
    public void testDeserializeSimple() throws Exception {

        HL7SerDe serde = new HL7SerDe();
        initialize(serde);

        String hl7Data = getDataFromClassResourceFile("hl7.txt");
        Writable w = new Text(hl7Data);

        Object result = serde.deserialize(w);
        assertTrue(result instanceof ArrayList);
    }

    @Test
    public void testDeserializeGetValues() throws Exception {

        HL7SerDe serde = new HL7SerDe();
        initialize(serde);
        Writable w = new Text(hl7String);

        Object result = serde.deserialize(w);
        assertTrue(result instanceof ArrayList);

        assertEquals(((ArrayList) result).get(0), "107311769");
        assertEquals(((ArrayList) result).get(1), "MARY");
        assertEquals(((ArrayList) result).get(2), "19231101");
        assertEquals(((ArrayList) result).get(3), "F");

    }


    private HL7SerDe createSerDe(String fieldNames, String fieldTypes,
                                 String inputRegex, String outputFormatString) throws Throwable {
        Properties schema = new Properties();
        schema.setProperty(serdeConstants.LIST_COLUMNS, fieldNames);
        schema.setProperty(serdeConstants.LIST_COLUMN_TYPES, fieldTypes);
        schema.setProperty("input.regex", inputRegex);
        schema.setProperty("output.format.string", outputFormatString);

        HL7SerDe serde = new HL7SerDe();
        serde.initialize(new Configuration(), schema);
        return serde;
    }

    @Test
    public void testGetObjectInspector() throws Exception {
        HL7SerDe serde = new HL7SerDe();
        initialize(serde);
        assertNotNull(serde.getObjectInspector());
    }

    /// END OF TESTS ///

    private String getDataFromClassResourceFile(String file) {

        StringBuffer sb = new StringBuffer();
        String str = "";

        InputStream is = this.getClass().getClassLoader().getResourceAsStream(file);
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            if (is != null) {
                while ((str = reader.readLine()) != null) {
                    sb.append(str + "\n");
                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            try {
                is.close();
            } catch (Throwable ignore) {
            }
        }
        return sb.toString();

    }

}