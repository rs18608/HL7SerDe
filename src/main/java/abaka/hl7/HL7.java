package abaka.hl7;

import abaka.Json;
import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.CanonicalModelClassFactory;
import ca.uhn.hl7v2.parser.PipeParser;
import ca.uhn.hl7v2.util.Terser;
import ca.uhn.hl7v2.validation.impl.NoValidation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.json.simple.JSONObject;

import javax.xml.bind.ValidationException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HL7 {

    private static final Log LOG = LogFactory.getLog(HL7.class.getName());
    private Terser terser;
    private JSONObject jsonObject;

    private Map<String, Object> values = new HashMap<String, Object>();

    public HL7(Writable msg, ArrayList<String> fieldNames) throws ValidationException {

        LOG.debug("Enter HL7 class constructor: " + msg.toString());
        HapiContext context = new DefaultHapiContext();
        //We want all parsed messages to be for HL7 version 2.5, despite what MSH-12 says.
        //See http://hl7api.sourceforge.net/xref/ca/uhn/hl7v2/examples/HandlingMultipleVersions.html
        CanonicalModelClassFactory mcf = new CanonicalModelClassFactory("2.6");
        context.setModelClassFactory(mcf);

        jsonObject = Json.getJsonMapperObject();
        PipeParser pipeParser = context.getPipeParser();

        try {


           // ca.uhn.hl7v2.model.v25.message.ORU_R30 message1 = (ca.uhn.hl7v2.model.v25.message.ORU_R30) pipeParser.parse(msg.toString());

            pipeParser.setValidationContext(new NoValidation());
            Message message = pipeParser.parse(msg.toString());
            LOG.info("Message to be parsed: " + message.toString());

            terser = new Terser(message);
            for (String field : fieldNames) {
                if (field.equals("patient_address")) {
                    values.put(field, getPatientAddress());
                    continue;
                }
                if (field.equals(Columns.OBX.toString())) {

                    List obxs = new ArrayList();
                    int i = 0;
                    while (true) {
                        String result = terser.get("/.OBSERVATION("+i+")/OBX-3-1");
                        if (result != null) {
                            HashMap map = new HashMap();
                            map.put(Columns.NOTES.toString(), "notes");
                            map.put(Columns.TEST_NAME.toString(), terser.get("/.OBSERVATION("+i+")/"+
                                    jsonObject.get(Columns.TEST_NAME.toString()).toString()));
                            map.put(Columns.TEST_UNITS.toString(), terser.get("/.OBSERVATION(" + i + ")/" +
                                    jsonObject.get(Columns.TEST_UNITS.toString()).toString()));
                            map.put(Columns.TEST_RESULTS.toString(), terser.get("/.OBSERVATION("+i+")/"+
                                    jsonObject.get(Columns.TEST_RESULTS.toString()).toString()));
                            map.put(Columns.REFERENCE_RANGE.toString(), terser.get("/.OBSERVATION("+i+")/"+
                                    jsonObject.get(Columns.TEST_RESULTS.toString()).toString()));
                            map.put(Columns.ABNORMAL_FLAGS.toString(), "abnormal_flags");  //TODO: find real mapping
                            map.put(Columns.PROBABILITY.toString(), "probability");        //TODO:  find real mapping
                            map.put(Columns.OBX_RESULT_STATUS.toString(), terser.get("/.OBSERVATION("+i+")/"+
                                    jsonObject.get(Columns.TEST_RESULTS.toString()).toString()));
                            map.put(Columns.DATE_OF_OBX.toString(), terser.get("/.OBSERVATION("+i+")/"+
                                    jsonObject.get(Columns.DATE_OF_OBX.toString()).toString()));
                            map.put(Columns.PRODUCER_ID.toString(), "producer_id"); //TODO: find real mapping
                            map.put(Columns.RESPONSIBLE_OBSERVER.toString(), "responsible_observer"); //TODO: find real mapping
                            map.put(Columns.OBX_METHOD.toString(), "observation_method"); //TODO: find real mapping
                            map.put(Columns.OBX_REQUEST_DATE.toString(), terser.get("/.ORDER_OBSERVATION("+i+")/"+
                                    jsonObject.get(Columns.OBX_REQUEST_DATE.toString()).toString()));
                            map.put(Columns.ORDERING_PROVIDER.toString(), terser.get("/.ORDER_OBSERVATION(" + i + ")/" +
                                    jsonObject.get(Columns.ORDERING_PROVIDER.toString()).toString()));
                            map.put(Columns.ORDER_DESCRIPTION.toString(), terser.get("/.ORDER_OBSERVATION("+i+")/"+
                                    jsonObject.get(Columns.ORDERING_PROVIDER.toString()).toString()));
                            obxs.add(map);
                            i++;
                        }
                        else {
                            break;
                        }
                    }
                    values.put(field, obxs);
                    continue;

                }
                values.put(field, terser.get(jsonObject.get(field).toString()) == null ? "" : terser.get(jsonObject.get(field).toString()));
            }

        } catch (HL7Exception e) {
            e.printStackTrace();
            throw new ValidationException(e);
        }
    }

    public Object getValue(String fieldName) {
        return values.get(fieldName);
    }

    public String getPatientAddress()  throws HL7Exception {
        String street = terser.get(jsonObject.get("street").toString()) == null ? "" : terser.get(jsonObject.get("street").toString());
        String city = terser.get(jsonObject.get("city").toString()) == null ? "" : terser.get(jsonObject.get("city").toString());
        String state = terser.get(jsonObject.get("state").toString()) == null ? "" : terser.get(jsonObject.get("state").toString());
        String zip = terser.get(jsonObject.get("zip").toString()) == null ? "" : terser.get(jsonObject.get("zip").toString());
        return (street + " " + city + " " + state + " " + zip);
    }

    private String formatDateTime(String dateTime) {

        if (dateTime.endsWith(""))
            return "";

        DateTime dt = DateTimeFormat.forPattern("yyyyMMddHHmm").parseDateTime(dateTime);

        return (dt.toDateTime().getDayOfMonth() + "/" + dt.toDateTime().getDayOfWeek() + "/" +
                dt.toDateTime().getYear() + " " + dt.toDateTime().getHourOfDay() + ":" +
                dt.toDateTime().getMinuteOfHour());

    }

}
