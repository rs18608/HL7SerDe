package abaka.hl7;

/**
 * Created by lynnscott on 9/23/14.
 */
public enum Columns {
    PATIENT_ID("patient_id"),
    PATIENT_NAME("patient_name"),
    PATIENT_DOB("dob"),
    PATIENT_GENDER("patient_gender"),
    STREET("street"),
    STATE("state"),
    ZIP("zip"),
    PATIENT_ADDRESS("patient_address"),
    OBX("obx"),
    NOTES("notes"),
    TEST_NAME("test_name"),
    TEST_UNITS("test_units"),
    TEST_RESULTS("test_results"),
    REFERENCE_RANGE("reference_range"),
    ABNORMAL_FLAGS("abnormal_flags"),
    PROBABILITY("probability"),
    OBX_RESULT_STATUS("observation_result_status"),
    DATE_OF_OBX("date_of_obx"),
    PRODUCER_ID("producer_id"),
    RESPONSIBLE_OBSERVER("responsible_observer"),
    OBX_METHOD("observation_method"),
    OBX_REQUEST_DATE("observation_requested_on"),
    ORDERING_PROVIDER("ordering_provider"),
    ORDER_DESCRIPTION("order_description");

    private final String name;

    private Columns(String s) {
        name = s;
    }

    public boolean equalsName(String otherName){
        return (otherName == null)? false:name.equals(otherName);
    }
    @Override
    public String toString(){
        return name;
    }
}

