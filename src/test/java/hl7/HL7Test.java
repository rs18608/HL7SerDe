package hl7;


import abaka.hl7.HL7;
import junit.framework.TestCase;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

public class HL7Test extends TestCase {

    Text text = new Text("MSH|^~\\&|XXXX|C|PRIORITYHEALTH|PRIORITYHEALTH|20080511103530||ORU^R01|Q335939501T337311002|P|2.3|||\rPID|1||94000000000^^^Priority Health||LASTNAME^FIRSTNAME^INIT||19460101|M||||| PD1|1|||1234567890^PCPLAST^PCPFIRST^M^^^^^NPI| OBR|1||185L29839X64489JLPF~X64489^ACC_NUM|JLPF^Lipid Panel - C||||||||||||1694^DOCLAST^DOCFIRST^^MD||||||20080511103529||| OBX|1|NM|JHDL^HDL Cholesterol (CAD)|1|62|CD:289^mg/dL|>40^>40|\"\"||\"\"|F|||20080511103500|||^^^\"\"| OBX|2|NM|JTRIG^Triglyceride (CAD)|1|72|CD:289^mg/dL|35-150^35^150|\"\"||\"\"|F|||20080511103500|||^^^\"\"| OBX|3|NM|JVLDL^VLDL-C (calc - CAD)|1|14|CD:289^mg/dL||\"\"||\"\"|F|||20080511103500|||^^^\"\"| OBX|4|NM|JLDL^LDL-C (calc - CAD)|1|134|CD:289^mg/dL|0-100^0^100|H||\"\"|F|||20080511103500|||^^^\"\"| OBX|5|NM|JCHO^Cholesterol (CAD)|1|210|CD:289^mg/dL|90-200^90^200|H||\"\"|F|||20080511103500|||^^^\"\"| - See more at: http://www.priorityhealth.com/provider/manual/office-mgmt/data-exchange/hl7/hl7-samples#sthash.9dMklkZF.dpuf");

    public void setUp() throws Exception {
        super.setUp();


    }

    public void tearDown() throws Exception {

    }

    public void testGetPatientId() throws Exception {

        HL7 hl7 = new HL7(text, new ArrayList<String>());


    }

}