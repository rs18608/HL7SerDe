package abaka.hadoop;

import abaka.hl7.HL7;
import ca.uhn.hl7v2.HL7Exception;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;

import javax.xml.bind.ValidationException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class Main {


    private String[] lines = null;
    private LongWritable key = null;
    private Text value = null;

    private FileSplit split;
    private Configuration job;
    private Path file;

    final String MSH_HEADER = "MSH|^";

    private long lastOccurenceOfHL7;
    private String parsedText;

    public static void main(String[] args) throws IOException {

        Main main = new Main();
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream("hl7.txt");
        //InputStream is = classloader.getResourceAsStream("SANITIZEDbendhl7.txt");
        byte[] contents = IOUtils.toByteArray(is);

        try {
            main.parsedText = new String(contents, "US-ASCII");//.replaceAll("\r\n", "\r");

            int pos = 0;
            int end = main.parsedText.length();
            main.lastOccurenceOfHL7 = main.parsedText.lastIndexOf(main.MSH_HEADER);

            while (pos < main.lastOccurenceOfHL7) {
                Text segment = new Text();
                pos = main.getNextSegment(segment, (int) pos);
                System.out.println("NEW POS: " + pos);

                try {
                    HL7 hl7 = new HL7(segment, new ArrayList<String>());
 //                   hl7.getPatientAddress();
 //                   hl7.getDateOfObx();

                } catch (ValidationException e) {
                    e.printStackTrace();
                }/* catch (HL7Exception hl7e) {
                    hl7e.getDetail();
                }*/


                String s = "";

            }

        } finally {
            //IOUtils.closeStream(in);
        }
    }


    private int getNextSegment(Text segment, int startOffset) {
        System.out.println("START OFFSET: " + startOffset);
        System.out.println("LAST OCCURANCE: " + lastOccurenceOfHL7);


        if (startOffset == lastOccurenceOfHL7) {
            String seg = parsedText.substring(startOffset);
            System.out.println("NEW STUFF: " + seg.substring(0, 25));
            segment.set(seg);

            return (int) lastOccurenceOfHL7;
        }
        int endOffset = parsedText.indexOf(MSH_HEADER, MSH_HEADER.length() + startOffset);
        String seg = parsedText.substring(startOffset, endOffset);
        System.out.println("NEW STUFF: " + seg.substring(0, 25));
        segment.set(seg);
        return endOffset;
    }

    private String getDataFromClassResources(String file) {

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
            }catch (Throwable ignore) {
            }
        }
        return sb.toString();

    }


}
