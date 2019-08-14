package abaka;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.*;

public final class Json {

    private static final Log LOG = LogFactory.getLog(Json.class.getName());
    public static String mappingDataSourceKey = "";

    private static Properties properties;
    private static Set<String> hl7SourcesSet;
    private static String hl7Root;
    private static final String MAPPING_FILE_NAME = "/mapping.json";
    private static Map<String, JSONObject> jsonMappers = new HashMap<String, JSONObject>();

    static {
        InputStream is = null;
        try {
            properties = new Properties();
            is = ClassLoader.class.getResourceAsStream("/config.properties");
            properties.load(is);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        hl7Root = properties.getProperty("hl7Root");
        String hl7Sources = properties.getProperty("hl7Sources");
        if (hl7Sources != null) {
            hl7SourcesSet = new HashSet<String>(Arrays.asList(hl7Sources.split(",")));
        }

        //TODO: setup some kind of factory to handel this.
        //For production
        loadUpJsonMappers();


        //For unit test.
        //loadUpDefaultJsonMappers();
    }

    public static JSONObject getJsonMapperObject() {
        return jsonMappers.get(mappingDataSourceKey);
    }

    private static void loadUpJsonMappers() {
        Configuration conf = new Configuration();
        try {
            for (String source : hl7SourcesSet) {
                Path path = new Path(hl7Root + source);
                FileSystem fs = path.getFileSystem(conf);

                ContentSummary cSummary = fs.getContentSummary(new Path(path + MAPPING_FILE_NAME));
                byte[] contents = new byte[(int) cSummary.getLength()];

                FSDataInputStream fsDataInputStream = fs.open(new Path(path + MAPPING_FILE_NAME));
                IOUtils.readFully(fsDataInputStream, contents, 0, contents.length);
                IOUtils.closeStream(fsDataInputStream);
                JSONParser parser = new JSONParser();
                try {
                    Object obj = parser.parse(new String(contents, "US-ASCII"));
                    JSONObject jsonObject = (JSONObject) obj;
                    jsonMappers.put(source, jsonObject);

                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void loadUpDefaultJsonMappers() {
        Configuration conf = new Configuration();


        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream("defaultMapping.json");

        String source = "defaultMapping.json";
        mappingDataSourceKey = source;

                JSONParser parser = new JSONParser();
        try {

            Reader reader = new InputStreamReader(is);
            Object obj = parser.parse(reader);
            JSONObject jsonObject = (JSONObject) obj;
            jsonMappers.put(source, jsonObject);

        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException ioe) {
        }
    }

    public static void main(String[] args) {
        Json.mappingDataSourceKey = "labcorp";
        JSONObject jsonObject = Json.getJsonMapperObject();
        String s = jsonObject.get("patientId").toString();

    }
}
