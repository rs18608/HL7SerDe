package abaka.hadoop;

import abaka.Json;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

public class HL7RecordReader implements RecordReader<LongWritable, Text> {

    private static final Log LOG = LogFactory.getLog(HL7RecordReader.class.getName());
    private FileSplit fileSplit;
    private long pos;
    private long end;
    private long lastOccurrenceOfHL7;
    private final String MSH_HEADER = "MSH|^";
    private final String MAPPING_FILE_NAME = "/mapping.json";
    String parsedText;

    public HL7RecordReader(FileSplit fileSplit, Configuration conf) throws IOException {

        this.fileSplit = fileSplit;
        Path path = fileSplit.getPath();
        this.pos = fileSplit.getStart();
        this.end = fileSplit.getLength();

        FileSystem fs = path.getFileSystem(conf);
        int index = path.getParent().toString().lastIndexOf("/");
        String hl7Source = path.getParent().toString().substring(index + 1);
        Json.mappingDataSourceKey=hl7Source;

        //hdfs://localhost:9000/user/hive/warehouse/hl7p/labcorp/hl7.txt
        byte[] contents = new byte[(int) this.end];

        FSDataInputStream fdis = fs.open(path);
        IOUtils.readFully(fdis, contents, 0, contents.length);
        IOUtils.closeStream(fdis);
        parsedText = new String(contents, "US-ASCII");
        this.lastOccurrenceOfHL7 = parsedText.lastIndexOf(MSH_HEADER);
    }

    public synchronized boolean next(LongWritable key, Text value) throws IOException {

        LOG.debug("position: " + pos);
        LOG.debug("end: " + end);

        while (pos < lastOccurrenceOfHL7) {
            key.set(pos);
            Text segment = new Text();
            pos = getNextSegment(segment, (int) pos);
            LOG.debug("new position: " + end);
            value.set(segment);
            return true;
        }
        return false;
    }


    private int getNextSegment(Text segment, int startOffset) {
        LOG.debug("start offset: " + startOffset);
        LOG.debug("last occurrence of HL7: " + lastOccurrenceOfHL7);

        if (startOffset == lastOccurrenceOfHL7) {
            LOG.debug("Reached last record!");
            String seg = parsedText.substring(startOffset);
            LOG.debug("New input value (arbitrary length): " + seg.substring(0, 25));
            segment.set(seg);
            return (int) lastOccurrenceOfHL7;
        }
        int endOffset = parsedText.indexOf(MSH_HEADER, MSH_HEADER.length() + startOffset);
        String seg = parsedText.substring(startOffset, endOffset);
        LOG.debug("New input value (arbitrary length): " + seg.substring(0, 25));
        segment.set(seg);
        return endOffset;
    }

    @Override
    public LongWritable createKey() {
        LOG.debug("createKey() called");
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        LOG.debug("createValue() called");
        return new Text();
    }

    @Override
    public long getPos() throws IOException {
        LOG.debug("getPos() called");
        return fileSplit.getLength();
    }

    @Override
    public float getProgress() throws IOException {
        LOG.debug("getProgress() called");
        return 0;
    }

    @Override
    public void close() throws IOException {
        LOG.debug("close() called");
    }

}
