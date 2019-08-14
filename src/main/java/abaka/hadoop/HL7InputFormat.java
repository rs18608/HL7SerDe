package abaka.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;


public class HL7InputFormat extends FileInputFormat<LongWritable, Text> {

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(
        //InputSplit is a chunk of the overall data organized into records.  In this case
        //it's the entire HL7 message file.
        InputSplit inputSplit, JobConf entries, Reporter reporter) throws IOException {
            return new HL7RecordReader((FileSplit) inputSplit, entries);
        }
}