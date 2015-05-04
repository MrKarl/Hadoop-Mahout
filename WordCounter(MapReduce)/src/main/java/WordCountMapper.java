import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * K1 : read key type
 * V2 : read value type
 * K2 : write key type
 * V2 : write value type
 */
//public class WordCountMapper implements Mapper<K1, V1, K2, V2> {
public class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	// map 결과는 reducer로 자동으로 던져진다 
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while(tokenizer.hasMoreTokens()) {
			Text outputKey = new Text(tokenizer.nextToken());
			// Hadoop 에서 wrapping한 Integer 타입의 객체를 넣어줌 
			// param1: outputKey, param2: outputValue
			output.collect(outputKey, new IntWritable(1));	
		}
	}
}