import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * K1 : Mapper의 K2 와 동일
 * V1 : Mapper의 V2 와 동일 
 */
public class WordCountReducer extends MapReduceBase 
							  implements Reducer<Text, IntWritable, Text, IntWritable> {

	/**
	 * V1 에서 values는 Iterator이다. 실제 같은 단어가 여러개 일 경우 
	 */
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		int sum = 0;
		while(values.hasNext()) {
			sum += values.next().get(); // get Integer value
		}
		output.collect(key, new IntWritable(sum));
	}
}