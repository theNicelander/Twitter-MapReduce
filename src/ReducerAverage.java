import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerAverage extends Reducer<IntWritable, IntWritable, IntWritable, LongWritable> {

    private LongWritable result = new LongWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

        long sumLength 	= 0;		//Length of the tweets, added
        long count 	= 0;			//Counter, number of tweets

        //For every tweet, add the length to sumLength and increase the count by one
        for (IntWritable value : values) {  	
        	sumLength += value.get();
        	count += 1;
        }
        
       //Set the average of the length and count of the tweets to result
        result.set(sumLength / count);

        context.write(key, result);
    }
}