import java.io.IOException;
import org.apache.hadoop.io.IntWritable;		//From the other additional mapper.java
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper sample input
//epoch_time;tweetId;tweet(hashtag contained); device

//Example
//1469453965000;
//757570957502394369;
//This is an example text;
//<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>

public class MapperAverage extends Mapper<Object, Text, IntWritable, IntWritable > { 

    private IntWritable data    = new IntWritable();    //Output key 	= Tweet length
    private IntWritable count   = new IntWritable(1);	//Output value  = Always 1, doesn't change
    private int tweetLength;							//Temp variable to find tweet length
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    	//Split the data by ";" and add the splits to the array "line"
        String[] input = value.toString().split(";");
  
	    //If the line array doesn't have 4 objects, the data is invalid 
	    if (input.length == 4){
	    	String tweet = input[2];
	    	
	    	//Replace non-alphanumeric characters with an alphanumeric and set as tweetLength
	    	tweet = tweet.replaceAll("[^A-Za-z0-9#]", " ");
	    	tweetLength = tweet.length();
    		    		
    		data.set(tweetLength);
    		context.write(count, data);
	    }       
    }
}

