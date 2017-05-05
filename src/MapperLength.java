import java.io.IOException;
import org.apache.hadoop.io.IntWritable;		//From the other additional mapper.java
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper sample input
//epoch_time;tweetId;tweet(hashtag contained);device

//Example
//1469453965000;
//757570957502394369;
//This is an example text;
//<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>

public class MapperLength extends Mapper<Object, Text, IntWritable, IntWritable > { 

    private IntWritable data    = new IntWritable();    //Output key 	= Tweet length, binned into groups of 5
    private IntWritable count   = new IntWritable(1);	//Output value  = Always 1, doesn't change
    private int tweetLength;							//Temp variable to find tweet length
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    	//Split the data by ";" and add the splits to the array "input"
        String[] input = value.toString().split(";");
  
	    //If the line array doesn't have 4 objects, discard it 
	    if (input.length == 4){
	    	String tweet = input[2];
	    	
	    	//Replace non-alphanumeric characters with an alphanumeric
	    	tweet = tweet.replaceAll("[^A-Za-z0-9#]", " ");
	    	
	    	

	    	//Add the length of the tweet to variable tweetLength and group into bins of 5
	    	tweetLength = tweet.length();
    		data.set(tweetLength - ((tweetLength-1) % 5 ) + 4);
    		
	        context.write(data, count);
	    }        
    }
}