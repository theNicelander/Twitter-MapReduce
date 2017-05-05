import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
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

public class MapperDate extends Mapper<Object, Text, Text, IntWritable > { 

    private IntWritable count   = new IntWritable(1);	//Output value  = Always 1, doesn't change
    private Text storeDate		= new Text();

    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split(";");
        
        try
        {
        	//Get the value of the date
        	String epoch_time = line[0];
            Date tweetsdate = new Date(Long.parseLong(epoch_time));
            
            //Convert it to a yyyy.MM.dd format
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd");
            storeDate.set(dateFormat.format(tweetsdate).toString());
            
            context.write(storeDate, count);
        	
        }
        catch (Exception e)
        {
        	
        }
    }
}