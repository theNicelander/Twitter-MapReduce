import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

  public static void runJob(String[] input, String output) throws Exception {

	Configuration conf = new Configuration();
	
    Job job = new Job(conf);
    
//job.setNumReduceTasks(3);
    
    job.setJarByClass(Main.class);
    															//To run different Mappers and reducers:
    job.setMapperClass(MapperHashtag.class);					//Change <Mapper>.class 	to name of desired mapper 	
    job.setReducerClass(ReducerHashtag.class);					//Change <Reducer>.class 	to name of desired reducer 
    
    job.setMapOutputKeyClass(Text.class);					//Change <type>.class 		to match mapper Key output
    job.setMapOutputValueClass(IntWritable.class);			//Change <type>.class 		to match mapper Value output		    
    
    Path outputPath = new Path(output);
    FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
    FileOutputFormat.setOutputPath(job, outputPath);
    outputPath.getFileSystem(conf).delete(outputPath,true);
    job.waitForCompletion(true);
    
  }

  public static void main(String[] args) throws Exception {
       runJob(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
       
  }

}