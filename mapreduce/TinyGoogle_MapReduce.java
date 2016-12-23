import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.File;

public class TinyGoogle_MapReduce {

  public static class PostingMapper
       extends Mapper<Object, Text, Text, TinyGoogle_MapWritable>{

    private final static IntWritable one = new IntWritable(1);
    private IntWritable count = new IntWritable(0);
    private Text word = new Text();
    private Text book = new Text();
    private TinyGoogle_MapWritable book_count = new TinyGoogle_MapWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String[] words = value.toString().replaceAll("[^a-zA-Z0-9 -]+", "").toLowerCase().split("\\s+");

      String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
      book.set(filename);

      book_count.put(book, one);

      for(String stringWord : words) {
      	stringWord = stringWord.replaceAll("\\s+","");
        if(stringWord == "") continue;
        word.set(stringWord);
      	context.write(word, book_count);
      }
    }
  }

  public static class InvertedIndexReducer
       extends Reducer<Text,TinyGoogle_MapWritable,Text,TinyGoogle_MapWritable> {
    private TinyGoogle_MapWritable result = new TinyGoogle_MapWritable();

    public void reduce(Text key, Iterable<TinyGoogle_MapWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      result.clear();
  		for (TinyGoogle_MapWritable book_count : values) {
  			for(Writable book : book_count.keySet()){
          // Text bookText = (Text)book;
  				if(result.containsKey(book)) {
  					result.put(book, new IntWritable((int)( ((IntWritable)result.get(book)).get() ) 
  													 + (int)( ((IntWritable)book_count.get(book)).get() )));
  				} else {
  					result.put(book, book_count.get(book));
  				}
  			}
  		}
  		context.write(key, result);
  	}
  }

  public static void main(String[] args) throws Exception {
    double startTime = System.nanoTime();

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "TinyGoogle_MapReduce");
    job.setJarByClass(TinyGoogle_MapReduce.class);
    job.setMapperClass(PostingMapper.class);
    job.setCombinerClass(InvertedIndexReducer.class);
    job.setReducerClass(InvertedIndexReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TinyGoogle_MapWritable.class);
    FileInputFormat.addInputPath(job, new Path("/Users/risinger/Documents/edu/CloudComputing/Project/books/*"));
    FileOutputFormat.setOutputPath(job, new Path("/Users/risinger/Documents/edu/CloudComputing/Project/output/hadoop/"));

    boolean finished = job.waitForCompletion(true);
    if(finished)
    {
		double stopTime = System.nanoTime();
		PrintWriter pw = new PrintWriter(new File("/Users/risinger/Documents/edu/CloudComputing/Project/output/hadoop_time.txt"));
		pw.write(Double.toString((stopTime - startTime)/1e9d) + "\n");
		pw.close();
    }

    System.exit(finished ? 0 : 1);
  }
}