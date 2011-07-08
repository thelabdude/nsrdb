package thelabdude.nsrdb;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MapReduce job for transforming the SequenceFile data into a TextOutputFormat file keyed by year
 * with station ID added to each record (useful for using the streaming API).
 */
public class GroupByYearJob implements Tool {

  private static final Charset UTF8 = Charset.forName("UTF-8");

  static class GroupByYearMapper extends Mapper<StationYearWritable, BytesWritable, IntWritable, Text> {
    private Text data = new Text();
    private IntWritable year = new IntWritable();

    public void map(StationYearWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
      // the value holds a CSV file containing data for a single station for one year
      // parse line-by-line (aren't you glad we have a hadoop cluster to work with)
      BufferedReader reader = null;
      String line = null;
      try {
        reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(value.getBytes(), 0, value.getLength()), UTF8));
        while ((line = reader.readLine()) != null) {
          if (line.length() > 0 && !line.startsWith("YYYY-MM-DD")) {
            // add the station ID on the front of the line
            year.set(key.year);
            data.set(key.station + "," + line);
            context.write(year, data);
          }
        }
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (Exception zzz) {
          }
        }
      }
    }
  }

  /**
   * Not used by this Job since we're only interested in the output of the Map step.
   * 
   * @author thelabdude
   */
  static class GroupByYearReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    public void reduce(IntWritable key, Iterable<StationDataWritable> values, Context context) throws IOException, InterruptedException {
      // no-op by design
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new GroupByYearJob(), args);
    System.exit(res);
  }
  
  private Configuration conf;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public int run(String[] args) throws Exception {  
    if (args.length != 2) {
      System.err.println("Usage: GroupByYearDataJob <input path> <output path>");
      return -1;
    }

    Job job = new Job();
    job.setJarByClass(GroupByYearJob.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(GroupByYearMapper.class);
    job.setReducerClass(GroupByYearReducer.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(0);

    return job.waitForCompletion(true) ? 0 : 1;
  }
}
