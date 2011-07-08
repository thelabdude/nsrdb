package thelabdude.nsrdb;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.aggregate.DoubleValueSum;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * MapReduce job for finding the station that measured the most Solar radiation during the winter months by year.
 */
public class MaxValueByYearJob implements Tool {

  private static Logger log = Logger.getLogger(MaxValueByYearJob.class);

  public static final Charset UTF8 = Charset.forName("UTF-8");
  public static final String CONF_DATA_COLUMN_INDEX = "data.column.index";
  public static final String STATION_METADATA_PATH = "/nsrdb/station-metadata.csv";

  // Counters we keep track of during map / reduce tasks
  // It's best to use a different counter for each event in your
  // job, especially error counters to make it easier to track
  // down runtime issues in a distributed cluster
  enum StationDataCounters {
    STATION_IGNORED, DATE_PARSE_ERROR, DATA_PARSE_ERROR
  }

  private static final double MISSING_DATA = -1d;

  private static final String EXPECTED_DATE_FORMAT = "yyyy-MM-dd";

  private static final Set<String> LOWER_48_US_STATES =
      new HashSet<String>(Arrays.asList("AL", "AR", "AZ", "CA", "CO", "CT", "DE", "FL",
          "GA", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN",
          "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK",
          "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"));

  /**
   * Load a mapping of station ID to station metadata from the Hadoop DistributedCache
   */
  private static Map<Long, StationMetadataWritable> getStationMetadataFromCache(Configuration config) throws IOException {
    Map<Long, StationMetadataWritable> map = null;
    Path[] cacheFiles = DistributedCache.getLocalCacheFiles(config);
    if (cacheFiles != null && cacheFiles.length > 0) {
      log.debug("Found " + cacheFiles.length + " files in distributed cache.");
      for (Path path : cacheFiles) {
        if ("station-metadata.csv".equals(path.getName())) {
          map = StationMetadataWritable.load(new FileReader(path.toString()));
          log.info("Successfully loaded metadata for " + map.size() + " stations from DistributedCache: "+path.toString());
          break;
        }
      }
    } else {
      log.debug("No files in distributed cache.");
    }

    if (map == null) {
      // For unit testing, try to load the path directly from the file system.
      FileSystem fs = FileSystem.get(config);
      Path localPath = new Path("./target/"+STATION_METADATA_PATH);
      if (fs.isFile(localPath)) {
        map = StationMetadataWritable.load(new FileReader(localPath.toString()));
        log.debug("Successfully loaded metadata for " + map.size() + " stations from local Path: "+localPath.toString());
      }

      if (map == null) {
        throw new IllegalStateException("No station metadata in DistributedCache! Job cannot run without station metadata.");
      }
    }

    return map;
  }

  static class MaxValueByYearMapper
      extends Mapper<StationYearWritable, BytesWritable, IntWritable, StationDataWritable> {
    private int dataColumnIndex = 6; // configurable via the 'data.column.index' value
    
    // It's a good idea to re-use writable objects instead of new'ing them, but
    // make sure you reset the values each time the map method is called!
    private StationDataWritable data = new StationDataWritable();
    private IntWritable year = new IntWritable();
    
    private SimpleDateFormat dateParser = new SimpleDateFormat(EXPECTED_DATE_FORMAT);
    private boolean isDebugEnabled = false;
    private Context hadoopContext = null;
    private Map<Long, StationMetadataWritable> stationMetadataMap = null;

    // called once to get the column index to read data from
    // and to setup access to station metadata
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);

      this.hadoopContext = context;

      // optimization to avoid formatting complex debug messages if debugging is off
      this.isDebugEnabled = log.isDebugEnabled();

      Configuration config = context.getConfiguration();
      dataColumnIndex = config.getInt(CONF_DATA_COLUMN_INDEX, 6);

      // Load station metadata from the distributed cache
      stationMetadataMap = MaxValueByYearJob.getStationMetadataFromCache(config);
    }

    @Override
    protected void map(StationYearWritable key, BytesWritable value, Context context)
        throws IOException, InterruptedException {
      // Save a ref to the Hadoop Context for accessing Counters in other methods
      this.hadoopContext = context;

      // first, verify that we care about this station ...
      StationMetadataWritable stationMetadata = (StationMetadataWritable) stationMetadataMap.get(key.station);
      if (stationMetadata == null || !isStationOfInterest(stationMetadata)) {
        // using counters helps make jobs easier to unit test
        if (isDebugEnabled) {
          log.debug("Skipping station " + key.station);
        }
        context.getCounter(StationDataCounters.STATION_IGNORED).increment(1);
        return;
      }

      if (isDebugEnabled) {
        log.debug("Parsing CSV for station/year " + key);
      }

      year.set(key.year);
      // reset the data counter for each key
      data.reset(key.station);

      // the value holds a CSV file containing data for a
      // single station for one year ... parse line-by-line
      BufferedReader reader = null;
      String[] record = null;
      String line = null;
      double rowData = MISSING_DATA;
      int row = 0;
      try {
        // Careful!!! BytesWritable.getBytes returns an array that may be bigger
        // than the valid number of bytes, so you have to constrain your reading to
        // value.getLength() ...
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(value.getBytes(), 0, value.getLength());
        reader = new BufferedReader(new InputStreamReader(bytesIn, UTF8));
        while ((line = reader.readLine()) != null) {
          ++row;
          if (line.length() == 0 || line.startsWith("YYYY-MM-DD"))
            continue; // ignore empty lines and headers

          record = line.split(",");
          rowData = extractDataFromRecord(key, row, record);
          if (rowData != MISSING_DATA) {
            // we could output each record separately, but that will send too
            // many unnecessary key/value pairs to the reducer which bogs down
            // the shuffle phase, so we summarize data for the same station / year
            data.addData(rowData);
          }
        }
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (Exception zzz) {}
        }
      }

      // output the summed data for the year for this station
      if (data.dataCount > 0) {
        context.write(year, data);
      } // else no data to output for this key
    }

    private final boolean isStationOfInterest(StationMetadataWritable stationMetadata) {
      if (stationMetadata == null) {
        return false; // invalid station
      }

      if (stationMetadata.classification > 2) {
        // data for this station is suspect ... skip it for now ...
        return false;
      }

      if (!LOWER_48_US_STATES.contains(stationMetadata.state)) {
        // not in the lower 48 US states ...
        return false;
      }

      return true;
    }

    @SuppressWarnings("deprecation")
    private final boolean inWinter(final StationYearWritable key, final String dateStr, final int rowNum, final String[] csvRow) {
      int month = Integer.MAX_VALUE;
      if (dateStr != null) {
        try {
          Date date = dateParser.parse(dateStr);
          month = date.getMonth();
        } catch (ParseException parseExc) {
          // keep a count of the number of parse errors
          hadoopContext.getCounter(StationDataCounters.DATE_PARSE_ERROR).increment(1);

          // Only log parse errors when debugging since there may be many
          log.error(String.format("Failed to parse '%s' into a Date at row %d for pair %s = %s",
                dateStr, rowNum, String.valueOf(key), Arrays.asList(csvRow).toString()));
        }
      }
      // return true if month is Dec, Jan, or Feb ...
      // java.util.Date month is 0-based
      return (month == 0 || month == 1 || month == 11);
    }

    // extract a float value from the configured 'dataColumnIndex' column of the given row
    private final double extractDataFromRecord(StationYearWritable key, final int rowNum, String[] csvRow) {
      if (csvRow.length > dataColumnIndex) {
        double data = MISSING_DATA;
        try {
          data = Double.parseDouble(csvRow[dataColumnIndex]);
        } catch (NumberFormatException nfe) {
          // keep a count of the number of parse errors
          hadoopContext.getCounter(StationDataCounters.DATA_PARSE_ERROR).increment(1);

          if (isDebugEnabled) {
            log.debug(String.format("Failed to parse '%s' into float at column %d at row %d for key %s",
                csvRow[dataColumnIndex], dataColumnIndex, rowNum, String.valueOf(key)));
          }
        }

        if (data > 0f) {
          return inWinter(key, csvRow[0], rowNum, csvRow) ? data : MISSING_DATA;
        }
      }
      return MISSING_DATA;
    }
  }

  /**
   * Sums the data for each station for each year to find the max.
   */
  static class MaxValueByYearReducer
      extends Reducer<IntWritable, StationDataWritable, IntWritable, Text> {
    private Map<Long, StationMetadataWritable> stationMetadataMap = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      // Load station metadata from the distributed cache
      stationMetadataMap = MaxValueByYearJob.getStationMetadataFromCache(context.getConfiguration());
    }

    @Override
    protected void reduce(IntWritable key, Iterable<StationDataWritable> values, Context context)
        throws IOException, InterruptedException {
            
      // aggregate the data values for each station for the given year ...
      Map<Long, DoubleValueSum> stationSumMap = new HashMap<Long, DoubleValueSum>();
      for (StationDataWritable next : values) {
        DoubleValueSum sum = stationSumMap.get(next.station);
        if (sum == null) {
          sum = new DoubleValueSum();
          stationSumMap.put(next.station, sum);
        }
        sum.addNextValue(next.data);
      }
      
      long maxStationId = -1L;
      double maxSum = Double.MIN_VALUE;
      for (Long stationId : stationSumMap.keySet()) {
        double sum = stationSumMap.get(stationId).getSum();
        if (sum > maxSum) {
          maxSum = sum;
          maxStationId = stationId;
        }
      }
      
      if (maxStationId > 0) {
        StationMetadataWritable maxStationMetadata = (StationMetadataWritable) stationMetadataMap.get(maxStationId);
        context.write(key, new Text(maxStationMetadata.toString() + "," + maxSum));
      } else {
        context.write(key, new Text("?"));
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
    }
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new MaxValueByYearJob(), args));
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
    if (args.length < 3) {
      System.err.println("Usage: MaxValueByYearJob <input path> <output path> <station metadata path> <num reducers>");
      return 1;
    }

    Job job = new Job();
    job.setJarByClass(MaxValueByYearJob.class);

    Path inputPath = new Path(args[0]);
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // Copy the station metadata CSV into HDFS so it is available from the DistributedCache
    Path stationMetadataLocal = new Path(args[2]);
    Path stationMetadataHdfs = new Path(STATION_METADATA_PATH);
    Configuration conf = job.getConfiguration();

    // If the CSV metadata is in S3, then you need to manually copy the data into HDFS
    // before putting it into the DistributedCache
    URI localUri = stationMetadataLocal.toUri();
    if ("s3".equals(localUri.getScheme()) || "s3n".equals(localUri.getScheme())) {
      copyFromS3File(conf, stationMetadataLocal, stationMetadataHdfs);
    } else {
      FileSystem fs = FileSystem.get(conf);
      fs.copyFromLocalFile(false, true, stationMetadataLocal, stationMetadataHdfs);
    }
    DistributedCache.addCacheFile(stationMetadataHdfs.toUri(), conf);
    log.debug("Added "+stationMetadataHdfs.toUri()+" to DistributedCache");

    job.setMapperClass(MaxValueByYearMapper.class);
    job.setReducerClass(MaxValueByYearReducer.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(StationDataWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    int numReducers = 2;
    if (args.length >= 4) {
      numReducers = Integer.parseInt(args[3]);
    }

    job.setNumReduceTasks(numReducers);

    return (job.waitForCompletion(true) ? 0 : 1);
  }

  private void copyFromS3File(Configuration conf, Path s3Path, Path hdfsPath) throws IOException {
    FileSystem s3fs = FileSystem.get(s3Path.toUri(), conf);
    FileSystem hdfs = FileSystem.get(conf);
    FSDataInputStream in = null;
    FSDataOutputStream out = null;
    byte[] aby = new byte[512];
    int r = 0;
    try {
      in = s3fs.open(s3Path);
      out = hdfs.create(hdfsPath, true);
      while ((r = in.read(aby)) > 0) {
        out.write(aby, 0, r);
      }
      out.flush();
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (Exception zzz) {}
      }
      if (out != null) {
        try {
          out.close();
        } catch (Exception zzz) {}
      }
    }
  }
}
