package thelabdude.nsrdb;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import thelabdude.nsrdb.MaxValueByYearJob.MaxValueByYearMapper;
import thelabdude.nsrdb.MaxValueByYearJob.MaxValueByYearReducer;

public class MaxValueByYearMRUnitTest {

  // test station metadata
  private static final String[][] csvStationMetadata = new String[][] {
    new String[] { "690140", "3", "0", " EL TORO MCAS", "CA", "33.667", "-117.733", "116", "-8", "33.667", "-117.733", "116" },
    new String[] { "690190", "2", "0", "ABILENE DYESS AFB", "TX", "32.433", "-99.85", "545", "-6", "32.433", "-99.85", "545" }
  };

  private Mapper<StationYearWritable, BytesWritable, IntWritable, StationDataWritable> mapper;
  private MapDriver<StationYearWritable, BytesWritable, IntWritable, StationDataWritable> mapDriver;

  private Reducer<IntWritable, StationDataWritable, IntWritable, Text> reducer;
  private ReduceDriver<IntWritable, StationDataWritable, IntWritable, Text> reduceDriver;

  /**
   * The MaxValueByYearJob Mapper and Reducer need access to a MapFile containing station metadata, so setup access to a
   * test MapFile in the local filesystem.
   * 
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {
    Configuration config = new Configuration();

    mapper = new MaxValueByYearMapper();
    mapDriver = new MapDriver<StationYearWritable, BytesWritable, IntWritable, StationDataWritable>(mapper);
    mapDriver.setConfiguration(config);

    reducer = new MaxValueByYearReducer();
    reduceDriver = new ReduceDriver<IntWritable, StationDataWritable, IntWritable, Text>(reducer);
    reduceDriver.setConfiguration(config);

    // setup the local filesystem so that our job can access station metadata as if it were
    // in the distributed cache ...
    RawLocalFileSystem localFs = new RawLocalFileSystem();
    localFs.initialize(localFs.getUri(), config);
    Path stationMetadataPath = new Path("./target/"+MaxValueByYearJob.STATION_METADATA_PATH);
    FSDataOutputStream tmp = localFs.create(stationMetadataPath, true);
    for (int r = 0; r < csvStationMetadata.length; r++) {
      StationMetadataWritable stationMetadata =
            StationMetadataWritable.fromCsvRecord(csvStationMetadata[r]);
      tmp.writeBytes(stationMetadata.toString() + "\n");
    }
    tmp.flush();
    IOUtils.closeStream(tmp);
  }

  /**
   * Test to see if the Mapper is able to parse valid records correctly.
   * 
   * @throws IOException
   */
  @Test
  public void testMapper() throws Exception {
    // test data taken from 690190_2005.csv
    String csv = "YYYY-MM-DD,HH:MM (LST),Zenith (deg),Azimuth (deg),ETR (W/m^2),ETRN (W/m^2)," +
          "Glo Mod (W/m^2),Glo Mod Unc (%),Glo Mod Source,Dir Mod (W/m^2),Dir Mod Unc (%)," +
          "Dir Mod Source,Dif Mod  (W/m^2),Dif Mod Unc (%),Dif Mod Source,Meas Glo (W/m^2)," +
          "Meas Glo Flg,Meas Dir (W/m^2),Meas Dir Flg,Meas Dif (W/m^2),Meas Dif Flg,TotCC (10ths)," +
          "Precip Wat (cm),Precip Wat Flg,AOD (unitless),AOD Flg\n" +
          "2005-01-01,8:00,88.6,118.3,10,413,1,8,2,0,15,2,0,8,2,-9900,99,-9900,99,-9900,99,10,2.0,3,0.036,2\n" +
          "2005-01-01,9:00,81.9,123.9,200,1415,124,8,2,530,15,2,49,8,2,-9900,99,-9900,99,-9900,99,9,2.0,3,0.036,2";

    // input
    StationYearWritable key = new StationYearWritable(690190, 2005);
    BytesWritable value = new BytesWritable(csv.getBytes(MaxValueByYearJob.UTF8));

    // expected output
    IntWritable expOutputKey = new IntWritable(2005);
    StationDataWritable expOutputValue = new StationDataWritable(690190, 124);
    expOutputValue.addData(1d);

    mapDriver.withInput(key, value)
                 .withOutput(expOutputKey, expOutputValue)
                 .runTest();
  }

  /**
   * Tests to see if the Mapper ignores stations correctly.
   * 
   * @throws Exception
   */
  @Test
  public void testMapperIgnoresStation() throws Exception {
    // send data for class 3 station ...
    mapDriver.withInput(new StationYearWritable(690140, 2005), null)
                 .runTest();

    Counters counters = mapDriver.getCounters();
    Counter counter = counters.findCounter(MaxValueByYearJob.StationDataCounters.STATION_IGNORED);
    assertTrue("Expected 1 ignored station record!", counter.getValue() == 1);
  }

  @Test
  public void testMapperIgnoreParseError() throws Exception {
    // data that produces parse errors
    String csv =
        "2005-,8:00,88.6,118.3,10,413,1,8,2,0,15,2,0,8,2,-9900,99,-9900,99,-9900,99,10,2.0,3,0.036,2\n" +
            "2005-01-01,9:00,81.9,123.9,200,1415,BAD_VALUE,8,2,530,15,2,49,8,2,-9900,99,-9900,99,-9900,99,9,2.0,3,0.036,2";

    BytesWritable value = new BytesWritable(csv.getBytes(MaxValueByYearJob.UTF8));
    mapDriver.withInput(new StationYearWritable(690190, 2005), value)
               .runTest();

    Counters counters = mapDriver.getCounters();
    Counter counter = counters.findCounter(MaxValueByYearJob.StationDataCounters.DATE_PARSE_ERROR);
    assertTrue("Expected 1 date parse rror", counter.getValue() == 1);

    counter = counters.findCounter(MaxValueByYearJob.StationDataCounters.DATA_PARSE_ERROR);
    assertTrue("Expected 1 data parse rror", counter.getValue() == 1);
  }

  @Test
  public void testReducer() throws Exception {
    // input
    IntWritable key = new IntWritable(2005);
    List<StationDataWritable> values = new ArrayList<StationDataWritable>();
    values.add(new StationDataWritable(690190, 8));
    values.add(new StationDataWritable(690190, 124));
    values.add(new StationDataWritable(690190, 12));
    values.add(new StationDataWritable(690140, 8));
    values.add(new StationDataWritable(690140, 123));
    values.add(new StationDataWritable(690140, 12));
    // bogus station data to test max logic
    values.add(new StationDataWritable(123456, 7));
    values.add(new StationDataWritable(123456, 124));

    // expected output
    StationMetadataWritable stationMetadata =
            StationMetadataWritable.fromCsvRecord(csvStationMetadata[1]);
    Text expValue = new Text(stationMetadata.toString()+","+(new Double(144)));

    reduceDriver.withInput(key, values)
                    .withOutput(key, expValue)
                    .runTest();
  }
}
