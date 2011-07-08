package thelabdude.nsrdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Intended to be used as a key for Map/Reduce jobs; rather than having each Mapper break up a
 * string containing the station and year, this is done once in the initial data load.
 */
public class StationYearWritable implements Writable {

  public long station;
  public int year;

  public StationYearWritable() {
  }

  public StationYearWritable(long station, int year) {
    this.station = station;
    this.year = year;
  }

  public String toString() {
    return station + "_" + year;
  }

  public int hashCode() {
    return year + (int) station;
  }

  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    if (obj == this)
      return true;
    if (!getClass().equals(obj.getClass()))
      return false;
    StationYearWritable that = (StationYearWritable) obj;
    return (this.station == that.station) && (this.year == that.year);
  }

  public void readFields(DataInput in) throws IOException {
    station = in.readLong();
    year = in.readInt();
  }

  public void write(DataOutput out) throws IOException {
    out.writeLong(station);
    out.writeInt(year);
  }
}
