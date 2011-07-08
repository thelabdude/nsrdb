package thelabdude.nsrdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Holds a double precision data point for a station.
 */
public class StationDataWritable implements Writable {

  public long station;
  public double data = 0d;
  public int dataCount = 0;

  public StationDataWritable() {
  }

  public StationDataWritable(long station, double data) {
    this.station = station;
    addData(data);
  }

  public void addData(double add) {
    this.data += add;
    ++this.dataCount;
  }
  
  public void reset(long station) {
    this.station = station;
    this.data = 0d;
    this.dataCount = 0;
  }

  public String toString() {
    return station + "=" + data + " ("+dataCount+")";
  }

  public int hashCode() {
    return (int) data + (int) station + dataCount;
  }

  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    if (obj == this)
      return true;
    if (!getClass().equals(obj.getClass()))
      return false;
    StationDataWritable that = (StationDataWritable) obj;
    return (this.station == that.station) && (this.data == that.data) && (this.dataCount == that.dataCount);
  }

  public void readFields(DataInput in) throws IOException {
    station = in.readLong();
    data = in.readDouble();
    dataCount = in.readInt();
  }

  public void write(DataOutput out) throws IOException {
    out.writeLong(station);
    out.writeDouble(data);
    out.writeInt(dataCount);
  }
}
