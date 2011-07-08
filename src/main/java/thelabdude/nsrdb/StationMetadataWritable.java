package thelabdude.nsrdb;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

/**
 * Holds a float precision data point for a station.
 */
public class StationMetadataWritable implements Writable {

  /**
   * Parses station metadata from a CSV input stream into a Map.
   * @param metadataReader
   * @return
   * @throws IOException 
   * @throws Exception
   */
  public static Map<Long, StationMetadataWritable> load(Reader metadataReader) throws IOException {
    BufferedReader reader = null;
    String line = null;
    Map<Long, StationMetadataWritable> map = new HashMap<Long, StationMetadataWritable>();
    try {
      reader = new BufferedReader(metadataReader);
      // iterate all lines in the CSV file to build the MapFile
      int rowNum = 0;
      while ((line = reader.readLine()) != null) {
        ++rowNum;
        if (line.length() == 0 || line.startsWith("USAF")) {
          continue; // ignore empty lines and headers
        }

        StationMetadataWritable stationMetadata = StationMetadataWritable.fromCsvRecord(line.split(","));
        if (stationMetadata != null) {
          map.put(stationMetadata.station, stationMetadata);
        } else {
          System.out.println("WARNING: Invalid station metadata at row " + rowNum);
        }
      }
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception zzz) {}
      }
    }
    return map;
  }

  /**
   * Converts a CSV record (String[]) into a StationMetadataWritable.
   *
   * @param record
   * @return a valid StationMetadataWritable or null.
   */
  public static StationMetadataWritable fromCsvRecord(String[] record) {
    if (record[0] == null || record[0].trim().length() == 0)
      return null;

    StationMetadataWritable station = new StationMetadataWritable();
    station.station = Long.parseLong(record[0]);
    station.classification = Short.parseShort(record[1]);
    station.solar = "1".equals(record[2]);
    station.name = (record[3] != null) ? record[3] : "?name?";
    station.state = (record[4] != null) ? record[4] : "?state?";
    station.latitude = Float.parseFloat(record[5]);
    station.longitude = Float.parseFloat(record[6]);
    station.elevation = Integer.parseInt(record[7]);
    station.timezone = Integer.parseInt(record[8]);
    station.ishLatitude = Float.parseFloat(record[9]);
    station.ishLongitude = Float.parseFloat(record[10]);
    station.ishElevation = Integer.parseInt(record[11]);
    return station;
  }

  public long station;
  public short classification;
  public boolean solar;
  public String name;
  public String state;
  public float latitude;
  public float longitude;
  public int elevation;
  public float ishLatitude;
  public float ishLongitude;
  public int ishElevation;
  public int timezone;

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(station).append(",");
    sb.append(classification).append(",");
    sb.append(solar).append(",");
    sb.append(name).append(",");
    sb.append(state).append(",");
    sb.append(latitude).append(",");
    sb.append(longitude).append(",");
    sb.append(elevation).append(",");
    sb.append(timezone).append(",");
    sb.append(ishLatitude).append(",");
    sb.append(ishLongitude).append(",");
    sb.append(ishElevation);
    return sb.toString();
  }

  public int hashCode() {
    return (int) station;
  }

  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    if (obj == this)
      return true;
    if (!getClass().equals(obj.getClass()))
      return false;
    StationMetadataWritable that = (StationMetadataWritable) obj;
    return (this.station == that.station) && (this.classification == that.classification) && (this.solar == that.solar) && (this.name.equals(that.name)) && (this.state.equals(that.state)) && (this.latitude == that.latitude) && (this.longitude == that.longitude) && (this.elevation == that.elevation)
        && (this.timezone == that.timezone) && (this.ishLatitude == that.ishLatitude) && (this.ishLongitude == that.ishLongitude) && (this.ishElevation == that.ishElevation);
  }

  public void readFields(DataInput in) throws IOException {
    station = in.readLong();
    classification = in.readShort();
    solar = in.readBoolean();
    name = in.readUTF();
    state = in.readUTF();
    latitude = in.readFloat();
    longitude = in.readFloat();
    elevation = in.readInt();
    timezone = in.readInt();
    ishLatitude = in.readFloat();
    ishLongitude = in.readFloat();
    ishElevation = in.readInt();
  }

  public void write(DataOutput out) throws IOException {
    out.writeLong(station);
    out.writeShort(classification);
    out.writeBoolean(solar);
    out.writeUTF(name);
    out.writeUTF(state);
    out.writeFloat(latitude);
    out.writeFloat(longitude);
    out.writeInt(elevation);
    out.writeInt(timezone);
    out.writeFloat(ishLatitude);
    out.writeFloat(ishLongitude);
    out.writeInt(ishElevation);
  }
}
