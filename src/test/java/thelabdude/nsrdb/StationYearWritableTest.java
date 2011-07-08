package thelabdude.nsrdb;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.util.GenericsUtil;
import org.junit.Test;

public class StationYearWritableTest {
    /**
     * Utility method for testing whether a Writable object is implemented correctly.
     * @param w
     * @throws Exception
     */
    public static void assertWritable(Writable w) throws Exception {
        WritableSerialization ws = new WritableSerialization();
        Serializer<Writable> serializer = ws.getSerializer(GenericsUtil.getClass(w));
        Deserializer<Writable> deserializer = ws.getDeserializer(GenericsUtil.getClass(w));

        DataOutputBuffer out = new DataOutputBuffer();
        serializer.open(out);
        serializer.serialize(w);
        serializer.close();

        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), out.getLength());
        deserializer.open(in);
        Object after = deserializer.deserialize(null);
        deserializer.close();

        assertEquals(w, after);
    }
    
    @Test
    public void testWritableImpl() throws Exception {
        // fail("Not yet implemented");
        long station = 1000;
        int year = 2005;
        StationYearWritable tmp = new StationYearWritable(station, year);
        assertEquals(station, tmp.station);
        assertEquals(year, tmp.year);
        
        StationYearWritable equiv = new StationYearWritable(station, year);
        assertEquals(equiv, tmp);

        assertWritable(tmp);
    }
}
