package thelabdude.nsrdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class StationMetadataWritableTest {
    
    @Test
    public void testWritableImpl() throws Exception {
        String[] csv = new String[] {
          "123", "1", "1",
          "Test", "CO", "33.667",
          "-117.733", "116", "-8",
          "33.667", "-117.733", "116"
        };
        StationMetadataWritable tmp = StationMetadataWritable.fromCsvRecord(csv);
        assertNotNull(tmp);

        assertEquals(123L, tmp.station);
        assertEquals((short)1, tmp.classification);
        assertEquals(true, tmp.solar);
        assertEquals("Test", tmp.name);
        assertEquals("CO", tmp.state);
        assertEquals(new Float(33.667), new Float(tmp.latitude));
        assertEquals(new Float(-117.733), new Float(tmp.longitude));
        assertEquals(116, tmp.elevation);
        assertEquals(-8, tmp.timezone);        
        assertEquals(new Float(33.667), new Float(tmp.ishLatitude));
        assertEquals(new Float(-117.733), new Float(tmp.ishLongitude));
        assertEquals(116, tmp.ishElevation);

        StationMetadataWritable equiv = StationMetadataWritable.fromCsvRecord(csv);
        assertEquals(equiv, tmp);
        
        StationYearWritableTest.assertWritable(tmp);
    }
}
