package thelabdude.nsrdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class StationDataWritableTest {
    
    @Test
    public void testWritableImpl() throws Exception {
        long station = 1000;
        double data = Double.MAX_VALUE;
        StationDataWritable tmp = new StationDataWritable(station, data);
        assertEquals(station, tmp.station);
        assertTrue(data == tmp.data);
        
        StationDataWritable equiv = new StationDataWritable(station, data);
        assertEquals(equiv, tmp);

        StationYearWritableTest.assertWritable(tmp);
    }
}
