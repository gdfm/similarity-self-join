package aps;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import aps.io.VectorComponent;

public class TextVectorConverterTest {
    private static final String feature = "2:11.5011";

    @Test
    public void testParseVectorComponent() {
        VectorComponent vc = TextVectorConverter.parseVectorComponent(feature);
        assertEquals(2, vc.getID());
        assertEquals(11.5011, vc.getWeight(), 0);
    }

}
