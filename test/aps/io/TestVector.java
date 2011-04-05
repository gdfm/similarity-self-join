package aps.io;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestVector {

    @Test
    public void testFindLargestCommonTermID() {
        // vector components must always be sorted descending
        Vector v1 = new Vector();
        v1.set(1, new VectorComponentArrayWritable(new VectorComponent[] { new VectorComponent(3, 0),
                new VectorComponent(1, 0) }));
        Vector v2 = new Vector();
        v2.set(2, new VectorComponentArrayWritable(new VectorComponent[] { new VectorComponent(4, 0),
                new VectorComponent(2, 0) }));
        assertEquals(-1, Vector.findSmallestCommonTermID(v1, v2));
        v2.set(2, new VectorComponentArrayWritable(new VectorComponent[] { new VectorComponent(3, 0),
                new VectorComponent(2, 0) }));
        assertEquals(3, Vector.findSmallestCommonTermID(v1, v2));
        v2.set(2, new VectorComponentArrayWritable(new VectorComponent[] { new VectorComponent(5, 0),
                new VectorComponent(1, 0) }));
        assertEquals(1, Vector.findSmallestCommonTermID(v1, v2));
        v2.set(2, new VectorComponentArrayWritable(new VectorComponent[] { new VectorComponent(5, 0),
                new VectorComponent(4, 0), new VectorComponent(1, 0) }));
        assertEquals(1, Vector.findSmallestCommonTermID(v1, v2));

        v1.set(1, new VectorComponentArrayWritable(new VectorComponent[] { new VectorComponent(5, 0),
                new VectorComponent(3, 0), new VectorComponent(2, 0), new VectorComponent(1, 0) }));
        v2.set(2, new VectorComponentArrayWritable(new VectorComponent[] { new VectorComponent(6, 0),
                new VectorComponent(4, 0), new VectorComponent(3, 0), new VectorComponent(1, 0) }));
        assertEquals(1, Vector.findSmallestCommonTermID(v1, v2));
        v2.set(2, new VectorComponentArrayWritable(new VectorComponent[] { new VectorComponent(6, 0),
                new VectorComponent(4, 0), new VectorComponent(2, 0)}));
        assertEquals(2, Vector.findSmallestCommonTermID(v1, v2));
    }
}
