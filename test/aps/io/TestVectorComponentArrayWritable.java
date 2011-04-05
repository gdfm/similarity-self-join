package aps.io;

import static org.junit.Assert.*;

import org.junit.Test;

import aps.io.VectorComponent;
import aps.io.VectorComponentArrayWritable;

public class TestVectorComponentArrayWritable {

	@Test
	public void testDotProduct() {
		VectorComponent[] v1 = { 
				new VectorComponent(100, 3), 
				new VectorComponent(99, 4),
				new VectorComponent(98, 5), 
				new VectorComponent(97, 6), };

		VectorComponent[] v2 = { 
				new VectorComponent(102, 1), 
				new VectorComponent(101, 2),
				new VectorComponent(99, 3), 
				new VectorComponent(97, 4), 
				new VectorComponent(96, 5), };

		VectorComponent[] v3 = {};

		assertEquals((double) 3 * 4 + 6 * 4, VectorComponentArrayWritable.dotProduct(v1, v2), 0);
		assertEquals(0.0, VectorComponentArrayWritable.dotProduct(v3, v1), 0);
		assertEquals(0.0, VectorComponentArrayWritable.dotProduct(v2, v3), 0);
		assertEquals((double) 3 * 4, VectorComponentArrayWritable.dotProduct(v1, v2, 97), 0);
		assertEquals((double) 3 * 4 + 6 * 4, VectorComponentArrayWritable.dotProduct(v1, v2, 96), 0);
	}
	
	@Test
	public void testMyTest() {
		VectorComponent[] v47 = {
				new VectorComponent(1939387, 0.25),
				new VectorComponent(1938830, 0.25),
				new VectorComponent(1938762, 0.25),
				new VectorComponent(1938181, 0.25),
				new VectorComponent(1937946, 0.25),
				new VectorComponent(1936746, 0.25),
				new VectorComponent(1927445, 0.25),
				new VectorComponent(1851190, 0.75),
		};
		
		VectorComponent[] v48 = {
				new VectorComponent(1939387, 0.35355339059327373),
				new VectorComponent(1938830, 0.35355339059327373),
				new VectorComponent(1938762, 0.35355339059327373),
				new VectorComponent(1938181, 0.35355339059327373),
				new VectorComponent(1937946, 0.35355339059327373),
				new VectorComponent(1936746, 0.35355339059327373),
				new VectorComponent(1918536, 0.35355339059327373),
				new VectorComponent(1851190, 0.35355339059327373),
		};
		
		assertEquals(0.795495128834866, VectorComponentArrayWritable.dotProduct(v47, v48), 0); 
	}
}
