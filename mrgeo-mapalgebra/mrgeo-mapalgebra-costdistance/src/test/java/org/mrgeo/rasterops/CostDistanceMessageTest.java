package org.mrgeo.rasterops;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.TestUtils;
import org.mrgeo.utils.HadoopUtils;

import java.io.IOException;
import java.util.ArrayList;

public class CostDistanceMessageTest {
	final CostDistanceVertex.Position[] POSITIONS = CostDistanceVertex.Position.values();	// each message gets a new position
	final int NUM_MESSAGES = POSITIONS.length; 		// number of messages
	final int NUM_POINTS = 10; 						// number of points per message

	ArrayList<PointWithFriction> createTenPointsWithFriction(int val) {
		ArrayList<PointWithFriction> points = new ArrayList<PointWithFriction>(NUM_POINTS);
		for(int i=0; i < NUM_POINTS; i++) {
			short valShort = (short) (val+i);
			float valFloat = val+i;
			points.add(PointWithFriction.createPointWithFriction(
							valShort,valShort,valFloat,valFloat,valFloat));
		}
		return points;
	}
	
	@Test
	@Category(UnitTest.class)
	public void testSerDes() throws IOException {
		Configuration conf = HadoopUtils.createConfiguration();
		Path messageFilePath = new Path(TestUtils.composeOutputHdfs(CostDistanceMessageTest.class), "messages.seq");

		FileSystem fs = messageFilePath.getFileSystem(conf);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, messageFilePath, 
				IntWritable.class, CostDistanceMessage.class);

		IntWritable key = new IntWritable();
		CostDistanceMessage value = new CostDistanceMessage();
		
		for(int i=0; i < POSITIONS.length; i++) {
			key.set(i);
			value.setPosition(POSITIONS[i]);
			value.setPoints(createTenPointsWithFriction(i));
			writer.append(key, value);
//			System.out.println("Writing entry #" + i + ":" + value);
		}	
			
		writer.close();

		int foundNumEntries = 0;
		IntWritable foundKey = new IntWritable();
		CostDistanceMessage foundValue = new CostDistanceMessage();
		
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, messageFilePath, conf);
		while(reader.next(foundKey,foundValue)) {
//			System.out.println("Reading entry #" + foundNumEntries + ":" + foundValue);

			// check key 
			Assert.assertEquals(foundNumEntries, foundKey.get());

			ArrayList<PointWithFriction> points = foundValue.getPoints();
			Assert.assertEquals(NUM_POINTS, points.size());

			
			for(int i=0; i < points.size(); i++) {
				short valShort = (short) (foundNumEntries+i);
				float valFloat = foundNumEntries+i;
				PointWithFriction expectedPoint 
					= PointWithFriction.createPointWithFriction(valShort,valShort,
																	valFloat,valFloat,valFloat);
//				System.out.println("Expected point " + expectedPoint);
//				System.out.println("Checking point " + points.get(i));
				Assert.assertEquals(true, expectedPoint.equals(points.get(i)));

			}
						
			foundNumEntries++;
		}
		reader.close();
		Assert.assertEquals(NUM_MESSAGES, foundNumEntries);
	}
//	@Test
//	@Category(UnitTest.class)
//	public void testPrint() throws IOException {
//		short shortVar = (short)0;
//		float floatVar = 0.0f;
//		Point p = Point.createPoint(shortVar, shortVar, floatVar);
//		PointWithFriction pW = PointWithFriction.createPointWithFriction(shortVar, shortVar, floatVar, floatVar, floatVar);
//		System.out.println(p.toString());
//		System.out.println(pW.toString());
//		
//		Point p2 = (Point)pW;
//		System.out.println(p2.toString());
//
//	}

}
