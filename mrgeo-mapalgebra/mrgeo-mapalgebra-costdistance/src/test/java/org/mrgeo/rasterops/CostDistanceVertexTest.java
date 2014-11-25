package org.mrgeo.rasterops;

import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.utils.MockUtils;
import org.apache.hadoop.io.ByteWritable;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.mrgeo.core.Defs;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.rasterops.CostDistanceVertex.MockCostDistanceWorkerContext;
import org.mrgeo.rasterops.CostDistanceVertex.Position;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.hdfs.utils.HadoopFileUtils;
import org.mrgeo.junit.UnitTest;
import org.mrgeo.test.MapOpTestUtils;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.LongRectangle;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.Pixel;
import org.mrgeo.utils.TMSUtils.Tile;

import javax.media.jai.operator.ConstantDescriptor;

import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertTrue;

@SuppressWarnings("static-method")
public class CostDistanceVertexTest {
  private static final String PYRAMID_PATH = Defs.CWD + "/" + Defs.INPUT + "all-ones";
  private static final int IMAGE_ZOOM = 10;

  private MrsImage image;
  private MockCostDistanceWorkerContext mockContext;
  private PixelSizeMultiplier psm;

	@Before
	public void setUp() throws IOException 
	{
		image = MrsImage.open(PYRAMID_PATH, IMAGE_ZOOM, null);
		psm = new PixelSizeMultiplier(image.getMetadata());
		mockContext = new MockCostDistanceWorkerContext(psm, image.getMetadata());			
	}

	@After
	public void tearDown()
	{
		image.close();
		mockContext.closeRVImage();
	}
	
  
  @Test
  @Category(UnitTest.class)
  public void testWorkerContextWithMultiSourcePoint() throws IOException {
    // for multi source point related tests
    final String RASTERIZED_VECTOR_INPUT = "rasterized_vector";
    final int RASTERIZED_VECTOR_INPUT_ZOOM = 10;

    MapOpTestUtils rvrTestUtils; // RasterizeVectorReader dir contains input data
    String rvPyramidPath;
    
    
    rvrTestUtils = new MapOpTestUtils(RasterizeVectorReaderTest.class);
    HadoopFileUtils.delete(rvrTestUtils.getInputHdfs());    
    HadoopFileUtils.copyToHdfs(rvrTestUtils.getInputLocal(), rvrTestUtils.getInputHdfs(), RASTERIZED_VECTOR_INPUT);
    rvPyramidPath = rvrTestUtils.getInputHdfs() + "/" + RASTERIZED_VECTOR_INPUT;  
    mockContext.setMultiSourcePoints(rvPyramidPath, RASTERIZED_VECTOR_INPUT_ZOOM,
        "349880,352955,358081", HadoopUtils.createConfiguration());
    
    // test rvImage
    Assert.assertNotNull(mockContext.getRVImage());
    
    // test isMultiSourcePoint
    Assert.assertEquals(true, mockContext.isMultiSourcePoint());
    
    // test isSource on above tile ids
    Assert.assertEquals(true, mockContext.isSource(349880L));
    Assert.assertNull(mockContext.getSourcePixels(349880L));

    Assert.assertEquals(true, mockContext.isSource(352955L));
    Assert.assertNull(mockContext.getSourcePixels(352955L));

    Assert.assertEquals(true, mockContext.isSource(358081L));
    Assert.assertNull(mockContext.getSourcePixels(358081L));
    
    // negative test for isSource
    Assert.assertEquals(false, mockContext.isSource(-1L));    
    Assert.assertNull(mockContext.getSourcePixels(-1L));
  }
  
	@Test
	@Category(UnitTest.class)
	public void testInitializeSingleSourcePoint() {
		final long tileId = 208787;

		double[] latLonArray = getSomeLatLons(tileId, image.getMaxZoomlevel(), image.getTilesize());
    
		
		for(int i=0; i < latLonArray.length; i+=2) {
		    final double lat = latLonArray[i];
        final double lon = latLonArray[i+1];
        
        // Don't use String.format with %f as it loses precision - kept it here as a reminder
        //final String sourcePoint = String.format("'POINT(%f %f)'", lon, lat);
        final String sourcePoint = "'POINT(" + Double.valueOf(lon) 
                                             + " " 
                                             + Double.valueOf(lat)
                                             + ")'";
      
        
        //System.out.println("setting source point " + sourcePoint);
        mockContext.setSingleSourcePoint(sourcePoint);
        List<Pixel> pixels = mockContext.getSourcePixels(tileId);
        assert(pixels != null);
        assert(pixels.size() == 1);
        
        Pixel p = pixels.get(0);
        
        //final long expectedPy = (long)(i / 2);
        final long expectedPy = (i/2) % image.getTilesize();
        //System.out.println(String.format("Run %d: Expected py = %d, Got: px/py = %f/%f", 
        //                                    i, expectedPy, p.px,p.py));
        
        // check that it is indeed the pixel we expect
        Assert.assertArrayEquals(new long[]{0,expectedPy}, new long[]{p.px,p.py});
		}
	}

	@Ignore
	@Test
	@Category(UnitTest.class)
	public void testInitializeVertex() throws Exception {
		final String sourcePoint = "'POINT(-101.25 33.75)'";
		final int maxSuperSteps = 1;
		final float maxCost = -1;
		
		mockContext.setMaxCost(maxCost);
		mockContext.setSingleSourcePoint(sourcePoint);
		mockContext.setMaxSupersteps(maxSuperSteps);
		CostDistanceVertex vertex = new CostDistanceVertex();
		vertex.setMockWorkerContext(mockContext);
		vertex.initialize(null, null);
		vertex.addEdge(EdgeFactory.create(new TileIdWritable(10L), new ByteWritable(Position.toByte(Position.LEFT))));
		vertex.addEdge(EdgeFactory.create(new TileIdWritable(20L), new ByteWritable(Position.toByte(Position.RIGHT))));

		MockUtils.MockedEnvironment<TileIdWritable, InMemoryRasterWritable, ByteWritable,
		CostDistanceMessage> env = MockUtils.prepareVertex(vertex, 1L,
				new TileIdWritable(7L), createInMemoryRasterWritable(), false);

//		Mockito.when(env.getConfiguration().getLong(
//				SimpleShortestPathsVertex.SOURCE_ID,
//				SimpleShortestPathsVertex.SOURCE_ID_DEFAULT)).thenReturn(2L);

		final Iterable<CostDistanceMessage> EMPTY_MESSAGE_LIST 
		          = Collections.<CostDistanceMessage>emptyList();

		vertex.compute(EMPTY_MESSAGE_LIST);

		assertTrue(vertex.isHalted());

		// no messages should be sent to either neighbor
		env.verifyNoMessageSent();
	}

	@Ignore
	@Test
	@Category(UnitTest.class)
	public void verifyCostSurfaceAllTiles() {		  
//		final Iterable<CostDistanceMessage> EMPTY_MESSAGE_LIST 
//		          = Collections.<CostDistanceMessage>emptyList();
//		CostSurfaceCalculator csc = new CostSurfaceCalculator();
//		float maxCost = 10000;
//
//		String inputDir = TestUtils.composeInputDir(CostDistanceVertexTest.class) + 
//		                        "VerifyCostSurfaceAllTiles/";		
//		String outputDir = TestUtils.composeOutputDir(CostDistanceVertexTest.class) + "/";
//		for(long ty=image.getMinTileY(); ty <= image.getMaxTileY(); ty++) {
//			for(long tx=image.getMinTileX(); tx <= image.getMaxTileX(); tx++) {
//				long tileId = TMSUtils.tileid(tx, ty, image.getMaxZoomlevel());
//				Raster raster = image.getTile(tx,ty);
//				Assert.assertNotNull(raster);		
//				WritableRaster writableRaster = psm.toWritableRaster(tileId, raster);	
//				setSourcePointToCenter(writableRaster);					
//				//System.out.println(String.format("Testing tile id %d with tx %d ty %d",tileId,tx,ty));
//								
//				ChangedPixelsMap map = csc.updateCostSurface(writableRaster, EMPTY_MESSAGE_LIST,maxCost);
//				
//				verifyCost(writableRaster,maxCost);
//				
//				//map.printStats();
//				Bounds bounds =	TMSUtils.tileBounds(tx, ty, image.getMaxZoomlevel(), 
//														image.getTilesize());
//				GeneralEnvelope envelope = new GeneralEnvelope(new double[] { bounds.w, bounds.s }, 
//															new double[] { bounds.e, bounds.n });
//
//				String fn = "tile_" + tx + "_" + ty + ".tif";
//				String input = inputDir + fn;
//				String output = outputDir + fn;
//				System.out.println("Saving results to " + output);
//				GeotoolsRasterUtils.saveLocalGeotiff(writableRaster, envelope, output);
//
//				RenderedImage imgIn = GeoTiffDescriptor.create(input,null);
//				RenderedImage imgOut = GeoTiffDescriptor.create(output,null);
//				TestUtils.compareRenderedImages(imgIn,imgOut);
//
//				Map<Position,ArrayList<PointWithFriction>> readOnlyMap = map.getMap();
//
//				// TODO - currently doesn't do range tests (e.g., BorderChecker.Top 
//				// should check 0 <= x <= width-1 since I couldn't get assertThat to work 
//				final short TOP_Y = (short)(writableRaster.getHeight()-1); 
//				final short BOTTOM_Y = 0; 
//				final short RIGHT_X = (short)(writableRaster.getWidth()-1); 
//				final short LEFT_X = 0; 
//
//				for(Position pos : readOnlyMap.keySet()) {
//					ArrayList<PointWithFriction> points = readOnlyMap.get(pos);
//					for(Point point : points) {
//						switch(pos) {
//						case TOPLEFT:
//							Assert.assertEquals(TOP_Y, point.y);
//							Assert.assertEquals(LEFT_X, point.x);
//							break;
//						case TOP:
//							Assert.assertEquals(TOP_Y, point.y);
//							break;						
//						case TOPRIGHT:
//							Assert.assertEquals(TOP_Y, point.y);
//							Assert.assertEquals(RIGHT_X, point.x);
//							break;					
//						case RIGHT:
//							Assert.assertEquals(RIGHT_X, point.x);
//							break;				
//						case BOTTOMRIGHT:
//							Assert.assertEquals(BOTTOM_Y, point.y);
//							Assert.assertEquals(RIGHT_X, point.x);
//							break;					
//						case BOTTOM:
//							Assert.assertEquals(BOTTOM_Y, point.y);
//							break;				
//						case BOTTOMLEFT:
//							Assert.assertEquals(BOTTOM_Y, point.y);
//							Assert.assertEquals(LEFT_X, point.x);
//							break;				
//						case LEFT:
//							Assert.assertEquals(LEFT_X, point.x);
//							break;				
//						default:
//							throw new IllegalStateException(
//									"CostSurfaceCalculatorTest detected illegal value in writableRaster");
//						}
//					}
//				}
//			}
//		}		 
	}
		
	 @Ignore
	  @Test
	  @Category(UnitTest.class)
	  public void testSuperstepTwo() throws Exception {
	    final float maxCost = -1;
	    final Iterable<CostDistanceMessage> EMPTY_MESSAGE_LIST 
	              = Collections.<CostDistanceMessage>emptyList();
	    CostSurfaceCalculator csc = new CostSurfaceCalculator();
	    
	    WritableRaster wr83 = createWritableRaster(83L, true);
	    WritableRaster wr67 = createWritableRaster(67L, false);
	  
	    CostSurfaceCalculator.ChangedPixelsMap map = csc.updateCostSurface(wr83, EMPTY_MESSAGE_LIST, maxCost);
	    ArrayList<PointWithFriction> changedPoints = map.getMap().get(Position.BOTTOM);
	    CostDistanceMessage m1 = new CostDistanceMessage(Position.BOTTOM, changedPoints);
	    
	    csc.updateCostSurface(wr67, Collections.singleton(m1), maxCost);


	  }
	 
	@Ignore
	@Test
	@Category(UnitTest.class)
	public void printGraph() {
		MrsImagePyramidMetadata metadata = image.getMetadata();
		int zoomlevel = metadata.getMaxZoomLevel();
		LongRectangle rectangle = metadata.getTileBounds(zoomlevel);
		System.out.println(String.format("printGraph: using zoomlevel %d and bounds %d %d %d %d",
				metadata.getMaxZoomLevel(), rectangle.getMinX(), rectangle.getMinY()
				,rectangle.getMaxX(),rectangle.getMaxY()));
		EdgeBuilder edgeBuilder = new EdgeBuilder(rectangle.getMinX(), rectangle.getMinY(),
				rectangle.getMaxX(), rectangle.getMaxY(),zoomlevel);

		for(int ty=(int)image.getMinTileY(); ty <= (int)image.getMaxTileY(); ty++) {
			for(int tx=(int)image.getMinTileX(); tx <= (int)image.getMaxTileX(); tx++) {
				long tileId = TMSUtils.tileid(tx, ty, image.getMaxZoomlevel());

				List<EdgeBuilder.PositionEdge> neighbors;

				// bottom row
				neighbors = edgeBuilder.getNeighbors(tileId);
				System.out.println(String.format("tileId %d, neighbors %s", tileId, neighbors));
			}
		}
	}
	
	private InMemoryRasterWritable createInMemoryRasterWritable() throws IOException {
		final float RASTER_SIZE = image.getTilesize();
		float PIXEL_VALUE = 1.0f;
		Raster srcRaster = ConstantDescriptor.create(RASTER_SIZE, RASTER_SIZE, 
		                                                new Float[]{PIXEL_VALUE}, null).getData();
		return InMemoryRasterWritable.toInMemoryRasterWritable(RasterWritable.toWritable(srcRaster));
	}

	private WritableRaster createWritableRaster(long tileId, boolean setSourcePoint) {
		for(long ty=image.getMinTileY(); ty <= image.getMaxTileY(); ty++) {
			for(long tx=image.getMinTileX(); tx <= image.getMaxTileX(); tx++) {
				if(tileId == TMSUtils.tileid(tx, ty, image.getMaxZoomlevel())) {
					Raster raster = image.getTile(tx,ty);
					Assert.assertNotNull(raster);		
					WritableRaster writableRaster = psm.toWritableRaster(tileId, raster);	
					if(setSourcePoint) {
						setSourcePointToCenter(writableRaster);
					}
					System.out.println(String.format("createWritableRaster tile id %d with tx %d ty %d " + 
					                                  "and setSourcePoint %b",tileId,tx,ty,setSourcePoint));
					return writableRaster;
				}
			}
		}
		return null;
	}
  private void setSourcePointToCenter(WritableRaster writableRaster) {
		final int px = writableRaster.getHeight() / 2;
		final int py = writableRaster.getHeight() / 2;

		float pixelArr[] = new float[3];
		pixelArr = writableRaster.getPixel(px, py, pixelArr);					
		pixelArr[2] = 0;
		writableRaster.setPixel(px, py, pixelArr);			
	}
	
//	private void verifyCost(WritableRaster writableRaster,  final float maxCost) {
//		
//		// cost is stored in the third band
//		final int bandCost = 2;
//		for (short y = 0; y < writableRaster.getHeight(); y++)
//		{
//			for (short x = 0; x < writableRaster.getWidth(); x++)
//			{
//				float cost = writableRaster.getSampleFloat(x, y, bandCost);
//				if(cost > maxCost) {
//					throw new RuntimeException(
//								String.format("Pixel %d,%d has cost %.2f whereas maxCost is %.2f",
//												x,y,cost,maxCost));
//				}
//			}
//		}
//	}
  
  private double[] getSomeLatLons(long tileId, int zoomLevel, int tileSize) {                                  
    final int maxPy = 511;
    final double latLonArray[] = new double[(maxPy+1) * 2];

    final double res = TMSUtils.resolution(zoomLevel, tileSize);
    Tile tile = TMSUtils.tileid(tileId, zoomLevel);

    final double startPx = tile.tx * tileSize;
    final double startPy = (tile.ty * tileSize) + tileSize - 1;

    int i = 0;
    for(int py = 0; py < tileSize; py++) {
      final double lat = (startPy-py) * res - 90.0;
      for(int px = 0; px < tileSize; px++) {
        final double lon = (startPx+px) * res - 180.0;
        if(px == 0 && py <= maxPy) {
          //System.out.println(String.format("Interesting pixel %d/%d of tile %d with tx/ty = %d/%d, lat = %f, lon = %f", 
          //    px,py,tileId, tile.tx, tile.ty, lat,lon));
          latLonArray[i++] = lat;
          latLonArray[i++] = lon;
        }
      }   
    }
    return latLonArray;
  }
  
}
