package org.mrgeo.rasterops;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ByteWritable;
import org.apache.log4j.Logger;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.data.raster.RasterWritable;
import org.mrgeo.data.tile.TileIdWritable;
import org.mrgeo.utils.TMSUtils;
import org.mrgeo.utils.TMSUtils.Pixel;
import org.mrgeo.utils.TMSUtils.Tile;

import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CostDistanceVertex extends Vertex<TileIdWritable, 
													InMemoryRasterWritable, 
													ByteWritable,
													CostDistanceMessage> {
	public static final String MAX_SUPERSTEPS = CostDistanceVertex.class.getSimpleName() +
			".maxSupersteps";
	public static final String MAX_COST = CostDistanceVertex.class.getSimpleName() +
			".maxCost";
	public static final String SOURCE_POINTS = CostDistanceVertex.class.getSimpleName() +
			".sourcePoints";
	public static final String RV_TILE_IDS = CostDistanceVertex.class.getSimpleName() +
	    ".rvTileIds";
	public static final String RV_PYRAMID = CostDistanceVertex.class.getSimpleName() +
	      ".rvPyramid";
  public static final String RV_ZOOM_LEVEL = CostDistanceVertex.class.getSimpleName() +
      ".rvZoomLevel";
  public static final String SCHEME_FOR_BOUNDS_CALCULATION = CostDistanceVertex.class.getSimpleName() +
      ".schemeForBoundsCalculation";
  
	private static final Logger LOG = Logger.getLogger(CostDistanceVertex.class);
	
	private CostDistanceWorkerContext mockWorkerContext;
	
	public void setMockWorkerContext(final MockCostDistanceWorkerContext mockWorkerContext) {
		this.mockWorkerContext = mockWorkerContext;
	}
	@Override
  public CostDistanceWorkerContext getWorkerContext() {
		if(mockWorkerContext == null)
			return (CostDistanceWorkerContext)super.getWorkerContext();
		return mockWorkerContext;
	}
	
	/* 
	 * Used to inject a worker context for testing - in lieu of calling CostDistanceWorkerContext::preApplication 
	 * the constructor takes in all fields required by the vertex from the context (currently the 
	 * PixelSizeMultiplier, the source id, and the maximum number of supersteps). 
	 * If you add a required field, you need to add it here in the constructor of 
	 * MockCostDistanceWorkerContext, otherwise the mock object doesn't have everything it needs to 
	 * be initialized, and unit tests will fail 
	 */
	public static class MockCostDistanceWorkerContext extends CostDistanceWorkerContext {
		public MockCostDistanceWorkerContext(
				final PixelSizeMultiplier psm,
				final MrsImagePyramidMetadata metadata) {
			super();
			setPixelSizeMultiplier(psm);
			setPyramidMetadata(metadata);
		}
		public void setSingleSourcePoint(String sourcePoint) {
			super.setSingleSourcePoint(PointsParser.parseSourcePoints(sourcePoint, super.getPyramidMetadata()));
		}
    public void setMultiSourcePoint(String rvPyramid, int zoomLevel,
        String rvTileIds, Configuration conf) throws IOException {
      super.setMultiSourcePoints(rvPyramid, zoomLevel, rvTileIds, conf);
    }
	}
	
	/* Note: Any changes to this enum should be reflected in toByte/toPosition below 
	 */
	public enum Position {
		TOPLEFT, TOP, TOPRIGHT, RIGHT, BOTTOMRIGHT, BOTTOM, BOTTOMLEFT, LEFT, SELF;
		
		public static byte toByte(Position position) {
			switch(position) {
			case TOPLEFT: return 0x00;
			case TOP: return 0x01;
			case TOPRIGHT: return 0x02;
			case RIGHT: return 0x03;
			case BOTTOMRIGHT: return 0x04;
			case BOTTOM: return 0x05;
			case BOTTOMLEFT: return 0x06;
			case LEFT: return 0x07;
			case SELF: return 0x08;
			default: throw new IllegalStateException("Unexpected position in toByte " + position);						
			}
		}
		public static Position toPosition(byte posByte) {
			Position position;
			switch(posByte) {
			case 0x00: position = Position.TOPLEFT; break;
			case 0x01: position = Position.TOP; break; 
			case 0x02: position = Position.TOPRIGHT; break;
			case 0x03: position = Position.RIGHT; break;
			case 0x04: position = Position.BOTTOMRIGHT; break;
			case 0x05: position = Position.BOTTOM; break;
			case 0x06: position = Position.BOTTOMLEFT; break;
			case 0x07: position = Position.LEFT; break;
			case 0x08: position = Position.SELF; break;
			default: throw new IllegalStateException("Unexpected ordinal expected in toPosition " + posByte);			
			}
			return position;
		}
		public static Position read(DataInput in) throws IOException {
			byte posByte = in.readByte();
			return toPosition(posByte);
		}
		
		public static void write(Position position, DataOutput out) throws IOException {
			out.write((byte)position.ordinal());			
		}
	}
	
	
	@Override
	public void compute(Iterable<CostDistanceMessage> messages) throws IOException {							

		if(!getValue().isInitialized())
			initialize();
		
		int numMessages=0, numChangedPixels = 0;
		for(CostDistanceMessage message : messages) {
			numMessages++;
			numChangedPixels += message.getPoints().size();
		}
		
		LOG.info(String.format(
				"CostDistanceVertex::compute id=%d superstep=%d got_messages=%d got_cp=%d", 
				getId().get(), getSuperstep(), numMessages, numChangedPixels));


		if(numMessages > 0 || isSourceVertex()) {

			/* here we need to do following
			 * 1. if there are messages from neighbors, consolidate it with our WritableRaster
			 *    our WritableRaster at this point is guaranteed to be there
			 * 2. run ChangedPixelMap = CostSurfaceCalculator.updateCostSurface
			 * 3. send ChangedPixelMap to our neighbors
			 * 
			 */
			CostSurfaceCalculator csc = new CostSurfaceCalculator();
			CostSurfaceCalculator.ChangedPixelsMap changedPixels = csc.updateCostSurface(getValue().getWritableRaster(),
					messages, 
					getWorkerContext().getMaxCost());

			Map<Position,ArrayList<PointWithFriction>> readOnlyMap = changedPixels.getMap();

			for (Edge<TileIdWritable, ByteWritable> edge : getEdges()) {
				Position position = Position.toPosition(edge.getValue().get());

				ArrayList<PointWithFriction> changedPoints = readOnlyMap.get(position);
				if(changedPoints != null) {
					sendMessage(edge.getTargetVertexId(), new CostDistanceMessage(position, changedPoints));
					LOG.info(String.format("\tSending %d cp to %d position %s",
								changedPoints.size(), edge.getTargetVertexId().get(), position));

				}
			}
		}
		
		voteToHalt();
	}

	private void initialize() throws IOException {
		LOG.info(String.format("\tCalling initialize on tile %d", getId().get()));		
		assert(getValue() != null);		
		
		Raster raster = RasterWritable.toRaster(getValue());
		//writableRaster = makeRasterWritable(raster);
		WritableRaster writableRaster = getWorkerContext()
								.getPixelSizeMultiplier().toWritableRaster(getId().get(), raster);

		if(isSourceVertex())
			setSourcePoints(writableRaster);
		
		getValue().setWritableRaster(writableRaster);
	}
	
	public boolean isSourceVertex() {
		return getWorkerContext().isSource(getId().get());
	}
	
	private void setSourcePoints(WritableRaster writableRaster) {
	  CostDistanceWorkerContext cdwc = getWorkerContext();
	  if(cdwc.isMultiSourcePoint()) {
	    Tile tile = TMSUtils.tileid(getId().get(), cdwc.getRVImage().getZoomlevel());
	    Raster rvRaster = getWorkerContext().getRVImage().getTile(tile.tx, tile.ty);
	    int countZeroPixels = 0; 
	    for(int py = 0; py < rvRaster.getHeight(); py++) {
	      for(int px = 0; px < rvRaster.getWidth(); px++) {
	        float v = rvRaster.getSampleFloat(px, py, 0);
	        if(!Float.isNaN(v) && v == 0f) {
	          countZeroPixels++;
	          writableRaster.setSample(px, py, 2, v);
	        }	          
	      }      
	    }
	    if(countZeroPixels == 0) {
	      throw new IllegalStateException("CostDistanceVertex: No zero-valued pixels in tile " + 
	                                      getId().get());
	    }
	    LOG.info(String.format("\tPainted %d pixels onto tile %d", countZeroPixels, getId().get()));    	    
	  } else {
	    final List<Pixel> pixels = getWorkerContext().getSourcePixels(getId().get());
	    float pixelArr[] = new float[PixelSizeMultiplier.NUM_BANDS];
	    for(Pixel pixel : pixels) {
	      pixelArr = writableRaster.getPixel((int)pixel.px, (int)pixel.py, pixelArr);					
	      pixelArr[2] = 0;
	      writableRaster.setPixel((int)pixel.px, (int)pixel.py, pixelArr);			
	    }
	  }
	}
}