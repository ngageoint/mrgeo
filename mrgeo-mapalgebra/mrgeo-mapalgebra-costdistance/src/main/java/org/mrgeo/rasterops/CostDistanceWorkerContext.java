package org.mrgeo.rasterops;

import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.mrgeo.image.MrsImage;
import org.mrgeo.image.MrsImagePyramidMetadata;
import org.mrgeo.data.DataProviderFactory;
import org.mrgeo.data.DataProviderFactory.AccessMode;
import org.mrgeo.data.image.MrsImageDataProvider;
import org.mrgeo.utils.HadoopUtils;
import org.mrgeo.utils.TMSUtils.Pixel;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CostDistanceWorkerContext extends WorkerContext {
	private static final Logger LOG = Logger.getLogger(CostDistanceWorkerContext.class);

	/** Default maximum number of iterations */
	private static final int DEFAULT_MAX_SUPERSTEPS = 30;
	/** Maximum number of iterations */
	private static int MAX_SUPERSTEPS;

	/* default maximum cost -1 means no maximum cost */
	private static final float DEFAULT_MAX_COST = -1;
	private static float MAX_COST;
		
	private static Map<Long,List<Pixel>> SOURCE_POINTS;

  private static MrsImagePyramidMetadata METADATA;
  private static PixelSizeMultiplier PSM;
    
  private static boolean isMultiSourcePoint = false;
  private static MrsImage rvImage;
  
	@Override
	public void preApplication() throws InstantiationException, IllegalAccessException {
		Configuration conf = this.getContext().getConfiguration();
		MAX_SUPERSTEPS = conf.getInt(CostDistanceVertex.MAX_SUPERSTEPS,
				DEFAULT_MAX_SUPERSTEPS);	
		MAX_COST = conf.getFloat(CostDistanceVertex.MAX_COST, DEFAULT_MAX_COST);
		METADATA = HadoopUtils.getSingleMetadata(conf);
		
		String rvTileIds = conf.get(CostDistanceVertex.RV_TILE_IDS, null);
		if(rvTileIds == null) {
	    String sourcePoints = getContext().getConfiguration().get(CostDistanceVertex.SOURCE_POINTS);
	    if(sourcePoints == null) {
	      throw new RuntimeException("Oops - couldn't find \"" + CostDistanceVertex.SOURCE_POINTS
	                    + "\" in job configuration. Cannot run Cost Distance "
	                    + "without source point(s)");
	    }
	    SOURCE_POINTS = PointsParser.parseSourcePoints(sourcePoints, METADATA);		  
		} else {		  		  
		  try
      {
        setMultiSourcePoints(conf.get(CostDistanceVertex.RV_PYRAMID),
            conf.getInt(CostDistanceVertex.RV_ZOOM_LEVEL, 0),
            rvTileIds, conf);
      }
      catch (IOException e)
      {
        LOG.error("Error setting up cost distance", e);
        throw new InstantiationException("Error setting up cost distance: " + e);
      }
		}

				
		PSM = new PixelSizeMultiplier(METADATA);
		
		LOG.info(String.format("Initializing worker with maxCost %.2f, maxSuperSteps %d - " + 
								"source points is printed elsewhere", MAX_COST, MAX_SUPERSTEPS));
	}
	
	@Override
	public void postApplication() {
	  // closeRVImage is idempotent so can be called repeatedly by different vertices
	  if(isMultiSourcePoint) {
	    closeRVImage();
	  }
	}

	@Override
	public void preSuperstep() {
		// TODO Auto-generated method stub

	}

	@Override
	public void postSuperstep() {
		// TODO Auto-generated method stub

	}
	
	public int getMaxSupersteps() {
		if (MAX_SUPERSTEPS == 0) {
			throw new IllegalStateException(
					CostDistanceWorkerContext.class.getSimpleName() +
					" was not initialized completely: MAX_SUPERSTEPS.");
		}
		return MAX_SUPERSTEPS;
	}
	protected void setMaxSupersteps(final int maxSuperSteps) {
		MAX_SUPERSTEPS = maxSuperSteps;
	}
	public float getMaxCost() {
		if (MAX_COST == 0) {
			throw new IllegalStateException(
					CostDistanceWorkerContext.class.getSimpleName() +
					" was not initialized completely: MAX_COST.");
		}
		return MAX_COST;
	}
	protected void setMaxCost(final float maxCost) {
		MAX_COST = maxCost;
	}
	protected void setPyramidMetadata(final MrsImagePyramidMetadata metadata) {
		METADATA = metadata;
	}

	public MrsImagePyramidMetadata getPyramidMetadata() {
		if(METADATA == null) {
			throw new IllegalStateException(
					CostDistanceWorkerContext.class.getSimpleName() +
					" was not initialized completely: MrsImagePyramidMetadata.");			
		}

		return METADATA;
	}
	public PixelSizeMultiplier getPixelSizeMultiplier() {
		if(PSM == null) {
			throw new IllegalStateException(
					CostDistanceWorkerContext.class.getSimpleName() +
					" was not initialized completely: PixelSizeMultiplier.");			
		}

		return PSM;
	}
	protected void setPixelSizeMultiplier(final PixelSizeMultiplier psm) {
		PSM = psm;
	}	
	protected void setSingleSourcePoint(Map<Long, List<Pixel>> sourcePoints) {
     SOURCE_POINTS = sourcePoints;
  }
	protected void setMultiSourcePoints(String rvPyramid, int zoomLevel,
	    String rvTileIds, final Configuration conf) throws IOException {
	  isMultiSourcePoint = true;

	  MrsImageDataProvider dp = DataProviderFactory.getMrsImageDataProvider(rvPyramid, AccessMode.READ, conf);
	  rvImage = MrsImage.open(dp, zoomLevel);

	  String[] tileIds = rvTileIds.split(",");
	  SOURCE_POINTS = new HashMap<Long,List<Pixel>>(); 
	  for(String tileId : tileIds) {
	    SOURCE_POINTS.put(Long.valueOf(tileId), null);
	  }
	  if(SOURCE_POINTS.isEmpty()) {
	    throw new RuntimeException("CostDistanceWorkerContext could not find any tile ids " + 
	        " in the configuration.");
	  }   
	}
	public boolean isMultiSourcePoint() {
	  return isMultiSourcePoint;
	}
	public MrsImage getRVImage() {
	  return rvImage;
	}
	public void closeRVImage() {
	  if(rvImage != null) {
	    rvImage.close();
	    rvImage = null;
	  }
	}
	public List<Pixel> getSourcePixels(long tileId) {
		return (SOURCE_POINTS == null ? null : SOURCE_POINTS.get(tileId));
	}
	public boolean isSource(long tileId) {
		return SOURCE_POINTS.containsKey(tileId);
	}
}
