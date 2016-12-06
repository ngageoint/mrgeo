package org.mrgeo.publisher.rest.geoserver;

import com.google.gson.*;
import com.google.gson.annotations.SerializedName;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ericwood on 8/29/16.
 */
@SuppressFBWarnings(value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD",
    justification = "Data structure used for JSON construction via Gson")
public class GeoserverJsonUtils
{
private static Gson gson = new Gson();

public static List<String> getWorkspaceNames(String workspacesJson)
{
  return getNames("workspace", workspacesJson);
}

public static List<String> getCoverageStoreNames(String coverageStoresJson)
{
  return getNames("coverageStore", coverageStoresJson);
}

public static List<String> getCoverageNames(String coveragesJson)
{
  return getNames("coverage", coveragesJson);
}

public static GeoserverWorkspaceBuilder createWorkspace(String name)
{
  return new GeoserverWorkspaceBuilder(name);
}

public static GeoserverCoverageStoreBuilder createCoverageStore(String name)
{
  return new GeoserverCoverageStoreBuilder(name);
}

public static GeoserverCoverageBuilder createCoverage(String name)
{
  return new GeoserverCoverageBuilder(name);
}

private static List<String> getNames(String type, String json)
{
  List<String> names = new ArrayList<>();
  String typePlural = type + "s";
  JsonObject root = new JsonParser().parse(json).getAsJsonObject();
  JsonElement jsonWrapperObject = root.get(typePlural);
  if (jsonWrapperObject != null && jsonWrapperObject.isJsonObject())
  {
    JsonArray jsonArray = jsonWrapperObject.getAsJsonObject().getAsJsonArray(type);
    for (JsonElement element : jsonArray)
    {
      names.add(element.getAsJsonObject().get("name").getAsString());
    }
  }
  return names;
}

public static class GeoserverWorkspaceBuilder
{

  public GeoserverWorkspace workspace;


  public GeoserverWorkspaceBuilder(String name)
  {
    workspace = new GeoserverWorkspace();
    workspace.name = name;
  }

  public String getName()
  {
    return workspace.name;
  }

  public String toJson()
  {
    return gson.toJson(this);
  }

  private static class GeoserverWorkspace
  {
    public String name;
  }
}

public static class GeoserverCoverageStoreBuilder
{

  public GeoserverCoverageStore coverageStore;

  public GeoserverCoverageStoreBuilder(String name)
  {
    coverageStore = new GeoserverCoverageStore();
    coverageStore.name = name;
  }

  public String getName()
  {
    return coverageStore.name;
  }

  public String toJson()
  {
    return gson.toJson(this);
  }

  public GeoserverCoverageStoreBuilder workspace(String workspace)
  {
    coverageStore.workspace = workspace;
    return this;
  }

  public GeoserverCoverageStoreBuilder type(String type)
  {
    coverageStore.type = type;
    return this;
  }

  public GeoserverCoverageStoreBuilder description(String description)
  {
    coverageStore.description = description;
    return this;
  }

  public GeoserverCoverageStoreBuilder enabled(boolean enabled)
  {
    coverageStore.enabled = enabled;
    return this;
  }

  public GeoserverCoverageStoreBuilder configUrl(String configUrl)
  {
    coverageStore.url = configUrl;
    return this;
  }

  private static class GeoserverCoverageStore
  {
    public String name;
    public String workspace;
    public String type;
    public String description;
    public boolean enabled;
    public String url;

  }
}

public static class GeoserverCoverageBuilder
{

  private final GeoserverCoverage coverage;

  public GeoserverCoverageBuilder(String name)
  {
    coverage = new GeoserverCoverage();
    coverage.name = name;
  }

  public String getName()
  {
    return coverage.name;
  }

  public GeoserverCoverageBuilder nativeName(String nativeName)
  {
    coverage.nativeName = nativeName;
    return this;
  }

  public GeoserverCoverageBuilder title(String title)
  {
    coverage.title = title;
    return this;
  }

  public GeoserverCoverageBuilder description(String description)
  {
    coverage.description = description;
    return this;
  }

  public GeoserverCoverageBuilder nativeCRS(String nativeCRS)
  {
    coverage.nativeCRS = nativeCRS;
    return this;
  }

  public GeoserverCoverageBuilder srs(String srs)
  {
    coverage.srs = srs;
    return this;
  }

  public GeoserverCoverageBuilder nativeBoundingBox(BoundingBox nativeBoundingBox)
  {
    coverage.nativeBoundingBox = nativeBoundingBox;
    return this;
  }

  public GeoserverCoverageBuilder latLonBoundingBox(BoundingBox latLonBoundingBox)
  {
    coverage.latLonBoundingBox = latLonBoundingBox;
    return this;
  }

  public GeoserverCoverageBuilder enabled(boolean enabled)
  {
    coverage.enabled = enabled;
    return this;
  }

  public GeoserverCoverageBuilder nativeFormat(String nativeFormat)
  {
    coverage.nativeFormat = nativeFormat;
    return this;
  }

  public GeoserverCoverageBuilder grid(Grid grid)
  {
    coverage.grid = grid;
    return this;
  }

  public GeoserverCoverageBuilder supportedFormats(String[] supportedFormats)
  {
    coverage.supportedFormats.string = new String[supportedFormats.length];
    System.arraycopy(supportedFormats, 0, coverage.supportedFormats.string, 0, supportedFormats.length);
    return this;
  }

  public GeoserverCoverageBuilder coverageDimension(CoverageDimension coverageDimension)
  {
    coverage.dimensions.coverageDimension.add(coverageDimension);
    return this;
  }

  public GeoserverCoverageBuilder requestSRS(String requestSRS)
  {
    coverage.requestSRS.string = new String[]{requestSRS};
    return this;
  }

  public GeoserverCoverageBuilder responseSRS(String responseSRS)
  {
    coverage.responseSRS.string = new String[]{responseSRS};
    return this;
  }

  public GeoserverCoverage build()
  {
    return coverage;
  }

  public String toJson()
  {
    return gson.toJson(this);
  }
}

protected static class GeoserverBoundingBoxBuilder
{
  private final BoundingBox boundingBox;

  public GeoserverBoundingBoxBuilder()
  {
    this.boundingBox = new BoundingBox();
  }

  public GeoserverBoundingBoxBuilder minx(double minx)
  {
    boundingBox.minx = minx;
    return this;
  }

  public GeoserverBoundingBoxBuilder maxx(double maxx)
  {
    boundingBox.maxx = maxx;
    return this;
  }

  public GeoserverBoundingBoxBuilder miny(double miny)
  {
    boundingBox.miny = miny;
    return this;
  }

  public GeoserverBoundingBoxBuilder maxy(double maxy)
  {
    boundingBox.maxy = maxy;
    return this;
  }

  public GeoserverBoundingBoxBuilder crs(String crs)
  {
    boundingBox.crs = crs;
    return this;
  }

  public BoundingBox build()
  {
    return boundingBox;
  }
}

protected static class GeoserverGridBuilder
{
  private final Grid grid;

  public GeoserverGridBuilder()
  {
    this.grid = new Grid();
  }

  public GeoserverGridBuilder dimension(int dimension)
  {
    grid.dimension = dimension;
    return this;
  }

  public GeoserverGridBuilder range(GridRange gridRange)
  {
    this.grid.range = gridRange;
    return this;
  }

  public GeoserverGridBuilder transform(Transform gridTransform)
  {
    this.grid.transform = gridTransform;
    return this;
  }

  public GeoserverGridBuilder crs(String crs)
  {
    grid.crs = crs;
    return this;
  }

  public Grid build()
  {
    return grid;
  }
}

protected static class GeoserverGridRangeBuilder
{
  private final GridRange range;

  public GeoserverGridRangeBuilder()
  {
    this.range = new GridRange();
  }

  public GeoserverGridRangeBuilder low(long minX, long minY)
  {
    range.low = String.format("%1$d %2$d", minX, minY);
    return this;
  }

  public GeoserverGridRangeBuilder high(long maxX, long maxY)
  {
    range.high = String.format("%1$d %2$d", maxX, maxY);
    return this;
  }

  public GridRange build()
  {
    return range;
  }
}

protected static class GeoserverTransformBuilder
{
  private final Transform transform;

  public GeoserverTransformBuilder()
  {
    this.transform = new Transform();
  }

  public GeoserverTransformBuilder scaleX(double scaleX)
  {
    transform.scaleX = scaleX;
    return this;
  }

  public GeoserverTransformBuilder scaleY(double scaleY)
  {
    transform.scaleY = scaleY;
    return this;
  }

  public GeoserverTransformBuilder shearX(double shearX)
  {
    transform.shearX = shearX;
    return this;
  }

  public GeoserverTransformBuilder shearY(double shearY)
  {
    transform.shearY = shearY;
    return this;
  }

  public GeoserverTransformBuilder translateX(double translateX)
  {
    transform.translateX = translateX;
    return this;
  }

  public GeoserverTransformBuilder translateY(double translateY)
  {
    transform.translateY = translateY;
    return this;
  }

  public Transform build()
  {
    return transform;
  }
}


protected static class GeoserverCoverageDimensionBuilder
{
  private final CoverageDimension coverageDimension;

  public GeoserverCoverageDimensionBuilder()
  {
    this.coverageDimension = new CoverageDimension();
  }

  public GeoserverCoverageDimensionBuilder name(String name)
  {
    coverageDimension.name = name;
    return this;
  }

  public GeoserverCoverageDimensionBuilder range(double min, double max)
  {
    if (coverageDimension.range == null)
    {
      coverageDimension.range = new DimensionRange();
    }
    coverageDimension.range.min = min;
    coverageDimension.range.max = max;
    return this;
  }

  public GeoserverCoverageDimensionBuilder type(String type)
  {
    coverageDimension.type = type;
    return this;
  }

  public GeoserverCoverageDimensionBuilder description(String description)
  {
    coverageDimension.description = description;
    return this;
  }

  public CoverageDimension build()
  {
    return coverageDimension;
  }
}

private static class GeoserverCoverage
{
  public String name;
  public String nativeName;
  public String title;
  public String description;
  public String nativeCRS;
  public String srs;
  public BoundingBox nativeBoundingBox = new BoundingBox();
  public BoundingBox latLonBoundingBox = new BoundingBox();
  public boolean enabled;
  public String nativeFormat;
  public Grid grid = new Grid();
  public StringList supportedFormats = new StringList();
  public CoverageDimensionList dimensions = new CoverageDimensionList();
  public StringList requestSRS = new StringList();
  public StringList responseSRS = new StringList();

}

// Geoserver wants the array to be a value of the type, so this class exists soley to generate the correct JSON
private static class StringList
{
  public String[] string;
}

// Geoserver wants the array to be a value of the type, so this class exists soley to generate the correct JSON
private static class CoverageDimensionList
{
  public List<CoverageDimension> coverageDimension = new ArrayList<>();
}

private static class BoundingBox
{
  public double minx;
  public double maxx;
  public double miny;
  public double maxy;
  public String crs;
}

private static class Grid
{
  @SerializedName("@dimension")
  public int dimension;
  public GridRange range;
  public Transform transform;
  public String crs;
}

private static class CoverageDimension
{
  public String name;
  public DimensionRange range;
  public String type;
  public String description;
}

private static class GridRange
{
  public String low;
  public String high;
}

private static class Transform
{
  public double scaleX;
  public double scaleY;
  public double shearX;
  public double shearY;
  public double translateX;
  public double translateY;
}

private static class DimensionRange
{
  public double min;
  public double max;
}
}
