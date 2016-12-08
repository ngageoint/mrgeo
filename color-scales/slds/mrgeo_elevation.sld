<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.0.0" xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc"
  xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.opengis.net/sld http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd">
  <NamedLayer>
    <Name>MrGeo Elevation</Name>
    <UserStyle>
      <Name>mrgeo_elevation</Name>
      <Title>MrGeo Elevation Style</Title>
      <Abstract>Elevation SLD for global layers</Abstract>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <Opacity>1.0</Opacity>
            <ColorMap>
              <ColorMapEntry color="#000000" quantity="-9999" label="nodata" opacity="0.0" />
              <ColorMapEntry color="#003300" quantity="0" />
              <ColorMapEntry color="#006600" quantity="200" />
              <ColorMapEntry color="#33cc33" quantity="1000"/>
              <ColorMapEntry color="#cccc33" quantity="1200" />
              <ColorMapEntry color="#cc9933" quantity="1400" />
              <ColorMapEntry color="#996633" quantity="1600" />
              <ColorMapEntry color="#FFFFFF" quantity="2000" />
            </ColorMap>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>