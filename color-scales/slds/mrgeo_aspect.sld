<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.0.0" xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc"
  xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.opengis.net/sld http://schemas.opengis.net/sld/1.0.0/StyledLayerDescriptor.xsd">
  <NamedLayer>
    <Name>MrGeo Aspect</Name>
    <UserStyle>
      <Name>mrgeo_aspect</Name>
      <Title>MrGeo Degrees Aspect Style</Title>
      <Abstract></Abstract>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <Opacity>1.0</Opacity>
            <ColorMap>
              <ColorMapEntry color="#000000" quantity="-9999" label="nodata" opacity="0.0" />
              <ColorMapEntry color="#0000cc" quantity="0" opacity="0.5"/>
              <ColorMapEntry color="#ffff00" quantity="90" opacity="0.5"/>
              <ColorMapEntry color="#006600" quantity="180" opacity="0.5"/>
              <ColorMapEntry color="#ff0000" quantity="270" opacity="0.5"/>
            </ColorMap>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>