<?xml version="1.0" encoding="ISO-8859-1"?>
<StyledLayerDescriptor version="1.0.0"
    xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd"
    xmlns="http://www.opengis.net/sld"
    xmlns:ogc="http://www.opengis.net/ogc"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <NamedLayer>
    <Name>Simple Rainbow Style</Name>
    <UserStyle>
      <Title></Title>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <ColorMap>
              <ColorMapEntry color="#FF0000" quantity="0"  opacity="0.50"/>
              <ColorMapEntry color="#FF9600" quantity="64"  opacity="0.50"/>
              <ColorMapEntry color="#FFFF00" quantity="128"  opacity="0.50"/>
              <ColorMapEntry color="#00FF00" quantity="192"  opacity="0.50"/>
              <ColorMapEntry color="#0000FF" quantity="255"  opacity="0.50"/>
            </ColorMap>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>