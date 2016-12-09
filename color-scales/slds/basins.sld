<?xml version="1.0" encoding="ISO-8859-1"?>
<StyledLayerDescriptor version="1.0.0"
    xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd"
    xmlns="http://www.opengis.net/sld"
    xmlns:ogc="http://www.opengis.net/ogc"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <NamedLayer>
    <Name>Simple Basin Symbology</Name>
    <UserStyle>
      <Title></Title>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <ColorMap>
              <ColorMapEntry color="#FF0000" quantity="99"  opacity="0.0"/>
              <ColorMapEntry color="#5e4fa2" quantity="100"  opacity="0.75"/>
              <ColorMapEntry color="#3288bd" quantity="500"  opacity="0.75"/>
              <ColorMapEntry color="#66c2a5" quantity="2000"  opacity="0.75"/>
              <ColorMapEntry color="#abdda4" quantity="5000"  opacity="0.75"/>
              <ColorMapEntry color="#e6f598" quantity="7500"  opacity="0.75"/>
              <ColorMapEntry color="#fdae61" quantity="10000"  opacity="0.75"/>
              <ColorMapEntry color="#f46d43" quantity="12000"  opacity="0.75"/>
              <ColorMapEntry color="#d53e4f" quantity="15000"  opacity="0.75"/>
              <ColorMapEntry color="#9e0142" quantity="17000"  opacity="0.75"/>
            </ColorMap>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>