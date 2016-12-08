<?xml version="1.0" encoding="ISO-8859-1"?>
<StyledLayerDescriptor version="1.0.0" 
    xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd" 
    xmlns="http://www.opengis.net/sld" 
    xmlns:ogc="http://www.opengis.net/ogc" 
    xmlns:xlink="http://www.w3.org/1999/xlink" 
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <NamedLayer>
    <Name>Stream Order Style</Name>
    <UserStyle>
      <Title>Stream Order</Title>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <ColorMap>
              <ColorMapEntry color="#0080FF" quantity="0"  opacity="0.0"/>
              <ColorMapEntry color="#0080FF" quantity="1"  opacity="0.75"/>
              <ColorMapEntry color="#00FF00" quantity="2"  opacity="0.75"/>
              <ColorMapEntry color="#FFFF00" quantity="3"  opacity="0.75"/>
              <ColorMapEntry color="#FFA200" quantity="4"  opacity="0.75"/>
              <ColorMapEntry color="#FF0000" quantity="5"  opacity="0.75"/>
            </ColorMap>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>