<?xml version="1.0" encoding="ISO-8859-1"?>
<StyledLayerDescriptor version="1.0.0" 
    xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd" 
    xmlns="http://www.opengis.net/sld" 
    xmlns:ogc="http://www.opengis.net/ogc" 
    xmlns:xlink="http://www.w3.org/1999/xlink" 
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <NamedLayer>
    <Name>Dynamic Rainbow</Name>
    <UserStyle>
      <Title></Title>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <ColorMap>
              <ColorMapEntry color="#00FF00" quantity="${env('one',0)}"    label="one" opacity="0.50"/>
              <ColorMapEntry color="#FFFF00" quantity="${env('two',64)}" label="two" opacity="0.50"/>
              <ColorMapEntry color="#ffa500" quantity="${env('three',128)}" label="three" opacity="0.50"/>
              <ColorMapEntry color="#FF0000" quantity="${env('four',192)}" label="four" opacity="0.50"/>
              <ColorMapEntry color="#800080" quantity="${env('five',255)}" label="five" opacity="0.50"/>
            </ColorMap>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>