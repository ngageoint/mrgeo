/*
 * Copyright 2009-2014 DigitalGlobe, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package org.mrgeo.data.shp.esri;

import org.mrgeo.data.shp.esri.geom.JShape;

import java.io.IOException;


public interface ShpData
{
  public void addShape(JShape obj) throws FormatException;

  public int getCount();

  public JShape getShape(int i) throws IOException;

  public void load(int i, byte[] record);

  public void resizeCache(int size);

  public byte[] save(int i);

  public void setParent(ESRILayer parent);
}
