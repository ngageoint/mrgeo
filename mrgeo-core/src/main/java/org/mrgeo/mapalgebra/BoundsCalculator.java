/*
 * Copyright 2009-2015 DigitalGlobe, Inc.
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

package org.mrgeo.mapalgebra;

import org.mrgeo.utils.Bounds;

import java.io.IOException;

/**
 * Only map ops that compute the bounds of their output directly
 * should implement this interface. If the bounds of a map op
 * can be gotten from its input(s), then do not implement this
 * interface.
 */
public interface BoundsCalculator
{
  /**
   * Calculate the bounds of the output produced by this map op.
   */
  public Bounds calculateBounds() throws IOException;
}
