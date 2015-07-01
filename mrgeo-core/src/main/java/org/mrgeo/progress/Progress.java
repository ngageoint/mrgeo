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

package org.mrgeo.progress;

/**
 * 
 */
public interface Progress
{
  void complete();

  /**
   * Marks the job as complete and sets the result and kml
   * 
   * @param result
   *          Human readable result of the job as HTML
   * @param kml
   *          Most reasonable result of the job as KML, or null if this does not
   *          apply.
   */
  void complete(String result, String kml);

  /**
   * Marks the job as failed and sets the result
   * 
   * @param result
   *          Human readable error message as HTML
   */
  void failed(String result);

  float get();

  public String getResult();

  public boolean isFailed();

  /**
   * Sets the progress. Progress is a value from 0 to 100.
   * 
   * @param progress
   */
  void set(float progress);

  void starting();
  
  void cancelled();
}
