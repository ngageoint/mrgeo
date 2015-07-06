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

package org.mrgeo.data;

import org.mrgeo.mapreduce.splitters.TiledInputSplit;

import java.util.List;

public class SplitIterator
{
    private int splitIndex;
    private List<TiledInputSplit> splits;
    private SplitVisitor visitor;

    public SplitIterator(List<TiledInputSplit> splits, SplitVisitor visitor)
    {
        this.splits = splits;
        this.splitIndex = 0;
        this.visitor = visitor;
    }

    public TiledInputSplit next()
    {
        if (splitIndex > splits.size())
        {
            return null;
        }
        while (splitIndex < splits.size())
        {
            TiledInputSplit split = splits.get(splitIndex);
            splitIndex++;
            if (visitor != null)
            {
                if (visitor.accept(split))
                {
                    return split;
                }
            }
            else
            {
                return split;
            }
        }
        return null;
    }
}
