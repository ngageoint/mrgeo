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
