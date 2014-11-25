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

package org.mrgeo.mapalgebra.optimizer;

import org.mrgeo.mapalgebra.MapAlgebraParser;
import org.mrgeo.mapalgebra.MapOp;

import java.util.*;

public class Optimizer
{
  private MapOp _startingRoot;
  ArrayList<Rule> _rules = new ArrayList<Rule>();
  HashMap<Class<? extends MapOp>, ArrayList<Rule>> _ruleMap;
  HashSet<String> _visited;
  int _maxIterations = 100;
  PriorityQueue<Entry> _queue;
  Heuristic _heuristic = new Heuristic();

  private class Entry implements Comparable<Entry>
  {
    public MapOp op;
    public double score;

    public Entry(MapOp op, double score)
    {
      this.op = op;
      this.score = score;
    }

    @Override
    public int compareTo(Entry o)
    {
      return Double.compare(this.score, o.score);
    }
  }

  public Optimizer(MapOp root)
  {
    _startingRoot = root;
    _registerRules();
  }

  private double _calculateHeuristic(MapOp op)
  {
    return _heuristic.estimate(op);
  }

  public MapOp optimize()
  {
    _visited = new HashSet<String>();
    // use the starting root as the starting position
    Entry best = new Entry(_startingRoot, _calculateHeuristic(_startingRoot));
    _queue = new PriorityQueue<Entry>();
    _queue.add(best);

    int i = 0;

    // go until we run out of options or we hit max iterations.
    do
    {
      // pop off the best option
      Entry current = _queue.remove();

      // generate new permutations
      _generatePermutations(current.op);

      // System.out.println(current.score);
      // System.out.println("---");
      // System.out.println(MapAlgebraParser.toString(current.op));

      if (current.score < best.score)
      {
        best = current;
      }

      i++;
    } while (_queue.size() > 0 && i < _maxIterations);

    return best.op;
  }

  private void _applyRules(MapOp root, MapOp subject)
  {
    if (_ruleMap.containsKey(subject.getClass()))
    {
      ArrayList<Rule> rules = _ruleMap.get(subject.getClass());

      for (Rule r : rules)
      {
        _applyRule(root, subject, r);
      }
    }

    ArrayList<Rule> rules = _ruleMap.get(null);

    for (Rule r : rules)
    {
      _applyRule(root, subject, r);
    }
    
    for (MapOp child : subject.getInputs())
    {
      _applyRules(root, child);
    }
  }

  private void _applyRule(MapOp root, MapOp subject, Rule r)
  {
    if (r.isApplicable(root, subject))
    {
      ArrayList<MapOp> ops = r.apply(root, subject);

      for (MapOp mo : ops)
      {
        String s = MapAlgebraParser.toString(mo);
        if (_visited.contains(s) == false)
        {
//           System.out.println(r.getClass().getName());
//           System.out.println(MapAlgebraParserv1.toString(root));
//           System.out.println("to:");
//           System.out.println(s);

          _visited.add(s);
          _queue.add(new Entry(mo, _calculateHeuristic(mo)));
        }
      }
    }

    for (MapOp op : subject.getInputs())
    {
      _applyRule(root, op, r);
    }
  }

  private void _generatePermutations(MapOp root)
  {
    _applyRules(root, root);
  }

  private void _registerRules()
  {
    ServiceLoader<Rule> loader = ServiceLoader.load(Rule.class);
    
    for (Rule r : loader)
    {
      _rules.add(r);
    }

    _ruleMap = new HashMap<Class<? extends MapOp>, ArrayList<Rule>>();
    
    // this represents classes that can be applied to all MapOps
    _ruleMap.put(null, new ArrayList<Rule>());

    for (Rule r : _rules)
    {
      ArrayList<Class<? extends MapOp>> candidates = r.getCandidates();
      for (Class<? extends MapOp> c : r.getCandidates())
      {
        if (_ruleMap.containsKey(c) == false)
        {
          _ruleMap.put(c, new ArrayList<Rule>());
        }
        ArrayList<Rule> rs = _ruleMap.get(c);
        rs.add(r);
      }

      // if no candidates are specified then it will be evaluated for every node
      if (candidates.size() == 0)
      {
        ArrayList<Rule> rs = _ruleMap.get(null);
        rs.add(r);
      }
    }
  }
}
