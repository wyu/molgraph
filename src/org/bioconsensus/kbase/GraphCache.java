package org.bioconsensus.kbase;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import grph.in_memory.InMemoryGrph;
import org.ms2ms.graph.Property;
import org.ms2ms.graph.PropertyEdge;
import org.ms2ms.utils.Strs;
import org.ms2ms.utils.Tools;
import toools.collections.AutoGrowingArrayList;
import toools.set.IntHashSet;
import toools.set.IntSet;

/** provide an in-memory cache of graph which can be pushed to Titan via BatchGraph
 *
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   2/3/15
 */
class GraphCache extends InMemoryGrph
{
  private Table<String, String, IntSet> label_val_node = HashBasedTable.create();
  private Table<Integer, String, String> node_label_val = HashBasedTable.create();
  private Table<String, String, IntSet> label_val_edge = HashBasedTable.create();
  private Table<Integer, String, String> edge_label_val = HashBasedTable.create();

//  private Map<String, Integer>           label_edge     = new HashMap<>();
//  private Multimap<String, PropertyNode>      name_acc       = HashMultimap.create();

  public GraphCache()         { super(); }

  AutoGrowingArrayList<Float> weights = new AutoGrowingArrayList<>();

  public IntSet getNodeByLabelProperty(String lable, String val) { return label_val_node.get(lable, val); }
  public String getPropertyByNodeLabel(int node, String lable)   { return node_label_val.get(node, lable); }
  public IntSet getEdgeByLabelProperty(String lable, String val) { return label_val_edge.get(lable, val); }
  public String getPropertyByEdgeLabel(int node, String lable)   { return edge_label_val.get(node, lable); }
  public Float  getEdgeWeight(int e)
  {
    assert getEdges().contains(e);
    return weights.get(e);
  }
  public GraphCache setNodeLabelProperty(int n, String lable, String val)
  {
    if (Strs.isSet(lable) && Strs.isSet(val))
    {
      Tools.put(label_val_node, lable, val, n);
      node_label_val.put(n, lable, val);
    }
    return this;
  }
  public GraphCache setEdgeLabelProperties(int E, String... tag_val)
  {
    if (Tools.isSet(tag_val))
      for (int i=0; i<tag_val.length; i+=2)
        setEdgeLabelProperty(E, tag_val[i], tag_val[i+1]);

    return this;
  }
  public GraphCache setEdgeLabelProperty(int E, String lable, String val)
  {
    if (Strs.isSet(lable) && Strs.isSet(val))
    {
      Tools.put(label_val_edge, lable, val, E);
      edge_label_val.put(E, lable, val);
    }
    return this;
  }
  public GraphCache setEdgeLabelProperty(int E, PropertyEdge p)
  {
    setEdgeLabelProperties(E, GraphHandler.LABEL, p.getLabel(), "desc", p.getDescription(), "id", p.getId(), "url", p.getUrl());
    setEdgeWeight(E, p.getScore()!=null?p.getScore().floatValue():null);

    if (Tools.isSet(p.getProperties()))
      for (String k : p.getProperties().keySet())
        setEdgeLabelProperty(E, k, p.getProperty(k));

    return this;
  }
  public void setEdgeWeight (int e, Float newWeight)
  {
    if (newWeight!=null)
    {
      assert getEdges().contains(e);
      weights.set(e, newWeight) ;
    }
  }
}
