package org.bioconsensus.kbase;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import grph.in_memory.InMemoryGrph;
import org.ms2ms.graph.Property;
import org.ms2ms.graph.PropertyEdge;
import org.ms2ms.utils.IOs;
import org.ms2ms.utils.Strs;
import org.ms2ms.utils.Tools;
import toools.collections.AutoGrowingArrayList;
import toools.set.IntHashSet;
import toools.set.IntSet;
import toools.set.IntSingletonSet;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.Collection;

/** provide an in-memory cache of graph which can be pushed to Titan via BatchGraph
 *
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   2/3/15
 */
class PropertyGraph extends InMemoryGrph implements Serializable
{
  public long nodes=0, edges=0;

  private Table<String,  String, IntSet> label_val_node = HashBasedTable.create();
  private Table<Integer, String, String> node_label_val = HashBasedTable.create();
  private Table<String,  String, IntSet> label_val_edge = HashBasedTable.create();
  private Table<Integer, String, String> edge_label_val = HashBasedTable.create();

  public PropertyGraph()         { super(); }

  AutoGrowingArrayList<Float> weights = new AutoGrowingArrayList<>();

  synchronized public IntSet getNodeByLabelProperty(String lable, String val) { return label_val_node.get(lable, val); }
  public String getPropertyByNodeLabel(int node, String lable)   { return node_label_val.get(node, lable); }
  public IntSet getEdgeByLabelProperty(String lable, String val) { return label_val_edge.get(lable, val); }
  public String getPropertyByEdgeLabel(int node, String lable)   { return edge_label_val.get(node, lable); }
  public Float  getEdgeWeight(int e)
  {
    assert getEdges().contains(e);
    return weights.get(e);
  }
  public IntSet putDirectedEdges(IntSet A, IntSet B)
  {
    if (!Tools.isSet(A) && !Tools.isSet(B)) return null;
    IntSet Es = new IntHashSet();
    for (int a : A.toIntArray())
      for (int b : B.toIntArray())
      {
        // fetch the existing edges
        if (!Tools.isSet(getEdgesConnecting(a,b)))
        {
          Es.add(addDirectedSimpleEdge(a, b)); edges++;
        }
      }

    return Es;
  }
  public IntSet putDirectedEdges(IntSet A, IntSet B, String tag, String val)
  {
    if (!Tools.isSet(A) && !Tools.isSet(B)) return null;
    IntSet Es = new IntHashSet();
    for (int a : A.toIntArray())
      for (int b : B.toIntArray())
      {
        // fetch the existing edges
        IntSet e = getEdgesConnecting(a,b);
        boolean found=false;
        if (Tools.isSet(e))
          for (int edge : e.toIntArray())
            if (Strs.equals(getPropertyByEdgeLabel(edge, tag), val)) { found=true; break; }

        if (!Tools.isSet(e) || !found)
        {
          Es.add(addDirectedSimpleEdge(a, b)); edges++;
        }
      }

    return Es;
  }
  // add a new node if not already present
  public IntSet putNode(String... tagvals)
  {
    IntSet combined = null;
    if (Tools.isSet(tagvals) && tagvals.length%2==0)
    {
      for (int i=0; i<tagvals.length; i+=2)
      {
        IntSet As = getNodeByLabelProperty(tagvals[i], tagvals[i+1]);
        combined=(combined==null?As:Tools.intersect(combined, As));
      }
    }
    if (combined==null || combined.size()==0)
    {
      int N = addVertex(); nodes++;
      for (int i=0; i<tagvals.length; i+=2)
      {
        setNodeLabelProperty(N, tagvals[i], tagvals[i + 1]);
      }
      combined = new IntSingletonSet(N);
    }
    return combined;
  }
  public IntSet putDirectedEdges(IntSet starters, String tag, Collection<PropertyEdge> ps,
                                     String edge_tag, String edge_val)
  {
    IntSet L = null;
    if (Tools.isSet(ps))
      for (PropertyEdge E : ps)
      {
        // deposit the node if necessary
        IntSet N = putNode(tag, E.getProperty(tag));
               L = putDirectedEdges(starters, N, edge_tag, edge_val);

        setNodeLabelProperty(N, E);

        // populate the properties
        if (Tools.isSet( L))
          for (int idx : L.toIntArray())
          {
            setEdgeLabelProperty(idx, edge_tag, edge_val);
            setEdgeLabelProperty(idx, E);
          }
      }
    return L;
  }
  public boolean hasNodeLabel(String lab, String val)
  {
    return getNodeByLabelProperty(lab, val)==null;
  }
  public PropertyGraph setNodeLabelProperty(int n, String lable, String val)
  {
    if (Strs.isSet(lable) && Strs.isSet(val))
    {
      Tools.put(label_val_node, lable, val, n);
      node_label_val.put(n, lable, val);
    }
    return this;
  }
  public PropertyGraph setNodeLabelProperty(IntSet ns, Property p)
  {
    if (Tools.isSet(ns) && p!=null && Tools.isSet(p.getProperties()))
      for (int n : ns.toIntArray())
        for (String tag : p.getProperties().keySet())
          if (tag.charAt(0)!='^')
            setNodeLabelProperty(n, tag, p.getProperty(tag));

    return this;
  }
  public PropertyGraph setEdgeLabelProperties(int E, String... tag_val)
  {
    if (Tools.isSet(tag_val))
      for (int i=0; i<tag_val.length; i+=2)
        setEdgeLabelProperty(E, tag_val[i], tag_val[i+1]);

    return this;
  }
  synchronized public PropertyGraph setEdgeLabelProperty(int E, String lable, String val)
  {
    if (Strs.isSet(lable) && Strs.isSet(val))
    {
      Tools.put(label_val_edge, lable, val, E);
      edge_label_val.put(E, lable, val);
    }
    return this;
  }
  public PropertyGraph setEdgeLabelProperty(int E, PropertyEdge p)
  {
    setEdgeLabelProperties(E, GraphHandler.LABEL, p.getLabel(), "desc", p.getDescription(), "id", p.getId(), "url", p.getUrl());
    setEdgeWeight(E, p.getScore()!=null?p.getScore().floatValue():null);

    if (Tools.isSet(p.getProperties()))
      for (String k : p.getProperties().keySet())
        if (k.charAt(0)=='^')
          setEdgeLabelProperty(E, k.substring(1), p.getProperty(k));

    return this;
  }
  public PropertyGraph setEdgeLabelProperty(int E, Property p)
  {
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
  // produce an inventory table
  public StringBuffer inventory()
  {
    StringBuffer buf = new StringBuffer();
    buf.append("nodes/edges: " + (Tools.isSet(node_label_val)?node_label_val.rowKeySet().size():0) + "/" +
                                 (Tools.isSet(edge_label_val)?edge_label_val.rowKeySet().size():0) + "\n");
    if (Tools.isSet(node_label_val))
    {
      buf.append("Node Property\tCounts\n");
      for (String label : node_label_val.columnKeySet())
        buf.append(label + "\t\t" + Sets.newHashSet(node_label_val.column(label).values()).size() + "\n");
    }

    buf.append("\n");
    if (Tools.isSet(edge_label_val))
    {
      buf.append("Edge Property\tCounts\n");
      for (String label : edge_label_val.columnKeySet())
        buf.append(label + "\t\t" + Sets.newHashSet(edge_label_val.column(label).values()).size() + "\n");
    }
    return buf;
  }
  public void write(String out)
  {
    RandomAccessFile bin = null;
    try
    {
      // save the native graph data to a separate file
      bin = new RandomAccessFile(out+".grh", "rw");
      IOs.write(bin, toGrphBinary());
      bin.close();
      // save the auxillary data to another file
      bin = new RandomAccessFile(out+".data", "rw");
      IOs.writeIntStr2(bin, node_label_val);
      IOs.writeIntStr2(bin, edge_label_val);
      IOs.writeStr2IntSet(bin, label_val_edge);
      IOs.writeStr2IntSet(bin, label_val_node);
      bin.close();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }
  public static PropertyGraph fromBinary(String in)
  {
    RandomAccessFile bin = null;
    PropertyGraph out = null;
    try
    {
      // save the native graph data to a separate file
      bin = new RandomAccessFile(in+".grh", "rw");
      out = (PropertyGraph)fromGrphBinary(IOs.readBytes(bin));
      bin.close();
      // save the auxillary data to another file
      bin = new RandomAccessFile(in+".data", "rw");
      out.node_label_val = IOs.readIntStr2(bin);
      out.edge_label_val = IOs.readIntStr2(bin);
      out.label_val_edge = IOs.readStr2IntSet(bin);
      out.label_val_node = IOs.readStr2IntSet(bin);
      bin.close();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    return out;
  }
}
