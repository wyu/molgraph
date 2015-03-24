package org.bioconsensus.kbase;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.collect.*;
import grph.Grph;
import grph.in_memory.InMemoryGrph;
import org.ms2ms.graph.Property;
import org.ms2ms.graph.PropertyEdge;
import org.ms2ms.utils.IOs;
import org.ms2ms.utils.Reporters;
import org.ms2ms.utils.Strs;
import org.ms2ms.utils.Tools;
import toools.NotYetImplementedException;
import toools.collections.AutoGrowingArrayList;
import toools.set.IntHashSet;
import toools.set.IntSet;
import toools.set.IntSingletonSet;

import java.io.*;
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
  private static final long serialVersionUID = 8472752523296641668L;

  public long nodes=0, edges=0;

  public Table<String,  String, IntSet> label_val_node = HashBasedTable.create();
  public Table<Integer, String, String> node_label_val = HashBasedTable.create();
  public Table<String,  String, IntSet> label_val_edge = HashBasedTable.create();
  public Table<Integer, String, String> edge_label_val = HashBasedTable.create();

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
  public IntSet putDirectedEdges(IntSet A, IntSet B, String... tagval)
  {
    if (!Tools.isSet(A) && !Tools.isSet(B)) return null;
    IntSet Es = new IntHashSet();
    for (int a : A.toIntArray())
      for (int b : B.toIntArray())
      {
        // fetch the existing edges
        IntSet e = getEdgesConnecting(a,b);
        boolean found=false;
        if (Tools.isSet(e) && tagval!=null && tagval.length>1)
          for (int edge : e.toIntArray())
            if (Strs.equals(getPropertyByEdgeLabel(edge, tagval[0]), tagval[1])) { found=true; break; }

        if (!Tools.isSet(e) || !found)
        {
          Es.add(addDirectedSimpleEdge(a, b)); edges++;
        }
      }

    // deposit the property for the edges
    setEdgeLabelProperties(Es, tagval);

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
            setEdgeLabelProperty(idx, E, '^');
          }
      }
    return L;
  }
  public boolean hasNodeLabel(String lab, String val)
  {
    return getNodeByLabelProperty(lab, val)!=null;
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
  public PropertyGraph setNodeLabelProperty(int n, Property p)
  {
    for (String tag : p.getProperties().keySet())
      if (tag.charAt(0)!='^')
        setNodeLabelProperty(n, tag, p.getProperty(tag));

    return this;
  }
  public PropertyGraph setNodeLabelProperty(IntSet ns, Property p)
  {
    if (Tools.isSet(ns) && p!=null && Tools.isSet(p.getProperties()))
      for (int n : ns.toIntArray()) setNodeLabelProperty(n, p);

    return this;
  }
  public PropertyGraph setEdgeLabelProperties(IntSet Es, String... tag_val)
  {
    if (Tools.isSet(Es))
      for (int E : Es.toIntArray())
        setEdgeLabelProperties(E, tag_val);

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
  public PropertyGraph setEdgeLabelProperty(int E, PropertyEdge p, char... lead)
  {
    setEdgeLabelProperties(E, GraphHandler.LABEL, p.getLabel(), "desc", p.getDescription(), "id", p.getId(), "url", p.getUrl());
    setEdgeWeight(E, p.getScore()!=null?p.getScore().floatValue():null);

    if (Tools.isSet(p.getProperties()))
      for (String k : p.getProperties().keySet())
        if (!Tools.isSet(lead) || k.charAt(0)==lead[0])
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
  // consolidate duplicate nodes as specified by the fields
  public void consolidate(String... fields)
  {
    Multimap<String, Integer> field_node = HashMultimap.create();
    for (Integer row : node_label_val.rowKeySet())
    {
      String tag = null;
      for (String field : fields)
        tag = Strs.extend(tag, node_label_val.get(row, field), "~");
      // deposit the row
      field_node.put(tag, row);
    }
    // combine the nodes and edges
    for (String tag : field_node.keySet())
    {
      consolidate(field_node.get(tag));
    }
  }
  public void consolidate(Collection<Integer> rows)
  {
    if (rows==null || rows.size()<2) return;
    Integer pivot=null;
    for (Integer row : rows)
    {
      if (pivot==null)  pivot=row;
      else
      {
        for (String t : node_label_val.row(row).keySet())
          if (!Strs.equals(node_label_val.get(pivot, t), node_label_val.get(row, t)))
            node_label_val.put(pivot, t, node_label_val.get(row, t));
        // move the edges
        IntSet edges = getInEdges(row);
        if (Tools.isSet(edges))
          for (int edge : edges.toIntArray())
          {

          }
        // remove the row
        node_label_val.row(row).clear();
      }
    }
  }
  public void transferEdges(int from, int to)
  {
    for (String t : edge_label_val.row(from).keySet())
      if (!Strs.equals(edge_label_val.get(to, t), edge_label_val.get(from, t)))
        edge_label_val.put(to, t, edge_label_val.get(from, t));
  }
  // produce an inventory table
  public StringBuffer inventory()
  {
    StringBuffer buf = new StringBuffer();
    buf.append("nodes/edges: " + nodes + "/" + edges + "\n\n");
    buf = Reporters.inventory_col(buf, node_label_val, 50);
    buf = Reporters.inventory_col(buf, edge_label_val, 50);
/*
    if (Tools.isSet(node_label_val))
    {
      buf.append("Node Property\tCounts\n");
      for (String label : node_label_val.columnKeySet())
        buf.append(Sets.newHashSet(node_label_val.column(label).values()).size() + "\t\t" + label + "\n");
    }

    buf.append("\n");
    if (Tools.isSet(edge_label_val))
    {
      buf.append("Edge Property\tCounts\n");
      for (String label : edge_label_val.columnKeySet())
        buf.append(label + "\t\t" + Sets.newHashSet(edge_label_val.column(label).values()).size() + "\n");
    }
*/
    return buf;
  }
  public void write(String out)
  {
    RandomAccessFile bin = null;
    try
    {
      // save the native graph data to a separate file
      bin = new RandomAccessFile(out, "rw");
      writeGrph(bin);

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
  public static PropertyGraph read(String in)
  {
    RandomAccessFile bin = null;
    PropertyGraph out = null;
    try
    {
      // save the native graph data to a separate file
      bin = new RandomAccessFile(in, "r");
      out = (PropertyGraph)readGrph(bin);

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
/*
  public static PropertyGraph fromBytes(byte[] s)
  {
    try
    {
      ByteArrayInputStream bai = new ByteArrayInputStream(s);
      ObjectInputStream in = new ObjectInputStream(bai);
      PropertyGraph          e = (PropertyGraph ) in.readObject();
      in.close(); bai.close();
      return e;
    }
    catch(IOException i)
    {
      i.printStackTrace();
    }
    catch(ClassNotFoundException c)
    {
      System.out.println("Employee class not found");
      c.printStackTrace();
    }
    return null;
  }
  public static byte[] toBytes(PropertyGraph s)
  {
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    try
    {
      ObjectOutputStream out = new ObjectOutputStream(bao);
      out.writeObject(s);
      out.close(); bao.close();
    }
    catch (IOException e)
    { throw new RuntimeException("Error during persistence", e); }

    return bao.toByteArray();
  }
*/
  private void writeGrph(DataOutput ds) throws IOException
  {
    int numberOfVertices = getVertices().size();
    ds.writeInt(numberOfVertices);

    if (numberOfVertices > 0)
    {
      int greatestVertexID = getVertices().getGreatest();
      ds.writeInt(greatestVertexID);
      writeVertices(ds, getIsolatedVertices(), this);

      int numberOfEdges = getEdges().size();
      ds.writeInt(numberOfEdges);

      if (numberOfEdges > 0)
      {
        int greatestEdgeID = getEdges().getGreatest();
        ds.writeInt(greatestEdgeID);
        ds.writeInt(getNumberOfUndirectedSimpleEdges());

        for (int e : getEdges().toIntArray())
        {
          if (isUndirectedSimpleEdge(e))
          {
            writeEdge(ds, e, this);
            int a = getOneVertex(e);
            writeVertex(ds, a, this);
            writeVertex(ds, getTheOtherVertex(e, a), this);
          }
        }

        ds.writeInt(getNumberOfDirectedSimpleEdges());

        for (int e : getEdges().toIntArray())
        {
          if (isDirectedSimpleEdge(e))
          {
            writeEdge(ds, e, this);
            writeVertex(ds, getDirectedSimpleEdgeTail(e), this);
            writeVertex(ds, getDirectedSimpleEdgeHead(e), this);
          }
        }

        ds.writeInt(getNumberOfUndirectedHyperEdges());

        for (int e : getEdges().toIntArray())
        {
          if (isUndirectedHyperEdge(e))
          {
            writeEdge(ds, e, this);
            writeVertices(ds, getUndirectedHyperEdgeVertices(e), this);
          }
        }

        ds.writeInt(getNumberOfDirectedHyperEdges());

        for (int e : getEdges().toIntArray())
        {
          if (isDirectedHyperEdge(e))
          {
            writeEdge(ds, e, this);
            writeVertices(ds, getDirectedHyperEdgeTail(e), this);
            writeVertices(ds, getDirectedHyperEdgeHead(e), this);
          }
        }
      }
    }

/*
    IntSet isolatedV = getIsolatedVertices();

    ds.writeInt(isolatedV.size());

    if (!isolatedV.isEmpty())
      for (IntCursor v : isolatedV) ds.writeInt(v.value);

    IntSet edges = getEdges();

    ds.writeInt(edges.size());
    for (IntCursor c : getEdges())
    {
      int e = c.value; ds.writeInt(e);
      if (isUndirectedSimpleEdge(e)) // 1 2
      {
        ds.writeInt(0); // type if the edge
        IOs.write(ds, getVerticesIncidentToEdge(e));
      }
      else if (isDirectedSimpleEdge(e)) // 1 > 2
      {
        ds.writeInt(1); // type if the edge
        ds.writeInt(getDirectedSimpleEdgeTail(e));
        ds.writeInt(getDirectedSimpleEdgeHead(e));
      }
      else if (isUndirectedHyperEdge(e)) // 1 2 3 4
      {
        ds.writeInt(2); // type if the edge
        IOs.write(ds, getUndirectedHyperEdgeVertices(e));
      }
      else
      {
        ds.writeInt(3); // type if the edge
        IOs.write(ds, getDirectedHyperEdgeTail(e));
        IOs.write(ds, getDirectedHyperEdgeHead(e));
      }
    }
*/
  }
  private static PropertyGraph readGrph(DataInput ds) throws IOException
  {
    PropertyGraph g = new PropertyGraph();

    int numberOfVertices = ds.readInt();
    if (numberOfVertices > 0)
    {
      int greatestVertexID = ds.readInt();
      // read isolated vertices
      for (int i = ds.readInt(); i > 0; --i)
        g.addVertex(readInteger(ds, greatestVertexID));

      int numberOfEdges = ds.readInt();

      if (numberOfEdges > 0)
      {
        int greatestEdgeID = ds.readInt();
        // read undirected simple edges
        for (int i = ds.readInt(); i > 0; --i)
        {
          int edge = readInteger(ds, greatestEdgeID);
          int a = readInteger(ds, greatestVertexID);
          int b = readInteger(ds, greatestVertexID);
          g.addUndirectedSimpleEdge(edge, a, b);
        }
        // read directed simple edges
        for (int i = ds.readInt(); i > 0; --i)
        {
          int edge = readInteger(ds, greatestEdgeID);
          int t = readInteger(ds, greatestVertexID);
          int h = readInteger(ds, greatestVertexID);
          g.addDirectedSimpleEdge(t, edge, h);
        }
        // read undirected hyper edges
        for (int i = ds.readInt(); i > 0; --i)
        {
          int edge = readInteger(ds, greatestEdgeID);
          g.addUndirectedHyperEdge(edge);

          for (IntCursor v : parseVerticeList(ds, greatestVertexID))
          {
            g.addToUndirectedHyperEdge(edge, v.value);
          }
        }
        // read directed hyper edges
        for (int i = ds.readInt(); i > 0; --i)
        {
          throw new NotYetImplementedException("directed hyperedges are not yet supported");
					/*
					 * int edge = readInteger(dis, greatestEdgeID);
					 * graph.addDirectedHyperEdge(edge);
					 *
					 * for (IntCursor v : parseVerticeList(dis, graph,
					 * greatestVertexID)) { // graph.getDi(edge, v.value); }
					 *
					 * for (IntCursor v : parseVerticeList(dis, graph,
					 * greatestVertexID)) { //
					 * graph.addToDirectedHyperEdge(edge, v.value); }
					 */
        }
      }
    }

/*
    int isolated = ds.readInt();
    if (isolated>0)
      for (int i=0; i<isolated; i++)
        g.addVertex(ds.readInt());

    int edges = ds.readInt();
    for (int i=0; i<edges; i++)
    {
      int edge=ds.readInt(), edge_type=ds.readInt();
      if      (edge_type==0)
      {
        int[] ints = IOs.readIntSet(ds).toIntArray();
        if (ints.length == 2)
          g.addUndirectedSimpleEdge(ints[0], ints[1], edge);
        else
          throw new IllegalStateException("only 2 incident vertices are allowed");
      }
      else if (edge_type==1)
      {
        int head=ds.readInt(), tail=ds.readInt();
        g.addDirectedSimpleEdge(head, tail, edge);
      }
      else if (edge_type==2)
      {
        g.addUndirectedHyperEdge(edge);
        IntSet ints = IOs.readIntSet(ds);
        for (int v : ints.toIntArray())
        {
          g.addToUndirectedHyperEdge(edge, v);
        }
      }
      else if (edge_type==3)
      {
        g.addDirectedHyperEdge(edge);
        IntSet heads=IOs.readIntSet(ds), tails=IOs.readIntSet(ds);
        for (int v : heads.toIntArray()) g.addToDirectedHyperEdgeTail(edge, v);
        for (int v : tails.toIntArray()) g.addToDirectedHyperEdgeHead(edge, v);
      }
    }
*/
    return g;
  }
  public static void writeVertices(DataOutput ds, IntSet set, Grph g) throws IOException
  {
    ds.writeInt(set.size());

    for (IntCursor v : set) writeVertex(ds, v.value, g);
  }

  public static void writeEdge(DataOutput ds, int e, Grph g) throws IOException
  {
    writeInteger(e, ds, g.getEdges().getGreatest());
  }

  public static void writeVertex(DataOutput ds, int v, Grph g) throws IOException
  {
    writeInteger(v, ds, g.getVertices().getGreatest());
  }
  public static void writeInteger(int n, DataOutput ds, int greatestItem) throws IOException
  {
    if (greatestItem < 256)
    {
      ds.writeByte(n);
    }
    else if (greatestItem < 65536)
    {
      ds.writeChar(n);
    }
    else
    {
      ds.writeInt(n);
    }
  }
  public static int readInteger(DataInput ds, int greatestValue) throws IOException
  {
    if (greatestValue < 256)
    {
      return (int) ds.readByte() & 0xFF;
    }
    else if (greatestValue < 65536)
    {
      return ds.readChar();
    }
    else
    {
      return ds.readInt();
    }
  }
  private static IntArrayList parseVerticeList(DataInput ds, int numberOfVertices) throws IOException
  {
    IntArrayList set = new IntArrayList();
    for (int i = ds.readInt(); i > 0; --i)
    {
      set.add(readInteger(ds, numberOfVertices));
    }

    return set;
  }
}
