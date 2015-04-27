package org.bioconsensus.kbase;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.collect.*;
import grph.Grph;
import grph.in_memory.InMemoryGrph;
import org.ms2ms.graph.Graphs;
import org.ms2ms.graph.Property;
import org.ms2ms.graph.PropertyEdge;
import org.ms2ms.math.Stats;
import org.ms2ms.r.Dataframe;
import org.ms2ms.utils.*;
import toools.NotYetImplementedException;
import toools.collections.AutoGrowingArrayList;
import toools.set.IntHashSet;
import toools.set.IntSet;
import toools.set.IntSingletonSet;

import java.io.*;
import java.util.*;

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

  public static PropertyGraph fixup(PropertyGraph graph, Dataframe mapping)
  {
    Map es2gene = mapping!=null?mapping.toMap("Ensembl Gene ID", "Approved Symbol"):null;
    if (graph.node_label_val.column(GraphHandler.ENSEMBLE)!=null)
    {
      for (Integer row : graph.node_label_val.column(GraphHandler.ENSEMBLE).keySet())
      {
        Object g = es2gene.get(graph.node_label_val.get(row, GraphHandler.ENSEMBLE));
        if (g!=null)
          graph.node_label_val.put(row, Graphs.GENE, g.toString().toUpperCase());
      }
      graph.node_label_val.column(GraphHandler.ENSEMBLE).clear();
    }
    if (graph.node_label_val.column("gene name")!=null)
      for (Integer row : graph.node_label_val.column("gene name").keySet())
      {
        if (Tools.equals(graph.node_label_val.get(row, "gene name"), graph.node_label_val.get(row, Graphs.GENE)))
          graph.node_label_val.remove(row, "gene name");
      }

    // setup the node type
    for (Integer row : graph.node_label_val.rowKeySet())
    {
      String type=null;
      if      (graph.node_label_val.contains(row, Graphs.GENE))
      {
        String gene = graph.node_label_val.get(row, Graphs.GENE);
        graph.setNode(Graphs.UID, gene, row);
        type = Graphs.GENE;
        graph.node_label_val.remove(row, Graphs.GENE);
        graph.label_val_node.remove(Graphs.GENE, gene);
      }
      else if (graph.node_label_val.contains(row, PsiMI25Reader.TYPE_ACTOR))
      {
        type = graph.node_label_val.get(row, PsiMI25Reader.TYPE_ACTOR).toUpperCase();
      }
      graph.setNode(Graphs.TYPE, type, row);
    }
    return graph;
  }

  synchronized public IntSet getNodeByLabelProperty(String lable, String val) { return label_val_node.get(lable, val); }
//  public String getPropertyByNodeLabel(int node, String lable)   { return node_label_val.get(node, lable); }
//  public IntSet getEdgeByLabelProperty(String lable, String val) { return label_val_edge.get(lable, val); }
  public String getPropertyByEdgeLabel(int node, String lable)   { return edge_label_val.get(node, lable); }
  public Float  getEdgeWeight(int e)
  {
    assert getEdges().contains(e);
    return weights.get(e);
  }
/*
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
*/
  public IntSet putEdges(IntSet A, IntSet B, boolean directed, Float weight, String... tagval)
  {
    if (!Tools.isSet(A) || !Tools.isSet(B)) return null;
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
          Es.add(addSimpleEdge(a, b, directed)); edges++;
        }
      }

    // deposit the property for the edges
    if (Tools.isSet(Es))
    {
      setEdgeLabelProperties(Es, tagval);
      if (weight!=null)
       for (int i : Es.toIntArray()) setEdgeWeight(i, weight);
    }

    return Es;
  }
  public void setNode(String label, String val, Integer row)
  {
    if (Strs.isSet(label) && Strs.isSet(val) && row!=null)
    {
      node_label_val.put(row, label, val);
      label_val_node.put(label, val, Tools.newIntSet(row));
    }
  }
  public IntSet putNode(Property p, String idtag)
  {
    if (p==null || !Tools.isSet(p.getProperties())) return null;
    // fetch the existing node if there
    IntSet combined = (idtag!=null && p.getProperty(idtag)!=null) ? getNodeByLabelProperty(idtag, p.getProperty(idtag)) : null;

    if (!Tools.isSet(combined))
    {
      int N = addVertex(); nodes++;
      for (String tag : p.getProperties().keySet())
      {
        setNodeLabelProperty(N, tag, p.getProperty(tag));
      }
      combined = new IntSingletonSet(N);
    }

    return combined;
  }
  // add a new node if not already present
  public IntSet putNode(String... tagvals)
  {
    Map<String, String> out = Strs.toStrMap(tagvals);
    // assume the first pair is the primary key
    IntSet combined = (Tools.isSet(tagvals) && tagvals.length>1) ? getNodeByLabelProperty(tagvals[0], tagvals[1]) : null;
    if (!Tools.isSet(combined))
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
  public IntSet putNodeByUIDType(String... tagvals)
  {
    Map<String, String> out = Strs.toStrMap(tagvals);
    IntSet combined = getNodeByLabelProperty(Graphs.UID, out.get(Graphs.UID));
    if (combined!=null)
    {
      // check by the type again
      IntSet typed = getNodeByLabelProperty(Graphs.TYPE, out.get(Graphs.TYPE));
      if (typed!=null) combined = Tools.intersect(combined, typed);
    }
    if (!Tools.isSet(combined))
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
               L = putEdges(starters, N, true, null, edge_tag, edge_val);

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
    setEdgeLabelProperties(E, Graphs.LABEL, p.getLabel(), "desc", p.getDescription(), "id", p.getId(), "url", p.getUrl());
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
  public void writeNodes2CSV(String filename)
  {
    char d = '\t';
    try
    {
      FileWriter w = new FileWriter(filename+".nodes");
      // name:ID – global id column by which the node is looked up for later reconnecting, if property name is left off
      //           it will be not stored (temporary), this is what the --id-type refers to currently this node-id has
      //           to be globally unique even across entities
      // :LABEL  – label column for nodes, multiple labels can be separated by delimiter
      // all other columns are treated as properties but skipped if empty
      // type conversion is possible by suffixing the name, e.g. by :INT, :BOOLEAN, etc.
//      w.write("name:ID"+d+":LABEL");
      w.write("id"+d+"label");
      List<String> cols = new ArrayList<>();
//      cols.add(Graphs.LABEL);
      for (String col : node_label_val.columnKeySet())
      {
        if (Strs.isA(col, Graphs.LABEL, Graphs.ID)) continue;
        w.write(d+col); cols.add(Strs.equals(col, Graphs.TYPE)?"label":col);
      }
      w.write("\n");
      for (Integer row : node_label_val.rowKeySet())
      {
        w.write(row.toString()+d+node_label_val.get(row, Graphs.UID)+d);
        IOs.write(w,d, Tools.toCols(node_label_val.row(row), cols));
        w.write("\n");
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }
  public void writeNodes2CSVByLabel(String filename)
  {
    char d = '\t';
    try
    {
      // divide the nodes by their types
      for (String type : label_val_node.row(Graphs.TYPE).keySet())
        writeNodes2CSV(filename+"_nodes."+type.replaceAll(" ", "_"), type, d);
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }
  public void writeNodes2CSV(String nodefile, String type, char d) throws IOException
  {
    // name:ID – global id column by which the node is looked up for later reconnecting, if property name is left off
    //           it will be not stored (temporary), this is what the --id-type refers to currently this node-id has
    //           to be globally unique even across entities
    // :LABEL  – label column for nodes, multiple labels can be separated by delimiter
    // all other columns are treated as properties but skipped if empty
    // type conversion is possible by suffixing the name, e.g. by :INT, :BOOLEAN, etc.
    Set<String> cols = new HashSet<>();
    for (int i : getVertices().toIntArray())
      if (Strs.equals(node_label_val.row(i).get(Graphs.TYPE), type))
        cols.addAll(node_label_val.row(i).keySet());

    cols.remove(Graphs.TYPE);cols.remove(Graphs.LABEL);
    List<String> columns = new ArrayList<>(cols);
//      cols.add(Graphs.LABEL);
    FileWriter w = new FileWriter(nodefile);

    w.write("id"+Strs.toString(cols, d+"")+"\n");
    for (Integer row : getVertices().toIntArray())
      if (Strs.equals(node_label_val.row(row).get(Graphs.TYPE), type))
      {
        w.write(row.toString());
        if (Tools.isSet(columns))
        {
          w.write(d);
          IOs.write(w,d, Tools.toCols(node_label_val.row(row), columns));
        }
        w.write("\n");
      }

    w.close();
  }
  public void writeEdges2CSV(String filename)
  {
    char d = '\t';
    try
    {

      FileWriter w = new FileWriter(filename+".edges");
      // :START_ID, :END_ID – relationship file columns referring to global node-lookup-id
      // :TYPE – relationship-type column
      // all other columns are treated as properties but skipped if empty
      // type conversion is possible by suffixing the name, e.g. by :INT, :BOOLEAN, etc.
      //w.write(":START_ID"+d+":END_ID"+d+":TYPE");
      w.write("start"+d+"end"+d+"type"+d+"weight");
      List<String> cols = new ArrayList<>();
//      cols.add(Graphs.LABEL);
      for (String col : edge_label_val.columnKeySet())
      {
        if (Strs.isA(col, Graphs.TYPE)) continue;
        w.write(d+col); cols.add(col);
      }
      w.write("\n");
      for (int i : getEdges().toIntArray())
      {
        IntSet nodes = getVerticesIncidentToEdge(i);
        if (nodes==null || nodes.size()!=2)
        {
          System.out.println("edge="+i+", nodes="+nodes.size());
          continue;
        }
//        assert nodes!=null && nodes.size()==2;
        IOs.write(w,d,nodes.toIntArray()[0]+"", nodes.toIntArray()[1]+"", edge_label_val.get(i, Graphs.TYPE), (getEdgeWeight(i)!=null?getEdgeWeight(i).toString():""));
        w.write(d+"");
        IOs.write(w,d, Tools.toCols(edge_label_val.row(i), cols));
        w.write("\n");
      }
      w.close();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }
  public void writeEdges2CSVByType(String filename)
  {
    char d = '\t';
    try
    {
      // divide the nodes by their types
      for (String type : label_val_edge.row(Graphs.TYPE).keySet())
      {
        writeEdges2CSV(filename+"_edges."+type.replaceAll(" ", "_"), type, d);
      }
/*

      FileWriter w = new FileWriter(filename);
      // :START_ID, :END_ID – relationship file columns referring to global node-lookup-id
      // :TYPE – relationship-type column
      // all other columns are treated as properties but skipped if empty
      // type conversion is possible by suffixing the name, e.g. by :INT, :BOOLEAN, etc.
      w.write(":START_ID"+d+":END_ID"+d+":TYPE");
      List<String> cols = new ArrayList<>();
//      cols.add(Graphs.LABEL);
      for (String col : edge_label_val.columnKeySet())
      {
        if (Strs.isA(col, Graphs.TYPE)) continue;
        w.write(d+col); cols.add(col);
      }
      w.write("\n");
      for (int i : getEdges().toIntArray())
      {
        IntSet nodes = getVerticesIncidentToEdge(i);
        if (nodes==null || nodes.size()!=2)
        {
          System.out.println("edge="+i+", nodes="+nodes.size());
          continue;
        }
//        assert nodes!=null && nodes.size()==2;
        IOs.write(w,d,nodes.toIntArray()[0]+"", nodes.toIntArray()[1]+"", edge_label_val.get(i, Graphs.TYPE));
        w.write(d+"");
        IOs.write(w,d, Tools.toCols(edge_label_val.row(i), cols));
        w.write("\n");
      }
      w.close();
*/
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }
  public void writeEdges2CSV(String filename, String type, char d) throws IOException
  {
    Set<String> cols = new HashSet<>();
    for (int i : getEdges().toIntArray())
      if (Strs.equals(edge_label_val.row(i).get(Graphs.TYPE), type))
        cols.addAll(edge_label_val.row(i).keySet());

    cols.remove(Graphs.TYPE); cols.remove(Graphs.LABEL);
    List<String> columns = new ArrayList<>(cols);

    FileWriter w = new FileWriter(filename);

    // :START_ID, :END_ID – relationship file columns referring to global node-lookup-id
    // :TYPE – relationship-type column
    // all other columns are treated as properties but skipped if empty
    // type conversion is possible by suffixing the name, e.g. by :INT, :BOOLEAN, etc.
    w.write("start"+d+"end"+d+"weight"+Strs.toString(cols, d+"")+"\n");
    for (Integer row : getEdges().toIntArray())
      if (Strs.equals(edge_label_val.row(row).get(Graphs.TYPE), type))
      {
        IntSet nodes = getVerticesIncidentToEdge(row);
        if (nodes==null || nodes.size()!=2)
        {
          System.out.println("edge="+row+", nodes="+nodes.size());
          continue;
        }
        IOs.write(w,d,nodes.toIntArray()[0]+"", nodes.toIntArray()[1]+"", (getEdgeWeight(row)!=null?getEdgeWeight(row).toString():""));
        if (Tools.isSet(columns))
        {
          w.write(d);
          IOs.write(w,d, Tools.toCols(edge_label_val.row(row), columns));
        }
        w.write("\n");
//        w.write(row.toString()+d+edge_label_val.get(row, Graphs.LABEL));
//        if (Tools.isSet(columns))
//        {
//          w.write(d);
//          IOs.write(w,d, Tools.toCols(node_label_val.row(row), columns));
//        }
//        w.write("\n");
      }
    w.close();
/*


    FileWriter w = new FileWriter(filename);
    w.write(":START_ID"+d+":END_ID");
    List<String> cols = new ArrayList<>();
//      cols.add(Graphs.LABEL);
    for (String col : edge_label_val.columnKeySet())
    {
      if (Strs.isA(col, Graphs.TYPE)) continue;
      w.write(d+col); cols.add(col);
    }
    w.write("\n");
    for (int i : getEdges().toIntArray())
    {
      IntSet nodes = getVerticesIncidentToEdge(i);
      if (nodes==null || nodes.size()!=2)
      {
        System.out.println("edge="+i+", nodes="+nodes.size());
        continue;
      }
//        assert nodes!=null && nodes.size()==2;
      IOs.write(w,d,nodes.toIntArray()[0]+"", nodes.toIntArray()[1]+"", edge_label_val.get(i, Graphs.TYPE));
      w.write(d+"");
      IOs.write(w,d, Tools.toCols(edge_label_val.row(i), cols));
      w.write("\n");
    }
    w.close();
*/
  }
  /** read a tab-delimited file that contains the private set of nodes
   *
   type    label   abbr    ID
   disease ulcerative colitis      UC      EFO_0000729
   *
   * @return the type and labels of the nodes
   */
  public Collection<Integer> curation(String file, Map<String, String> props)
  {
    try
    {
      TabFile d = new TabFile(file, TabFile.tabb);
//      // come up with the headers. need to preserve the order of the keys
//      LinkedHashMap<String, String> props = new LinkedHashMap<>();
//      props.put("ID",    Graphs.UID);
//      props.put("type",  Graphs.TYPE);
//      props.put("label", Graphs.TITLE);

      for (String col : d.getHeaders())
        if (!props.keySet().contains(col)) props.put(col, col);

      Set<Integer> curated = new HashSet<>();
      while (d.hasNext())
      {
        Tools.add(curated, putNode(Tools.toColsHdr(d.nextRow(), props)));
      }
      return curated;
    }
    catch (IOException e) { e.printStackTrace(); }

    return null;
  }
  // Attach the newly created nodes to the curation if exist
  public void curates(Collection<Integer> curated, String type, int... nodes)
  {
    if (!Tools.isSet(curated)) return;

    for (Integer node : nodes)
      for (Integer c : curated)
      {
        int E = addUndirectedSimpleEdge(node, c);
        if (Strs.isSet(type)) setEdgeLabelProperty(E, Graphs.TYPE, type);
      }
  }
}
