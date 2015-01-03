package org.bioconsensus.kbase;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hfg.util.FileUtil;
import com.hfg.xml.XMLNode;
import com.hfg.xml.XMLTag;
import com.hfg.xml.parser.XMLTagSAXListener;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DirectedMultigraph;
import org.jgrapht.graph.DirectedWeightedMultigraph;
import org.ms2ms.data.XMLs;
import org.ms2ms.data.collect.MapOfMultimap;
import org.ms2ms.graph.PropertyEdge;
import org.ms2ms.graph.PropertyNode;
import org.ms2ms.utils.Strs;
import org.ms2ms.utils.Tools;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/** User: wyu
 *  Date: 11/20/14
 * To change this template use File | Settings | File Templates.
 */
public class XgmmlReader implements XMLTagSAXListener
{
  public  static final String XML_NODES = "nodes";
  public  static final String XML_NODE  = "node";
  public  static final String XML_EDGE  = "edge";
  public  static final String XML_GENE_NAME = "gene_name";
  public  static final String XML_LOCATION  = "localization";
  public  static final String XML_CLASS     = "class";

  public int node_excluded, edge_excluded;
  private Multimap<Long, PropertyNode> mIdNode = HashMultimap.create();
  private Multimap<Long, PropertyEdge> mIdEdge = HashMultimap.create();
  private Map<String, PropertyNode> mGeneNode = new HashMap<>();

  private MapOfMultimap<String, String, Long>
      mNodeIndice = new MapOfMultimap<>(),
      mEdgeIndice = new MapOfMultimap<>();

  private DirectedMultigraph<PropertyNode, PropertyEdge> mGraph = new DirectedWeightedMultigraph<>(PropertyEdge.class);

  public XgmmlReader() { super(); }
//  public XgmmlReader(String root, String fs) { super(); read(root, fs); }
  //-----------------------------------------------------------------------
  public void startDocument()
  {
  }
  public Collection<String> getTargetTags()
  {
    Collection<String> tags = new ArrayList<String>();
//    tags.add(XML_NODES);
    tags.add(XML_NODE);
    tags.add(XML_EDGE);

    return tags;
  }
  public PropertyNode getNodeByGene(String g) { return mGeneNode!=null?mGeneNode.get(g):null; }
  public DirectedGraph<PropertyNode, PropertyEdge> getGraph() { return mGraph; }

  /** deposit the interaction. create the nodes if necessary
   *
   //  "", "link_id",    "id1",                          "id2",      "effect",           "mechanism",      "trust"
   //  "1",-1421811665,  "N-(5-isopropyl-thiazol-2-yl)phenylacetylamide intracellular",  "CDK2", "Inhibition", "Binding",    "Present"
   //  "2",-2078649201,  "BMS-265246 intracellular",     "CDK2",     "Inhibition",       "Binding",        "Present"
   //  "3",-805465678,   "miR-125b-2-3p",                "B-TAF",    "Inhibition",       "miRNA binding",  "NLP"
   //  "4",-1301496342,  "ABT 866 extracellular region", "Alpha-1D adrenergic receptor","Inhibition","Binding","Probably present (animal model)"
   //  "5",-1668167596,  "PICK1",                        "MITF",     "Inhibition",       "Binding",        "Probably present (animal model)"
   *
   * @param node1 and node2 are the names corresponding to the starting and targeting nodes
   * @param properties
   * @return
   */
  public XgmmlReader deposit(String node1, String node2, Map<String, String> properties)
  {
    return this;
  }
  protected Collection<PropertyNode> newNodes(XMLTag tag)
  {
    if (tag==null || !Tools.equals(tag.getTagName(), XML_NODE)) return null;

//      <node id="-1547657770" label="CCL2">
//        <att name="url" value="portal.genego.com/cgi/entity_page.cgi?id=-1547657770&amp;term=100"/>
//        <att name="class" value="Receptor ligand"/>
//        <att name="localization" value="extracellular region"/>
//        <att name="entrezgene" value="6347"/>
//        <att name="gene_name" value="CCL2"/>
//        <att name="loc" value="23"/>
//        <graphics type="circle" x="342.5" y="499.5"/>
//      </node>

    PropertyNode node = new PropertyNode();

    node.setName(XMLs.getString(tag, "label")).setID(XMLs.getLong(tag, "id"));
    for (XMLNode t : tag.getXMLNodeSubtags())
    {
      node.set(t, "class").set(t, "localization").set(t, "entrezgene").set(t, "gene_name").set(t, "loc");
    }
    Collection<PropertyNode> nodes = new ArrayList<PropertyNode>();

    String gene = node.getProperty("gene_name");
    if (Strs.isSet(gene) && !Strs.equals(gene, "NA"))
    {
      for (String g : Strs.split(gene, ','))
      {
        PropertyNode N = node.clone();
        N.setName(g).setProperty("gene", g);
        nodes.add(N);
      }
    }
    else
    {
      node.setName(gene);
      nodes.add(node);
    }

    return nodes;
  }
  protected PropertyEdge newEdge(XMLTag tag)
  {
    if (tag==null || !Strs.equals(tag.getTagName(), XML_EDGE)) return null;

//    <edge source="-1135984555" target="431" label="-839153712">
//      <att name="url" value="portal.genego.com/cgi/regulation/link_info.cgi?id=-839153712"/>
//      <att name="effect" value="Activation"/>
//      <att name="mechanism" value="Binding"/>
//      <att name="trust" value="Present"/>
//    </edge>

    PropertyEdge edge = new PropertyEdge();

    edge.setID(    XMLs.getLong(tag, "label")
       ).setTarget(XMLs.getLong(  tag, "target")).setSource(XMLs.getLong(tag, "source"));
    for (XMLNode t : tag.getXMLNodeSubtags())
    {
      edge.set(t, "effect").set(t, "mechanism").set(t, "trust");
    }
    return edge;
  }
  public XgmmlReader add(Collection<PropertyNode> nodes)
  {
    if (nodes!=null)
      for (PropertyNode node : nodes) add(node);
    return this;
  }
  public XgmmlReader add(PropertyNode node)
  {
    mIdNode.put(node.getID(), node);
    mGraph.addVertex(node);
    mGeneNode.put(node.getProperty("gene"), node);

    for (String p : node.getProperties().keySet())
      mNodeIndice.put(p, node.getProperty(p), node.getID());

    return this;
  }
  public XgmmlReader add(PropertyEdge edge)
  {
    if (edge!=null && edge.getID()!=null && !mIdEdge.containsKey(edge.getID()) && edge.getSource()!=null && edge.getTarget()!=null)
    {
      mIdEdge.put(edge.getID(), edge);
      for (String p : edge.getProperties().keySet())
        mEdgeIndice.put(p, edge.getProperty(p), edge.getID());
      try
      {
        for (PropertyNode S : mIdNode.get(edge.getSource()))
          for (PropertyNode T : mIdNode.get(edge.getTarget()))
          {
            PropertyEdge E = edge.clone();
            if (mGraph.addEdge(S, T, E.setTarget(T.getID()).setSource(S.getID())))
              E.setLabel(S.getName() + " --> " + T.getName());
          }
      }
      catch (IllegalArgumentException loops)
      {
        if (loops.getMessage().indexOf("loops")>0) edge_excluded++; else node_excluded++;
        System.out.print(".");
      }
    }
    return this;
  }
  public void receive(XMLTag tag)
  {
    if     (tag.getTagName().equals(XML_NODE))
    {
      Collection<PropertyNode> nodes = newNodes(tag);
      // only add if not already present
      if (nodes!=null && nodes.size()>0)
      {
        for (PropertyNode node : nodes)
        {
          add(node);
//          mIdNode.put(node.getID(), node);
//          mGraph.addVertex(node);
//          for (String p : node.getFeature().getProperties().keySet())
//            mNodeIndice.put(p, node.getFeature().getProperty(p), node.getID());
        }
      }
    }
    else if (tag.getTagName().equals(XML_EDGE))
    {
      add(newEdge(tag));
//      PropertyEdge edge = newEdge(tag);
//      if (edge!=null && edge.getID()!=null && !mIdEdge.containsKey(edge.getID()) && edge.getSource()!=null && edge.getTarget()!=null)
//      {
//        mIdEdge.put(edge.getID(), edge);
//        for (String p : edge.getFeature().getProperties().keySet())
//          mEdgeIndice.put(p, edge.getFeature().getProperty(p), edge.getID());
//        try
//        {
//          for (PropertyNode S : mIdNode.get(edge.getSource()))
//            for (PropertyNode T : mIdNode.get(edge.getTarget()))
//              if (mGraph.addEdge(S, T, edge))
//                edge.setLabel(S.getName() + " --> " + T.getName());
//        }
//        catch (IllegalArgumentException loops)
//        {
//          if (loops.getMessage().indexOf("loops")>0) edge_excluded++; else node_excluded++;
//          System.out.print(".");
//        }
//      }
    }
  }
  public void endDocument()
  {
  }
  public StringBuffer print()
  {
    StringBuffer buf = new StringBuffer();

    buf.append("data imported (edge/node): " + getGraph().edgeSet().size()+"/"+getGraph().vertexSet().size()+"\n");
    buf.append("data excluded (edge/node): " + edge_excluded+"/"+node_excluded+"\n");
    buf.append("\nNode Properties\n");
    for (String p : mNodeIndice.keySet())
    {
      buf.append(p + "("+mNodeIndice.get(p).keySet().size()+"): \t");
      int counts=0;
      for (String v : mNodeIndice.get(p).keySet())
      {
        buf.append(v);
        if (mNodeIndice.get(p,v).size()>1) buf.append("("+mNodeIndice.get(p,v).size()+")");
        buf.append("; ");
        if (++counts>12) { buf.append("..."); break; }
      }
      buf.append("\n");
    }

    buf.append("\nEdge Properties\n");
    for (String p : mEdgeIndice.keySet())
    {
      buf.append(p + "("+mEdgeIndice.get(p).keySet().size()+"): \t");
      int counts=0;
      for (String v : mEdgeIndice.get(p).keySet())
      {
        buf.append(v);
        if (mEdgeIndice.get(p,v).size()>1) buf.append("("+mEdgeIndice.get(p,v).size()+")");
        buf.append("; ");
        if (++counts>12) { buf.append("..."); break; }
      }
      buf.append("\n");
    }

    return buf;
  }
/*
  public XgmmlReader read(String root, String xgmml)
  {
    //XMLUtil.setXMLReaderClass("com.hfg.xml.parser.SaxyParser");
    AmgnXMLHandler  parser = new AmgnXMLHandler();
//    XgmmlReader listener = new XgmmlReader(); // convert it PPM

    edge_excluded=0; node_excluded=0;
    try
    {
      InputStream stream = FileUtil.getFileInputStream(new File(root, xgmml));
      parser.parse(stream, getTargetTags(), this);
      stream.close();
    }
    catch (IOException ie)
    {
      throw new RuntimeException("Unable to open the file: " + xgmml, ie);
    }
//    System.out.println();

    return this;
  }
  public XgmmlReader accumulate(XgmmlReader reader)
  {
    if (reader==null || reader.getGraph()==null) return this;

    node_excluded +=   reader.node_excluded;
    edge_excluded +=   reader.edge_excluded;
//    mIdNode.putAll(    reader.mIdNode);
//    mIdEdge.putAll(    reader.mIdEdge);
//    mGeneNode.putAll(  reader.mGeneNode);
//
//    mNodeIndice.putAll(reader.mNodeIndice);
//    mEdgeIndice.putAll(reader.mEdgeIndice);

    for (PropertyEdge edge : reader.mGraph.edgeSet())
    {
      if (!mIdNode.containsKey(edge.getTarget())) add(reader.mIdNode.get(edge.getTarget()));
      if (!mIdNode.containsKey(edge.getSource())) add(reader.mIdNode.get(edge.getSource()));
      // save the edge
      if (!mIdEdge.containsKey(edge.getID())) add(edge);
    }
    return this;
  }
*/
}
