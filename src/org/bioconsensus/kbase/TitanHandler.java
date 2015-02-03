package org.bioconsensus.kbase;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.ms2ms.graph.Property;
import org.ms2ms.graph.PropertyNode;
import org.ms2ms.utils.Strs;
import org.ms2ms.utils.Tools;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.util.*;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   1/31/15
 */
abstract public class TitanHandler  extends DefaultHandler
{
  public static final String LABEL    = "_label_";
  public static final String NAME     = "name";
  public static final String ORGANISM = "organism";
  TitanGraph G;
  TitanTransaction titan;
//  TitanManagement mgmt;

  Table<String, String, TitanVertex> label_val_node = HashBasedTable.create();
  // the stack of opening tags
  LinkedList<String> stack = new LinkedList<>();
  StringBuilder content = new StringBuilder();
  boolean isParsing = false;
  Attributes attrs;
  Map<String, PropertyKey> prop_key = new HashMap<>();
  Map<String, VertexLabel> label_vertex = new HashMap<>();
  Map<String, EdgeLabel> label_edge = new HashMap<>();
  Multimap<String, PropertyNode> name_acc = HashMultimap.create();
  TitanVertex _PROTEIN_, _GENE_;

  String[] species;

  long entries=0, nodes=0, edges=0;

  public TitanHandler()                 { super(); }
  public TitanHandler(TitanGraph graph) { super(); G = graph; }
  public TitanHandler(TitanGraph graph, String... s)
  {
    super(); G = graph;
    species = s;
  }
  abstract protected void prepareIndex();

  public void parseDocument(String fname)
  {
    prepareIndex();

    TransactionBuilder tx = G.buildTransaction();
//    tx.enableBatchLoading();
    titan = tx.start();

    // parse
    SAXParserFactory factory = SAXParserFactory.newInstance();
    try {
      SAXParser parser = factory.newSAXParser();
      parser.parse(fname, this);
    } catch (ParserConfigurationException e) {
      System.out.println("ParserConfig error");
    } catch (SAXException e) {
      System.out.println("SAXException : xml not well formed");
    } catch (IOException e) {
      System.out.println("IO error");
    }
    titan.commit();
    System.out.println(fname + " imported (node/edge): " + nodes + "/" + edges);
  }

  protected void prepareCompositeIndex(TitanManagement m, String key)
  {
    makeStrKeys(m, key);
    m.buildIndex("by_" + key, Vertex.class).addKey(getStrKey(key) ).buildCompositeIndex();
  }
  protected void makeStrKeys(TitanManagement m, String...  keys)
  {
    if (Tools.isSet(keys))
      for (String key : keys)
        prop_key.put(key, m.makePropertyKey(key).dataType(String.class).make());
  }
  protected void makeStrKeys(String...  keys)
  {
    if (Tools.isSet(keys))
      for (String key : keys)
        prop_key.put(key, titan.makePropertyKey(key).dataType(String.class).make());
  }
  protected PropertyKey getStrKey(String key)
  {
    if (prop_key.get(key)==null) makeStrKeys(key);
    return prop_key.get(key);
  }
  protected VertexLabel getVertexLabel(String key)
  {
    if (label_vertex.get(key)==null)
    {
      label_vertex.put(key, titan.makeVertexLabel(key).make());
    }
    return label_vertex.get(key);
  }

  /** titan.getOrCreateEdgeLabel() still yield Exception.
   *
   * @param key
   * @return
   */
  protected EdgeLabel getEdgeLabel(String key)
  {
    if (label_edge.get(key)==null)
    {
      label_edge.put(key, titan.makeEdgeLabel(key).make());
    }
    return label_edge.get(key);
  }
  protected PropertyNode set(PropertyNode p, String tag, StringBuilder s)
  {
    return set(p, tag, s.toString());
  }
  protected PropertyNode set(PropertyNode p, String tag, String s)
  {
    if (p!=null && s!=null)
      p.setProperty(tag, s.toString());
    return p;
  }
  protected Property set(Property p, Attributes attr, String... tags)
  {
    if (p!=null && attr!=null)
      for (String tag : tags)
        if (attr.getValue(tag)!=null)
          p.setProperty(tag, attr.getValue(tag));
    return p;
  }
  protected TitanVertex getVertex(PropertyNode node, String... tags)
  {
    String label=node.getProperty("_label_"), val=(label!=null?node.getProperty(label):null);
    if (label==null || val==null) return null;

    return getVertex(label, val, node, tags);
  }
  protected TitanVertex getVertex(String label, String val, PropertyNode node, String... tags)
  {
    if (titan!=null && label_val_node.get(label, val)==null)
    {
      TitanVertex v = titan.addVertexWithLabel(getVertexLabel(label));
      if (val!=null)
      {
//        v.setProperty(getStrKey(label), val);
        v.setProperty(label, val);
        label_val_node.put(label, val, v);
      }
      // copy the properties
      Collection<String> props = Tools.isSet(tags) ? Arrays.asList(tags) : node.getProperties().keySet();
      for (String tag : props)
//        if (!Strs.equals(tag, "_label_"))
//        v.setProperty(getStrKey(tag), node.getProperty(tag));
        v.setProperty(tag, node.getProperty(tag));

      nodes++;
    }
    return label_val_node.get(label, val);
  }
  protected static PropertyNode newNode(String label, String val) { return newNode(label, val, null); }
  protected static PropertyNode newNode(String label, String val, Attributes attrs, String... tags)
  {
    PropertyNode node = new PropertyNode();
    if (label!=null) node.setProperty("_label_", label);
    if (val  !=null) node.setProperty(label,   val);
    if (attrs!=null && Tools.isSet(tags))
      for (String tag : tags)
        node.setProperty(tag, attrs.getValue(tag));

    return node;
  }
  protected Edge addEdge(TitanVertex v1, TitanVertex v2, String label)
  {
    Edge e=null;
    if (titan!=null && v1!=null && v2!=null) { e=titan.addEdge(v1, v2, getEdgeLabel(label)); edges++; }
    return e;
  }
  protected void prepareIndices(String... keys)
  {
    // https://groups.google.com/forum/#!topic/aureliusgraphs/lGA3Ye4RI5E
    // transactional scope difference between management and graph
    if (Tools.isSet(keys))
    {
      TitanManagement m = G.getManagementSystem();

      for (String key : keys)
        prepareCompositeIndex(m, key);
//      makeStrKeys(m, ACC,NAME,ORGANISM,SUBCELOC,LABEL,DB,TYPE,PROTEIN,GENE,TERM,PATHWAY,MOLTYPE,EVIDENCE,ENTRY,PROTID);
//      m.buildIndex("byAcc",  Vertex.class).addKey(getStrKey(ACC) ).buildCompositeIndex();
//      m.buildIndex("byName", Vertex.class).addKey(getStrKey(NAME)).buildCompositeIndex();
      m.commit();
    }
  }
  @Override
  public void characters(char[] ac, int i, int j) throws SAXException
  {
    if (isParsing && content!=null) content.append(new String(ac, i, j));
  }
  public boolean matchStack(String... tags)
  {
    if (Tools.isSet(tags) && Tools.isSet(stack))
    {
      for (int i=0; i<tags.length; i++)
        if (!Strs.equals(tags[i], stack.get(stack.size()-i-1))) return false;

      return true;
    }
    return false;
  }
}
