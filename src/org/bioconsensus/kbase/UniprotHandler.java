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
 * Date:   12/11/14
 */
public class UniprotHandler extends DefaultHandler
{
  public static final String ORGANISM = "organism";
  public static final String ACC      = "accession";
  public static final String LABEL    = "_label_";
  public static final String DB       = "db";
  public static final String TERM     = "term";
  public static final String PATHWAY  = "pathway name";
  public static final String MOLTYPE  = "molecule type";
  public static final String EVIDENCE = "evidence";
  public static final String ENTRY    = "entry name";
  public static final String PROTID   = "protein sequence ID";
  public static final String NAME     = "name";
  public static final String TYPE     = "type";
  public static final String SUBCELOC = "subcellularLocation";
  public static final String PROTEIN  = "protein";
  public static final String GENE     = "gene";

  TitanGraph G;
  TitanTransaction titan;
//  TitanManagement mgmt;

  Table<String, String, TitanVertex> label_val_node = HashBasedTable.create();
  PropertyNode protein, gene, primary_gene, dataset, loc, dbref, organism, interact;
  Multimap<String, PropertyNode> name_acc = HashMultimap.create();
  TitanVertex _PROTEIN_, _GENE_;
  // the stack of opening tags
  LinkedList<String> stack = new LinkedList<>();
  StringBuilder content = new StringBuilder();
  String[] species;
  boolean isParsing = false;
  Attributes attrs;
  Map<String, PropertyKey> prop_key = new HashMap<>();
  Map<String, VertexLabel> label_vertex = new HashMap<>();
  Map<String, EdgeLabel> label_edge = new HashMap<>();

  long entries=0, nodes=0, edges=0;

  public UniprotHandler()                 { super(); }
  public UniprotHandler(TitanGraph graph) { super(); G = graph; }
  public UniprotHandler(TitanGraph graph, String... s)
  {
    super(); G = graph;
    species = s;
  }
  public void parseDocument(String fname)
  {
    prepareIndices(ACC,NAME,ORGANISM,SUBCELOC,LABEL,DB,TYPE,PROTEIN,GENE,TERM,PATHWAY,MOLTYPE,EVIDENCE,ENTRY,PROTID);

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

  /** Clear the cache vars between the entries
   *
   */
  private void clearEntry()
  {
    protein=null; organism=null; gene=null; primary_gene=null; dataset=null; loc=null; dbref=null; organism=null; interact=null;
    _PROTEIN_ =null; _GENE_ =null; attrs=null; isParsing=false;
    name_acc.clear();
  }
  private void prepareIndices(String... keys)
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
  private void prepareCompositeIndex(TitanManagement m, String key)
  {
    makeStrKeys(m, key);
    m.buildIndex("by_" + key, Vertex.class).addKey(getStrKey(key) ).buildCompositeIndex();
  }
  private void makeStrKeys(TitanManagement m, String...  keys)
  {
    if (Tools.isSet(keys))
      for (String key : keys)
        prop_key.put(key, m.makePropertyKey(key).dataType(String.class).make());
  }
  private void makeStrKeys(String...  keys)
  {
    if (Tools.isSet(keys))
      for (String key : keys)
        prop_key.put(key, titan.makePropertyKey(key).dataType(String.class).make());
  }
  private PropertyKey getStrKey(String key)
  {
    if (prop_key.get(key)==null) makeStrKeys(key);
    return prop_key.get(key);
  }
  private VertexLabel getVertexLabel(String key)
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
  private EdgeLabel getEdgeLabel(String key)
  {
    if (label_edge.get(key)==null)
    {
      label_edge.put(key, titan.makeEdgeLabel(key).make());
    }
    return label_edge.get(key);
  }
  private PropertyNode set(PropertyNode p, String tag, StringBuilder s)
  {
    return set(p, tag, s.toString());
  }
  private PropertyNode set(PropertyNode p, String tag, String s)
  {
    if (p!=null && s!=null)
          p.setProperty(tag, s.toString());
    return p;
  }
  private Property set(Property p, Attributes attr, String... tags)
  {
    if (p!=null && attr!=null)
      for (String tag : tags)
        if (attr.getValue(tag)!=null)
          p.setProperty(tag, attr.getValue(tag));
    return p;
  }
//  private TitanVertex set(TitanVertex p, Attributes attr, String... tags)
//  {
//    if (p!=null && attr!=null)
//      for (String tag : tags)
//        if (attr.getValue(tag)!=null)
//          p.setProperty(tag, attr.getValue(tag));
//    return p;
//  }
//  private Collection<TitanVertex> getVertices(Attributes attrs, String label)
//  {
//    if (label_val_node.get(label, attrs.getValue(label))==null)
//    {
//      TitanVertex v = titan.getVertexLabel(label);
//    }
//    return label_val_node.get(label, attrs.getValue(label));
//  }
//  /** grab the vertex from the cache. create it for the graph if not there already. e.g.
//   *
//   *  accession = ("accession", "PQ12345");
//   *  gene1  = ("gene","HLA-B")
//   *  gene1a = ("gene","HLAB")
//   *
//   * @param label
//   * @param val
//   * @return a vertex from the cache
//   */
//  private TitanVertex getVertex(String label, StringBuilder val)
//  {
//    return getVertex(label, val.toString());
//  }
  private TitanVertex getVertex(PropertyNode node, String... tags)
  {
    String label=node.getProperty("_label_"), val=(label!=null?node.getProperty(label):null);
    if (label==null || val==null) return null;

    return getVertex(label, val, node, tags);
  }
  private TitanVertex getVertex(String label, String val, PropertyNode node, String... tags)
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
//  private TitanVertex getVertex(String label, String val)
//  {
//    if (titan!=null)
//    {
//      TitanVertex v = titan.addVertexWithLabel(getVertexLabel(label));
//      if (val!=null)
//      {
//        v.setProperty(getStrKey(label), val);
//        if (label_val_node.get(label, val)==null) label_val_node.put(label, val, v);
//      }
//      nodes++;
//    }
//    return label_val_node.get(label, val);
//  }
//  private TitanVertex getVertex(String label)
//  {
//    TitanVertex n = null;
//    if (titan!=null)
//    {
//      n = titan.addVertexWithLabel(label); nodes++;
//    }
//    return n;
//  }
//  private TitanVertex getVertex(StringBuilder label, Attributes attrs)
//  {
//    return getVertex(label.toString(), attrs.getValue(label.toString()));
//  }
//  private TitanVertex getVertex(String label, Attributes attrs)
//  {
//    return getVertex(label, attrs.getValue(label));
//  }
//  private TitanVertex getVertex(StringBuilder label, Attributes attrs, StringBuilder tag)
//  {
//    return getVertex(label.toString(), attrs.getValue(tag.toString()));
//  }
//  private TitanVertex getVertex(String label, Attributes attrs, String tag)
//  {
//    return getVertex(label, attrs.getValue(tag));
//  }
//  private static PropertyNode newNode(String label) { return newNode(label, null, null); }
  private static PropertyNode newNode(String label, String val) { return newNode(label, val, null); }
  private static PropertyNode newNode(String label, String val, Attributes attrs, String... tags)
  {
    PropertyNode node = new PropertyNode();
    if (label!=null) node.setProperty("_label_", label);
    if (val  !=null) node.setProperty(label,   val);
    if (attrs!=null && Tools.isSet(tags))
      for (String tag : tags)
        node.setProperty(tag, attrs.getValue(tag));

    return node;
  }
  private Edge addEdge(TitanVertex v1, TitanVertex v2, String label)
  {
    Edge e=null;
    if (titan!=null && v1!=null && v2!=null) { e=titan.addEdge(v1, v2, getEdgeLabel(label)); edges++; }
    return e;
  }
  @Override
  public void startElement(String uri, String localName, String elementName, Attributes attributes) throws SAXException
  {
    stack.add(elementName); attrs = attributes;

    if      (elementName.equalsIgnoreCase("entry"))
    {
      // add to the graph. it's a new node by definition, assuming we're not re-import the same uniprot
      protein = newNode(PROTEIN, null, null);
      set(protein, "db", attributes.getValue("dataset"));
      dataset = newNode("db", attributes.getValue("dataset"));
//      if (entries%10000==0) System.out.println(entries+"$");
//      if (label_val_node.get("dataset", attributes.getValue("dataset"))!=null) dataset=titan.getVertexLabel("dataset");
    }
    else if (elementName.equalsIgnoreCase("dbReference"))
    {
      if (organism!=null && Strs.equals(Tools.fromLast(stack, 2), "organism") &&
          Strs.equals(attributes.getValue("type"), "NCBI Taxonomy"))
      {
        set(organism, attributes, "id"); organism.rename("id", "taxon");
      }
      if (Strs.equals(Tools.fromLast(stack, 2), "entry"))
      {
        dbref = newNode("dbref", attributes.getValue("id"));
        dbref.setProperty("db",  attributes.getValue("type"));
      }
    }
    else if (elementName.equalsIgnoreCase("property"))
    {
      if (Strs.equals(Tools.fromLast(stack, 2), "dbReference"))
      {
        dbref.setProperty(attributes.getValue("type"), attributes.getValue("value"));
      }
    }
    else if (elementName.equalsIgnoreCase("interactant"))
    {
      interact = newNode("interact", attributes.getValue("intactId"));
    }
    else if (Strs.isA(elementName, "fullName",ACC,"location","proteinExistence","name"))
    {
      isParsing=true; content=new StringBuilder();
    }
  }
  @Override
  public void endElement(String uri, String localName, String element) throws SAXException
  {
    if (!stack.removeLast().equals(element)) throw new RuntimeException("Unmatched element!");

    // if end of book element add to list
    if (element.equals("entry"))
    {
      // TODO deposit the protein and
      if (protein!=null && Strs.isA(protein.getProperty(ORGANISM), species))
      {
        if (++entries%1000==0) System.out.print(entries+".");

        // map the properties
        protein.rename(NAME,PROTEIN).rename("fullName", NAME);
        TitanVertex PROTEIN = getVertex(protein);
        if (PROTEIN!=null)
        {
          if (primary_gene!=null)
          {
            TitanVertex GENE=getVertex(primary_gene.getProperty("_label_"), primary_gene.getProperty("name"), primary_gene);
            addEdge(PROTEIN, GENE, "protein-gene");
            for (PropertyNode n : name_acc.get("geneid"))
              addEdge(getVertex(n), GENE, "id-gene");
          }
          // hookup the other accessions
          if (name_acc.get(ACC)!=null)
            for (PropertyNode n : name_acc.get(ACC))
              addEdge(getVertex(n), PROTEIN, "id-protein");
          // setup the dbrefs
          if (name_acc.get("dbref")!=null)
            for (PropertyNode n : name_acc.get("dbref"))
            {
              n.rename(DB, DB).rename("dbref", ACC);
              if (n.hasProperty(PROTID))
              {
                PropertyNode n1 = newNode("dbref", null,null);
                n1.setProperty(DB, n.getProperty("db"));
                n1.setProperty(ACC,n.getProperty(PROTID));
                addEdge(getVertex(n1.getProperty(LABEL), n1.getProperty(ACC), n1), PROTEIN, "dbref-protein");
              }
              addEdge(getVertex(n.getProperty(LABEL), n.getProperty(ACC), n), PROTEIN, "dbref-protein");
            }
        }
      }

      // clear out the record
      clearEntry();
    }
//    else if (Strs.equals(element, "id") && Strs.isSet(content))
//    {
//      if (Strs.equals(stack.getLast(), "interactant") && interact!=null)
//      {
//        set(interact, "id", content);
//      }
//    }
//    else if (Strs.equals(element, "label") && Strs.isSet(content))
//    {
//      if (Strs.equals(stack.getLast(), "interactant") && interact!=null)
//      {
//        set(interact, "LABEL", content);
//      }
//    }
    else if (Strs.equals(element, "proteinExistence") && Strs.isSet(content))
    {
      set(protein, "existence", content);
    }
    else if (Strs.equals(element,"location") &&
             Strs.equals(stack.getLast(), SUBCELOC))
    {
      set(protein, SUBCELOC, content);
//      addEdge(loc, protein, "loc-protein");
    }
    else if (Strs.equals(element,ACC))
    {
      // setup the accession nodes that point up to the protein node
      PropertyNode n = newNode(ACC, content.toString());
      n.setProperty(DB, dataset.getProperty(DB));
//      n.rename("accession", "id");
      name_acc.put(ACC, n);
    }
    else if (Strs.equals(element, "fullName") &&
             Strs.equals(stack.getLast(), "recommendedName") && protein!=null)
    {
      set(protein, "fullName", content);
    }
    else if (element.equals("dbReference"))
    {
      if (dbref!=null) { name_acc.put("dbref", dbref); dbref=null; }
    }
    else if (element.equals(ORGANISM) && organism!=null && organism.getProperty("scientific")!=null)
    {
        protein.setProperty(ORGANISM, organism.getProperty("scientific"));
    }
    else if (Strs.equals(element, NAME))
    {
      if (Strs.equals(stack.getLast(), GENE))
      {
        gene = newNode("geneid", content.toString(), attrs, TYPE);
        gene.rename(   "geneid", ACC);
        name_acc.put(  "geneid", gene);
        // the primary gene which is usually defined by organization such as HUGO or MGD
        // primary, synonym, ORF
        if (primary_gene==null && Strs.equals(attrs.getValue(TYPE), "primary"))
        {
          primary_gene = gene.clone();
          primary_gene.setProperty(LABEL, GENE);
          primary_gene.getProperties().remove(TYPE);
          primary_gene.rename(ACC,NAME);
        }
      }
      else if (Strs.equals(stack.getLast(), "entry"))
      {
        set(protein, NAME, content);
      }
      else if (Strs.equals(stack.getLast(), ORGANISM))
      {
        if (organism==null) organism = new PropertyNode();
        set(organism, attrs.getValue(TYPE), content);
      }
    }
    isParsing=false;
  }
  @Override
  public void characters(char[] ac, int i, int j) throws SAXException
  {
    if (isParsing && content!=null) content.append(new String(ac, i, j));
  }
}