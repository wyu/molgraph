package org.bioconsensus.kbase;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.ms2ms.graph.Graphs;
import org.ms2ms.graph.Property;
import org.ms2ms.graph.PropertyNode;
import org.ms2ms.r.Dataframe;
import org.ms2ms.utils.IOs;
import org.ms2ms.utils.Strs;
import org.ms2ms.utils.TabFile;
import org.ms2ms.utils.Tools;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import toools.set.IntHashSet;
import toools.set.IntSet;

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
 * Date:   2/3/15
 */
abstract public class GraphHandler extends DefaultHandler
{
//  public static final String LABEL    = "_label_";
  public static final String NAME     = "name";
  public static final String DESC     = "description";
  public static final String ORGANISM = "organism";
  public static final String ENSEMBLE = "ENSG";
  public static final String DRUGID   = "drugBankID";
  public static final String RSID     = "rsid";
//  public static final String ID       = "ID";
  public static final String CONTEXT  = "context";
  public static final String TYPE_ACTOR   = "actorType";
  public static final String TYPE_ACTION  = "actionType";

  public static final String DATASET  = "dataset";

  PropertyGraph G=null;

  // the stack of opening tags
  LinkedList<String> stack = new LinkedList<>();
  StringBuilder content = new StringBuilder();
  boolean isParsing = false, canConnect;
  Attributes attrs; String ele;
  Collection<Integer> curated;

  String[] species, contentList;

  public GraphHandler()                 { super(); }
  public GraphHandler(PropertyGraph g)  { super(); G=g; }
  public GraphHandler(String... s)
  {
    super(); species = s;
  }

  public void parseDocument(String fname)
  {
    if (G==null) G=new PropertyGraph();

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
  }

  public void clearCuration() { curated=null; }

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

  /** assign the 'tag' attribute from 'attr' to 'name' property of node 'p' */
  protected Property set(String name, Property p, Attributes attr, String tag)
  {
    if (p!=null && attr!=null && attr.getValue(tag)!=null && Strs.isSet(name))
      p.setProperty(name, attrs.getValue(tag));

    return p;
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
  @Override
  public void characters(char[] ac, int i, int j) throws SAXException
  {
    if (isParsing && content!=null) content.append(new String(ac, i, j));
  }
  protected void setContentList(String... s) { contentList=s; }
  public boolean matchElementStack(String... tags)
  {
    return Tools.isSet(tags) && tags.length>0 ?
          (Strs.equals(ele, tags[0]) && matchStack(Arrays.copyOfRange(tags, 1, tags.length))) : false;
  }

  public boolean matchStack(String... tags)
  {
    if (Tools.isSet(tags) && Tools.isSet(stack))
    {
      for (int i=0; i<tags.length; i++)
        if (!Strs.equals(tags[i], stack.get(stack.size() - i - 1))) return false;

      return true;
    }
    return false;
  }
  protected boolean matchStack(int fromLast, String s)
  {
    return Strs.equals(Tools.fromLast(stack, fromLast), s);
  }
  /** read a tab-delimited file that contains the private set of nodes
   *
   type    label   abbr    ID
   disease ulcerative colitis      UC      EFO_0000729
   *
   * @return the type and labels of the nodes
   */
  public Collection<Integer> curation(String file)
  {
    try
    {
      if (G==null) G=new PropertyGraph();

      TabFile d = new TabFile(file, TabFile.tabb);
      // come up with the headers. need to preserve the order of the keys
      LinkedHashMap<String, String> props = new LinkedHashMap<>();
      props.put("ID",    Graphs.UID);
      props.put("type",  Graphs.TYPE);
      props.put("label", Graphs.TITLE);

      for (String col : d.getHeaders())
        if (!Strs.isA(col, "type", "label", "ID"))
          props.put(col, Strs.equals(col,"abbr")?Graphs.TITLE:col);

      curated = new HashSet<>();
      while (d.hasNext())
      {
        Tools.add(curated, G.putNode(Tools.toColsHdr(d.nextRow(), props)));
        //curated.put(d.get("type"), d.get("label"));
      }
      return curated;
    }
    catch (IOException e) {}

    return null;
  }

  public static PsiMI25Reader readRecursive(String... folders)
  {
    if (Tools.isSet(folders))
    {
      PsiMI25Reader interact = new PsiMI25Reader();

      Multimap<String, String> dir_file = HashMultimap.create();
      for (String flder : folders)
      {
        //dir_file.putAll(flder, IOs.listFiles(flder, new WildcardFileFilter("*.xml")));
        dir_file.putAll(IOs.listDirFiles(flder, new WildcardFileFilter("*.xml")));
      }
      if (Tools.isSet(dir_file))
      {
        int counts=0;
        for (String fldr : dir_file.keySet())
        {
          System.out.println("Reading PSI-MI contents from the folder: " + fldr);

          List<String> diseases = IOs.listFiles(fldr, new WildcardFileFilter("nodes*"));
          // setup the disease node
          if (Tools.isSet(diseases)) interact.curation(diseases.get(0));
          for (String fname : dir_file.get(fldr))
          {
            if (++counts%25==0) System.out.print(".");
            interact.parseDocument(fname);
          }
          System.out.println();
          interact.clearCuration();
        }
      }
      return interact;
    }

    return null;
  }
  public static PsiMI25Reader read(String... fnames)
  {
    PsiMI25Reader interact = new PsiMI25Reader();

    if (Tools.isSet(fnames))
      for (String fname : fnames)
      {
        System.out.println("Reading PSI-MI contents from " + fname);
        interact.parseDocument(fname);
      }

    return interact;
  }
  /** An unit of kbase builder where the contents from 'fnames' with the 'root' are read and saved to 'out'
   *
   * @param root is the top level folder where the contents reside
   * @param out is a binary file where the graph data will be stored
   * @param fnames are the file or folder names where the source data are located.
   * @return we should return a stat object that summarize the data inventory
   */
  public static PsiMI25Reader build(String out, String root, String... fnames)
  {
    PsiMI25Reader interact = new PsiMI25Reader();

    if (Tools.isSet(fnames))
      for (String fname : fnames)
        // expand the file list if this is a folder
        for (String fn : IOs.listFiles(root+fname, new WildcardFileFilter("*.xml")))
        {
          System.out.println("Reading PSI-MI contents from " + fn);
          interact.parseDocument(fn);
        }

    if (Strs.isSet(out))
    {
      System.out.println("Writing the graph contents to " + out);
      interact.G.write(out);
    }
    return interact;
  }
  public static PropertyGraph fixup(PropertyGraph graph, Dataframe mapping)
  {
    Map es2gene = mapping!=null?mapping.toMap("Ensembl Gene ID", "Approved Symbol"):null;
    if (graph.node_label_val.column(ENSEMBLE)!=null)
    {
      for (Integer row : graph.node_label_val.column(ENSEMBLE).keySet())
      {
        Object g = es2gene.get(graph.node_label_val.get(row, ENSEMBLE));
        if (g!=null)
          graph.node_label_val.put(row, Graphs.GENE, g.toString().toUpperCase());
      }
      graph.node_label_val.column(ENSEMBLE).clear();
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
  // update the property if the element was in the pre-defined list
  public Property set(String label, Property p, StringBuilder content)
  {
    if (p==null) return p;
    if (!Tools.isA(ele, contentList)) throw new RuntimeException("element " + ele + " not specified in the pre-defined content list");

    p.setProperty(label, content);
    return p;
  }
  @Override
  public void startElement(String uri, String localName, String elementName, Attributes attributes) throws SAXException
  {
    stack.add(elementName); attrs=attributes; ele=elementName;
    if (Strs.isA(elementName, contentList))
    {
      isParsing=true; content=new StringBuilder();
    }
  }
  @Override
  public void endElement(String uri, String localName, String element) throws SAXException
  {
    ele=element;
    if (!stack.removeLast().equals(ele)) throw new RuntimeException("Unmatched element!");
  }
}
