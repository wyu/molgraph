package org.bioconsensus.kbase;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.ms2ms.graph.Property;
import org.ms2ms.graph.PropertyNode;
import org.ms2ms.r.Dataframe;
import org.ms2ms.utils.IOs;
import org.ms2ms.utils.Strs;
import org.ms2ms.utils.Tools;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
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
  public static final String LABEL    = "_label_";
  public static final String NAME     = "name";
  public static final String DESC     = "description";
  public static final String ORGANISM = "organism";
  public static final String GENE     = "gene";
  public static final String ENSEMBLE = "ENSG";
  public static final String DRUGID   = "drugBankID";
  public static final String RSID     = "rsid";
  public static final String ID       = "id";
  public static final String DISEASE  = "disease";
  public static final String CONTEXT  = "context";
  public static final String TRAIT    = "trait";
  public static final String TISSUE   = "tissue";
  public static final String ASSAY    = "assay";
  public static final String TYPE_ACTOR   = "actorType";
  public static final String TYPE_ACTION  = "actionType";
  public static final String DATASET  = "dataset";

  PropertyGraph G=null;

  // the stack of opening tags
  LinkedList<String> stack = new LinkedList<>();
  StringBuilder content = new StringBuilder();
  boolean isParsing = false, canConnect;
  Attributes attrs; String ele;

  String[] species, contentList;

//  long nodes=0, edges=0;

  public GraphHandler()                 { super(); }
  public GraphHandler(PropertyGraph g)     { super(); G=g; }
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
//    System.out.println(" (node/edge): " + nodes + "/" + edges);
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
  public static PsiMI25Reader readRecursive(String... folders)
  {
    if (Tools.isSet(folders))
    {
      List<String> files = new ArrayList<>();
      for (String flder : folders)
      {
        files.addAll(IOs.listFiles(flder, new WildcardFileFilter("*.xml")));
      }
      if (Tools.isSet(files)) return read(files.toArray(new String[]{}));
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
  public static PropertyGraph ESGN2Gene(PropertyGraph graph, Dataframe mapping)
  {
    Map es2gene = mapping!=null?mapping.toMap("Ensembl Gene ID", "Approved Symbol"):null;
    if (graph.node_label_val.column(ENSEMBLE)!=null)
    {
      for (Integer row : graph.node_label_val.column(ENSEMBLE).keySet())
      {
        Object g = es2gene.get(graph.node_label_val.get(row, ENSEMBLE));
        if (g!=null)
          graph.node_label_val.put(row, GENE, g.toString().toUpperCase());
      }
      graph.node_label_val.column(ENSEMBLE).clear();
    }
    if (graph.node_label_val.column("gene name")!=null)
      for (Integer row : graph.node_label_val.column("gene name").keySet())
      {
        if (Tools.equals(graph.node_label_val.get(row, "gene name"), graph.node_label_val.get(row, GENE)))
          graph.node_label_val.remove(row, "gene name");
      }

/*
    if (graph.label_val_node.row(ENSEMBLE)!=null)
    {
      for (String row : graph.label_val_node.row(ENSEMBLE).keySet())
      {
        Object g = es2gene.get(graph.node_label_val.get(row, ENSEMBLE));
        if (g!=null)
          graph.node_label_val.put(row, GENE, g.toString());
      }
      graph.node_label_val.column(ENSEMBLE).clear();
    }
*/
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
