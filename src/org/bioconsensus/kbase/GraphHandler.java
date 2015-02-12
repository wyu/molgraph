package org.bioconsensus.kbase;

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
 * Date:   2/3/15
 */
abstract public class GraphHandler extends DefaultHandler
{
  public static final String LABEL    = "_label_";
  public static final String NAME     = "name";
  public static final String ORGANISM = "organism";

//  GraphCache<PropertyNode, PropertyEdge> G;
//  SimpleDirectedWeightedGraph<PropertyNode, PropertyEdge> G;
  GraphCache G;

  // the stack of opening tags
  LinkedList<String> stack = new LinkedList<>();
  StringBuilder content = new StringBuilder();
  boolean isParsing = false, canConnect;
  Attributes attrs;
  PropertyNode _PROTEIN_, _GENE_;

  String[] species;

  long entries=0, nodes=0, edges=0;

  public GraphHandler()                 { super(); }
  public GraphHandler(String... s)
  {
    super(); species = s;
  }

  public void parseDocument(String fname)
  {
    G = new GraphCache();
//    G = new GraphCache(PropertyEdge.class);
//    G = new SimpleDirectedWeightedGraph<>(PropertyEdge.class);

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
    System.out.println(fname + " imported (node/edge): " + nodes + "/" + edges);
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
  public static PsiMI25Reader read(String... fnames)
  {
    PsiMI25Reader interact = new PsiMI25Reader();

    if (Tools.isSet(fnames))
      for (String fname : fnames)
        interact.parseDocument(fname);

    return interact;
  }
}
