package org.bioconsensus.kbase;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Edge;
import org.ms2ms.graph.PropertyEdge;
import org.ms2ms.graph.PropertyNode;
import org.ms2ms.utils.Strs;
import org.ms2ms.utils.Tools;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import java.util.*;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   1/29/15
 */
public class Psi25Handler extends TitanHandler
{
  private Map<Long, TitanVertex>   actors;
  private PropertyEdge              interaction;
  private Collection<Long> participants = new HashSet<>();;
  private Long                      lastID;

  public Psi25Handler()                               { super(); }
  public Psi25Handler(TitanGraph graph)               { super(graph); }
  public Psi25Handler(TitanGraph graph, String... s)  { super(graph, s); }

  /** Clear the cache vars between the entries
   *
   */
  private void clearEntry()
  {
//    protein=null; organism=null; gene=null; primary_gene=null; dataset=null; loc=null; dbref=null; organism=null; interact=null;
//    _PROTEIN_ =null; _GENE_ =null; attrs=null; isParsing=false;
    name_acc.clear();
  }
  protected void prepareIndex()
  {
//    prepareIndices(ACC,NAME,ORGANISM,SUBCELOC,LABEL,DB,TYPE,PROTEIN,GENE,TERM,PATHWAY,MOLTYPE,EVIDENCE,ENTRY,PROTID);
  }

  @Override
  public void startElement(String uri, String localName, String elementName, Attributes attributes) throws SAXException
  {
    stack.add(elementName); attrs = attributes;

    if      (elementName.equalsIgnoreCase("interaction"))
    {
      // add to the graph. it's a new node by definition, assuming we're not re-import the same uniprot
      interaction = new PropertyEdge().setID(new Long(attributes.getValue("id")));
    }
    else if (elementName.equalsIgnoreCase("entrySet"))
    {
      actors = new HashMap<>();
    }
    else if (elementName.equalsIgnoreCase("interactor"))
    {
      // add to the graph. it's a new node by definition, assuming we're not re-import the same uniprot
      lastID = new Long(attributes.getValue("id"));
      actors.put(lastID, titan.addVertexWithLabel("interactor"));
    }
    else if (Strs.isA(elementName, "shortLabel", "interactorRef"))
    {
      isParsing=true; content=new StringBuilder();
    }
  }
  @Override
  public void endElement(String uri, String localName, String element) throws SAXException
  {
    if (!stack.removeLast().equals(element)) throw new RuntimeException("Unmatched element!");

    // if end of book element add to list
    if (Strs.equals(element, "entry"))
    {
      // clear out the record
      clearEntry();
    }
    else if (Strs.equals(element, "shortLabel"))
    {
      if      (matchStack("names", "interaction"))
      {
        interaction.setDescription(content.toString());
      }
      else if (matchStack("names", "interactor"))
      {
        actors.get(lastID).setProperty(NAME, content.toString());
      }
      else if (matchStack("names", "interactorType"))
      {
        actors.get(lastID).setProperty("Type", content.toString());
      }
      else if (matchStack("names", ORGANISM))
      {
        actors.get(lastID).setProperty(ORGANISM, content.toString());
      }
      else if (matchStack("names", "interactionType"))
      {
        interaction.setProperty("Type", content.toString());
      }
    }
    else if (Strs.equals(element, "interactorRef"))
    {
      participants.add(new Long(content.toString()));
    }
    else if (Strs.equals(element, "interaction"))
    {
      // done with this group of interactors
      if (Tools.isSet(participants))
      {
        Set<Integer> hashes = new HashSet<>();
        for (Long A : participants)
          for (Long B : participants)
          {
            if (!Tools.isSet(hashes) || !hashes.contains(A.hashCode()+B.hashCode()))
            {
              Edge E = titan.addEdge(actors.get(A), actors.get(B), interaction.getDescription());
              E.setProperty("Type", interaction.getProperty("Type"));
              hashes.add(A.hashCode()+B.hashCode());
            }
          }
        participants.clear();
      }
    }
    isParsing=false;
  }
}
