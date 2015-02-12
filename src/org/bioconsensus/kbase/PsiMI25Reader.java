package org.bioconsensus.kbase;

import grph.properties.StringProperty;
import org.ms2ms.graph.PropertyEdge;
import org.ms2ms.graph.PropertyNode;
import org.ms2ms.utils.Strs;
import org.ms2ms.utils.Tools;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import toools.set.IntSet;

import java.util.*;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   2/3/15
 */
public class PsiMI25Reader extends GraphHandler
{
//  private Map<Long, PropertyNode> actors;
  private PropertyEdge        interaction;
  private PropertyNode        expt;
  private Collection<Integer> participants = new HashSet<>();
  private int                 lastID;
  private Map<Integer, PropertyNode> expts = new HashMap<>();

  public PsiMI25Reader()             { super(); }
  public PsiMI25Reader(String... s)  { super(s); }

  /** Clear the cache vars between the entries
   *
   */
  private void clearEntry()
  {
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
//      nodes=0; edges=0;
//      actors = new HashMap<>();
    }
    else if (elementName.equalsIgnoreCase("interactorList"))
    {
//      System.out.println("Gathering the interactors");
    }
    else if (elementName.equalsIgnoreCase("interactionList"))
    {
//      System.out.println("Gathering the interactions");
    }
    else if (elementName.equalsIgnoreCase("interactor"))
    {
      // add to the graph. it's a new node by definition, assuming we're not re-import the same uniprot
      lastID = G.addVertex();
      G.setNodeLabelProperty(lastID, "id", attributes.getValue("id")).setVerticesLabel(new StringProperty("interactor"));
      if (++nodes%5000==0) System.out.print(".");
    }
    else if (Strs.equals(elementName, "experimentDescription"))
    {
      expt = new PropertyNode().setID(new Long(attributes.getValue("id")));
    }
    else if (Strs.isA(elementName, "shortLabel","interactorRef","experimentRef","attribute","fullName"))
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
    else if (Strs.equals(element,"attribute"))
    {
      if (Strs.equals(attrs.getValue("name"), "dataset") && expt!=null)
      {
        expt.setProperty("datasetAc", attrs.getValue("nameAc"));
        expt.setProperty("dataset", content.toString());
      }
    }
    else if (Strs.equals(element,"experimentDescription") && expt!=null)
    {
      expts.put(expt.getID().intValue(), expt);
    }
    else if (Strs.equals(element,"interactorList"))
    {
//      System.out.println("Total interactors: " + nodes);
    }
    else if (Strs.equals(element,"fullName"))
    {
      if      (matchStack("names", "experimentDescription") && expt!=null)
      {
        expt.setName(content.toString());
      }
    }
    else if (Strs.equals(element,"interactionList"))
    {
//      System.out.println("Total interactions: " + nodes);
    }
    else if (Strs.equals(element, "shortLabel"))
    {
      if (matchStack(1, "names"))
      {
        if      (matchStack(2, "interaction"))
        {
          interaction.setDescription(content.toString());
        }
        else if (matchStack(2, "interactor"))
        {
          G.setNodeLabelProperty(lastID, NAME, content.toString());
        }
        else if (matchStack(2, "interactorType"))
        {
          G.setNodeLabelProperty(lastID, "intType", content.toString());
        }
        else if (matchStack(2, ORGANISM))
        {
          G.setNodeLabelProperty(lastID, ORGANISM, content.toString());
        }
        else if (matchStack(2, "interactionType"))
        {
          interaction.setProperty("Type", content.toString());
        }
        else if (matchStack(2, "experimentDescription") && expt!=null)
        {
          expt.setProperty("exptLabel", content.toString());
        }
        else if (matchStack(2, "hostOrganism") && expt!=null)
        {
          expt.setProperty("exptOrganism", content.toString());
        }
      }
    }
    else if (Strs.equals(element, "interactorRef"))
    {
      IntSet ids = G.getNodeByLabelProperty("id", content.toString());
      if (ids!=null) participants.addAll(ids.toIntegerArrayList());
    }
    else if (Strs.equals(element, "experimentRef"))
    {
      PropertyNode expt = expts.get(new Integer(content.toString()));
      if (interaction!=null && expt!=null)
      {
        interaction.setProperty("exptName", expt.getName());
        interaction.mergeProperty(expt);
      }
    }
    else if (Strs.equals(element, "interaction"))
    {
      // done with this group of interactors
      if (Tools.isSet(participants))
      {
        Set<Integer> hashes = new HashSet<>();
        for (Integer A : participants)
          for (Integer B : participants)
          {
            if (!Tools.equals(A,B) && (!Tools.isSet(hashes) || !hashes.contains(A.hashCode()+B.hashCode())))
            {
              int E = G.addUndirectedSimpleEdge(A, B);
              G.setEdgeLabelProperty(E, interaction);
              hashes.add(A.hashCode() + B.hashCode());
              if (++edges%10000==0) System.out.print(".");
            }
          }
        participants.clear();
      }
    }
    isParsing=false;
  }
}
