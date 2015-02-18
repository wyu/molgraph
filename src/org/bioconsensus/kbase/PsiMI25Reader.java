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
  public static String INTACT_ID = "intactID";

//  private Map<Long, PropertyNode> actors;
  private PropertyEdge        interaction;
  private PropertyNode        expt, actor;
  private Collection<Integer> participants = new HashSet<>();
  private int                 lastID;
  private Map<Integer, PropertyNode> expts = new HashMap<>();

  public PsiMI25Reader()             { super(); init(); }
  public PsiMI25Reader(String... s)  { super(s); init(); }

  public void init()
  {
    setContentList("shortLabel","interactorRef","experimentRef","attribute","fullName");
  }
  @Override
  public void startElement(String uri, String localName, String elementName, Attributes attributes) throws SAXException
  {
    super.startElement(uri, localName, elementName, attributes);

    if      (elementName.equalsIgnoreCase("interaction"))
    {
      // add to the graph. it's a new node by definition, assuming we're not re-import the same uniprot
      interaction = new PropertyEdge().setID(new Long(attributes.getValue("id")));
    }
//    else if (elementName.equalsIgnoreCase("entrySet"))
//    {
//    }
//    else if (elementName.equalsIgnoreCase("interactorList"))
//    {
////      System.out.println("Gathering the interactors");
//    }
//    else if (elementName.equalsIgnoreCase("interactionList"))
//    {
////      System.out.println("Gathering the interactions");
//    }
    else if (elementName.equalsIgnoreCase("interactor"))
    {
      actor = new PropertyNode("interactor").setID(new Long(attributes.getValue("id")));
      if (++nodes%5000==0) System.out.print(".");
    }
    else if (Strs.equals(elementName, "experimentDescription"))
    {
      expt = new PropertyNode().setID(new Long(attributes.getValue("id")));
    }
  }
  @Override
  public void endElement(String uri, String localName, String element) throws SAXException
  {
    super.endElement(uri, localName, element);
//
    // if end of book element add to list
    if (Strs.equals(element, "entry"))
    {
      // clear out the record
//      clearEntry();
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
    else if (Strs.equals(element,"interactor") && actor!=null)
    {
      if (!G.hasNodeLabel("intactID", attrs.getValue("id")))
      {
        lastID = G.addVertex();
        G.setNodeLabelProperty(lastID, "intactID", attrs.getValue("id")).setVerticesLabel(new StringProperty("interactor"));
      }
      if (++nodes%5000==0) System.out.print(".");
    }
    else if (Strs.equals(element, "shortLabel"))
    {
      if (matchStack(1, "names"))
      {
        if      (matchStack(2, "interaction"))
        {
          interaction.setDescription(content.toString());
        }
        else if (matchStack(2, "interactor"))             set(NAME, actor, content);
        else if (matchStack(2, "interactorType"))         set("intType", actor, content);
        else if (matchStack(2, ORGANISM))                 set(ORGANISM, actor, content);
        else if (matchStack(2, "interactionType"))        set("Type", interaction, content);
        else if (matchStack(2, "experimentDescription"))  set("exptLabel", expt, content);
        else if (matchStack(2, "hostOrganism"))           set("exptOrganism", expt, content);
      }
    }
    else if (matchElementStack("interactorRef","participant","participantList","interaction"))
    {
      IntSet N = G.putNode("intactID", content.toString());
      if (Tools.isSet(N)) participants.addAll(N.toIntegerArrayList());
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
