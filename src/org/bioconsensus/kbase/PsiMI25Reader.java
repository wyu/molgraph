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
  private Map<String, PropertyNode> expts = new HashMap<>();

  public PsiMI25Reader()             { super(); init(); }
  public PsiMI25Reader(String... s)  { super(s); init(); }

  public void init()
  {
    setContentList("shortLabel","interactorRef","experimentRef","attribute","fullName","alias");
  }
  @Override
  public void startElement(String uri, String localName, String elementName, Attributes attributes) throws SAXException
  {
    super.startElement(uri, localName, elementName, attributes);

    if      (elementName.equalsIgnoreCase("interaction"))
    {
      // add to the graph. it's a new node by definition, assuming we're not re-import the same uniprot
      interaction = new PropertyEdge();
      interaction.setProperty(ID, attributes.getValue("id"));
    }
    else if (matchStack("primaryRef","xref","interactor") && Strs.equals(attributes.getValue("db"), "ensembl"))
    {
      actor.setProperty(ENSEMBLE, attributes.getValue("id"));
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
      actor = new PropertyNode("interactor");
      actor.setProperty(ID, attributes.getValue("id"));
//      if (++G.nodes%5000==0) System.out.print(".");
    }
    else if (Strs.equals(elementName, "experimentDescription"))
    {
      expt = new PropertyNode();
      expt.setProperty(ID, attributes.getValue("id"));
    }
  }
  @Override
  public void endElement(String uri, String localName, String element) throws SAXException
  {
    super.endElement(uri, localName, element);
//
    // if end of book element add to list
//    if (Strs.equals(element, "entry"))
//    {
//      // clear out the record
////      clearEntry();
//    }
    if     (matchElementStack("attribute","attributeList","experimentDescription") &&
            Strs.equals(attrs.getValue("name"), "dataset") && expt!=null)
    {
//      set("datasetAc", expt, attrs, "nameAc");
      set(DATASET, expt, content);
    }
    else if (matchElementStack("alias","names","interactor") && Strs.equals(attrs.getValue("type"), "gene name"))
    {
      set("gene name", actor, content);
    }
    else if (Strs.equals(element,"experimentDescription") && expt!=null)
    {
      expts.put(expt.getProperty(ID), expt);
    }
//    else if (Strs.equals(element,"interactorList"))
//    {
////      System.out.println("Total interactors: " + nodes);
//    }
    else if (matchElementStack("fullName","names", "experimentDescription") && expt!=null)
    {
      set(NAME, expt, content);
    }
//    else if (Strs.equals(element,"interactionList"))
//    {
////      System.out.println("Total interactions: " + nodes);
//    }
    else if (Strs.equals(element,"interactor") && actor!=null && !G.hasNodeLabel("intactID", actor.getProperty(ID)))
    {
      if (Strs.equals(actor.getProperty("actorType"), "protein") &&
          Strs.isSet(actor.getProperty("gene name")))
      {
        actor.rename("gene name", GENE);
      }
      lastID = G.addVertex();
//      G.setNodeLabelProperty(lastID, "intactID", actor.getProperty(ID));
      G.setNodeLabelProperty(lastID, actor);
      if (++G.nodes%5000==0) System.out.print(".");
    }
    else if (Strs.equals(element, "shortLabel"))
    {
      if (matchStack(1, "names"))
      {
        if      (matchStack(2, "interaction"))            interaction.setDescription(content.toString());
        else if (matchStack(2, "interactor"))             set(GENE,        actor, content);
        else if (matchStack(2, "interactorType"))         set(TYPE_ACTOR,  actor, content);
        else if (matchStack(2, ORGANISM))                 set(ORGANISM,    actor, content);
//        else if (matchStack(2, "experimentDescription"))  set(LABEL,       expt,  content);
        else if (matchStack(2, "hostOrganism"))           set(ORGANISM,    expt,  content);
        else if (matchStack(2, "tissue"))                 set(TISSUE, expt, content);
        else if (matchStack(2, "interactionDetectionMethod")) set(ASSAY, expt, content);
        else if (matchStack(2, "interactionType"))        set(TYPE_ACTION, interaction, content);
      }
    }
    else if (matchElementStack("interactorRef","participant","participantList","interaction"))
    {
      IntSet N = G.putNode("intactID", content.toString());
      if (Tools.isSet(N)) participants.addAll(N.toIntegerArrayList());
    }
    else if (Strs.equals(element, "experimentRef"))
    {
      PropertyNode expt = expts.get(content.toString());
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
              if (++G.edges%10000==0) System.out.print(".");
            }
          }
        participants.clear();
      }
    }
    isParsing=false;
  }
}
