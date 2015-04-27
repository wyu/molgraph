package org.bioconsensus.kbase;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.ms2ms.graph.Graphs;
import org.ms2ms.graph.PropertyEdge;
import org.ms2ms.graph.PropertyNode;
import org.ms2ms.r.Dataframe;
import org.ms2ms.utils.IOs;
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
  public static Dataframe mapping;
  public static Map       es2gene;

  private PropertyEdge        interaction;
  private PropertyNode        expt, actor;
  private Collection<Integer> participants = new HashSet<>();
  private int                 lastID;
  private Map<String, PropertyNode> expts = new HashMap<>();

  public PsiMI25Reader()                { super(); init(); }
  public PsiMI25Reader(PropertyGraph g) { super(g); init(); }
  public PsiMI25Reader(String... s)     { super(s); init(); }

  public PsiMI25Reader readRecursive(String... folders)
  {
    if (Tools.isSet(folders))
    {
      Multimap<String, String> dir_file = HashMultimap.create();
      for (String flder : folders)
      {
        dir_file.putAll(IOs.listDirFiles(flder, new WildcardFileFilter("*.xml")));
      }
      if (Tools.isSet(dir_file))
      {
        int counts=0;
        for (String fldr : dir_file.keySet())
        {
          List<String> diseases = IOs.listFiles(fldr, new WildcardFileFilter("nodes*"));
          // setup the disease node
          if (Tools.isSet(diseases)) curated = G.curation(diseases.get(0), Strs.newMap('=', "ID=" + Graphs.UID, "type=" + Graphs.TYPE, "label=" + Graphs.TITLE));
          for (String fname : dir_file.get(fldr))
          {
            if (++counts%25==0) System.out.print(".");
            parseDocument(fname);
          }
          System.out.println();
          clearCuration();
        }
      }
      PropertyGraph.fixup(G, mapping);

      return this;
    }

    return null;
  }

  public PsiMI25Reader read(String... fnames)
  {
    if (Tools.isSet(fnames))
      for (String fname : fnames)
      {
        System.out.println("Reading PSI-MI contents from " + fname);
        parseDocument(fname);
      }

    return this;
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

  public void init()
  {
    setContentList("shortLabel","interactorRef","experimentRef","attribute","fullName","alias");
    if (G==null) G= new PropertyGraph();
  }
  @Override
  public void startElement(String uri, String localName, String elementName, Attributes attributes) throws SAXException
  {
    super.startElement(uri, localName, elementName, attributes);

    if      (elementName.equalsIgnoreCase("interaction"))
    {
      // add to the graph. it's a new node by definition, assuming we're not re-import the same uniprot
      interaction = new PropertyEdge();
      interaction.setProperty(Graphs.UID, attributes.getValue("id"));
    }
    else if (matchStack("primaryRef","xref","interactor") && Strs.equals(attributes.getValue("db"), "ensembl"))
    {
      actor.setProperty(ENSEMBLE, attributes.getValue("id"));
      Object g = (es2gene!=null?es2gene.get(actor.getProperty(GraphHandler.ENSEMBLE)):null);
      if (g!=null)
        actor.setProperty(Graphs.GENE, g.toString().toUpperCase());
    }
    else if (elementName.equalsIgnoreCase("interactor"))
    {
      actor = new PropertyNode("interactor");
      actor.setProperty(Graphs.UID, attributes.getValue("id"));
    }
    else if (Strs.equals(elementName, "experimentDescription"))
    {
      expt = new PropertyNode();
      expt.setProperty(Graphs.UID, attributes.getValue("id"));
    }
  }
  @Override
  public void endElement(String uri, String localName, String element) throws SAXException
  {
    super.endElement(uri, localName, element);
    if     (matchElementStack("attribute","attributeList","experimentDescription") &&
            Strs.equals(attrs.getValue("name"), "dataset") && expt!=null)
    {
      set(DATASET, expt, content);
    }
    else if (matchElementStack("alias","names","interactor") && Strs.equals(attrs.getValue("type"), "gene name"))
    {
      set("gene name", actor, content);
    }
    else if (Strs.equals(element,"experimentDescription") && expt!=null)
    {
      expts.put(expt.getProperty(Graphs.UID), expt);
    }
    else if (matchElementStack("fullName","names", "experimentDescription") && expt!=null)
    {
      set(NAME, expt, content);
    }
//    else if (matchElementStack("fullName","names", "hostOrganism") && expt!=null)
//    {
//      set(ORGANISM, expt, content);
//    }
    else if (matchElementStack("fullName","names", "organism") && actor!=null)
    {
      set(ORGANISM, actor, content);
    }
    else if (Strs.equals(element,"interactor") && actor!=null && !G.hasNodeLabel(Graphs.UID, actor.getProperty(Graphs.UID)) &&
        (!Tools.isSet(species) || !Strs.isSet(actor.getProperty(ORGANISM)) || Strs.isA(actor.getProperty(ORGANISM), species)))
    {
      if (/*Strs.equals(actor.getProperty("actorType"), "protein") &&*/
          Strs.isSet(actor.getProperty("gene name")))
      {
        actor.rename("gene name", Graphs.GENE);
      }
//      lastID = G.addVertex();
//      G.setNodeLabelProperty(lastID, actor);
      IntSet N = G.putNode(actor, Graphs.UID);
      if (N!=null)
      {
        lastID=N.toIntArray()[0];
        G.curates(curated, "curated_to", N.toIntArray());
      }

      if (++G.nodes%5000==0) System.out.print(".");
    }
    else if (matchElementStack("primaryRef","xref", "tissue","hostOrganism","experimentDescription") && interaction!=null)
    {
      // <primaryRef db="brenda" dbAc="MI:0864" id="BTO:0000776" refType="identity" refTypeAc="MI:0356"/>
      interaction.setProperty("BRENDA_ID", attrs.getValue("id"));
    }
    else if (Strs.equals(element, "shortLabel"))
    {
      if (matchStack(1, "names"))
      {
        if      (matchStack(2, "interaction"))            interaction.setDescription(content.toString());
        else if (matchStack(2, "interactor"))             set(Graphs.GENE,        actor, content);
        else if (matchStack(2, "interactorType"))         set(TYPE_ACTOR,  actor, content);
//        else if (matchStack(2, ORGANISM))                 set(ORGANISM,    actor, content);
        else if (matchStack(2, "hostOrganism"))           set(ORGANISM,    expt,  content);
        else if (matchStack(2, "tissue"))                 set(Graphs.TISSUE, expt, content);
        else if (matchStack(2, "interactionDetectionMethod")) set(Graphs.ASSAY, expt, content);
        else if (matchStack(2, "interactionType"))        set(Graphs.TYPE, interaction, content);
      }
    }
    else if (matchElementStack("interactorRef","participant","participantList","interaction"))
    {
      IntSet N = G.putNodeByUIDType(Graphs.UID, content.toString());
      if (Tools.isSet(N))
      {
        participants.addAll(N.toIntegerArrayList());
        G.curates(curated, "curated_to", N.toIntArray());
      }
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
      if (Tools.isSet(participants) &&
        (!Tools.isSet(species) || !Strs.isSet(interaction.getProperty(ORGANISM)) || Strs.isA(interaction.getProperty(ORGANISM), species)))
      {
        Set<Integer> hashes = new HashSet<>();
        for (Integer A : participants)
          for (Integer B : participants)
            if (!Tools.equals(A,B) && (!Tools.isSet(hashes) || !hashes.contains(A.hashCode()+B.hashCode())))
            {
              int E = G.addUndirectedSimpleEdge(A, B);
              G.setEdgeLabelProperty(E, interaction);
              hashes.add(A.hashCode() + B.hashCode());
              if (++G.edges%10000==0) System.out.print(".");
            }

        participants.clear();
      }
    }
    isParsing=false;
  }
}
