package org.bioconsensus.kbase;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.ms2ms.graph.Graphs;
import org.ms2ms.graph.PropertyEdge;
import org.ms2ms.graph.PropertyNode;
import org.ms2ms.utils.IOs;
import org.ms2ms.utils.Strs;
import org.ms2ms.utils.Tools;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import toools.set.IntSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   2/13/15
 */
public class DrugBankReader extends GraphHandler
{
  public static final String DRUG_NAME    = "drugName";
  public static final String DRUG_DESC    = "drugDesc";
  public static final String DRUG_INCT    = "drugIntAct";
  public static final String DRUG_TRGT    = "drugTarget";
  public static final String DRUG_INCT_DESC = "^drugIntActDesc";
  public static final String TRGT_ACTION  = "^targetAction";

  Map<String, String> properties = new HashMap<>();;
  Multimap<String, PropertyEdge> interactors = HashMultimap.create();

  PropertyNode drug;
  PropertyEdge edge;
  IntSet drugIdx;

  public DrugBankReader()
  {
    super(); init();
  }
  public DrugBankReader(PropertyGraph g)
  {
    super(g); init();
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

  private void init()
  {
    setContentList("name", "description","drugbank-id","identifier","resource");
  }
  @Override
  public void startElement(String uri, String localName, String elementName, Attributes attributes) throws SAXException
  {
    super.startElement(uri, localName, elementName, attributes);
//    stack.add(elementName); attrs = attributes;
    if      (matchStack("drug", "drugbank"))
    {
      // add to the graph. it's a new node by definition, assuming we're not re-import the same uniprot
      drug    = new PropertyNode();
      drug.setProperty("drugType", attributes.getValue("type"));
    }
    else if (matchStack("target","targets","drug"))
    {
      edge = new PropertyEdge();
    }
    else if (matchStack("drug-interaction", "drug-interactions", "drug"))
    {
      edge = new PropertyEdge();
    }
  }
  @Override
  public void endElement(String uri, String localName, String element) throws SAXException
  {
    super.endElement(uri, localName, element);
//    if (!stack.removeLast().equals(element)) throw new RuntimeException("Unmatched element!");
    // if end of book element add to list
    if (matchElementStack("drug", "drugbank"))
    {
      if (G.nodes%100 ==0) System.out.print(".");
      if (G.nodes%5000==0) System.out.println(G.nodes + "/" + G.edges + ".");
      // save the drug node
      drugIdx = G.putNode(DRUGID, drug.getProperty(DRUGID));
      G.setNodeLabelProperty(drugIdx, drug);
      // deposit the drug
      G.putDirectedEdges(drugIdx, Graphs.GENE, interactors.get(DRUG_TRGT), Graphs.LABEL, DRUG_TRGT);
      G.putDirectedEdges(drugIdx, DRUGID, interactors.get(DRUG_INCT), Graphs.LABEL, DRUG_INCT);
      // clear the cache
      interactors.clear();
    }
    else if (matchElementStack("name","drug","drugbank"))           set(DRUG_NAME, drug, content);
    else if (matchElementStack("name","drug-interaction"))          set(DRUG_NAME, edge, content);
    else if (matchElementStack("name","target"))                    set(NAME, edge, content);
    else if (matchElementStack("description", "drug"))              set(DRUG_DESC, drug, content);
    else if (matchElementStack("description", "drug-interaction"))  set(DRUG_INCT_DESC, edge, content);
    else if (matchElementStack("drugbank-id", "drug-interaction"))  set(DRUGID, edge, content);
    else if (matchElementStack("drugbank-id","drug") && Strs.equals(attrs.getValue("primary"), "true"))
    {
      set(DRUGID, drug, content);
    }
    else if (matchElementStack("external-identifier", "external-identifiers","polypeptide","target"))
    {
      if (Strs.equals(properties.get("resource"), "GeneCards") && edge!=null)
      {
        // setup the drug target link
        edge.setProperty(Graphs.GENE, properties.get("identifier"));
      }
      properties.clear();
    }
    else if (matchElementStack("identifier","external-identifier") ||
             matchElementStack("resource",  "external-identifier"))
    {
      properties.put(element,content.toString());
    }
    else if (Strs.equals(element,"drug-interaction") && edge!=null && Tools.isSet(edge.getProperties()))
    {
      interactors.put(DRUG_INCT, edge);
      edge=null;
    }
    else if (matchElementStack("target","targets"))
    {
      if (edge!=null && Tools.isSet(edge.getProperties())) interactors.put(DRUG_TRGT, edge);
      edge=null;
    }
    else if (matchElementStack("action","target","targets"))
    {
      set(TRGT_ACTION, edge, content);
    }
    isParsing=false;
  }
}
