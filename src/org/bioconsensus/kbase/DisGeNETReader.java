package org.bioconsensus.kbase;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.ms2ms.graph.Graphs;
import org.ms2ms.r.Dataframe;
import org.ms2ms.utils.IOs;
import org.ms2ms.utils.Strs;
import org.ms2ms.utils.TabFile;
import org.ms2ms.utils.Tools;
import toools.set.IntSet;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   4/26/15
 */
public class DisGeNETReader extends TabReader
{
  public DisGeNETReader(PropertyGraph g) { super(g); }

  public DisGeNETReader readRecursive(Dataframe mapping, String... folders)
  {
    if (Tools.isSet(folders))
    {
      Multimap<String, String> dir_file = HashMultimap.create();
      for (String flder : folders)
      {
        dir_file.putAll(IOs.listDirFiles(flder, new WildcardFileFilter("*.txt")));
      }
      if (Tools.isSet(dir_file))
      {
        int counts=0;
        for (String fldr : dir_file.keySet())
        {
          System.out.println("Reading eQTL contents from the folder: " + fldr);

//          List<String> diseases = IOs.listFiles(fldr, new WildcardFileFilter("nodes"));
//          // setup the disease node
//          // type	title	uid_tissue	uid_location	file
//          if (Tools.isSet(diseases)) curated=G.curation(diseases.get(0), Strs.newMap('=', "uid_tissue=" + Graphs.UID, "type=" + Graphs.TYPE, "title=" + Graphs.TITLE));

          for (String fname : dir_file.get(fldr))
          {
            if (++counts%25==0) System.out.print(".");
            parseDocument(fname);
          }
          System.out.println();
//          clearCuration();
        }
      }
      return this;
    }

    return null;
  }

//  geneId	geneSymbol	geneName	diseaseId	diseaseName	score	NumberOfPubmeds	associationType	source
//  540	ATP7B	ATPase, Cu++ transporting, beta polypeptide	umls:C0019202	Hepatolenticular Degeneration	0.98987635081179	229	GeneticVariation	UNIPROT, CTD_human, MGD, RGD, GAD, LHGDN, BeFree
//  540	ATP7B	ATPase, Cu++ transporting, beta polypeptide	umls:C0019202	Hepatolenticular Degeneration	0.98987635081179	229	Biomarker	UNIPROT, CTD_human, MGD, RGD, GAD, LHGDN, BeFree
//  540	ATP7B	ATPase, Cu++ transporting, beta polypeptide	umls:C0019202	Hepatolenticular Degeneration	0.98987635081179	229	AlteredExpression	UNIPROT, CTD_human, MGD, RGD, GAD, LHGDN, BeFree
//  4160	MC4R	melanocortin 4 receptor	umls:C0028754	Obesity	0.94	234	GeneticVariation	UNIPROT, CTD_human, MGD, RGD, GAD, BeFree

  public void parseDocument(String doc)
  {
    TabFile tab;
    try
    {
      tab = new TabFile(doc, TabFile.tabb);
      while (tab.hasNext())
      {
        IntSet As=null, Bs=null;

        if (     tab.get("geneSymbol")!=null)
          Bs = G.putNodeByUIDType(Graphs.UID, tab.get("geneSymbol").toUpperCase(), Graphs.TYPE, Graphs.GENE, Graphs.NAME, tab.get("geneName"));

        if (tab.get("diseaseId")!=null)
          As = G.putNodeByUIDType(Graphs.UID, tab.get("diseaseId"),  Graphs.TYPE, Graphs.DISEASE, Graphs.NAME, tab.get("diseaseName"));

        if (Tools.isSet(As) && Tools.isSet(Bs))
        {
          int E = G.addDirectedSimpleEdge(Bs.toIntArray()[0], As.toIntArray()[0]);
          G.setEdgeLabelProperties(E, Graphs.TYPE, tab.get("associationType"));
          G.setEdgeLabelProperties(E, "Source", tab.get("source"), "nPubMed", tab.get("NumberOfPubmeds"));
          G.setEdgeWeight(E, new Float(tab.get("score")));
        }
      }

    }
    catch (IOException e)
    {

    }

  }
}
