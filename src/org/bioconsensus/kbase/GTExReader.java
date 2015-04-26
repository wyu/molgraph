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
import java.util.List;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   4/25/15
 */
public class GTExReader extends TabReader
{
  public GTExReader(PropertyGraph g) { super(g); }

  public GTExReader readRecursive(Dataframe mapping, String... folders)
  {
    if (Tools.isSet(folders))
    {
      Multimap<String, String> dir_file = HashMultimap.create();
      for (String flder : folders)
      {
        dir_file.putAll(IOs.listDirFiles(flder, new WildcardFileFilter("*.eqtl")));
      }
      if (Tools.isSet(dir_file))
      {
        int counts=0;
        for (String fldr : dir_file.keySet())
        {
          System.out.println("Reading eQTL contents from the folder: " + fldr);

          List<String> diseases = IOs.listFiles(fldr, new WildcardFileFilter("nodes*"));
          // setup the disease node
          if (Tools.isSet(diseases)) G.curation(diseases.get(0), Strs.newMap('=', "ID="+Graphs.UID, "type="+Graphs.TYPE, "label="+Graphs.TITLE));

          for (String fname : dir_file.get(fldr))
          {
            if (++counts%25==0) System.out.print(".");
            parseDocument(fname);
          }
          System.out.println();
          clearCuration();
        }
      }
      return this;
    }

    return null;
  }

  public void parseDocument(String doc)
  {
    TabFile tab;
    try
    {
      tab = new TabFile(doc, TabFile.tabb);
      while (tab.hasNext())
      {
        IntSet As=null, Bs=null;

        if (     tab.get("HGNCName")!=null)
          Bs = G.putNodeByUIDType(Graphs.UID, tab.get("HGNCName").toUpperCase(), Graphs.TYPE, Graphs.GENE);
        else if (tab.get("HUGO")!=null)
          Bs = G.putNodeByUIDType(Graphs.UID, tab.get("HUGO").toUpperCase(), Graphs.TYPE, Graphs.GENE);

        if (tab.get("SNPName")!=null)
          As = G.putNodeByUIDType(Graphs.UID, tab.get("SNPName"),  Graphs.TYPE, Graphs.SNP, Graphs.CHR, tab.get("SNPChr"), Graphs.CHR_POS, tab.get("SNPChrPos"));

        if (As!=null && Bs!=null)
          // only expect one rs and one gene
          if (As.size()==1 && Bs.size()==1)
          {
            int E = G.addDirectedSimpleEdge(As.toIntArray()[0], Bs.toIntArray()[0]);
            G.setEdgeLabelProperties(E, Graphs.TYPE, "SNP-eQTL-GENE");
            G.setEdgeLabelProperties(E, "CisTrans", (Strs.equals(tab.get("CisTrans"), "cis") ? "cis" : "trans"));
            G.setEdgeWeight(E, -10f * (float) Math.log10(new Double(tab.get("PValue"))));
          }
          else
          {
            // unexpected situation
            System.out.println("Rs# "+As.size() + "/Gene# " + Bs.size());
          }
      }

    }
    catch (IOException e)
    {

    }

  }

}
