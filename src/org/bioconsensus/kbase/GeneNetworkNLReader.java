package org.bioconsensus.kbase;

import org.ms2ms.graph.Graphs;
import org.ms2ms.utils.Strs;
import org.ms2ms.utils.TabFile;
import toools.set.IntSet;

import java.io.IOException;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   1/1/15
 */
public class GeneNetworkNLReader extends TabReader
{
  public GeneNetworkNLReader(PropertyGraph g) { super(g); }

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
            G.setEdgeLabelProperties(E, Graphs.TYPE, "is_eQTL_of");
            G.setEdgeLabelProperties(E, "CisTrans", (Strs.equals(tab.get("CisTrans"), "cis") ? "Y" : "N"));
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
