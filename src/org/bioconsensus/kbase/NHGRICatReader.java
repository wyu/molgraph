package org.bioconsensus.kbase;

import org.ms2ms.utils.Strs;
import org.ms2ms.utils.TabFile;
import org.ms2ms.utils.Tools;
import toools.set.IntHashSet;
import toools.set.IntSet;

import java.io.IOException;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   2/19/15
 */
public class NHGRICatReader extends TabReader
{
  public NHGRICatReader(PropertyGraph g) { super(g); }

  public void parseDocument(String doc)
  {
    TabFile tab;
    try
    {
      tab = new TabFile(doc, TabFile.tabb);
      while (tab.hasNext())
      {
        IntSet As = G.putNode(GraphHandler.RSID, tab.get("SNPs")), Bs = new IntHashSet();
        // deposit the genes
        String[] genes = Strs.split(tab.get("Reported Gene(s)"), ',');
        if (Tools.isSet(genes))
          for (String g : genes)
            Bs.addAll(G.putNode(GraphHandler.GENE, g));

        IntSet E = G.putDirectedEdges(As, Bs,
            GraphHandler.DISEASE, tab.get("Disease/Trait"),
            GraphHandler.CONTEXT, tab.get("Context"));
        if (Tools.isSet(E))
          for (int edge : E.toIntArray())
            if (Strs.isSet(tab.get("Pvalue_mlog")))
              G.setEdgeWeight(edge, new Float(tab.get("Pvalue_mlog")));
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }
}
