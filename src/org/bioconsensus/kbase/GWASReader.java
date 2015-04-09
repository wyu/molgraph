package org.bioconsensus.kbase;

import org.ms2ms.math.Stats;
import org.ms2ms.utils.Strs;
import org.ms2ms.utils.TabFile;
import org.ms2ms.utils.Tools;
import toools.set.IntHashSet;
import toools.set.IntSet;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   2/19/15
 */
public class GWASReader extends TabReader
{
  public GWASReader(PropertyGraph g) { super(g); }

  public void parseDocument(String doc)
  {
    TabFile tab;
    try
    {
      tab = new TabFile(doc, TabFile.tabb);
      while (tab.hasNext())
      {
        IntSet As = putNodes(Strs.newMap('=', "CHR_ID=Chr","CHR_POS=ChrPos","CONTEXT=Context"), GraphHandler.RSID,  tab, "SNPS", 'x'),
               Bs = new IntHashSet(),
               Cs = putNodes(Strs.newMap('=', "MAPPED_TRAIT_URI=EFO_ID"), GraphHandler.TRAIT, tab, "MAPPED_TRAIT", ',');

        // need to figure out the special cases for the genes
        Set<String> genes = parseGenes(parseGenes(null, tab.get("REPORTED GENE(S)"), '-', "NR", "Intergenic"),
                                                        tab.get("MAPPED_GENE"),      '-', "NR", "Intergenic");
        if (Tools.isSet(genes))
          for (String gene : genes) Bs.addAll(G.putNode(GraphHandler.GENE, gene));

        G.putEdges(As, Cs, false, null);
        G.putEdges(As, Bs, false,
            Stats.toFloat(tab.get("PVALUE_MLOG")), GraphHandler.DISEASE, tab.get("DISEASE/TRAIT"));
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }
  private Set<String> parseGenes(Set<String> genes, String genestr, char linker, String... ignored)
  {
    if (!Strs.isSet(genestr)) return genes;
    if (genes==null) genes = new HashSet<>();

    String[] strs = Strs.split(genestr, ',', true);
    if (Tools.isSet(strs))
      for (String str : strs)
      {
        if (!Strs.isSet(str) || Strs.isA(str, ignored)) continue;
        Tools.add(genes, Strs.split(str, linker, true));
      }
    return genes;
  }
}
