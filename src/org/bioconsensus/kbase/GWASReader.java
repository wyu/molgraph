package org.bioconsensus.kbase;

import org.ms2ms.graph.Graphs;
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
        IntSet As = putNodes(Graphs.SNP, "SNPS", Strs.newMap('=', "CHR_ID=Chr","CHR_POS=ChrPos","CONTEXT=Context"), tab, 'x'),
               Bs = new IntHashSet(),
               Cs = putNodes(Graphs.TRAIT, "MAPPED_TRAIT_URI", Strs.newMap('=', "MAPPED_TRAIT=Trait"), tab, ','),
               Ds = putNodes(Graphs.STUDY, "LINK", Strs.newMap('=', "DATE=Date","FIRST AUTHOR=Author","INITIAL SAMPLE DESCRIPTION=Sample","JOURNAL=Journal","STUDY=Study"), tab, ',');

        // need to figure out the special cases for the genes
        Set<String> genes = parseGenes(parseGenes(null, tab.get("REPORTED GENE(S)"), '-', "NR", "Intergenic"),
                                                        tab.get("MAPPED_GENE"),      '-', "NR", "Intergenic");
        if (Tools.isSet(genes))
          for (String gene : genes) Bs.addAll(G.putNode(Graphs.LABEL, gene, Graphs.TYPE, Graphs.GENE));

        G.putEdges(As, Cs, false, null, Graphs.TYPE, "SNP-TRAIT");
        G.putEdges(As, Ds, false, null, Graphs.TYPE, "SNP-STUDY");
        G.putEdges(As, Bs, false,
            Stats.toFloat(tab.get("PVALUE_MLOG")), Graphs.DISEASE, tab.get("DISEASE/TRAIT"), "Context", tab.get("P-VALUE (TEXT)"), Graphs.TYPE, "SNP-GENE");

        //if (G.getVertices().size()>20000) break;
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
