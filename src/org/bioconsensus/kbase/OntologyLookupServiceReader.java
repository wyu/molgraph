package org.bioconsensus.kbase;

import org.ms2ms.graph.Graphs;
import org.ms2ms.math.Stats;
import org.ms2ms.r.Dataframe;
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
 * Date:   6/26/15
 */
public class OntologyLookupServiceReader extends TabReader
{
  public OntologyLookupServiceReader(PropertyGraph g) { super(g); }

  public void parseDocument(String root)
  {
    // term_pk	ontology_id	term_name	identifier	definition	namespace	is_obsolete	is_root_term    is_leaf
    Dataframe terms = new Dataframe(root+"term.txt", '\t', "term_pk"),
    // annotation_id	term_pk	annotation_num_value	annotation_name	annotation_str_value
             annots = new Dataframe(root+"annotation.txt", '\t', "annotation_id");
    // prepare the merged records for the terms
    Dataframe data = Dataframe.merge(terms, annots, true, true, "term_pk");

    TabFile tab;
    try
    {
      tab = new TabFile(doc, TabFile.tabb);
      while (tab.hasNext())
      {
        IntSet As = putNodes(Graphs.SNP, "SNPS", Strs.newMap('=', "CHR_ID=" + Graphs.CHR + ":int", "CHR_POS=" + Graphs.CHR_POS + ":long", "CONTEXT=Context"), tab, 'x'),
            Bs = new IntHashSet(),
            Cs = putNodes(Graphs.TRAIT, "MAPPED_TRAIT_URI", Strs.newMap('=', "MAPPED_TRAIT=Trait"), tab, ','),
            Ds = putNodes(Graphs.STUDY, "LINK", Strs.newMap('=', "DATE=Date","FIRST AUTHOR=Author","INITIAL SAMPLE DESCRIPTION=Sample","JOURNAL=Journal","STUDY=Study"), tab, ',');

        // need to figure out the special cases for the genes
        Set<String> genes = parseGenes(parseGenes(null, tab.get("REPORTED GENE(S)"), '-', "NR", "Intergenic"),
            tab.get("MAPPED_GENE"),      '-', "NR", "Intergenic");
        if (Tools.isSet(genes))
          for (String gene : genes) Bs.addAll(G.putNodeByUIDType(Graphs.UID, gene, Graphs.TYPE, Graphs.GENE));

        G.putDirectedEdgesByUIDType(As, Cs, null, Strs.newMap('=', Graphs.TYPE+"=has_trait"));
        G.putDirectedEdgesByUIDType(As, Ds, null, Strs.newMap('=', Graphs.TYPE+"=from_study"));
        G.putDirectedEdgesByUIDType(As, Bs, Stats.toFloat(tab.get("PVALUE_MLOG")),
            Strs.newMap('=', Graphs.DISEASE+"="+tab.get("DISEASE/TRAIT"), "Context="+tab.get("P-VALUE (TEXT)"), Graphs.TYPE+"=related_to"));

//        G.putEdges(As, Cs, false, null, Graphs.TYPE, "has_trait");
//        G.putEdges(As, Ds, false, null, Graphs.TYPE, "from_study");
//        G.putEdges(As, Bs, false,
//            Stats.toFloat(tab.get("PVALUE_MLOG")), Graphs.DISEASE, tab.get("DISEASE/TRAIT"), "Context", tab.get("P-VALUE (TEXT)"), Graphs.TYPE, "related_to");

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
