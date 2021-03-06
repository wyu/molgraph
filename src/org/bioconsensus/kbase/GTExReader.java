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
import toools.set.IntHashSet;
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
 * Date:   4/25/15
 */
public class GTExReader extends TabReader
{
  public GTExReader(PropertyGraph g) { super(g); }

  public GTExReader readRecursive(String... folders)
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
//          System.out.println("Reading eQTL contents from the folder: " + fldr);
          List<String> diseases = IOs.listFiles(fldr, new WildcardFileFilter("nodes"));
          // setup the disease node
          // type	title	uid_tissue	uid_location	file
          if (Tools.isSet(diseases)) curated=G.curation(diseases.get(0), Strs.newMap('=', "uid="+Graphs.UID, "type="+Graphs.TYPE, "title="+Graphs.TITLE));

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
      IntSet active = new IntHashSet();
      String tissues = null;
      if (Tools.isSet(curated))
        for (Integer i : curated)
          if (Strs.indexOf(doc, G.node_label_val.get(i, "file"))>=0)
          {
            active.add(i);
            tissues = Strs.extend(tissues, G.node_label_val.get(i, "uid"), ";");
          }

      while (tab.hasNext())
      {
        IntSet As=null, Bs=null;

        if (     tab.get("Gene_Name")!=null)
          Bs = G.putNodeByUIDType(Graphs.UID, tab.get("Gene_Name").toUpperCase(), Graphs.TYPE, Graphs.GENE);

        if (tab.get("SNP")!=null)
          As = G.putNodeByUIDType(Graphs.UID, tab.get("SNP"),  Graphs.TYPE, Graphs.SNP, Graphs.CHR, tab.get("SNP_Chr"), Graphs.CHR_POS, tab.get("SNP_Pos"));

        if (G.nodes%1000  ==0) System.out.print(".");
        if (G.nodes%100000==0) System.out.println();

        if (Tools.isSet(As) && Tools.isSet(Bs))
        {
          G.putDirectedEdgesByUIDType(As.toIntArray()[0], Bs.toIntArray()[0],
              -10f * (float) Math.log10(new Double(tab.get("P_Val"))),
              Strs.newMap('=', Graphs.TYPE+"=is_eQTL_of", Graphs.UID+"="+tissues));
//          int E = G.addDirectedSimpleEdge(As.toIntArray()[0], Bs.toIntArray()[0]);
//          G.setEdgeLabelProperties(E, Graphs.TYPE, "is_eQTL_of");
//          G.setEdgeWeight(E, -10f * (float) Math.log10(new Double(tab.get("P_Val"))));
          // copy the UID of the tissue
//          if (Strs.isSet(tissues)) G.setEdgeLabelProperties(E, "tissue", tissues);
//
//          G.edges++;
        }
//        if (Tools.isSet(active) && Tools.isSet(Bs))
//          G.putDirectedEdgesByUIDType(Bs, active, null, Strs.newMap('=', Graphs.TYPE+"=in"));

//        for (Integer tissue : active)
//            G.putDirectedEdgesByUIDType(As.toIntArray()[0], Bs.toIntArray()[0], -10f * (float) Math.log10(new Double(tab.get("P_Val"))), Strs.newMap('=', Graphs.TYPE+"=is_eQTL_of"));
      }
    }
    catch (IOException e)
    {

    }

  }

}
