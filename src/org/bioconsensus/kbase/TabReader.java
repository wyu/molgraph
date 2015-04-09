package org.bioconsensus.kbase;

import org.ms2ms.utils.Strs;
import org.ms2ms.utils.TabFile;
import org.ms2ms.utils.Tools;
import toools.set.IntHashSet;
import toools.set.IntSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   2/12/15
 */
abstract public class TabReader
{
  protected PropertyGraph G=null;

  public TabReader(PropertyGraph g) { super(); G=g; };

  public IntSet putNodes(Map<String, String> tag_name, String tag, TabFile tab, String name, char delimiter, IntSet... Bs)
  {
    if (tab==null || tab.get(name)==null) return Tools.isSet(Bs)?Bs[0]:null;

    if (!Tools.isSet(Bs)) Bs = new IntSet[] {new IntHashSet()};
    String[] genes = Strs.split(tab.get(name), delimiter, true);
    if (Tools.isSet(genes))
    {
      // fetch the properties
      List<String> ps = new ArrayList<>();
      if (Tools.isSet(tag_name))
        for (String p : tag_name.keySet())
          if (tab.get(p)!=null)
          {
            ps.add(tag_name.get(p)); ps.add(tab.get(p));
          }
      for (String g : genes)
      {
        Bs[0].addAll(G.putNode(Strs.toStringArrayHead(ps, tag, g)));
      }
    }
    return Bs[0];
  }
}
