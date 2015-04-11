package org.bioconsensus.kbase;

import org.ms2ms.utils.Tools;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   4/8/15
 */
public class BioGRIDReader extends PsiMI25Reader
{
  public BioGRIDReader(PropertyGraph g) { super(g); }

  public void parseDocuments(String... docs)
  {
    if (Tools.isSet(docs))
      for (String fname : docs)
      {
        System.out.println("Reading PSI-MI contents from " + fname);
        parseDocument(fname);
      }

    // fix up the BioGRID specific records
  }
}
