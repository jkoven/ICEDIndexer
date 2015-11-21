/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package icedindexer;

import java.util.BitSet;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;

/**
 *
 * @author jkoven
 */
public class HashCollector extends Collector {

    public int docBase;
    public BitSet docIds;

    public HashCollector() {
        docIds = new BitSet();
        docBase = 0;
    }

    @Override
    // We don't car about order
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    // ignore scorer
    @Override
    public void setScorer(Scorer scorer) {
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        this.docBase = context.docBase;
    }

    // The meat of this collector we take the incoming doc and stuff it into a
    // new index
    @Override
    public void collect(int doc) {
        docIds.set(doc + docBase);
    }

    
}
