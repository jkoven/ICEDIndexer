package icedindexer;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import static org.apache.lucene.queryparser.classic.QueryParserBase.OR_OPERATOR;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author jkoven
 */
public class IndexSearch {

    public static void search(String indexPath) {
        try {
            String searchString = "account";
            IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
            IndexSearcher searcher = new IndexSearcher(reader);
            CharArraySet stopWords = new CharArraySet(Version.LUCENE_40, 1000, true);
            File file = new File("/Users/jkoven/NetBeansProjects/IVEST/dist/datafiles/stopwords_en.txt");
            BufferedReader inFile = new BufferedReader(new FileReader(file));
            String word;
            while ((word = inFile.readLine()) != null) {
                stopWords.add(word.trim());
            }
            String[] fieldList = {"Subject", "content"};
            MultiFieldQueryParser mParser
                    = new MultiFieldQueryParser(Version.LUCENE_40, fieldList,
                            new ClassicAnalyzer(Version.LUCENE_40, stopWords));
            mParser.setDefaultOperator(OR_OPERATOR);
            Query q = mParser.parse(searchString);
            System.out.println(q.toString());
            sCollector sc = new sCollector();
            searcher.search(q, sc);
            System.out.println(sc.docIds.cardinality() + " Documents found");
            for (int i = sc.docIds.nextSetBit(0);  i >= 0; i = sc.docIds.nextSetBit(i + 1)){
                Document doc = reader.document(i);
                System.out.println(doc.get("path"));
            }
        } catch (Exception ex) {
            Logger.getLogger(IndexSearch.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static class sCollector extends Collector {

        BitSet docIds;
        int docBase;

        public sCollector() {
            docIds = new BitSet();
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
}
