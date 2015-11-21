/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package icedindexer;

/**
 *
 * @author jkoven
 */
import java.io.BufferedReader;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.commons.mail.util.MimeMessageParser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.activation.DataSource;
import javax.mail.*;
import javax.mail.internet.MimeMessage;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StraightBytesDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import static org.apache.lucene.queryparser.classic.QueryParserBase.AND_OPERATOR;
import static org.apache.lucene.queryparser.classic.QueryParserBase.OR_OPERATOR;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.jsoup.Jsoup;

/**
 * Index all text files under a directory.
 * <p>
 * This is a modification of the command-line application demonstrating simple
 * Lucene indexing.
 */
public class IndexFiles {

    private static long uid = 0;
    private static HashMap<String, Long> indexed = new HashMap<>();
    private static long threadId = 0;
    private static CharArraySet stopWords;
    private static boolean findTimeStamps = false;
    private static boolean createMergeInfo = false;

    public IndexFiles() {
    }

    /**
     * Index all text files under a directory.
     */
    public static void indexMain(String[] args, boolean createHashes) {
        String usage = "java org.apache.lucene.demo.IndexFiles"
                + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update]\n\n"
                + "This indexes the documents in DOCS_PATH, creating a Lucene index"
                + "in INDEX_PATH that can be searched with SearchFiles";
        String indexPath = "index";
        String hashIndexPath = "hashindex";
        String workingIndexPath = "workingindex";
        String linkIndexPath = "linkindex";
        String attachmentPath = "attachments";
        String baseDir = null;
        String docsPath = null;
        boolean create = true;
        boolean noDownload = false;
        EntityExtractor ee = new EntityExtractor("datafiles");
        PosExtractor tagger = new PosExtractor("datafiles/models");

        for (int i = 0; i < args.length; i++) {
            if ("-index".equals(args[i])) {
                indexPath = args[i + 1];
                i++;
            } else if ("-hashindex".equals(args[i])) {
                hashIndexPath = args[i + 1];
                i++;
            } else if ("-linkindex".equals(args[i])) {
                linkIndexPath = args[i + 1];
                i++;
            } else if ("-workingindex".equals(args[i])) {
                workingIndexPath = args[i + 1];
                i++;
            } else if ("-attachments".equals(args[i])) {
                attachmentPath = args[i + 1];
                i++;
            } else if ("-basedirectory".equals(args[i])) {
                baseDir = args[i + 1];
                i++;
            } else if ("-docs".equals(args[i])) {
                docsPath = args[i + 1];
                i++;
            } else if ("-update".equals(args[i])) {
                create = false;
            }
        }

        if (docsPath == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        final File docDir = new File(docsPath);
        if ((!docDir.exists() || !docDir.canRead()) && !noDownload) {
            System.out.println("Document directory '" + docDir.getAbsolutePath() + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        Date start = new Date();
        try {
            stopWords = new CharArraySet(Version.LUCENE_40, 1000, true);
            File file = new File("datafiles/stopwords_en.txt");
            BufferedReader inFile = new BufferedReader(new FileReader(file));
            String word;
            while ((word = inFile.readLine()) != null) {
                stopWords.add(word.trim());
            }
            System.out.println("Indexing to directory '" + indexPath + "'...");
            Directory dir = FSDirectory.open(new File(indexPath));
            Directory hashDir = FSDirectory.open(new File(hashIndexPath));
            Directory linkDir = FSDirectory.open(new File(linkIndexPath));
            Analyzer analyzer = new ClassicAnalyzer(Version.LUCENE_40, stopWords);
            QueryParser parser = new QueryParser(Version.LUCENE_40, "contents", analyzer);
            IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_40, analyzer);
            IndexWriterConfig wiwc = new IndexWriterConfig(Version.LUCENE_40, analyzer);
            IndexWriterConfig liwc = new IndexWriterConfig(Version.LUCENE_40, analyzer);

            if (create) {
                // Create a new index in the directory, removing any
                // previously indexed documents:
                iwc.setOpenMode(OpenMode.CREATE);
                wiwc.setOpenMode(OpenMode.CREATE);
                liwc.setOpenMode(OpenMode.CREATE);
            } else {
                // Add new documents to an existing index:
                wiwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
            }

            // Optional: for better indexing performance, if you
            // are indexing many documents, increase the RAM
            // buffer.  But if you do this, increase the max heap
            // size to the JVM (eg add -Xmx512m or -Xmx1g):
            //
            // iwc.setRAMBufferSizeMB(256.0);
            if (createHashes) {
                IndexWriter workingWriter = new IndexWriter(hashDir, wiwc);
                indexHashes(workingWriter, docDir);
                workingWriter.close();

//                IndexReader hashReader = DirectoryReader.open(FSDirectory.open(new File(workingIndexPath)));
//                IndexSearcher hashSearcher = new IndexSearcher(hashReader);
//                IndexWriter hashWriter = new IndexWriter(hashDir, iwc);
//                createHashThreads(hashWriter, hashReader, hashSearcher, parser);
//                hashReader.close();
//                hashWriter.close();
            } else {
                IndexWriter writer = new IndexWriter(dir, iwc);
                IndexReader hashReader = DirectoryReader.open(FSDirectory.open(new File(hashIndexPath)));
                IndexSearcher hashSearcher = new IndexSearcher(hashReader);
                uid = 0;
                indexDocs(writer, docDir, ee, tagger, hashReader, hashSearcher, parser);
                writer.close();
                hashReader.close();
                //create the link index
                IndexReader reader = DirectoryReader.open(FSDirectory.open(new File(indexPath)));
                writer = new IndexWriter(linkDir, liwc);
                createLinkDir(writer, reader);
                extractAttachments(reader, new File(attachmentPath), docsPath);
                writer.close();
                reader.close();
            }

            // NOTE: if you want to maximize search performance,
            // you can optionally call forceMerge here.  This can be
            // a terribly costly operation, so generally it's only
            // worth it when your index is relatively static (ie
            // you're done adding documents to it):
            //
            // writer.forceMerge(1);
            Date end = new Date();
            System.out.println(end.getTime() - start.getTime() + " total milliseconds");

        } catch (IOException e) {
            System.out.println(" caught a " + e.getClass()
                    + "\n with message: " + e.getMessage());
        }
    }

    /**
     * Indexes the given file using the given writer, or if a directory is
     * given, recurses over files and directories found under the given
     * directory.
     *
     * NOTE: This method indexes one document per input file. This is slow. For
     * good throughput, put multiple documents into your input file(s). An
     * example of this is in the benchmark module, which can create "line doc"
     * files, one document per line, using the
     * <a
     * href="../../../../../contrib-benchmark/org/apache/lucene/benchmark/byTask/tasks/WriteLineDocTask.html"
     * >WriteLineDocTask</a>.
     *
     * @param writer Writer to the index where the given file/dir info will be
     * stored
     * @param file The file to index, or the directory to recurse into to find
     * files to index
     * @throws IOException If there is a low-level I/O error
     */
    static void indexDocs(IndexWriter writer, File file, EntityExtractor ee, PosExtractor tagger,
            IndexReader hashReader, IndexSearcher hashSearcher, QueryParser parser)
            throws IOException {
        // do not try to index files that cannot be read
        if (file.canRead()) {
            if (file.isDirectory()) {
                System.out.println("Indexing: " + file.getPath());
                String[] files = file.list();
                // an IO error could occur
                if (files != null) {
                    for (int i = 0; i < files.length; i++) {
                        indexDocs(writer, new File(file, files[i]), ee, tagger, hashReader, hashSearcher, parser);
                    }
                }
            } else {
                Pattern timePattern = Pattern.compile("\\p{Digit}\\p{Digit}:\\p{Digit}\\p{Digit}:\\p{Digit}\\p{Digit}");
                Pattern computerPattern = Pattern.compile("Computer:");
                Pattern userPattern = Pattern.compile("User:");
                MimeMessage message = null;
                MimeMessageParser mmp = null;
                Enumeration headers;
                FileInputStream fis = null;
                Session s;
                // First we need the hash for the file to clear dups when searching later.
                String emailHash = getHash(Files.readAllBytes(file.toPath()));
                try {
                    s = Session.getDefaultInstance(new Properties());
                    fis = new FileInputStream(file);
                    message = new MimeMessage(s, fis);
                    mmp = new MimeMessageParser(message);
                    try {
                        mmp.parse();
                    } catch (Exception e) {
                        System.err.println("Could not parse file: " + file.getPath());
                    }
                    headers = message.getAllHeaders();
//                    while (headers.hasMoreElements()) {
//                        Header h = (Header) headers.nextElement();
                    // If the file is stored in a different directory 
                    // from its frist gmail label then ignore it so we 
                    // do not keep duplicates.  Only emails that are archived
                    // but not labeled will be lost.  In real investigation we probably 
                    // want the archived ones as well.
//                        if (h.getName().equalsIgnoreCase("X-Gmail-Labels")) {
//                            if (h.getValue().contains(",")) {
//                                try {
//                                    if (!file.toString().substring(FDG.messagePath.length() + 1).
//                                            startsWith(h.getValue().toString().
//                                                    substring(0, h.getValue().toString().indexOf(",")))) {
////                                        System.out.println("Skipping");
////                                        System.out.println(h.getValue().toString());
////                                        System.out.println(file.toString());
//                                        return;
//                                    }
//                                } catch (Exception e) {
//                                    System.out.println("Gmail label field exception: " + file.toString());
//                                }
//                            }
//                        }
//                    }
                } catch (FileNotFoundException fnfe) {
                    // at least on windows, some temporary files raise this exception with an "access denied" message
                    // checking if the file can be read doesn't help
                    return;
                } catch (MessagingException ex) {
                    // If we cant open this as an email just ignore the file.
                    return;
                } catch (Exception ex) {
                    Logger.getLogger(IndexFiles.class.getName()).log(Level.SEVERE, null, ex);
                }

                try {
                    // make a new, empty document
                    Document doc = new Document();
                    FieldType storeAll = new FieldType();
                    storeAll.setStoreTermVectors(true);
                    storeAll.setStoreTermVectorPositions(true);
                    storeAll.setIndexed(true);
                    storeAll.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
                    storeAll.setStored(true);
                    // Give each doc a new unique field identifier and its hash.
                    doc.add(new LongField("uid", uid++, Field.Store.YES));
                    doc.add(new StringField("emailhash", emailHash, Field.Store.YES));
                    // Add the path of the file as a field named "path".  Use a
                    // field that is indexed (i.e. searchable), but don't tokenize 
                    // the field into separate words and don't index term frequency
                    // or positional information:
                    Field pathField = new Field("path", file.getPath(), storeAll);
                    doc.add(pathField);

                    // Add the last modified date of the file a field named "modified".
                    // Use a LongField that is indexed (i.e. efficiently filterable with
                    // NumericRangeFilter).  This indexes to milli-second resolution, which
                    // is often too fine.  You could instead create a number based on
                    // year/month/day/hour/minutes/seconds, down the resolution you require.
                    // For example the long value  would mean
                    // February , 21, 2-3 PM.
                    doc.add(new LongField("modified", file.lastModified(), Field.Store.NO));

                    // Add the contents of the file to a field named "contents".  Specify a Reader,
                    // so that the text of the file is tokenized and indexed, but not stored.
                    // Note that FileReader expects the file to be in UTF-8 encoding.
                    // If that's not the case searching for special characters will fail.
                    SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z");
                    SimpleDateFormat altsdf = new SimpleDateFormat("EEE dd MMM yyyy HH:mm:ss Z");
                    SimpleDateFormat alt3sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy");
                    SimpleDateFormat alt2sdf = new SimpleDateFormat("dd MMM yyyy HH:mm:ss Z");
                    Date date = null;
                    Calendar cal = Calendar.getInstance();
                    headers = message.getAllHeaders();
                    while (headers.hasMoreElements()) {
                        Header h = (Header) headers.nextElement();
                        if (h.getName().equalsIgnoreCase("Date")) {
                            String eDate = h.getValue();
                            if (eDate.contains("UT")) {
                                eDate = eDate.replace("UT", "-0000 (GMT)");
                            }
                            try {
                                date = sdf.parse(eDate);
                            } catch (ParseException ex) {
                                try {
                                    date = altsdf.parse(eDate);
                                } catch (ParseException ex1) {
                                    try {
                                        date = alt2sdf.parse(eDate);
                                    } catch (ParseException ex2) {
                                        try {
                                            date = alt3sdf.parse(eDate);
                                        } catch (ParseException ex3) {
                                            System.out.println("Date formatting issue: " + eDate);
                                            continue;
                                        }
                                    }
                                }
                            }
                            cal.setTime(date);
                            if (cal.get(Calendar.YEAR) == 1979) {
                                cal.add(Calendar.YEAR, 20);
                                date = cal.getTime();
                            }
                            doc.add(new LongField(h.getName(), date.getTime(), Field.Store.YES));
                        } else {
                            doc.add(new Field(h.getName(), h.getValue(), storeAll));
                            if (h.getName().equals("Subject") || h.getName().equals("subject")) {
                                String subjectHash = getHash(h.getValue().getBytes("UTF-8"));
                                doc.add(new Field("subject_hash", subjectHash, storeAll));
                                HashMap<String, String> taggedWords = tagger.ExtractPos(h.getValue());
                                for (String key : taggedWords.keySet()) {
                                    doc.add(new Field(key, taggedWords.get(key), storeAll));
                                }
                            }
                        }
                    }
                    String content = "";
//                    String contentType = "";
//                    Multipart mp;
                    try {
//                        if (message.getContentType().toLowerCase().contains("multipart")) {
//                            content = "";
//                            contentType = "text";
//                            mp = (Multipart) message.getContent();
//                            int nextPart = 0;
//                            while (nextPart < mp.getCount()) {
////                                if (mp.getBodyPart(nextPart).getContentType().toLowerCase().contains("multipart")) {
////                                if (mp.getBodyPart(nextPart).isMimeType("multipart")) {
////                                    mp = (Multipart) mp.getBodyPart(nextPart).getContent();
////                                    nextPart = 0;
//////                                } else if (mp.getBodyPart(nextPart).getContentType().toLowerCase().contains("text")) {
//                                if (mp.getBodyPart(nextPart).getContentType().toLowerCase().contains("text")) {
//                                    content = (String) mp.getBodyPart(nextPart).getContent();
//                                    contentType = mp.getBodyPart(nextPart).getContentType().toLowerCase();
//                                    break;
//                                } else {
//                                    nextPart++;
//                                }
//                            }
//                        } else {
//                            content = (String) message.getContent();
//                            contentType = message.getContentType().toLowerCase();
//                        }
//                        if (contentType.contains("html")) {
//                            content = Jsoup.parse(content).text();
//                        }
                        if (mmp.hasPlainContent()) {
                            content = mmp.getPlainContent();
                        } else if (mmp.hasHtmlContent()) {
                            content = Jsoup.parse(mmp.getHtmlContent()).text();
                        }
                    } catch (Exception e) {
                        System.err.println(e);
                        System.err.println(file.getPath());
                        content = "Failed to read content";
                    }
                    if (createMergeInfo) {
                        Matcher m = timePattern.matcher(content);
                        if (m.find()) {
                            String[] tParts = m.group().split(":");
                            Calendar tcal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                            tcal.setTimeInMillis(date.getTime());
                            tcal.set(Calendar.HOUR, Integer.parseInt(tParts[0]));
                            tcal.set(Calendar.MINUTE, Integer.parseInt(tParts[1]));
                            tcal.set(Calendar.SECOND, Integer.parseInt(tParts[2]));
                            doc.add(new LongField("timestamp", tcal.getTimeInMillis(), Field.Store.YES));
                        }
                        m = computerPattern.matcher(content);
                        if (m.find()) {
                            doc.add(new StringField("computer", content.substring(m.end(), content.indexOf("\n", m.end())).trim(), Field.Store.YES));
                        }
                        m = userPattern.matcher(content);
                        if (m.find()) {
                            doc.add(new StringField("user", content.substring(m.end(), content.indexOf("\n", m.end())).trim(), Field.Store.YES));
                        }
                    }
//                    content = content.replaceAll(
//                            "[\u0001\u0002\u0003\u0004\u0005\u0006\u0007"
//                            + "\u0008\u0009\u000B\u000C\u000E\u000F\u0010\u0011\u0012\u0013"
//                            + "\u0014\u0015\u0016\u0017\u0019\u001A\u001B"
//                            + "\u001C\u001D\u001E\u0030\u2010\u0086\u0088"
//                            + "\u008C\u008E\u0090\u0099\u009A\u009B\u009E"
//                            + "\u009D\u0098\u0095\u009C\u0084\u0082\u0081"
//                            + "\u0087\u009F\u008b\u0083\u008F\ufffd]", " ");
                    content = content.replaceAll("\n\r", "\n");
                    content = content.replaceAll("\r\n", "\n");
                    content = content.replaceAll("\r", "\n");
                    content = content.replaceAll("\n[ >|]+", "\n");
                    content = content.replaceAll("\n\n[\n]+", "\n\n");
                    String[] blocks = content.split("\n\n");
                    String indexContent = "";
                    HashCollector hc = new HashCollector();
                    for (String block : blocks) {
                        block = block.trim();
                        if (block.length() > 2) {
                            hc.docIds.clear();
                            String hash = getHash(block.getBytes("UTF-8"));
                            hc.docIds.clear();
                            hashSearcher.search(parser.parse("contents: " + "\"" + hash + "\""), hc);
                            if (hc.docIds.cardinality() < 10) {
                                if (hc.docIds.cardinality() > 1) {
                                    if (indexed.containsKey(hash)) {
                                        doc.add(new LongField("hashthread", indexed.get(hash), Field.Store.YES));
                                    } else {
                                        indexContent += block + " ";
                                        indexed.put(hash, threadId++);
                                    }
                                }
                            }
//                            System.out.println(block);
//                            System.out.println(hash + " " + hc.docIds.cardinality());
//                            System.out.println();
//                    System.out.println(block);
//                    System.out.println();
                        }
                    }
//                    doc.add(new Field("contents", indexContent, storeAll));
                    doc.add(new Field("contents", content, storeAll));
//                    System.out.println(file.getPath() + "\n" + indexContent);
                    // Now add the entities to the index (Slow so comment out if
                    // not needed
                    HashMap<String, String> entList = ee.extract(indexContent);
                    for (String key : entList.keySet()) {
//                        System.out.println(key + " " + entList.get(key));
                        doc.add(new Field(key, entList.get(key), storeAll));
                    }
//          doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(fis, "UTF-8"))));

                    if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                        // New index, so we just add the document (no old document can be there):
                        //                      System.out.println("adding " + file);
                        writer.addDocument(doc);
                    } else {
                        // Existing index (an old copy of this document may have been indexed) so 
                        // we use updateDocument instead to replace the old one matching the exact 
                        // path, if present:
                        //                      System.out.println("updating " + file);
                        writer.updateDocument(new Term("path", file.getPath()), doc);
                    }
                    if (uid % 1000 == 0) {
                        System.out.println(uid);
                    }
                } catch (MessagingException ex) {
                    System.out.println(ex + ": " + file.getPath());
                } catch (org.apache.lucene.queryparser.classic.ParseException ex) {
                    Logger.getLogger(IndexFiles.class.getName()).log(Level.SEVERE, null, ex);
                } finally {
                    fis.close();
                }
            }
        }
    }

    static void indexHashes(IndexWriter writer, File file)
            throws IOException {
        // do not try to index files that cannot be read
        if (file.canRead()) {
            if (file.isDirectory()) {
                System.out.println("Indexing: " + file.getPath());
                String[] files = file.list();
                // an IO error could occur
                if (files != null) {
                    for (int i = 0; i < files.length; i++) {
                        indexHashes(writer, new File(file, files[i]));
                    }
                }
            } else {
                MimeMessage message = null;
                MimeMessageParser mmp = null;
                Enumeration headers;
                FileInputStream fis = null;
                Session s;
                try {
                    s = Session.getDefaultInstance(new Properties());
                    fis = new FileInputStream(file);
                    message = new MimeMessage(s, fis);
                    mmp = new MimeMessageParser(message);
                    try {
                        mmp.parse();
                    } catch (Exception e) {
                        System.err.println("Could not parse file: " + file.getPath());
                    }
                    headers = message.getAllHeaders();
                    while (headers.hasMoreElements()) {
                        Header h = (Header) headers.nextElement();
                    }
                } catch (FileNotFoundException fnfe) {
                    // at least on windows, some temporary files raise this exception with an "access denied" message
                    // checking if the file can be read doesn't help
                    return;
                } catch (MessagingException ex) {
                    // If we cant open this as an email just ignore the file.
                    return;
                } catch (Exception ex) {
                    Logger.getLogger(IndexFiles.class.getName()).log(Level.SEVERE, null, ex);
                }

                try {
                    // make a new, empty document
                    Document doc = new Document();
                    FieldType storeAll = new FieldType();
                    storeAll.setStoreTermVectors(true);
                    storeAll.setStoreTermVectorPositions(true);
                    storeAll.setIndexed(true);
                    storeAll.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
                    storeAll.setStored(true);
                    // Give each doc a new unique field identifier.
                    doc.add(new LongField("uid", uid++, Field.Store.YES));
                    // Add the path of the file as a field named "path".  Use a
                    // field that is indexed (i.e. searchable), but don't tokenize 
                    // the field into separate words and don't index term frequency
                    // or positional information:
                    Field pathField = new Field("path", file.getPath(), storeAll);
                    doc.add(pathField);

                    // Add the contents of the file to a field named "contents".  Specify a Reader,
                    // so that the text of the file is tokenized and indexed, but not stored.
                    // Note that FileReader expects the file to be in UTF-8 encoding.
                    // If that's not the case searching for special characters will fail.
                    SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z");
                    headers = message.getAllHeaders();
                    while (headers.hasMoreElements()) {
                        Header h = (Header) headers.nextElement();
                        if (h.getName().equalsIgnoreCase("Date")) {
                            Date date;
                            try {
                                date = sdf.parse(h.getValue());
                            } catch (ParseException ex) {
                                try {
                                    date = sdf.parse("Wed, 25 Dec 2013 15:03:28 -0600");
                                } catch (ParseException ex1) {
                                    continue;
                                }
                            }
                            Calendar cal = Calendar.getInstance();
                            cal.setTime(date);
                            if (cal.get(Calendar.YEAR) == 1979) {
                                cal.add(Calendar.YEAR, 20);
                                date = cal.getTime();
                            }
                            doc.add(new LongField(h.getName(), date.getTime(), Field.Store.YES));
                        } else {
                            if (h.getName().equalsIgnoreCase("Subject")) {
                                doc.add(new Field("Subject", getHashes(h.getValue().toString()), storeAll));
                            }
                        }
                    }
                    String content = "";
//                    String contentType = "";
//                    Multipart mp;
                    try {
//                        if (message.getContentType().toLowerCase().contains("multipart")) {
//                            content = "";
//                            contentType = "text";
//                            mp = (Multipart) message.getContent();
//                            int nextPart = 0;
//                            while (nextPart < mp.getCount()) {
////                                if (mp.getBodyPart(nextPart).getContentType().toLowerCase().contains("multipart")) {
////                                if (mp.getBodyPart(nextPart).isMimeType("multipart")) {
////                                    mp = (Multipart) mp.getBodyPart(nextPart).getContent();
////                                    nextPart = 0;
//////                                } else if (mp.getBodyPart(nextPart).getContentType().toLowerCase().contains("text")) {
//                                if (mp.getBodyPart(nextPart).getContentType().toLowerCase().contains("text")) {
//                                    content = (String) mp.getBodyPart(nextPart).getContent();
//                                    contentType = mp.getBodyPart(nextPart).getContentType().toLowerCase();
//                                    break;
//                                } else {
//                                    nextPart++;
//                                }
//                            }
//                        } else {
//                            content = (String) message.getContent();
//                            contentType = message.getContentType().toLowerCase();
//                        }
//                        if (contentType.contains("html")) {
//                            content = Jsoup.parse(content).text();
//                        }
                        if (mmp.hasPlainContent()) {
                            content = mmp.getPlainContent();
                        } else if (mmp.hasHtmlContent()) {
                            content = Jsoup.parse(mmp.getHtmlContent()).text();
                        }
                    } catch (Exception e) {
                        System.err.println(e);
                        System.err.println(file.getPath());
                        content = "Failed to read content";
                    }
//                    content = content.replaceAll(
//                            "[\u0001\u0002\u0003\u0004\u0005\u0006\u0007"
//                            + "\u0008\u0009\u000B\u000C\u000E\u000F\u0010\u0011\u0012\u0013"
//                            + "\u0014\u0015\u0016\u0017\u0019\u001A\u001B"
//                            + "\u001C\u001D\u001E\u0030\u2010\u0086\u0088"
//                            + "\u008C\u008E\u0090\u0099\u009A\u009B\u009E"
//                            + "\u009D\u0098\u0095\u009C\u0084\u0082\u0081"
//                            + "\u0087\u009F\u008b\u0083\u008F\ufffd]", " ");
//                    System.out.println(content);
                    content = content.replaceAll("\n\r", "\n");
                    content = content.replaceAll("\r\n", "\n");
                    content = content.replaceAll("\r", "\n");
                    content = content.replaceAll("\n[>|]+", "\n");
                    content = content.replaceAll("\n\n[\n]+", "\n\n");
//                    System.out.println(content);
                    doc.add(new Field("contents", getHashes(content), storeAll));
                    // Now add the entities to the index (Slow so comment out if
                    // not needed
//          doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(fis, "UTF-8"))));

                    if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                        // New index, so we just add the document (no old document can be there):
                        //                      System.out.println("adding " + file);
                        writer.addDocument(doc);
                    } else {
                        // Existing index (an old copy of this document may have been indexed) so 
                        // we use updateDocument instead to replace the old one matching the exact 
                        // path, if present:
                        //                      System.out.println("updating " + file);
                        writer.updateDocument(new Term("path", file.getPath()), doc);
                    }
                    if (uid % 1000 == 0) {
                        System.out.println(uid);
                    }
                } catch (MessagingException ex) {
                    System.out.println(ex + ": " + file.getPath());
                } finally {
                    fis.close();
                }
            }
        }
    }

    public static String getHashes(String s) {
        String hashes = "";
        try {
            String[] blocks = s.split("\n\n");
            for (String block : blocks) {
                block = block.trim();
                if (block.length() > 2) {
                    hashes += getHash(block.getBytes("UTF-8")) + " ";
//                    System.out.println(block);
//                    System.out.println();
                }
            }
            return hashes;
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(IndexFiles.class.getName()).log(Level.SEVERE, null, ex);
        }
        return hashes.trim();
    }

    public static String getHash(byte[] stuff) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
            Logger.getLogger(IndexFiles.class.getName()).log(Level.SEVERE, null, ex);
        }
        return ("\"" + bytesToHexString(md.digest(stuff)) + "\"");
    }

    public static String bytesToHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return (sb.toString());
    }

    static void createHashThreads(IndexWriter hashWriter,
            IndexReader hashReader, IndexSearcher hashSearcher, QueryParser parser)
            throws IOException {
        indexed.clear();
        FieldType storeAll = new FieldType();
        storeAll.setStoreTermVectors(true);
        storeAll.setStoreTermVectorPositions(true);
        storeAll.setIndexed(true);
        storeAll.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        storeAll.setStored(true);
        long threadId = 0;
        HashCollector hc = new HashCollector();
        for (int i = 0; i < hashReader.maxDoc(); i++) {
            if (i % 1000 == 0) {
                System.out.println(i);
            }
            Document doc = hashReader.document(i);
            String[] hashes = doc.getValues("contents");
            if (hashes.length > 0) {
                for (String hash : hashes[0].split(" ")) {
                    try {
                        hc.docIds.clear();
                        hashSearcher.search(parser.parse("contents: " + "\"" + hash + "\""), hc);
                        if (hc.docIds.cardinality() > 1) {
                            if (indexed.containsKey(hash)) {
                                doc.add(new LongField("hashthread", indexed.get(hash), Field.Store.YES));
                            } else {
                                indexed.put(hash, threadId++);
                            }
                        }
                    } catch (org.apache.lucene.queryparser.classic.ParseException ex) {
                        Logger.getLogger(IndexFiles.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
            hashWriter.addDocument(doc);
        }
    }

    static void createLinkDir(IndexWriter writer, IndexReader reader) {
        //First build a hashmap of bitsets for the links
        FieldType storeAll = new FieldType();
        storeAll.setStoreTermVectors(true);
        storeAll.setStoreTermVectorPositions(true);
        storeAll.setIndexed(true);
        storeAll.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        storeAll.setStored(true);
        FieldType storeB = new FieldType();
        storeB.setStoreTermVectors(false);
        storeB.setStoreTermVectorPositions(false);
        storeB.setIndexed(false);
        storeB.setStored(true);
        storeB.setDocValueType(FieldInfo.DocValuesType.BINARY);
        FieldType storeMB = new FieldType();
        storeMB.setStoreTermVectors(false);
        storeMB.setStoreTermVectorPositions(false);
        storeMB.setIndexed(false);
        storeMB.setStored(true);
//        storeMB.setDocValueType(FieldInfo.DocValuesType.BINARY);
        IndexSearcher searcher = new IndexSearcher(reader);
        String[] fieldList = {"To", "From", "Cc", "Bcc", "to", "from", "cc", "bcc"};
        MultiFieldQueryParser mParser
                = new MultiFieldQueryParser(Version.LUCENE_40, fieldList,
                        new ClassicAnalyzer(Version.LUCENE_40, stopWords));
        mParser.setDefaultOperator(OR_OPERATOR);
        TermsEnum userI = null;
        Set<String> allUsers = new HashSet<>();
        Map<String, Set<String>> userLinks = new HashMap<>();
        System.out.println("Starting link build");
        try {
            for (int nextId = 0; nextId < reader.maxDoc(); nextId++) {
                Set<String> eUsers = new HashSet<>();
                for (String field : fieldList) {
                    Terms users = reader.getTermVector(nextId, field);
                    if (users != null) {
                        BytesRef user;
                        userI = users.iterator(userI);
                        while ((user = userI.next()) != null) {
                            if (user.utf8ToString().contains("@") || 
                                    user.utf8ToString().toLowerCase().contains("guerrero")) {
                                allUsers.add(user.utf8ToString());
                                eUsers.add(user.utf8ToString());
                            }
                        }
                    }
                }
                for (String eUser : eUsers) {
                    if (!userLinks.containsKey(eUser)) {
                        userLinks.put(eUser, new HashSet(eUsers));
                    } else {
                        userLinks.get(eUser).addAll(eUsers);
                    }
                }
            }
//            for (String key : userLinks.keySet()) {
//                System.out.println(key);
//                int count = 0;
//                for (String link : userLinks.get(key)) {
//                    System.out.println("   " + count++ + " " + link);
//                }
//            }
            int docCount = 0;
            for (String user : allUsers) {
                Document doc = new Document();
                sCollector sc = new sCollector();
                doc.add(new Field("user", user, storeAll));
                Query q = mParser.parse("\"" + user + "\"");
                searcher.search(q, sc);
                doc.add(new IntField("emailcount", sc.docIds.cardinality(), Field.Store.YES));
                doc.add(new Field("emails", new BytesRef(sc.docIds.toByteArray()), storeB));
                BytesRef[] tRef = new BytesRef[userLinks.get(user).size()];
                Iterator<String> iter = userLinks.get(user).iterator();
                for (int i = 0; i < tRef.length; i++) {
                    doc.add(new Field("links", new BytesRef(iter.next()), storeMB));
                }
                if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                    // New index, so we just add the document (no old document can be there):
                    //                      System.out.println("adding " + file);
                    writer.addDocument(doc);
                } else {
                    // Existing index (an old copy of this document may have been indexed) so 
                    // we use updateDocument instead to replace the old one matching the exact 
                    // path, if present:
                    //                      System.out.println("updating " + file);
                    writer.updateDocument(new Term("user", user), doc);
                }
                if (++docCount % 100 == 0) {
                    System.out.println(docCount);
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(IndexFiles.class.getName()).log(Level.SEVERE, null, ex);
        } catch (org.apache.lucene.queryparser.classic.ParseException ex) {
            Logger.getLogger(IndexFiles.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    static void extractAttachments(IndexReader reader, File attachmentDirectory, String emailDir) {
        System.out.println("Extracting Attachments");
        try {
            if (attachmentDirectory.exists()) {
                attachmentDirectory.delete();
            }
            attachmentDirectory.mkdir();
            for (int nextId = 0; nextId < reader.maxDoc(); nextId++) {
                if (nextId % 1000 == 0) {
                    System.out.println(nextId);
                }
                Document doc = reader.document(nextId);
                String fileName = doc.get("path");
                Session s = Session.getDefaultInstance(new Properties());
                InputStream is = new FileInputStream(fileName);
                MimeMessage message = new MimeMessage(s, is);
                MimeMessageParser mp = new MimeMessageParser(message);
                is.close();
                try {
                    mp.parse();
                } catch (Exception ex) {
                    System.err.println("Can not parse file: " + doc.get("path"));
                }
                if (mp.hasAttachments()) {
                    File f = new File(fileName);
                    File extractDir = new File(attachmentDirectory, fileName.substring(emailDir.length()));
                    extractDir.mkdirs();
                    Enumeration headers = message.getAllHeaders();
                    int attachmentCount = 0;
                    String sName = "";
                    for (DataSource ds : mp.getAttachmentList()) {
                        if (ds.getName() == null) {
                            sName = "attachment" + attachmentCount++ + "."
                                    + ds.getContentType().substring(ds.getContentType().indexOf("/") + 1);
                        } else {
                            sName = attachmentCount++ + ds.getName();
                        }
                        sName = sName.replaceAll("/", "");
                        InputStream ist = ds.getInputStream();
                        FileOutputStream fos = new FileOutputStream(
                                new File(extractDir, sName));
                        IOUtils.copy(ist, fos);
                        ist.close();
                        fos.close();
                    }
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
            System.out.println(ex);
        } catch (MessagingException ex) {
            System.out.println(ex);
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
