package team.dsys.dssearch.search;

import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import team.dsys.dssearch.config.SearchConfig;
import team.dsys.dssearch.rpc.Doc;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

@Slf4j
@Component
public class StoreEngine {

    @Autowired
    SearchConfig searchConfig;

    // analyzer for word segmentation
    Analyzer analyzer = new EnglishAnalyzer();

    /**
     * note: This method automatically create index
     */
    public boolean writeDocList(List<Doc> newDocs, int shardId) {
        // build docs
        ArrayList<Document> docList = new ArrayList<>();
        for (Doc newDoc : newDocs) {
            Document doc = new Document();
            doc.add(new StringField("id", String.valueOf(newDoc.getId()), Field.Store.YES));
            doc.add(new TextField("content", newDoc.getContent(), Field.Store.YES));
            docList.add(doc);
        }

        // make sure shard path(Lucene path) exists
        String shardPath = generateShardPath(shardId);

        // add docs to shard(Lucene engine)
        IndexWriterConfig writerConfig = new IndexWriterConfig(analyzer);
        writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        try (
                Directory directory = FSDirectory.open(Paths.get(shardPath));
                IndexWriter writer = new IndexWriter(directory, writerConfig)) {
            writer.addDocuments(docList);
            writer.commit();
            log.info("Stored {} doc(s) on shard {}, node {}", docList, shardId, searchConfig.getNid());
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * note: This method automatically create index
     */
    public boolean writeDoc(Doc newDoc, int shardId) {
        ArrayList<Doc> list = new ArrayList<>();
        list.add(newDoc);
        return writeDocList(list, shardId);
    }

    /**
     * search phase 1
     */
    public List<ScoreDoc> queryTopN(String term, int n, int shardId) {
        return queryDocByCondition("content", term, n, shardId);
    }

    /**
     * search phase 2
     *
     * The result docs are in same order as the given docIds.
     */
    public List<Doc> getDocList(List<Integer> sortedDocIds, int shardId) {
        String shardPath = generateShardPath(shardId);

        try (Directory directory = FSDirectory.open(Paths.get(shardPath))) {
            IndexReader reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);

            List<Doc> sortedDocs = new ArrayList<>();
            for (Integer docId : sortedDocIds) {
                Document doc = searcher.doc(docId);
                Doc d = new Doc(1, Integer.parseInt(doc.get("id")), doc.get("content"));
                sortedDocs.add(d);
            }
            return sortedDocs;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private List<ScoreDoc> queryDocByCondition(String fieldName, String term, int topNHits, int shardId)  {
        String shardPath = generateShardPath(shardId);

        try (Directory directory = FSDirectory.open(Paths.get(shardPath))) {
            IndexReader reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);

            // search doc with a certain term in a certain field
            QueryParser queryParser = new QueryParser(fieldName, analyzer);
            Query query = queryParser.parse(term);
            TopDocs resultDocs = searcher.search(query, topNHits);
            log.info("Search {} for '{}', got {} docs!", fieldName, term, resultDocs.scoreDocs.length);

            // get searched docs ids and return
            return new ArrayList<>(Arrays.asList(resultDocs.scoreDocs));
        } catch (IndexNotFoundException e) {
            return Collections.emptyList();
        } catch (IOException | ParseException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    private String generateShardPath(int shardId) {
        String path = String.format("%s/%s/%d/", searchConfig.getIndexLibrary(), searchConfig.getNid(), shardId);
        makeSureDirExists(path);
        return path;
    }

    private void makeSureDirExists(String dirPath) {
        File dir = new File(dirPath);
        if (!dir.exists()) {
            dir.mkdir();
        }
    }
}
