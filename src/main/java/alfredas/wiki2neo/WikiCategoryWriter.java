package alfredas.wiki2neo;

import java.util.List;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class WikiCategoryWriter {

    static final String DBPATH = "/media/data/wikidb/db";

    GraphDatabaseService graphDb;
    MysqlReader categoryReader;
    String lastPage = "5622358";
    boolean write = false;

    public WikiCategoryWriter() {
        graphDb = new EmbeddedGraphDatabase(DBPATH);
        categoryReader = new MysqlReader();
    }

    public void iteratePages() {
        // Transaction tx = graphDb.beginTx();
        IndexHits<Node> nodes = null;
        try {
            Index<Node> pages = graphDb.index().forNodes("pages");
            // get all pages
            nodes = pages.query("title", "*");
            for (Node node : nodes) {
                Object pid = node.getProperty("pageid", null);
                if (pid != null) {
                    String pageid = pid.toString();
                    if (!write && lastPage.equals(pageid)) {
                        write = true;
                        continue;
                    }
                    if (write) {
                        List<String> categories = categoryReader.findCategoriesForPage(pageid);
                        createCategoryLinksForPage(node, categories);
                        log(pageid);
                    }
                }
            }
            // tx.success();
        } finally {
            if (nodes != null) {
                nodes.close();
            }
            categoryReader.close();
            close();
            // tx.finish();
        }
    }

    private long createOrFindCaterogy(String category) {
        long id = -1;
        Transaction tx = graphDb.beginTx();
        try {
            Index<Node> categories = graphDb.index().forNodes("categories");

            Node categoryNode = null;
            try {
                categoryNode = categories.get("title", category).getSingle();
            } catch (NullPointerException err) {
            }

            if (categoryNode == null) {
                categoryNode = graphDb.createNode();
                categoryNode.setProperty("title", category);
                categories.add(categoryNode, "title", category);
            }
            id = categoryNode.getId();
            tx.success();
        } finally {
            tx.finish();
        }
        return id;
    }

    public void createCategoryLinksForPage(Node pageNode, List<String> categories) {
        Transaction tx = graphDb.beginTx();
        try {
            for (String category : categories) {
                Node categoryNode = graphDb.getNodeById(createOrFindCaterogy(category));
                if (categoryNode != null) {
                    Relationship rel = pageNode.createRelationshipTo(categoryNode, DynamicRelationshipType.withName("category"));
                }
            }
            tx.success();
        } finally {
            tx.finish();
        }
    }

    public void close() {
        if (graphDb != null) {
            graphDb.shutdown();
        }
    }

    private void log(String msg) {
        System.out.println(msg);
    }

}
