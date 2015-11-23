package alfredas.wiki2neo;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import alfredas.wiki2neo.WikiDumpParser.Revision;
import alfredas.wiki2neo.WikiDumpParser.User;
import alfredas.wiki2neo.WikiDumpParser.WikiPage;

public class WikiToNeoWriter {

    static final String DBPATH = "/home/alfredas/wikipedia-dump/db";

    GraphDatabaseService graphDb;
    String flag;

    public WikiToNeoWriter(String flag) {
        graphDb = new EmbeddedGraphDatabase(DBPATH);
        this.flag = flag;
    }

    public long createOrFindUser(User user) {
        long id = -1;
        Transaction tx = graphDb.beginTx();
        try {
            Index<Node> users = graphDb.index().forNodes("users");

            Node userNode = null;
            try {
                userNode = users.get("username", user.username).getSingle();
            } catch (NullPointerException err) {
            }

            if (userNode == null) {
                userNode = graphDb.createNode();
                userNode.setProperty("username", user.username);
                userNode.setProperty("userid", user.id);
                users.add(userNode, "username", user.username);
            }
            id = userNode.getId();
            tx.success();
        } finally {
            tx.finish();
        }
        return id;
    }

    public long createPage(WikiPage page) {
    	long id = -1;
    	if (flag != null && page.title.equals(flag)) {
    		flag = null;
    		return id;
    	} else if (flag != null) {
    		return id;
    	}
        
        Transaction tx = graphDb.beginTx();
        try {
            Index<Node> pages = graphDb.index().forNodes("pages");

            Node pageNode = graphDb.createNode();
            pageNode.setProperty("title", page.title);
            pageNode.setProperty("pageid", page.id);
            pages.add(pageNode, "title", page.title);
            id = pageNode.getId();

            for (Revision revision : page.getRevisions()) {
                Node revisionNode = graphDb.getNodeById(createRevision(revision));
                Relationship rel = pageNode.createRelationshipTo(revisionNode, DynamicRelationshipType.withName("revision"));
            }
            log("Created page: " + page.title);
            tx.success();
        } finally {
            tx.finish();
        }
        return id;
    }

    public long createRevision(Revision revision) {
        long id = -1;
        Transaction tx = graphDb.beginTx();
        try {
            Node revisionNode = graphDb.createNode();
            revisionNode.setProperty("timestamp", revision.timestamp);
            id = revisionNode.getId();
            if (revision.user != null && revision.user.username != null) {
                Node userNode = graphDb.getNodeById(createOrFindUser(revision.user));
                Relationship rel = revisionNode.createRelationshipTo(userNode, DynamicRelationshipType.withName("user"));
            }

            tx.success();
        } finally {
            tx.finish();
        }
        return id;
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
