package alfredas.wiki2neo;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class DBSync {

    static final String DBPATH1 = "/home/alfredas/wikipedia-dump/db";
    static final String DBPATH2 = "/home/alfredas/wikipedia-dump/db2";

    GraphDatabaseService graphDb1;
    GraphDatabaseService graphDb2;

    public DBSync() {
        graphDb1 = new EmbeddedGraphDatabase(DBPATH1);
        graphDb2 = new EmbeddedGraphDatabase(DBPATH2);
    }

    public void syncDB() {
        Transaction tx2 = graphDb2.beginTx();
        try {
            Index<Node> pages = graphDb2.index().forNodes("pages");

            for (Node p : pages.query("title", "*")) {
                WikiPage page = new WikiPage();
                page.setTitle(p.getProperty("title").toString());
                page.setId(p.getProperty("pageid").toString());
                for (Relationship p_r : p.getRelationships(DynamicRelationshipType.withName("revision"))) {
                    Node rev = p_r.getOtherNode(p);
                    User user = null;
                    for (Relationship r_u : rev.getRelationships(DynamicRelationshipType.withName("user"))) {
                        Node usr = r_u.getOtherNode(rev);
                        user = new User();
                        user.setId(usr.getProperty("userid").toString());

                        user.setUsername(usr.getProperty("username").toString());
                    }
                    Revision revision = new Revision();

                    revision.setTimestamp(rev.getProperty("timestamp").toString());
                    if (user != null) {
                        revision.setUser(user);
                    }
                    page.getRevisions().add(revision);
                }
                createOrFindPage(page);
            }
        } finally {
            tx2.finish();
        }

    }

    public long createOrFindUser(User user) {
        long id = -1;
        Transaction tx = graphDb1.beginTx();
        try {
            Index<Node> users = graphDb1.index().forNodes("users");

            Node userNode = null;
            try {
                userNode = users.get("username", user.username).getSingle();
            } catch (NullPointerException err) {
            }

            if (userNode == null) {
                userNode = graphDb1.createNode();
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

    public long createOrFindPage(WikiPage page) {
        long id = -1;
        Transaction tx = graphDb1.beginTx();
        try {
            Index<Node> pages = graphDb1.index().forNodes("pages");

            Node pageNode = null;
            try {
                pageNode = pages.get("title", page.title).getSingle();
            } catch (NullPointerException err) {
            }

            if (pageNode == null) {
                pageNode = graphDb1.createNode();
                pageNode.setProperty("title", page.title);
                pageNode.setProperty("pageid", page.id);
                pages.add(pageNode, "title", page.title);
            }
            id = pageNode.getId();

            for (Revision revision : page.getRevisions()) {
                Node revisionNode = graphDb1.getNodeById(createRevision(revision));
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
        Transaction tx = graphDb1.beginTx();
        try {
            Node revisionNode = graphDb1.createNode();
            revisionNode.setProperty("timestamp", revision.timestamp);
            id = revisionNode.getId();
            if (revision.user != null && revision.user.username != null) {
                Node userNode = graphDb1.getNodeById(createOrFindUser(revision.user));
                Relationship rel = revisionNode.createRelationshipTo(userNode, DynamicRelationshipType.withName("user"));
            }

            tx.success();
        } finally {
            tx.finish();
        }
        return id;
    }

    public void close() {
        if (graphDb1 != null) {
            graphDb1.shutdown();
        }
    }

    private void log(String msg) {
        System.out.println(msg);
    }

    class WikiPage {
        String title;
        String id;
        List<Revision> revisions = new ArrayList<Revision>();

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public List<Revision> getRevisions() {
            return revisions;
        }

        public void setRevisions(List<Revision> revisions) {
            this.revisions = revisions;
        }
    }

    class Revision {
        String id;
        String timestamp;
        User user;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(String timestamp) {
            this.timestamp = timestamp;
        }

        public User getUser() {
            return user;
        }

        public void setUser(User user) {
            this.user = user;
        }
    }

    class User {
        String id;
        String username;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }
    }

}