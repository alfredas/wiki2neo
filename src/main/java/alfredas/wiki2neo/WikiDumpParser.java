package alfredas.wiki2neo;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WikiDumpParser {

    Pattern pageStartPattern = Pattern.compile("<page>");
    Pattern pageEndPattern = Pattern.compile("</page>");
    Pattern titlePattern = Pattern.compile("<title>(.*)</title>");
    Pattern idPattern = Pattern.compile("<id>(.*)</id>");
    Pattern timestampPattern = Pattern.compile("<timestamp>(.*)T(.*)</timestamp>");
    Pattern usernamePattern = Pattern.compile("<username>(.*)</username>");
    Pattern revisionStartPattern = Pattern.compile("<revision>");
    Pattern revisionEndPattern = Pattern.compile("</revision>");
    Pattern userStartPattern = Pattern.compile("<contributor>");
    Pattern userEndPattern = Pattern.compile("</contributor>");

    WikiPage page = null;
    Revision revision = null;
    User user = null;

    WikiToNeoWriter writer;

    public WikiDumpParser(WikiDumpReader reader, WikiToNeoWriter writer) {
        this.writer = writer;
        for (String line : reader) {
            parse(line);
        }
    }

    private void parse(String line) {
        // page start
        if (pageStartPattern.matcher(line).find()) {
            page = new WikiPage();
        } else if (page != null) {
            // in page
            if (idPattern.matcher(line).find() && revision == null) {
                page.id = val(idPattern.matcher(line));
            } else if (titlePattern.matcher(line).find() && revision == null) {
                page.title = val(titlePattern.matcher(line));
            } else if (revisionStartPattern.matcher(line).find() && revision == null) {
                revision = new Revision();
            } else if (revision != null) {
                // in revision
                if (idPattern.matcher(line).find() && user == null) {
                    revision.id = val(idPattern.matcher(line));
                } else if (timestampPattern.matcher(line).find() && user == null) {
                    revision.timestamp = val(timestampPattern.matcher(line));
                } else if (userStartPattern.matcher(line).find() && user == null) {
                    user = new User();
                } else if (user != null) {
                    // in user
                    if (idPattern.matcher(line).find()) {
                        user.id = val(idPattern.matcher(line));
                    } else if (usernamePattern.matcher(line).find()) {
                        user.username = val(usernamePattern.matcher(line));
                    } else if (userEndPattern.matcher(line).find()) {
                        revision.user = user;
                        user = null;
                    }
                } else if (revisionEndPattern.matcher(line).find()) {
                    page.revisions.add(revision);
                    revision = null;
                }
            } else if (pageEndPattern.matcher(line).find()) {
                writer.createPage(page);
                page = null;
            }
        }
    }

    private String val(Matcher m) {
        m.find();
        return m.group(1);
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
