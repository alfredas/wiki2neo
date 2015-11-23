package alfredas.wiki2neo;

public class Runner {

    public static void mainBck(String[] args) {
        String filename = args[0];
        try {
            if (filename.equals("sync")) {
                DBSync sync = new DBSync();
                sync.syncDB();
            } else {
                WikiToNeoWriter writer = new WikiToNeoWriter(null);
                WikiDumpParser parser = new WikiDumpParser(new WikiDumpReader(filename), writer);
                writer.close();
                Process rm = Runtime.getRuntime().exec("rm -f " + filename);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {
        WikiCategoryWriter wcw = new WikiCategoryWriter();
        wcw.iteratePages();
    }

}
