package alfredas.wiki2neo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class MysqlReader {

    Connection con = null;

    public List<String> findCategoriesForPage(String pageid) {

        if (con == null) {
            init();
        }

        List<String> results = new ArrayList<String>();

        String query = "SELECT `cl_to` FROM `categorylinks` WHERE `cl_from`=" + pageid;
        Statement st = null;
        ResultSet rs = null;

        try {
            st = con.createStatement();
            rs = st.executeQuery(query);

            while (rs.next()) {
                results.add(rs.getString(1));
            }

        } catch (SQLException ex) {

            log(ex.getMessage());

        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (st != null) {
                    st.close();
                }
            } catch (SQLException ex) {
                log(ex.getMessage());
            }
        }
        return results;
    }

    private void init() {
        String url = "jdbc:mysql://localhost:3306/wikipedia";
        String user = "root";
        String password = "root";

        try {
            con = DriverManager.getConnection(url, user, password);
        } catch (SQLException ex) {
            log(ex.getMessage());
        }

    }

    public void close() {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                log(e.getMessage());
            }
        }
    }

    private void log(String message) {
        System.out.println(message);
    }
}
