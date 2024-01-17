import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;

import org.json.JSONObject;
import org.json.JSONArray;

public class GetData {

    static String prefix = "project3.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding 
    // tables in your database
    String userTableName = null;
    String friendsTableName = null;
    String cityTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;

    // DO NOT modify this constructor
    public GetData(String u, Connection c) {
        super();
        String dataType = u;
        oracleConnection = c;
        userTableName = prefix + dataType + "_USERS";
        friendsTableName = prefix + dataType + "_FRIENDS";
        cityTableName = prefix + dataType + "_CITIES";
        currentCityTableName = prefix + dataType + "_USER_CURRENT_CITIES";
        hometownCityTableName = prefix + dataType + "_USER_HOMETOWN_CITIES";
    }

    // TODO: Implement this function
    @SuppressWarnings("unchecked")
    public JSONArray toJSON() throws SQLException {

        // This is the data structure to store all users' information
        JSONArray users_info = new JSONArray();
        try (Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            Statement fstmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            Statement cstmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            Statement htmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            ResultSet userInfo = stmt.executeQuery(
                "SELECT DISTINCT user_id, first_name, last_name, year_of_birth,month_of_birth, day_of_birth, gender " +
                "FROM " + userTableName 
            );
            while(userInfo.next()){
                int uid = userInfo.getInt(1);
                String first_name = userInfo.getString(2);
                String last_name = userInfo.getString(3);
                String gender = userInfo.getString(7);
                int YOB = userInfo.getInt(4);
                int MOB = userInfo.getInt(5);
                int DOB = userInfo.getInt(6);
                ResultSet f = fstmt.executeQuery("SELECT DISTINCT f.user1_id " +
                "FROM " + friendsTableName + " f " +
                "WHERE f.user2_id = " + uid + " " +
                "UNION ALL SELECT DISTINCT f2.user2_id " + 
                "FROM " + friendsTableName + " f2 " +
                "WHERE f2.user1_id = " + uid
                );
                JSONArray friendlist = new JSONArray();
                while(f.next()){
                    int f_id = f.getInt(1);
                    friendlist.put(f_id);
                }
                f.close();
                ResultSet c = cstmt.executeQuery("SELECT c.city_name, c.state_name, c.country_name " +
                "FROM " + cityTableName + " c " +
                "JOIN " + currentCityTableName + " c2 ON c2.current_city_id = c.city_id " +
                "WHERE c2.user_id = " + uid
                );
                JSONObject current = new JSONObject();
                if(c.next()){
                    current.put("country",c.getString("country_name"));
                    current.put("city", c.getString("city_name"));
                    current.put("state", c.getString("state_name"));
                }
                c.close();
                ResultSet h = htmt.executeQuery("SELECT c.city_name, c.state_name, c.country_name " +
                "FROM " + cityTableName + " c " +
                "JOIN " + hometownCityTableName + " h ON h.hometown_city_id = c.city_id " +
                "WHERE h.user_id = " + uid
                );
                JSONObject hometown = new JSONObject();
                if(h.next()){
                    hometown.put("country",h.getString("country_name"));
                    hometown.put("city", h.getString("city_name"));
                    hometown.put("state", h.getString("state_name"));
                }
                h.close();
                JSONObject complete = new JSONObject();
                complete.put("MOB", MOB);
                complete.put("hometown",  hometown);
                complete.put("current", current);
                complete.put("gender", gender);
                complete.put("user_id", uid);
                complete.put("DOB", DOB);
                complete.put("last_name", last_name);
                complete.put("first_name", first_name);
                complete.put( "YOB", YOB);
                complete.put("friends", friendlist);
                users_info.put(complete);
            }
            
            userInfo.close();
            
            //prob need to close other stmt
            fstmt.close();
            cstmt.close();
            htmt.close();
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return users_info;
    }

    // This outputs to a file "output.json"
    // DO NOT MODIFY this function
    public void writeJSON(JSONArray users_info) {
        try {
            FileWriter file = new FileWriter(System.getProperty("user.dir") + "/output.json");
            file.write(users_info.toString());
            file.flush();
            file.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}