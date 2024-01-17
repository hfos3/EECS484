package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }

    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                    "SELECT COUNT(*) AS Birthed, Month_of_Birth " + // select birth months and number of uses with that birth month
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth IS NOT NULL " + // for which a birth month is available
                            "GROUP BY Month_of_Birth " + // group into buckets by birth month
                            "ORDER BY Birthed DESC, Month_of_Birth ASC"); // sort by users born in that month, descending; break ties by birth month

            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) { // step through result rows/records one by one
                if (rst.isFirst()) { // if first record
                    mostMonth = rst.getInt(2); //   it is the month with the most
                }
                if (rst.isLast()) { // if last record
                    leastMonth = rst.getInt(2); //   it is the month with the least
                }
                total += rst.getInt(1); // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);

            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + mostMonth + " " + // born in the most popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + leastMonth + " " + // born in the least popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close(); // if you close the statement first, the result set gets closed automatically

            return info;

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }

    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
                
                ResultSet rst = stmt.executeQuery(
                      "SELECT COUNT(*) AS NoName, First_Name " + 
                            "FROM " + UsersTable + " " + 
                            "GROUP BY First_Name " + 
                            "ORDER BY NoName DESC, First_Name ASC"
                        ); 
                            
            //System.out.println("HERE");
            FirstNameInfo info = new FirstNameInfo();
            if (rst.next()) {
                int most = rst.getInt(1); // get and store max val
                String temp = rst.getString(2);
                //System.out.println(rst.getString(2));
                info.addCommonName(temp);
                info.setCommonNameCount(most);
                while (rst.next() && rst.getInt(1) == most) { // step through names and get all ties
                    //System.out.println(rst.getString(2));
                    info.addCommonName(rst.getString(2));
                }
            }
            

            rst = stmt.executeQuery(
                "SELECT MIN(LENGTH(First_Name)),MAX(LENGTH(First_Name)) " + // select birth months and number of uses with that birth month
                    "FROM " + UsersTable // from all users
            );
            int min_length = 0, max_length = 0; // declare and initialize min and max ints
            if (rst.next()) { // store min and max lengths
                min_length = rst.getInt(1);
                max_length = rst.getInt(2);      
            }

            rst = stmt.executeQuery(//can try selecting only names w certain length
                      "SELECT First_Name " + // select birth months and number of uses with that birth month
                            "FROM " + UsersTable + " " + // from all users
                            "GROUP BY First_Name " + 
                            "ORDER BY First_Name ASC"
                            );
            while(rst.next()){
                String name = rst.getString(1);
                if (name.length() == min_length){
                    info.addShortName(name);
                }
                if (name.length() == max_length){
                    info.addLongName(name);
                }
            }
            rst.close();
            stmt.close();
            return info;
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FirstNameInfo info = new FirstNameInfo();
                info.addLongName("Aristophanes");
                info.addLongName("Michelangelo");
                info.addLongName("Peisistratos");
                info.addShortName("Bob");
                info.addShortName("Sue");
                info.addCommonName("Harold");
                info.addCommonName("Jessica");
                info.setCommonNameCount(42);
                return info;
            */
    }

    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
                ResultSet rst = stmt.executeQuery(
                      "SELECT u.user_id, u.first_name, u.last_name " + 
                            "FROM " + UsersTable + " u " + 
                            "WHERE u.user_id NOT IN (SELECT f1.user1_id FROM " + FriendsTable + " f1) " + 
                            "AND u.user_id NOT IN (SELECT f2.user2_id FROM " + FriendsTable + " f2) " + 
                            "ORDER BY u.user_id ASC"
                        ); 
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
                UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
                results.add(u1);
                results.add(u2);
            */
            while (rst.next()) {
                UserInfo temp = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
                results.add(temp);
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
                ResultSet rst = stmt.executeQuery(
                      "SELECT pu.user_id, pu.first_name, pu.last_name " + 
                            "FROM " + UsersTable + " pu " + 
                            "JOIN " + HometownCitiesTable + " h ON h.user_id = pu.user_id " + 
                            "JOIN " + CurrentCitiesTable + " c ON c.user_id = pu.user_id " + 
                            "WHERE c.current_city_id != h.hometown_city_id " + 
                            "ORDER BY user_id ASC" 
                        ); 

            while (rst.next()) {
                UserInfo temp = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
                results.add(temp);
            }
            rst.close();
            stmt.close();
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
                UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
                results.add(u1);
                results.add(u2);
            */
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return results;
    }

    @Override
    // Query 4 : 
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly);
                Statement inner = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
                ResultSet rst = stmt.executeQuery(
                      "SELECT COUNT(*) AS NumTagged, p.photo_id, p.album_id, p.photo_link, a.album_name " + 
                            "FROM " + TagsTable + " t " + 
                            "JOIN " + PhotosTable + " p ON p.photo_id = t.tag_photo_id " + 
                            "JOIN " + AlbumsTable + " a ON p.album_id = a.album_id " + 
                            "GROUP BY p.photo_id, p.album_id, p.photo_link, a.album_name " + 
                            "ORDER BY NumTagged DESC, p.photo_id ASC" 
                        ); 
            
            while (num > 0 && rst.next()) {
                PhotoInfo p = new PhotoInfo(rst.getInt(2), rst.getInt(3), rst.getString(4), rst.getString(5));
                ResultSet rst2 = inner.executeQuery(
                      "SELECT u.user_id, u.first_name, u.last_name " + 
                            "FROM " + UsersTable + " u " + 
                            "JOIN " + TagsTable + " t ON t.tag_subject_id = u.user_id " + 
                            "WHERE t.tag_photo_id = " + rst.getInt(2) + " " +
                            "ORDER BY u.user_id ASC" 
                        );
                        TaggedPhotoInfo tp = new TaggedPhotoInfo(p); 
                        while(rst2.next()) {
                            UserInfo u1 = new UserInfo(rst2.getInt(1), rst2.getString(2), rst2.getString(3));
                            tp.addTaggedUser(u1);
                        }   
                        rst2.close();   
                results.add(tp);
                --num;
            }
            inner.close();
            rst.close();
            stmt.close();
            

            
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
                UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
                UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
                UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                tp.addTaggedUser(u1);
                tp.addTaggedUser(u2);
                tp.addTaggedUser(u3);
                results.add(tp);
            */

        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 5 : NOT SURE HOW TO SORT SO USER 1 ID < USER 2 ID (LOOK AT SQL CODE)
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly);
                Statement inner = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
                ResultSet rst = stmt.executeQuery(
                      "SELECT u.user_id, u.first_name, u.last_name, u.year_of_birth, u2.user_id AS U2ID, u2.first_name AS U2F, u2.last_name AS U2L, u2.year_of_birth AS U2Y, COUNT(*) AS count " + 
                            "FROM " + UsersTable + " u " + 
                            "JOIN " + UsersTable + " u2 ON u.gender = u2.gender AND u.gender IS NOT NULL AND u2.gender IS NOT NULL AND u.user_id != u2.user_id AND u.user_id < u2.user_id " + 
                            "JOIN " + TagsTable + " t ON t.tag_subject_id = u.user_id " + 
                            "JOIN " + TagsTable + " t2 ON t2.tag_photo_id = t.tag_photo_id AND t2.tag_subject_id = u2.user_id " +
                            "WHERE ABS(u.year_of_birth - u2.year_of_birth) <= " + yearDiff +  " " +
                            "AND (u.user_id, u2.user_id) NOT IN ( " + 
                            "SELECT f.user1_id, f.user2_id " + 
                            "FROM " + FriendsTable + " f " + 
                            "WHERE (f.user1_id = u.user_id AND f.user2_id = u2.user_id) OR " + 
                            "(f.user2_id = u.user_id AND f.user1_id = u2.user_id) " + 
                            " ) " + 
                            "GROUP BY u.user_id, u.first_name, u.last_name, u.year_of_birth, u2.user_id, u2.first_name, u2.year_of_birth, u2.last_name " + 
                            "ORDER BY count DESC, u.user_id ASC, u2.user_id ASC" 
                        );

            while (num > 0 && rst.next()) {
            
                UserInfo u1 = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
                UserInfo u2 = new UserInfo(rst.getInt(5), rst.getString(6), rst.getString(7));  
                MatchPair mp = new MatchPair(u1, rst.getInt(4), u2, rst.getInt(8));
                results.add(mp);

                ResultSet rst2 = inner.executeQuery(
                    "SELECT t.tag_photo_id, p.photo_link, p.album_id, a.album_name " + 
                            "FROM " + TagsTable + " t " + 
                            "JOIN " + TagsTable + " t2 ON t.tag_photo_id = t2.tag_photo_id " + 
                            "JOIN " + PhotosTable + " p ON p.photo_id = t.tag_photo_id " + 
                            "JOIN " + AlbumsTable + " a ON p.album_id = a.album_id " + 
                            "WHERE t.tag_subject_id = " + rst.getInt(1) + " AND " + "t2.tag_subject_id = " + rst.getInt(5) + " " + 
                            "ORDER BY t.tag_photo_id ASC" 
                );
                
                while(rst2.next()) {
                    
                    PhotoInfo p = new PhotoInfo(rst2.getInt(1), rst2.getInt(3), rst2.getString(2), rst2.getString(4));
                    mp.addSharedPhoto(p);
                    
                } // end of second while
                
                --num;

                rst2.close();
         
            } // end of first while
            inner.close();
            rst.close();
            stmt.close();
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
                UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
                MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
                PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
                mp.addSharedPhoto(p);
                results.add(mp);
            */
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly);
                Statement inner = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
                int temp = stmt.executeUpdate(
                    "CREATE VIEW Friendship AS " +
                    "SELECT f.user1_id, f.user2_id " +
                    "FROM " + FriendsTable + " f " +
                    "UNION " +
                    "SELECT f2.user2_id, f2.user1_id " +
                    "FROM " + FriendsTable + " f2"
                );
                temp = stmt.executeUpdate(
                            "CREATE VIEW Mutuals AS " +
                            "SELECT * FROM ( " + 
                            "SELECT DISTINCT f1.user1_id AS user1, f2.user1_id AS user2, f1.user2_id AS common " + 
                            "FROM Friendship f1 " + 
                            "JOIN Friendship f2 ON f1.user2_id = f2.user2_id AND f1.user1_id < f2.user1_id " +  
                            "WHERE NOT EXISTS ( " + 
                            "SELECT 1 " + 
                            "FROM Friendship f3 " + 
                            "WHERE (f3.user1_id = f1.user1_id AND f3.user2_id = f2.user1_id) OR (f3.user1_id = f2.user1_id AND f3.user2_id = f1.user1_id) " +
                            ") "               
                );

                ResultSet rst = stmt.executeQuery(
                    "SELECT u.user_id, u2.user_id AS u2id, u.first_name, u.last_name, u2.first_name AS u2f, u2.last_name AS u2l COUNT(*) AS num_mutuals" +
                    "FROM Mutuals m " +
                    "JOIN " + UsersTable + " u ON m.user1 = u.user_id " +
                    "JOIN " + UsersTable + " u2 ON m.user2 = u2.user_id " +
                    "ORDER BY num_mutuals DESC, ORDER BY u.user_id ASC, u2.user_id ASC " + 
                    "WHERE ROWNUM <= " + num
                );

                while (rst.next()) { // should have proper num of rows 
                    //System.out.println(rst.getInt(1));
                    //System.out.println(rst.getInt(2));
                    int uid1 = rst.getInt(1);
                    int uid2 = rst.getInt(2);
                    UserInfo u1 = new UserInfo(uid1, rst.getString(3), rst.getString(4));
                    UserInfo u2 = new UserInfo(uid2, rst.getString(5), rst.getString(6));
                    UsersPair up = new UsersPair(u1, u2);
                    results.add(up);

                    ResultSet rst2 = inner.executeQuery(
                        "SELECT DISTINCT u.user_id, u.first_name, u.last_name " +
                        "FROM " + UsersTable + " u " + 
                        "JOIN Mutuals M ON M.common = u.user_id " +
                        "WHERE (f1.user1_id = " + uid1 + " AND f2.user1_id = " + uid2 + " ) OR (f2.user1_id = " + uid1 + " AND f1.user1_id = " + uid2 + " ) " +
                        "ORDER BY u.user_id ASC"
                    );

                    while(rst2.next()) {
                        UserInfo u3 = new UserInfo(rst2.getInt(1), rst2.getString(2), rst2.getString(3)); 
                        up.addSharedFriend(u3);  
                    }
                    rst2.close();
                }

                temp = stmt.executeUpdate("DROP VIEW Mutuals");
                temp = stmt.executeUpdate("DROP VIEW Friendship");
                rst.close();
                inner.close();
                stmt.close();
        
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(16, "The", "Hacker");
                UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
                UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
                UsersPair up = new UsersPair(u1, u2);
                up.addSharedFriend(u3);
                results.add(up);
            */
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
                ResultSet rst = stmt.executeQuery(
                      "SELECT COUNT(*) AS NumEvents, c.state_name " + 
                            "FROM " + EventsTable + " e " + 
                            "JOIN " + CitiesTable + " c ON c.city_id = e.event_city_id " + 
                            "GROUP BY c.state_name " + 
                            "ORDER BY NumEvents DESC, c.state_name ASC"
                        );
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                EventStateInfo info = new EventStateInfo(50);
                info.addState("Kentucky");
                info.addState("Hawaii");
                info.addState("New Hampshire");
                return info;
            */
            if (rst.next()) {
                EventStateInfo info = new EventStateInfo(rst.getInt(1));
                int max = rst.getInt(1);
                info.addState(rst.getString(2));
                while (rst.next() && rst.getInt(1) == max) {
                    info.addState(rst.getString(2));
                } 
                return info;
            }
            return new EventStateInfo(-1); // placeholder for compilation
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }

    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) { 
                ResultSet rst = stmt.executeQuery(
                      "SELECT u.user_id, u.first_name, u.last_name " + 
                            "FROM " + UsersTable + " u " + 
                            "WHERE u.user_id IN ( " + 
                            "SELECT f.user2_id " +
                            "FROM " + FriendsTable + " f " + 
                            "WHERE f.user1_id = " + userID + " ) " +
                            "OR u.user_id IN ( " + 
                            "SELECT f2.user1_id " +
                            "FROM " + FriendsTable + " f2 " +
                            "WHERE f2.user2_id = " + userID + " ) " +
                            "AND (u.year_of_birth IS NOT NULL AND u.month_of_birth IS NOT NULL AND u.day_of_birth IS NOT NULL) " + 
                            "ORDER BY u.year_of_birth ASC, u.month_of_birth ASC, u.day_of_birth ASC, u.user_id DESC"
                        );
                  
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */
            // one method - redo the rst and arrange by youngest to get youngest
            // however that method might not pass the time limit on the autograder
                int o_id = 0;
                String o_first = null, o_last = null;
                if (rst.first()) { // find oldest user
                    o_id = rst.getInt(1);
                    o_first = rst.getString(2);
                    o_last = rst.getString(3);
                }

            // run new rst for youngest user
                rst = stmt.executeQuery(
                      "SELECT u.user_id, u.first_name, u.last_name " + 
                            "FROM " + UsersTable + " u " + 
                            "WHERE u.user_id IN ( " + 
                            "SELECT f.user2_id " +
                            "FROM " + FriendsTable + " f " + 
                            "WHERE f.user1_id = " + userID + " ) " +
                            "OR u.user_id IN ( " + 
                            "SELECT f2.user1_id " +
                            "FROM " + FriendsTable + " f2 " +
                            "WHERE f2.user2_id = " + userID + " ) " +
                            "AND (u.year_of_birth IS NOT NULL AND u.month_of_birth IS NOT NULL AND u.day_of_birth IS NOT NULL) " + 
                            "ORDER BY u.year_of_birth DESC, u.month_of_birth DESC, u.day_of_birth DESC, u.user_id DESC"
                        );
                int y_id = 0;
                String y_first = null, y_last = null;
                if (rst.first()) { // find youngest user w highest ID
                    y_id = rst.getInt(1);
                    y_first = rst.getString(2);
                    y_last = rst.getString(3);
                }
                UserInfo old = new UserInfo(o_id, o_first, o_last);
                UserInfo young = new UserInfo(y_id, y_first, y_last);
                rst.close();
                stmt.close();
                return new AgeInfo(old, young);

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }

    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
                ResultSet rst = stmt.executeQuery(
                      "SELECT u1.user_id, u1.first_name, u1.last_name, u2.user_id, u2.first_name, u2.last_name " + 
                            "FROM " + FriendsTable + " f " + 
                            "JOIN " + UsersTable + " u1 ON f.user1_id = u1.user_id " + 
                            "JOIN " + UsersTable + " u2 ON f.user2_id = u2.user_id " + 
                            "JOIN " + HometownCitiesTable + " h1 ON u1.user_id = h1.user_id " +
                            "JOIN " + HometownCitiesTable + " h2 ON u2.user_id = h2.user_id " +
                            "WHERE u1.last_name = u2.last_name AND h1.hometown_city_id = h2.hometown_city_id " +
                            "AND (u1.year_of_birth IS NOT NULL AND u2.year_of_birth IS NOT NULL AND ABS(u1.year_of_birth - u2.year_of_birth) < 10) " + 
                            "ORDER BY u1.user_id ASC, u2.user_id ASC"
                        );

            while(rst.next()) {
                UserInfo u1 = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
                UserInfo u2 = new UserInfo(rst.getInt(4), rst.getString(5), rst.getString(6));
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            }
            rst.close();
            stmt.close();
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
                UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            */
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}
