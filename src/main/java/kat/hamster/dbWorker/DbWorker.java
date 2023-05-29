package kat.hamster.dbWorker;

import kat.hamster.csvHandler.CsvHandler;
import kat.hamster.dbConnection.DbConnection;

import java.sql.*;
import java.util.List;

public interface DbWorker {
    public static void populateFromFile(String fileName) {
        List<String[]> strings = CsvHandler.readCsvFile(fileName, ",");
        Connection conn = DbConnection.getConnection();
        try {
            Statement cleaner = conn.createStatement();
            System.out.println("Deleted from ellipse: " + cleaner.executeUpdate("DELETE FROM ellipse"));
            System.out.println("Deleted from polygon: " + cleaner.executeUpdate("DELETE FROM polygon"));
            PreparedStatement ellipseSt = conn.prepareStatement(
                    "INSERT INTO ellipse (backgroundcolor, bordercolor, width, length) " +
                            "VALUES (?, ?, ?, ?)");
            PreparedStatement polygonSt = conn.prepareStatement(
                    "INSERT INTO polygon (backgroundcolor, bordercolor, type, numberofvertices) " +
                            "VALUES (?, ?, ?, ?)");

            for (String[] line : strings) {
                if (line[0].equals("0")) {
                    ellipseSt.setString(1, line[1]);
                    ellipseSt.setString(2, line[2]);
                    ellipseSt.setInt(3, Integer.parseInt(line[3]));
                    ellipseSt.setInt(4, Integer.parseInt(line[4]));
                    ellipseSt.addBatch();
                } else {
                    polygonSt.setString(1, line[1]);
                    polygonSt.setString(2, line[2]);
                    polygonSt.setString(3, line[3]);
                    polygonSt.setInt(4, Integer.parseInt(line[4]));
                    polygonSt.addBatch();
                }
            }
            int[] stRes = ellipseSt.executeBatch();
            int[] teacherRes = polygonSt.executeBatch();
            for (int num : stRes) {
                System.out.println("Insert int ellipse count: " + num);
            }

            for (int num : teacherRes) {
                System.out.println("Insert int polygon count: " + num);
            }
            cleaner.close();
            ellipseSt.close();
            polygonSt.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void demoQuery() {
        Connection conn = DbConnection.getConnection();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM polygon WHERE bordercolor like 'red'");
            while (rs.next()) {
                System.out.print(rs.getString("backgroundcolor"));
                System.out.print(" ");
                System.out.print(rs.getString("bordercolor"));
                System.out.print(" ");
                System.out.print(rs.getString("type"));
                System.out.print(" ");
                System.out.println(rs.getString("numberofvertices"));
            }
            rs.close();
            st.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void dirtyReadDemo() {
        Runnable first = () -> {
            Connection conn1 = DbConnection.getNewConnection();
            if (conn1 != null) {
                try {
                    conn1.setAutoCommit(false);
                    conn1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                    Statement upd = conn1.createStatement();
                    upd.executeUpdate("UPDATE polygon SET backgroundcolor='white' WHERE backgroundcolor='black'");
                    Thread.sleep(2000);
                    conn1.rollback();
                    upd.close();
                    Statement st = conn1.createStatement();
                    System.out.println("In the first thread:");
                    ResultSet rs = st.executeQuery("SELECT * FROM polygon");
                    while (rs.next()) {
                        System.out.println(rs.getString("backgroundcolor"));
                    }
                    st.close();
                    rs.close();
                    conn1.close();
                } catch (SQLException | InterruptedException throwables) {
                    throwables.printStackTrace();
                }
            }
        };

        Runnable second = () -> {
            Connection conn2 = DbConnection.getNewConnection();
            if (conn2 != null) {
                try {
                    Thread.sleep(500);
                    conn2.setAutoCommit(false);
                    conn2.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                    Statement st = conn2.createStatement();
                    System.out.println("In the second thread:");
                    ResultSet rs = st.executeQuery("SELECT * FROM polygon");
                    while (rs.next()) {
                        System.out.println(rs.getString("backgroundcolor"));
                    }
                    rs.close();
                    st.close();
                    conn2.close();
                } catch (SQLException | InterruptedException throwables) {
                    throwables.printStackTrace();
                }
            }
        };

        Thread th1 = new Thread(first);
        Thread th2 = new Thread(second);
        th1.start();
        th2.start();
    }
}
