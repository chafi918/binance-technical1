
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import utils.Utils;

import java.util.List;
import java.util.Map;

import static spark.Spark.get;
import static spark.Spark.port;

public class Main {

    public static void main(String args[]) {
        port(8080);

        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //Question 1
        System.out.println("Q1 started : ");
        List<String>  q1 = Utils.computeTopList(spark, "volume", "BTC", 5);
        //expose the result for question 1 under /q1
        get("/q1", (req, res)-> q1.toString());

        //Question 2
        System.out.println("Q2 started : ");
        List<String>  q2 = Utils.computeTopList(spark, "count", "USDT", 5);
        //expose the result for question 2 under /q2
        get("/q2", (req, res)-> q2.toString());

        //Question 3
        System.out.println("Q3 started : ");
        Map<String, Float> q3 = Utils.computeNotionalMap(spark);
        //expose the result for question 3 under /q3
        get("/q3", (req, res)-> q3.toString());

        //Question 4
        System.out.println("Q4 started : ");
        Map<String, Float> q4 = Utils.getPriceSpreadMap(spark);
        //expose the result for question 4 under /q4
        get("/q4", (req, res)-> q4.toString());

        //Question 5 & 6
        System.out.println("Q5 started : ");
        Utils.deltaSpreadList(spark, q2);

    }

}
