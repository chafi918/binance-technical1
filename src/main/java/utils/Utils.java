package utils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.prometheus.client.Collector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.explode_outer;
import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.sum;
import static spark.Spark.get;

public final class Utils {
    // private constructor to avoid unnecessary instantiation of the class
    private Utils(){}

    //this function is used to call httpURL and check for errors and store the response as json file
    private static String getResonseFromURL(String urlString, JsonType jsonType) {
        String filename = "response.json";
        
        try {

            URL url = new URL(urlString);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");

            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + conn.getResponseCode());
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (conn.getInputStream())));

            String output;
            String finalOutput = "";
            System.out.println("Output from Server .... \n");
            while ((output = br.readLine()) != null) {
                finalOutput += output ;
            }
            jsonToFile(finalOutput, filename, jsonType);
            conn.disconnect();
        } catch (MalformedURLException e) {

            e.printStackTrace();

        } catch (Exception e) {

            e.printStackTrace();

        }
        return filename;
    }

    //this function is used to store json as file in resources folder
    //depending on the jsonType (Array or Object)
    private static void jsonToFile(String json, String filename, JsonType jsonType){
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        try {
            switch (jsonType) {
                case Array :
                    final JSONArray array = new JSONArray(json);
                    mapper.writeValue(new File("src/main/resources/"+filename), array );
                    break;
                case Object:
                    final JSONObject obj = new JSONObject(json);
                    mapper.writeValue(new File("src/main/resources/"+filename), obj );
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //computes the top 'topCoun' elements as requested depending on the attribute and asset currency
    public static List<String> computeTopList(SparkSession spark, String attribute, String asset, int topCount){
        String filename = Utils.getResonseFromURL("https://api1.binance.com/api/v3/ticker/24hr", JsonType.Array);
        Dataset<Row> df = spark.read().json("src/main/resources/"+filename);

        Dataset<Row> result = df
                .withColumn("values", explode_outer(col("values")))
                .withColumn("values", col("values.nameValuePairs"))
                .withColumn("symbol", col("values.symbol"))
                .withColumn(attribute, col("values."+attribute))
                .filter(col("symbol").endsWith(asset))
                .orderBy(desc(attribute))
                .limit(topCount)
                .drop("values");

        List<String> resultList = result.select(col("symbol")).collectAsList().stream().map(e -> e.getString(0))
                .collect(Collectors.toList());

        return resultList;
    }

    //computes the notional values for the required list of symbols
    public static Map<String, Float> computeNotionalMap(SparkSession spark){
        List<String>  q1 = Utils.computeTopList(spark, "volume", "BTC", 5);
        Map<String, Float> result = new HashMap<>();
        q1.stream().forEach(s -> result.putAll( utils.Utils.computeNotional(spark, s)));
        return  result;
    }

    //computes the notional values for a symbols
    private static Map<String, Float> computeNotional(SparkSession spark, String symbol){
        String filename = Utils
                .getResonseFromURL("https://api1.binance.com/api/v3/depth?symbol="+symbol+"&limit=500", JsonType.Object);
        Dataset<Row> df = spark.read().json("src/main/resources/"+filename);

        Map<String, Float> resultMap = new HashMap<>();
        resultMap.put(symbol + "_asks", getNotionalSum(spark, df, "asks"));
        resultMap.put(symbol + "_bids", getNotionalSum(spark, df, "bids"));

        return resultMap;
    }

    //compute the sum of the notional values from a dataFrame
    private static float getNotionalSum(SparkSession spark, Dataset<Row> df, String PoC){
        Dataset<Row> poCs = df
                .withColumn(PoC, col("nameValuePairs."+PoC))
                .withColumn(PoC, explode_outer(col(PoC+".values")))
                .withColumn(PoC, col(PoC+".values"))
                .withColumn("price", element_at(col(PoC), 1).cast("float"))
                .withColumn("quantity", element_at(col(PoC), 2).cast("float"))
                .drop(col(PoC))
                .drop(col("nameValuePairs"))
                .orderBy(desc("price"))
                .limit(200);

        poCs.createOrReplaceTempView("poCs");
        Dataset<Row> sqlPoCs = spark.sql("SELECT *, price*quantity as notionalValue FROM poCs");

        return Float.valueOf(sqlPoCs.agg(sum("notionalValue")).first().get(0).toString()).floatValue();
    }

    //compute the priceSpred values for the required list of symbols
    public static Map<String, Float> getPriceSpreadMap(SparkSession spark){
        List<String>  q2 = Utils.computeTopList(spark, "count", "USDT", 5);
        Map<String, Float> result = new HashMap<>();
        q2.stream().forEach(s -> result.put( s, utils.Utils.getPriceSpread(spark, s)));
        return  result;
    }

    //compute the priceSpred values for a symbols
    private static float getPriceSpread(SparkSession spark, String symbol){
        String url = "https://api1.binance.com/api/v3/ticker/bookTicker?symbol="+symbol;
        System.out.println("url getPriceSpread : " + url );
        String filename = Utils
                .getResonseFromURL(url, JsonType.Object);
        Dataset<Row> df = spark.read().json("src/main/resources/"+filename);
        Dataset<Row> df2 = df
                .withColumn("symbol", col("nameValuePairs.symbol"))
                .withColumn("askPrice", col("nameValuePairs.askPrice").cast("float"))
                .withColumn("bidPrice", col("nameValuePairs.bidPrice").cast("float"))
                .drop(col("nameValuePairs"));

        df2.createOrReplaceTempView("df2");
        Dataset<Row> sqlSpread = spark.sql("SELECT *, askPrice-bidPrice as spreadPrice FROM df2");

        float spread =  Float.valueOf(sqlSpread.select(col("spreadPrice")).first().get(0).toString()).floatValue();

        return spread;
    }

    //transform a the result map to the prometheus metric format
    public static String getResponseAsPrometheuseFormat(Map<String, Float> map){
        LinkedList<Collector.MetricFamilySamples> expectedList = new LinkedList<>();

        List<Collector.MetricFamilySamples.Sample> samples =
        map.keySet().stream().map(key -> {
            Float value = map.get(key);
            Collector.MetricFamilySamples.Sample sample = new Collector.MetricFamilySamples.Sample(key, new LinkedList<String>(), new LinkedList<String>(), value);
            return sample;
        }).collect(Collectors.toList());

        Collector.MetricFamilySamples expectedMFS = new Collector.MetricFamilySamples("Q6 Metrics", Collector.Type.GAUGE, "", samples);
        expectedList.add(expectedMFS);
        Enumeration<Collector.MetricFamilySamples> expected = Collections.enumeration(expectedList);
        return expected.nextElement().toString();
    }

    //compute the priceSpred values for a list of symbols
    public static void deltaSpreadList(SparkSession spark, List<String> symbolbList) {
        Map<String, Float> valuesMap = new HashMap<String, Float>();
        try{
            while(true){
                symbolbList.stream().forEach(s -> {
                    Float oldValue = Utils.getPriceSpread(spark, s);
                    valuesMap.put(s+"_previous", oldValue);
                });

                System.out.println("sleeping 10s...");
                Thread.sleep(10 * 1000);
                System.out.println("wake up...");

                symbolbList.stream().forEach(s -> {
                    Float newValue = Utils.getPriceSpread(spark, s);
                    Float oldValue = valuesMap.get(s+"_previous");
                    valuesMap.put(s, Math.abs(newValue-oldValue));
                    valuesMap.put(s + "_previous", newValue);
                });


                System.out.println("valuesMap : " +  valuesMap);
                //Question 6
                //expose the result for question 6 under /metrics
                get("/metrics", (req, res) -> getResponseAsPrometheuseFormat(valuesMap.entrySet().stream()
                        .filter(k -> !k.getKey().contains("previous"))
                        .collect(Collectors.toMap(map -> map.getKey(), map -> map.getValue()))));
            }
        } catch(Exception e){
            e.printStackTrace();
        }

    }
}
