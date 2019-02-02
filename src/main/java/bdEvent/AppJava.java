package bdEvent;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AppJava {

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\winutils\\");
        SparkConf conf = new SparkConf().setMaster("local").setAppName("event");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        Dataset<Row> data = spark.read().json("C:\\Users\\Олег\\IdeaProjects\\template\\src\\main\\resources\\meteo_data.json");
//         JavaRDD<Row> javaRDD = data.javaRDD().groupBy();

        data.createOrReplaceTempView("meteo");

        data.printSchema();
//        Dataset<Row> meteoData = spark.sql("SELECT meteo.data.date, avg(data.tC) as temp FROM meteo where meteo.data.tC is not null group by meteo.data.date");

        Dataset<Row> meteoData = spark.sql("SELECT meteo.data.date, meteo.data.tC as temp FROM meteo where meteo.data.tC is not null");
        meteoData.show();
        Dataset<Row> avgData = meteoData.groupBy("date").avg("temp");
        avgData.show();

    }
}
