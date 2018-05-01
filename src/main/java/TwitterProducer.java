import java.util.Properties;

public class TwitterProducer {

    private static final String TOPIC = "twitterStream";

    public static void run(String consumerKey, String consumerSecret,
                           String token, String secret) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
    }
}
