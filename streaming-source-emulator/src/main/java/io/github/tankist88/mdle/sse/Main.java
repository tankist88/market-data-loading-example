package io.github.tankist88.mdle.sse;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.tankist88.mdle.sse.dto.MarketRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static final String PG_DB_HOST = "postgresql-tradesdb";
    private static final String PG_DB_PORT = "5432";
    private static final String PG_DB_NAME = "tradesdb";
    private static final String PG_DB_USER = "trades_user";
    private static final String PG_DB_PASSWORD = "password123";

    private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:9092";
    private static final String KAFKA_DEST_TOPIC = "stock.data.pub";

    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        try (Connection conn = DriverManager.getConnection(createDbUrl())) {
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("select * from DEAL LIMIT 10")) {
                    ObjectMapper mapper = createMapper();

                    System.out.println("Query executed");

                    while (rs.next()) {
                        KafkaProducer<String, String> producer = createKafkaProducer(KAFKA_BOOTSTRAP_SERVERS);
                        StringWriter sw = new StringWriter();
                        mapper.writeValue(sw, createMarketRecord(rs));
                        String json = sw.toString();
                        System.out.println(json);
                        producer.send(new ProducerRecord<>(KAFKA_DEST_TOPIC, json));
                        producer.flush();
                        producer.close();
                    }
                }
            }
        }
    }

    private static MarketRecord createMarketRecord(ResultSet rs) throws SQLException {
        MarketRecord record = new MarketRecord();

        record.setTradeNo(rs.getLong("TRADENO"));
        record.setTradeTime(rs.getString("TRADETIME"));
        record.setTradeDate(rs.getString("TRADEDATE"));
        record.setSecId(rs.getString("SECID"));
        record.setBoardId(rs.getString("BOARDID"));
        record.setPrice(rs.getDouble("PRICE"));
        record.setQuantity(rs.getInt("QUANTITY"));
        record.setValue(rs.getDouble("VALUE"));
        record.setType(rs.getString("TYPE"));
        record.setBuySell(rs.getString("BUYSELL"));
        record.setTradingSession(rs.getString("TRADINGSESSION"));

        return record;
    }

    private static String createDbUrl() {
        return  "jdbc:postgresql://" + PG_DB_HOST + ":" + PG_DB_PORT + "/" + PG_DB_NAME + "?" +
                "user=" + PG_DB_USER + "&" +
                "password=" + PG_DB_PASSWORD;
    }

    private static KafkaProducer<String, String> createKafkaProducer(String brokers) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(config);
    }

    private static ObjectMapper createMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
}
