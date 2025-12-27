package me.matthew.flink.backpacktfforward.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ListingUpdate {
    public String id;
    public String event;
    public Payload payload;
    
    // Generation timestamp for backfill conflict resolution (Unix timestamp in milliseconds)
    @JsonProperty("generation_timestamp")
    public Long generationTimestamp;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Payload {
        public String id;
        public String steamid;
        public int appid;
        public Currencies currencies;
        public Value value;
        public Boolean tradeOffersPreferred;
        public Boolean buyoutOnly;
        public String details;
        public long listedAt;
        public long bumpedAt;
        public String intent;
        public int count;
        public String status;
        public String source;
        public Item item;
        public UserAgent userAgent;
        public User user;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Currencies {
        public Double metal;
        public Long keys;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Value {
        public double raw;
        @JsonProperty("short")
        public String shortStr;
        @JsonProperty("long")
        public String longStr;
        public Price steam;
        public Price community;
        public Price suggested;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Price {
        public String currency;
        @JsonProperty("short") 
        public String shortStr;
        @JsonProperty("long")
        public String longStr;
        public double raw;
        public double value;
        public Double valueHigh;
        public double usd;
        public Long updatedAt;
        public Double difference;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Item {
        public int appid;
        public String baseName;
        public int defindex;
        public String id;
        public String imageUrl;
        public String marketName;
        public String name;
        public Origin origin;
        public String originalId;
        public Quality quality;
        public String summary;
        @JsonDeserialize(using = ItemPriceDeserializer.class)
        public ItemPrice price;
        public Integer level;
        public List<Equipped> equipped;
        public List<StrangePart> strangeParts;
        public List<Spell> spells;
        @JsonProperty("class")
        public List<String> clazz;
        public String slot;
        public Particle particle;
        public ItemStyle style;
        public Paint paint;
        public String paintSecondaryHex;
        public Boolean tradable;
        public Boolean craftable;
        public String priceindex;
        public String tag;
    }


    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Quality {
        public int id;
        public String name;
        public String color;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Particle {
        public int id;
        public String name;
        public String shortName;
        public String imageUrl;
        public String type;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class UserAgent {
        public String client;
        public long lastPulse;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class User {
        public String id;
        public String name;
        public String avatar;
        public String avatarFull;
        public Boolean premium;
        public Boolean online;
        public Boolean banned;
        public String customNameStyle;
        public String clazz; // `class` renamed to clazz
        public String style;
        public Object role;
        public String tradeOfferUrl;
        public Object bans;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Origin {
        public int id;
        public String name;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Equipped {
        @JsonProperty("class")
        public int clazz;
        public int slot;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Spell {
        public String id;
        public String spellId;
        public String name;
        public String type;
        public int defindex;
        public String color;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ItemStyle {
        public String name;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Paint {
        public int id;
        public String name;
        public String color;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ItemPrice {
        public Price steam;
        public Price community;
        public Price suggested;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StrangePart {
        public int score;
        public KillEater killEater;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class KillEater {
        public int id;
        public String name;
        public Item item;
    }

    public static class ItemPriceDeserializer extends JsonDeserializer<ItemPrice> {
        @Override
        public ItemPrice deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            ObjectMapper mapper = (ObjectMapper) p.getCodec();
            JsonNode node = mapper.readTree(p);
            
            // If it's an object with steam/community/suggested, deserialize directly
            if (node.isObject() && (node.has("steam") || node.has("community") || node.has("suggested"))) {
                return mapper.treeToValue(node, ItemPrice.class);
            }
            
            // If it's an array or other format, return null or create empty ItemPrice
            // This handles legacy cases where price might be in array format
            if (node.isArray()) {
                // For array format, we could try to extract the first element if it matches our structure
                // but for now, we'll return null to avoid errors
                return null;
            }
            
            // For any other case, try to deserialize as ItemPrice (might be null)
            return mapper.treeToValue(node, ItemPrice.class);
        }
    }

    public static class PriceDeserializer extends JsonDeserializer<List<Price>> {
        @Override
        public List<Price> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            ObjectMapper mapper = (ObjectMapper) p.getCodec();
            JsonNode node = mapper.readTree(p);
            List<Price> prices = new ArrayList<>();

            if (node.isArray()) {
                for (JsonNode element : node) {
                    prices.add(mapper.treeToValue(element, Price.class));
                }
            } else if (node.isObject()) {
                prices.add(mapper.treeToValue(node, Price.class));
            }

            return prices;
        }
    }

}
