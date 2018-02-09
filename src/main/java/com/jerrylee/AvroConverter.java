package com.jerrylee;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/**
 * @author jerrylee
 */
public class AvroConverter {

    private static final Logger logger = LoggerFactory.getLogger(AvroConverter.class);

    private final ObjectMapper mapper;

    /**
     * Constructor
     *
     * @param mapper to serialize
     */
    public AvroConverter(final ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * validation
     *
     * @param avroSchemaString to validate
     * @param jsonString       to validate
     * @return true if validated, false otherwise
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public boolean validate(final String avroSchemaString, final String jsonString) throws IOException {
        Schema.Parser t = new Schema.Parser();
        Schema schema = t.parse(avroSchemaString);
        @SuppressWarnings("rawtypes")
		GenericDatumReader reader = new GenericDatumReader(schema);
        JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, jsonString);
        reader.read(null, decoder);
        return true;
    }

    /**
     * convert to avro schema
     *
     * @param json to convert
     * @return avro schema json
     * @throws IOException
     */
    public String convert(final String json, String ns) throws IOException {
    	final JsonNode jsonNode = mapper.readTree(json);
        final ObjectNode finalSchema = mapper.createObjectNode();
        finalSchema.put(SchemaUtils.SchemaNS, "ns.com.jerrylee.ns"+ns);
        finalSchema.put(SchemaUtils.SchemaName, "com.jerrylee.schema"+ns);
        finalSchema.put(SchemaUtils.SchemaType, SchemaUtils.SchemaRecord);
        finalSchema.set(SchemaUtils.SchemaFields, getFields(jsonNode));
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(finalSchema);
    }

    /**
     * @param jsonNode to getFields
     * @return array nodes of fields
     */
    private ArrayNode getFields(final JsonNode jsonNode) {
        final ArrayNode fields = mapper.createArrayNode();
        final Iterator<Map.Entry<String, JsonNode>> elements = jsonNode.fields();

        Map.Entry<String, JsonNode> map;
        while (elements.hasNext()) {
            map = elements.next();
            final JsonNode nextNode = map.getValue();

            switch (nextNode.getNodeType()) {
            	case BOOLEAN:
            		fields.add(mapper.createObjectNode().put(SchemaUtils.SchemaName, map.getKey()).put(SchemaUtils.SchemaType, SchemaUtils.BooleanType));
                    break;
            
                case NUMBER:
                	String type = "int";
                	if(nextNode.isInt())
                		type = "int";
                	if(nextNode.isDouble())
                		type = "double";
                	if(nextNode.isFloat())
                		type = "float";
                	if(nextNode.isLong())
                		type = "long";
                    fields.add(mapper.createObjectNode().put(SchemaUtils.SchemaName, map.getKey()).put(SchemaUtils.SchemaType, type));
                    break;

                case STRING:
                    fields.add(mapper.createObjectNode().put(SchemaUtils.SchemaName, map.getKey()).put(SchemaUtils.SchemaType, SchemaUtils.StringType));
                    break;

                case ARRAY:
                    final ArrayNode arrayNode = (ArrayNode) nextNode;
                    final JsonNode element = arrayNode.get(0);
                    final ObjectNode objectNode = mapper.createObjectNode();
                    objectNode.put(SchemaUtils.SchemaName, map.getKey());

                    if (element.getNodeType() == JsonNodeType.NUMBER) {
                        objectNode.set(SchemaUtils.SchemaType, mapper.createObjectNode().put(SchemaUtils.SchemaType, SchemaUtils.ArrayType).put(SchemaUtils.SchemaItems, (nextNode.isLong() ? "long" : "double")));
                        fields.add(objectNode);
                    } else if (element.getNodeType() == JsonNodeType.STRING) {
                        objectNode.set(SchemaUtils.SchemaType, mapper.createObjectNode().put(SchemaUtils.SchemaType, SchemaUtils.ArrayType).put(SchemaUtils.SchemaItems, SchemaUtils.StringType));
                        fields.add(objectNode);
                    } else {
                        objectNode.set(SchemaUtils.SchemaType, mapper.createObjectNode().put(SchemaUtils.SchemaType, SchemaUtils.ArrayType).set(SchemaUtils.SchemaItems, mapper.createObjectNode()
                                .put(SchemaUtils.SchemaType, SchemaUtils.SchemaRecord).put(SchemaUtils.SchemaName, generateRandomNumber(map)).set(SchemaUtils.SchemaFields, getFields(element))));
                    }
                    fields.add(objectNode);
                    break;

                case OBJECT:

                    ObjectNode node = mapper.createObjectNode();
                    node.put(SchemaUtils.SchemaName, map.getKey());
                    node.set(SchemaUtils.SchemaType, mapper.createObjectNode().put(SchemaUtils.SchemaType, SchemaUtils.SchemaRecord).put(SchemaUtils.SchemaName, generateRandomNumber(map)).set(SchemaUtils.SchemaFields, getFields(nextNode)));
                    fields.add(node);
                    break;

                default:
                    logger.error("Node type not found - " + nextNode.getNodeType());
                    throw new RuntimeException("Unable to determine action for ndoetype "+nextNode.getNodeType()+"; Allowed types are ARRAY, STRING, NUMBER, OBJECT");
            }
        }
        return fields;
    }

    /**
     * @param map to create random number
     * @return random
     */
    private String generateRandomNumber(Map.Entry<String, JsonNode> map) {
        return (map.getKey() + "_" + new Random().nextInt(100));
    }
}
