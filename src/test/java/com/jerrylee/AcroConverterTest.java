package com.jerrylee;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.jerrylee.AvroConverter;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author jerrylee
 */
public class AcroConverterTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private AvroConverter converter;

    @Before
    public void setup(){
        converter = new AvroConverter(mapper);

    }

    @Test
    public void convertJson() throws IOException {
    	String ns = ".test";
    	String json = "{\"f1\": \"2018-01-09-14-49-48_5\", \"_UPT_\": 1515480595528, \"_UPUN_\": \"SA-ITS-ADFXD_TST\", \"_INST_\": 1515480595528, \"_INSUN_\": \"SA-ITS-ADFXD_TST\", \"_DELETED_\": false, \"__SYSTEM_NAME__\": \"/hcs/kna1\", \"__operation__\": \"CREATE\"}";
        String schema = "{\"namespace\":\"adf-kafka\",\"name\":\"adf-kafka\",\"type\":\"record\",\"fields\":[{\"name\":\"f1\",\"type\":\"boolean\"},{\"name\":\"_UPT_\",\"type\":\"long\"},{\"name\":\"_UPUN_\",\"type\":\"boolean\"},{\"name\":\"_INST_\",\"type\":\"long\"},{\"name\":\"_INSUN_\",\"type\":\"boolean\"},{\"name\":\"_DELETED_\",\"type\":\"boolean\"},{\"name\":\"__SYSTEM_NAME__\",\"type\":\"boolean\"},{\"name\":\"__operation__\",\"type\":\"boolean\"}]}";
        String schemaTest = converter.convert(json, ns).replaceAll("\r\n", "").replaceAll(" ", "");
        Assert.assertEquals(schema, schemaTest);
    }
}
