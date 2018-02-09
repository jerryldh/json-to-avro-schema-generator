package com.jerrylee;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class App {

    public static void main(String[] args) throws IOException {
    	String json = "{\"f1\": \"2018-01-09-14-49-48_5\", \"_UPT_\": 1515480595528, \"_UPUN_\": \"SA-ITS-ADFXD_TST\", \"_INST_\": 1515480595528, \"_INSUN_\": \"SA-ITS-ADFXD_TST\", \"_DELETED_\": false, \"__SYSTEM_NAME__\": \"/hcs/kna1\", \"__operation__\": \"CREATE\"}";
        System.out.println(json);
        String ns = ".hcs.t001w";
        String schema = new AvroConverter(new ObjectMapper()).convert(json, ns).replaceAll("\r\n", "").replaceAll(" ", "");
    	System.out.println(schema);
    }


}