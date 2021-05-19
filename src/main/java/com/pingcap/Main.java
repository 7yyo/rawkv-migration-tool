package com.pingcap;

import com.pingcap.util.PropertiesUtil;

import java.util.Properties;

public class Main {

    //    private static final String propertiesPath = System.getProperty("p");
    private static final String propertiesPath = "src/main/resources/importer.properties";

    public static void main(String[] args) {

        Properties properties = PropertiesUtil.getProperties(propertiesPath);




    }

}
