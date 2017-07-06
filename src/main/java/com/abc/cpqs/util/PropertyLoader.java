package com.abc.cpqs.util;

import java.io.*;
import java.util.Properties;

/**
 * Created by lijiax on 6/20/17.
 */
public class PropertyLoader {
    private static Properties properties;

    public static Properties getProperties(String filePath) {
        InputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(filePath));
            properties = new Properties();
            properties.load(in);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
