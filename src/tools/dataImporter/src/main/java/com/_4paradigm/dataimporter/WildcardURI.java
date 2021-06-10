
package com._4paradigm.dataimporter;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/*
 * Path may include wildcard, like 2018[8-9]*, but these characters are not valid in URI,
 * So we first encode the path, except '/' and ':'.
 * When we get path, we need to decode the path first.
 * eg:
 * hdfs://host/testdata/20180[8-9]*;
 * -->
 * hdfs://host/testdata/20180%5B8-9%5D*
 *
 * getPath() will return the decoded path, it's the same as input uri: /testdata/20180[8-9]*
 */
public class WildcardURI {
    private static Logger logger = LogManager.getLogger(WildcardURI.class);
    private boolean valid = false;

    private URI uri;

    public WildcardURI(String path) {
        try {
            // 1. call URLEncoder.encode to encode all special character, like /, *, [, %
            // 2. recover the : and /
            // 3. the space(" ") will be encoded to "+", we have to change it to "%20"
            // example can be found in WildcardURITest.java
            String encodedPath = URLEncoder.encode(path, StandardCharsets.UTF_8.toString()).replaceAll("%3A",
                    ":").replaceAll("%2F", "/").replaceAll("\\+", "%20");
            uri = new URI(encodedPath);
            uri.normalize();
            valid = true;
            logger.info(uri.getRawPath());
        } catch (UnsupportedEncodingException | URISyntaxException e) {
            logger.warn("failed to encoded uri: " + path, e);
            e.printStackTrace();
        }
    }

    public URI getUri() {
        if (!valid) {
            return null;
        }
        return uri;
    }

    public String getAuthority() {
        if (!valid) {
            return null;
        }
        return uri.getAuthority();
    }

    public String getPath() {
        if (!valid) {
            return null;
        }
        return uri.getPath();
    }
}
