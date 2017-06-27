package com.rainsoft.j2se;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by CaoWeiDong on 2017-06-26.
 */
public class TestLog {
    private static final Logger logger = LoggerFactory.getLogger(TestLog.class);

    public static void main(String[] args) {
        logger.error("Error Message!");
        logger.warn("Warn Message!");
        logger.info("Info Message!");
        logger.debug("Debug Message!");
        logger.trace("Trace Message!");
        logger.info("hello {} ", "曹伟东");

    }
}
