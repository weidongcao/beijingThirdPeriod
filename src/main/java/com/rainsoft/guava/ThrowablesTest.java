package com.rainsoft.guava;

import com.google.common.base.Throwables;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Created by CaoWeiDong on 2018-01-17.
 */
public class ThrowablesTest {

    public void testThrowables() {
        try {
            throw new Exception();
        } catch (Exception e) {
            String ss = Throwables.getStackTraceAsString(e);
            System.out.println("ss = " + ss);
            Throwables.propagate(e);
        }
    }

    public void call() throws IOException {
        try {
            throw new IOException();
        } catch (Throwable e) {
            Throwables.propagateIfInstanceOf(e, IOException.class);
            throw Throwables.propagate(e);
        }
    }

    public void testCheckException() {
        try {
            URL url = new URL("http://baidu.com");
            final InputStream in = url.openStream();
            in.close();

        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }

    @Test
    public void testPropagateIfPossible() throws Exception {
        try {
            throw new Exception();
        } catch (Throwable t) {
            Throwables.propagateIfPossible(t, Exception.class);
            Throwables.propagate(t);
        }
    }
}
