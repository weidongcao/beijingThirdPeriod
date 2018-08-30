package com.rainsoft.j2se;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * 演示如何将数据从一个通道复制到另一个通道或从一个文件复制到另一个文件
 * Created by CaoWeiDong on 2018-08-27.
 */
public class ChannelDemo {
    public static void main(String[] args) throws IOException {
        String path = System.getProperty("user.dir");
        System.out.println("path = " + path);
        FileInputStream input = new FileInputStream(path + "/testin.txt");
        ReadableByteChannel source = input.getChannel();
        FileOutputStream output = new FileOutputStream(path + "/testout.txt");
        WritableByteChannel destrination = output.getChannel();
        copyData(source, destrination);
        source.close();
        input.close();
        output.close();
        destrination.close();
        System.out.println("Copy Data finished...");
    }

    private static void copyData(ReadableByteChannel src, WritableByteChannel dest) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(20 * 1024);
        while (src.read(buffer) != -1) {
            buffer.flip();
            while (buffer.hasRemaining()) {
                dest.write(buffer);
            }
            buffer.clear();
        }
    }
}
