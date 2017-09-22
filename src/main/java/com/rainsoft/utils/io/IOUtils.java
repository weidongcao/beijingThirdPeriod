package com.rainsoft.utils.io;

import org.apache.commons.io.Charsets;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * @Name com.rainsoft.data.gateway.fin.utils.IOUtils
 * @Description
 * 
 * @Author Sugar
 * @Version 2017年2月7日 下午5:57:41
 * @Copyright 上海云辰信息科技有限公司
 */
public class IOUtils {

	public static void closeQuietly(InputStream input) {
		closeQuietly((Closeable) input);
	}

	public static void closeQuietly(Closeable closeable) {
		try {
			if (closeable != null) {
				closeable.close();
			}
		} catch (IOException ioe) {
			// ignore
		}
	}

	public static BufferedReader toBufferedReader(Reader reader) {
		return reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader(reader);
	}

	public static List<String> readLines(InputStream input, Charset encoding) throws IOException {
		InputStreamReader reader = new InputStreamReader(input, Charsets.toCharset(encoding));
		return readLines(reader);
	}

	public static List<String> readLines(Reader input) throws IOException {
		BufferedReader reader = toBufferedReader(input);
		List<String> list = new ArrayList<String>();
		String line = reader.readLine();
		while (line != null) {
			list.add(line);
			line = reader.readLine();
		}
		reader.close();
		return list;
	}
}
