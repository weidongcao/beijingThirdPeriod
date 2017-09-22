package com.rainsoft.utils.io;

import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @Name com.rainsoft.data.gateway.fin.utils.FileUtils
 * @Description
 * 
 * @Author Sugar
 * @Version 2016年12月26日 上午11:30:23
 * @Copyright 上海云辰信息科技有限公司
 */
public class FileUtils extends org.apache.commons.io.FileUtils {
	private static Logger logger = LoggerFactory.getLogger(FileUtils.class);
	/**
	 * 列出目录下的所有文件，包括子目录
	 * 
	 * @param dir
	 * @param filter
	 * @return
	 */
	public static List<String> list(String dir, final FileFilter filter) {
		return list(new File(dir), filter);
	}

	public static List<String> list(File dir, final FileFilter filter) {
		if (dir == null || !dir.exists()) {
			return null;
		}
		File[] files = null;
		if (filter != null) {
			files = dir.listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					if (pathname.isDirectory()) {
						return true;
					}
					return filter.accept(pathname);
				}
			});
		} else {
			files = dir.listFiles();
		}
		List<String> list = new ArrayList<String>();
		List<String> tmp = null;
		for (File f : files) {
			if (f.isDirectory()) {
				tmp = list(f, filter);
				if (tmp != null && tmp.size() > 0) {
					list.addAll(tmp);
				}
			} else {
				list.add(f.getAbsolutePath());
			}
		}
		return list;
	}

	public static List<String> readLines(File file) throws IOException {
		InputStream in = null;
		try {
			in = openInputStream(file);
			return IOUtils.readLines(in, Charsets.toCharset("UTF-8"));
		} finally {
			IOUtils.closeQuietly(in);
		}
	}

	public static boolean delete(File file) {
		return file.delete();
	}

	/**
	 * 删除文件及空目录
	 * 
	 * @param dir
	 * @param file
	 */
	public static void delete(String dir, File file) {
		delete(dir, file, true);
	}

	/**
	 * 删除文件
	 * 
	 * @param dir
	 * @param file
	 * @param delEmptyParent
	 */
	public static void delete(String dir, File file, boolean delEmptyParent) {
		long l = System.currentTimeMillis();
		boolean del = file.delete();
		if (file.isFile()) {
			while (!del && file.exists()) {
				del = file.delete();
				if (System.currentTimeMillis() - l >= 2000) {// 2秒内未删除成功就放弃
					if (!del && logger.isTraceEnabled()) {
						logger.trace("Delete file fail: {}", file.getName());
					}
					break;
				}
			}
		}
		if (del) {
			if (delEmptyParent && !file.getParent().equals(dir)) {// 删除空父级目录
				delete(dir, file.getParentFile(), delEmptyParent);
			}
		}
	}
}
