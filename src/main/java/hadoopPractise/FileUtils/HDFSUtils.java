package hadoopPractise.FileUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class HDFSUtils {

	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}

	public static void main(String[] args) throws IOException {
		String uri = "hdfs://localhost" + args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		listStatus(fs, args);
	}

	public static void read(FileSystem fs, String src) {
		InputStream in = null;
		try {
			in = fs.open(new Path(src));
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
	}

	public static void listStatus(FileSystem fs, String[] paths) throws IOException {
		Path[] dfsPaths = new Path[paths.length];
		for (int i = 0; i < paths.length; i++) {
			dfsPaths[i] = new Path(paths[i]);
		}
		FileStatus[] status = fs.listStatus(dfsPaths);
		Path[] listedPaths = FileUtil.stat2Paths(status);
		for (Path p : listedPaths) {
			System.out.println(p);
		}
	}

	public static void write(FileSystem fs, String src, String content) {

	}

	public static void copy(FileSystem fs, String src, String dst) throws IOException {
		InputStream in = new BufferedInputStream(new FileInputStream(src));
		OutputStream out = fs.create(new Path(dst), new Progressable() {
			@Override
			public void progress() {
				System.out.println(".");
			}
		});
		IOUtils.copyBytes(in, out, 4096, true);
	}
}
