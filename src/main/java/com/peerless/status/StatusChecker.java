package com.peerless.status;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.HttpsURLConnection;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public final class StatusChecker implements Runnable {
	static private final Pattern pipeFilePattern = Pattern.compile("^(.+)\\|(.+)\\|(.+)\\|$");
	static private final Options options;
	static public final String defaultPipeFileName = "CAXY_MEDIA_DOC.PIPE";
	static public final String defaultUrlBase = "https://docs.peerless-av.com/";
	static public final int defaultTimeout = 300;
	static public final int defaultMaxRetries = 3;
	static public final int defaultThreads = 50;
	static {
		options = new Options();
		options.addOption(new Option("f", "file", true, "The PIPE file to parse. (Defaults to CAXY_MEDIA_DOC.PIPE.)"));
		options.addOption(new Option("b", "base", true, "The URL prefix used to resolve filenames from the PIPE file. (Defaults to https://docs.peerless-av.com/.)"));
		options.addOption(new Option("t", "timeout", true, "The time in seconds before execution times out. (Defaults to 300.)"));
		options.addOption(new Option("r", "retries", true, "The number of failed retries before a request is discarded. (Defaults to 3.)"));
		options.addOption(new Option("p", "threads", true, "The number of threads to create. Set to 0 to use the automatic thread pool. (Defaults to 50.)"));
		options.addOption(new Option("d", "decode", false, "Decodes percent-encoded paths in output."));
		options.addOption(new Option("v", "verbose", false, "Prints detailed information on requests."));
		options.addOption(new Option("l", "list", false, "Prints all results regardless of status."));
		options.addOption(new Option("h", "help", false, "Prints this message."));
	}
	static public void main(final String[] args) {
		final CommandLineParser parser = new DefaultParser();
		CommandLine cl = null;
		try {
			cl = parser.parse(options, args);
		} catch (final ParseException e) {
			e.printStackTrace();
			System.exit(100);
		}
		if (cl.hasOption("help")) {
			new HelpFormatter().printHelp("java -jar *.jar <OPTIONS>", options);
			System.exit(0);
		}
		final File pipe = new File(cl.getOptionValue("file", defaultPipeFileName));
		final HashSet<String> files = new HashSet<String>(4096);
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(pipe)));
			String line = in.readLine(); // Skip the first line of input
			while ((line = in.readLine()) != null) {
				final Matcher matcher = pipeFilePattern.matcher(line);
				if (matcher.matches()) {
					files.add(matcher.group(3));
				}
			}
		} catch (final FileNotFoundException e) {
			e.printStackTrace();
			System.exit(404);
		} catch (final UnsupportedEncodingException e) {
			throw new Error(e); // UTF-8 should always be supported
		} catch (final IOException e) {
			e.printStackTrace();
			System.exit(500);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (final IOException e) {
					e.printStackTrace();
				}
			}
		}
		new StatusChecker(files.toArray(new String[] {}), Integer.valueOf(cl.getOptionValue("timeout", Integer.toString(defaultTimeout))), Integer.valueOf(cl.getOptionValue("retries", Integer.toString(defaultMaxRetries))), Integer.valueOf(cl.getOptionValue("threads", Integer.toString(defaultThreads))), cl.hasOption("decode"), cl.hasOption("verbose"), cl.hasOption("list")).run();
	}
	private final String[] files;
	private final int timeout;
	private final int maxRetries;
	private final int threads;
	private final boolean decode;
	private final boolean verbose;
	private final boolean list;
	private volatile int successes = 0;
	private volatile int failures = 0;
	private volatile double requestDuration = 0;
	private volatile int requestTotal = 0;
	private volatile int successRetries = 0;
	private volatile int failureRetries = 0;
	public StatusChecker(final String[] files, final int timeout, final int maxRetries, final int threads, final boolean decode, final boolean verbose, final boolean list) {
		this.files = files;
		this.timeout = timeout;
		this.maxRetries = maxRetries;
		this.threads = threads;
		this.decode = decode;
		this.verbose = verbose;
		this.list = list;
	}
	public void run() {
		final long start = System.nanoTime();
		final ExecutorService exec = this.threads == 0 ? Executors.newCachedThreadPool() : Executors.newFixedThreadPool(this.threads);
		for (final String s : this.files) {
			exec.execute(new Runnable() {
				public boolean request() {
					StatusChecker.this.requestTotal++;
					final long start = System.nanoTime();
					try {
						final URI uri = new URI(defaultUrlBase + s.replaceAll(" ", "%20"));
						final URL url = uri.toURL();
						final HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
						try {
							connection.connect();
							final int status = connection.getResponseCode();
							if (StatusChecker.this.list || (status < 200 || status >= 300)) {
								if (!StatusChecker.this.verbose) {
									System.out.println(StatusChecker.this.decode ? URLDecoder.decode(s, java.nio.charset.Charset.defaultCharset().name()) : s);
								} else {
									System.out.println(String.format("%1$d\t%2$s", status, StatusChecker.this.decode ? URLDecoder.decode(url.toExternalForm(), java.nio.charset.Charset.defaultCharset().name()) : url.toExternalForm()));
								}
							}
							connection.disconnect();
						} catch (final SocketException e) {
							return true; // Retry
						}
					} catch (final URISyntaxException e) {
						e.printStackTrace();
					} catch (final MalformedURLException e) {
						e.printStackTrace();
					} catch (final IOException e) {
						e.printStackTrace();
					}
					final long end = System.nanoTime();
					StatusChecker.this.requestDuration += (end - start) / 1000000000.0;
					return false;
				}
				public void run() {
					int retries = 0;
					boolean retry = this.request();
					while (retry) {
						if (retries < StatusChecker.this.maxRetries) {
							retries++;
							retry = this.request();
						} else {
							System.err.println("Retry limit exceeded for " + s);
							StatusChecker.this.failures++;
							StatusChecker.this.failureRetries += retries;
							return;
						}
					}
					StatusChecker.this.successes++;
					StatusChecker.this.successRetries += retries;
				}
			});
		}
		try {
			exec.shutdown();
			final int code = exec.awaitTermination(this.timeout, TimeUnit.SECONDS) ? 0 : 503;
			final long end = System.nanoTime();
			if (this.verbose) {
				System.out.println();
				System.out.println("// STATS");
				System.out.println(String.format("Executed %1$d requests in %2$f seconds, with an average of %3$f seconds per request.", this.requestTotal, (end - start) / 1000000000.0, this.requestDuration / this.requestTotal));
				System.out.println(String.format("%1$d succeeded (after %2$d retries) and %3$d failed (after %4$d retries) for a success rate of %5$f.", this.successes, this.successRetries, this.failures, this.failureRetries, (double) this.successes / this.failures));
			}
			System.exit(code);
		} catch (final InterruptedException e) {
			e.printStackTrace();
			System.exit(503);
		}
	}
}
