package thelabdude.nsrdb;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.xerces.parsers.AbstractSAXParser;
import org.apache.xerces.xni.Augmentations;
import org.apache.xerces.xni.QName;
import org.apache.xerces.xni.XMLAttributes;
import org.apache.xerces.xni.XNIException;
import org.cyberneko.html.HTMLConfiguration;
import org.xml.sax.InputSource;

/**
 * Downloads the NSRDB data set from the OpenEI.org site and saves it into a single SequenceFile in a Hadoop file
 * system.
 */
public class GetData {

  /**
   * Runs the GetData application.
   * 
   * @param args
   *          - Command-line args passed by the user.
   */
  public static void main(String[] args) {
    // setup the application
    GetData app = null;
    CommandLine cli = processCommandLineArgs("GetData", args);
    try {
      app = new GetData(cli);
    } catch (Exception exc) {
      showUsageAndExit(1, "GetData application is not configured correctly! " + exc, "GetData", cli.getOptions());
    }

    // if we get here, then we know it's configured correctly, so run it ...
    try {
      app.run();
    } catch (Exception exc) {
      System.err.print("GetData application failed due to: ");
      exc.printStackTrace(System.err);
    }
  }

  private static final String DEFAULT_URL = "http://en.openei.org/datasets/files/39/pub/";
  private static final String DEFAULT_REGEX = "^\\d.*\\.tar\\.gz$";
  private static final String DEFAULT_DOWNLOAD_DIR = System.getProperty("java.io.tmpdir") + "/nsrdb";
  private static final long MS_IN_MIN = 60000;
  private static final long MB = (1024 * 1024);

  protected URL dataIndexURL;
  protected Pattern dataLinkRegex;
  protected File downloadDir;
  protected FileSystem fs;
  protected Path hadoopDataPath;
  protected boolean skipDownloadStep = false;
  protected int chunkSize = -1;

  /**
   * Setup the GetData application before running.
   * 
   * @param cli
   * @throws Exception
   */
  public GetData(CommandLine cli) throws Exception {

    // required: where to save solar data in hadoop
    hadoopDataPath = new Path(cli.getOptionValue("hadoopDataPath"));

    // optional: where to look for data to download on the OpenEI.org site
    dataIndexURL = new URL(cli.getOptionValue("nsrdbIndexUrl", DEFAULT_URL));

    // optional: how do determine if a link points to a data file that needs to be downloaded
    dataLinkRegex = Pattern.compile(cli.getOptionValue("dataLinkRegex", DEFAULT_REGEX));

    // optional: where to download the raw data files to on this machine
    downloadDir = new File(cli.getOptionValue("downloadDir", DEFAULT_DOWNLOAD_DIR));
    if (!downloadDir.isDirectory()) {
      downloadDir.mkdirs();
      if (!downloadDir.isDirectory()) {
        showUsageAndExit(1, "ERROR: Cannot create local download directory " + downloadDir.getAbsolutePath(), "GetData", cli.getOptions());
      }
    }

    Configuration conf = new Configuration();

    // Is this an Amazon S3 URI? If so, setup AWS credentials in the Hadoop Configuration
    String scheme = hadoopDataPath.toUri().getScheme();
    if ("s3".equals(scheme) || "s3n".equals(scheme)) {
      String awsAccessKeyId = cli.getOptionValue(scheme + "AwsAccessKeyId");
      if (awsAccessKeyId != null) {
        conf.set("fs." + scheme + ".awsAccessKeyId", awsAccessKeyId);
        conf.set("fs." + scheme + ".awsSecretAccessKey", cli.getOptionValue(scheme + "AwsSecretAccessKey"));
      }
    }

    // determine how the user wants to split up the SequenceFile output
    String chunkSizeArg = cli.getOptionValue("chunkSize");
    if (chunkSizeArg != null) {
      chunkSize = Integer.parseInt(chunkSizeArg);
      if (chunkSize <= 0) {
        chunkSize = -1;
      }
      if (chunkSize > 1024) {
        chunkSize = 1024;
      }
    }

    fs = FileSystem.get(hadoopDataPath.toUri(), conf);
    if (fs.getConf() == null) {
      // extra sanity check on the Hadoop configuration
      throw new IllegalStateException("Hadoop file system configuration for " + hadoopDataPath + " is invalid!");
    }

    skipDownloadStep = cli.hasOption("skipDownloadStep");
    displayConfig(System.out);
  }

  /**
   * Downloads all the data files to a temporary local directory and then merges the contents of each tar.gz file into a
   * SequenceFile stored in a Hadoop file system.
   * 
   * @throws Exception
   */
  public void run() throws Exception {
    long entr = System.currentTimeMillis(); // started at ...

    // first, do download the data from the site
    Set<File> dataFiles = downloadData(entr);

    if (dataFiles == null || dataFiles.isEmpty()) {
      System.out.println("No downloaded data files found in " + downloadDir.getAbsolutePath());
      return;
    }

    System.out.format("Found %d downloaded files in %s%n", dataFiles.size(), downloadDir.getAbsolutePath());

    // next, merge the contents of each tar.gz file into one or more,
    // compressed SequenceFile(s) stored in a Hadoop file system or S3
    long maxSequenceFileBytes = (chunkSize > 0) ? (chunkSize * MB) : -1;
    int f = 0;
    int chunk = 0;
    Path chunkPath = hadoopDataPath;
    SequenceFile.Writer seqFileWriter = null;
    try {
      if (maxSequenceFileBytes > 0) {
        // user wants us to chunk the data
        chunkPath = new Path(hadoopDataPath, "chunk-0");        
      }
      seqFileWriter = SequenceFile.createWriter(fs, fs.getConf(), chunkPath, StationYearWritable.class, BytesWritable.class, SequenceFile.CompressionType.BLOCK);

      // append all data from all downloaded files into the SequenceFile
      for (File dataFile : dataFiles) {
        System.out.format("Processing data file %s at %d%n", dataFile.getName(), ++f);
        appendDataToSeqFile(seqFileWriter, dataFile);
        if (maxSequenceFileBytes > 0 && seqFileWriter.getLength() >= maxSequenceFileBytes) {
          IOUtils.closeStream(seqFileWriter);
          chunkPath = new Path(hadoopDataPath, "chunk-" + (++chunk));
          seqFileWriter = SequenceFile.createWriter(fs, fs.getConf(), chunkPath, StationYearWritable.class, BytesWritable.class, SequenceFile.CompressionType.BLOCK);
        }
      }
    } finally {
      IOUtils.closeStream(seqFileWriter);
    }

    System.out.println("GetData completed successfully after " + calcRunningTime(entr));
  }

  /**
   * Appends all the data in a GZipped TAR file into the SequenceFile.Writer.
   * 
   * @param seqFileWriter
   * @param tarGzFile
   * @throws Exception
   */
  protected int appendDataToSeqFile(SequenceFile.Writer seqFileWriter, File tarGzFile) throws Exception {
    int wroteBytes = 0;
    File tarFile = uncompressTarFile(tarGzFile);
    TarArchiveInputStream tarIn = null;
    TarArchiveEntry tarEntry = null;
    try {
      tarIn = new TarArchiveInputStream(new FileInputStream(tarFile));
      while ((tarEntry = tarIn.getNextTarEntry()) != null) {
        if (tarEntry.isDirectory())
          continue;

        byte[] content = processTarEntry(tarIn, tarEntry);
        if (content != null && content.length > 0) {
          StationYearWritable key = entryNameToKey(tarEntry.getName());
          if (key != null) {
            BytesWritable value = new BytesWritable(content);
            seqFileWriter.append(key, value);
            wroteBytes += value.getLength();
          }
        }
      }
    } finally {
      if (tarIn != null) {
        try {
          tarIn.close();
        } catch (Exception zzz) {
        }
      }
      tarFile.delete();
    }
    return wroteBytes;
  }

  /**
   * Downloads data files from the OpenEI.org Web site and stores them in a directory on the current server.
   * 
   * @param startedAt
   * @return A sorted Set containing the downloaded files.
   * @throws Exception
   */
  protected Set<File> downloadData(long startedAt) throws Exception {
    // for offline testing, you can skip downloading, which assumes that
    // you've already downloaded some data into a test directory
    long totalBytes = 0;
    int linkCount = 0;
    List<String> failedLinks = new ArrayList<String>();
    if (!skipDownloadStep) {
      // doing a real download ...
      Set<String> dataLinks = extractDataLinks();
      if (dataLinks == null || dataLinks.isEmpty()) {
        System.err.println("No data links found at " + dataIndexURL.toExternalForm());
        return null;
      }

      linkCount = dataLinks.size();
      System.out.println("Found " + linkCount + " data links to download and process.");
      System.out.println("Please be patient as this may take a while ...");

      Iterator<String> linkIter = dataLinks.iterator();
      int i = 0;
      while (linkIter.hasNext()) {
        String link = linkIter.next();
        ++i;

        long byteCount = 0;
        File dataFile = new File(downloadDir, link);
        if (dataFile.isFile()) {
          byteCount = dataFile.length();
        } else {
          byteCount = downloadLink(dataFile, link);
          Thread.sleep(2000L); // so we don't overwhelm the server
        }
        if (byteCount > 0) {
          totalBytes += byteCount;
          System.out.println("Downloaded " + byteCount + " bytes for " + link + " @" + i + "/" + linkCount);
        } else {
          failedLinks.add(link);
        }
      }
    }

    // Get the recently downloaded data files
    File[] dataFiles = downloadDir.listFiles(new java.io.FilenameFilter() {
      public boolean accept(File dir, String name) {
        return (name != null && name.endsWith(".tar.gz"));
      }
    });

    // return a sorted list of files so the logging output is more logical
    Set<File> dataFileSet = new TreeSet<File>();
    for (int f = 0; f < dataFiles.length; f++)
      dataFileSet.add(dataFiles[f]);

    // report the outcome of this download
    String exeTime = calcRunningTime(startedAt);
    double totalMB = (double) totalBytes / (1024 * 1024);
    double roundedMB = (double) Math.round(totalMB * 100) / 100;
    System.out.printf("Download of %s of %s data links containing %s MB completed after %s%n", dataFileSet.size(), linkCount, roundedMB, exeTime);

    if (!failedLinks.isEmpty()) {
      System.out.println("\tWARNING: " + failedLinks.size() + " failed links:");
      for (String failed : failedLinks)
        System.out.println("\t\t" + failed);
    }

    return dataFileSet;
  }

  /**
   * Converts a file name to a Writable key containing a station ID and year.
   * 
   * @param entryName
   * @return
   */
  protected StationYearWritable entryNameToKey(String entryName) {
    int lastSlashAt = entryName.lastIndexOf("/");
    String key = (lastSlashAt != -1) ? entryName.substring(lastSlashAt + 1) : entryName;
    int lastDotAt = key.lastIndexOf(".");
    if (lastDotAt != -1)
      key = key.substring(0, lastDotAt);

    int underAt = key.indexOf("_");
    if (underAt == -1)
      return null; // cannot handle this key ...

    String stationStr = key.substring(0, underAt);
    String yearStr = key.substring(underAt + 1);
    StationYearWritable writableKey = null;
    try {
      writableKey = new StationYearWritable(Long.parseLong(stationStr), Integer.parseInt(yearStr));
    } catch (NumberFormatException nfe) {
      System.err.println("Failed to parse station '" + stationStr + "' and/or year '" + yearStr + "' into a number due to: " + nfe.toString());
    }

    return writableKey;
  }

  protected byte[] processTarEntry(TarArchiveInputStream tarIn, TarArchiveEntry tarEntry) throws Exception {
    byte[] content = null;
    if (tarEntry.getName().endsWith(".csv")) {
      content = new byte[(int) tarEntry.getSize()];
      int offset = 0;
      while (offset < content.length)
        offset = tarIn.read(content, offset, content.length - offset);
    }
    return content;
  }

  protected File uncompressTarFile(File gzFile) throws Exception {
    File tarFile = new File(downloadDir, "tmp.tar");
    FileOutputStream out = null;
    GzipCompressorInputStream gzIn = null;
    final byte[] buffer = new byte[1024];
    int n = 0;
    try {
      gzIn = new GzipCompressorInputStream(new BufferedInputStream(new FileInputStream(gzFile)));
      out = new FileOutputStream(tarFile);
      while ((n = gzIn.read(buffer)) > 0)
        out.write(buffer, 0, n);
      out.flush();
    } catch (IOException ioExc) {
      System.err.print("Failed to un-compress " + gzFile.getAbsolutePath() + " due to: ");
      ioExc.printStackTrace(System.err);
      throw ioExc;
    } finally {
      if (gzIn != null) {
        try {
          gzIn.close();
        } catch (Exception zzz) {
        }
      }
      if (out != null) {
        try {
          out.close();
        } catch (Exception zzz) {
        }
      }
    }
    return tarFile;
  }

  protected long downloadLink(File dataFile, String link) {
    long byteCount = 0;
    OutputStream out = null;
    try {
      URL url = new URL(dataIndexURL, link);
      out = new FileOutputStream(dataFile);
      byteCount = saveURLToStream(url, out);
    } catch (Exception exc) {
      System.err.println("Failed to download [" + link + "] due to: " + exc.toString());
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (Exception zzz) {
        }
      }
    }
    return byteCount;
  }

  protected long saveURLToStream(URL url, OutputStream out) throws Exception {
    long byteCount = 0;
    InputStream in = null;
    byte[] buf = new byte[1024];
    int b = 0;
    try {
      in = url.openStream();
      while ((b = in.read(buf)) > 0) {
        out.write(buf, 0, b);
        byteCount += b;
      }
      out.flush();
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (Exception zzz) {
        }
      }
    }
    return byteCount;
  }

  protected Set<String> extractDataLinks() throws Exception {
    Set<String> links = null;
    InputStream in = null;
    try {
      in = dataIndexURL.openStream();
      LinkExtractor parser = new LinkExtractor(dataLinkRegex);
      parser.parse(new InputSource(in));
      links = parser.getMatchingLinks();
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (Exception zzz) {
        }
      }
    }
    return links;
  }

  protected String calcRunningTime(long entr) {
    long diff = (System.currentTimeMillis() - entr);
    double mins = Math.floor((double) diff / MS_IN_MIN);
    double secs = (diff - mins * MS_IN_MIN) / 1000;
    long tmp = Math.round(secs * 100);
    return Math.round(mins) + "m " + ((double) tmp / 100) + "s";
  }

  /**
   * Displays configuration information about this application.
   * 
   * @param out
   */
  protected void displayConfig(PrintStream out) {
    out.println("\nGetData configured successfully with:");
    out.println("\t nsrdbIndexUrl = " + dataIndexURL.toExternalForm());
    out.println("\t dataLinkRegex = " + dataLinkRegex.pattern());
    out.println("\t downloadDir = " + downloadDir.getAbsolutePath());
    out.println("\t hadoopDataPath = " + hadoopDataPath.toString());
    out.println("\t chunkSize = " + (chunkSize > 0 ? "" + chunkSize : "N/A"));
    out.println("\t skipDownloadStep = " + skipDownloadStep);
    out.println("\t Hadoop FS: " + fs.getUri());
    out.println();
  }

  // Parses Open EI HTML containing links to data files to download.
  class LinkExtractor extends AbstractSAXParser {
    private Set<String> links;
    private Pattern regex;

    public LinkExtractor(Pattern regex) {
      super(new HTMLConfiguration());
      this.links = new TreeSet<String>();
      this.regex = regex;
    }

    public Set<String> getMatchingLinks() {
      return links;
    }

    public void startElement(QName element, XMLAttributes attributes, Augmentations augs) throws XNIException {
      super.startElement(element, attributes, augs);
      // only interested in anchors with href starting with a number and ending with gz
      String tag = element.localpart.toLowerCase();
      if ("a".equals(tag)) {
        String href = attributes.getValue("href");
        if (href != null) {
          Matcher m = regex.matcher(href);
          if (m.matches()) {
            links.add(href);
          }
        }
      }
    }
  }

  /**
   * Parses the command-line arguments passed by the user.
   * 
   * @param action
   * @param args
   * @return CommandLine The Apache Commons CLI object.
   */
  public static CommandLine processCommandLineArgs(String appName, String[] args) {
    Options options = getCommandLineOptions();
    CommandLine cli = null;
    try {
      cli = (new GnuParser()).parse(options, args);
    } catch (ParseException exp) {
      boolean hasHelpArg = false;
      if (args != null && args.length > 0) {
        for (int z = 0; z < args.length; z++) {
          if ("-help".equals(args[z])) {
            hasHelpArg = true;
            break;
          }
        }
      }
      if (!hasHelpArg) {
        System.err.println("Failed to parse command-line arguments due to: " + exp.getMessage());
      }
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(appName, options);
      System.exit(1);
    }

    if (cli.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(appName, options);
      System.exit(0);
    }

    return cli;
  }

  private static Options getCommandLineOptions() {
    Options options = new Options();
    options.addOption("help", false, "Print this message");
    options.addOption("verbose", false, "Generate verbose log messages");
    options.addOption("debug", false, "Generate debug messages");
    options.addOption("skipDownloadStep", false, "Useful for testing on a small sub-set of existing data files");
    options.addOption(buildOption("hadoopDataPath", "Where to save solar data in your Hadoop file system or S3 (1 or more SequenceFile)", null));
    options.addOption(buildOption("chunkSize", "Size of each SequenceFile created in MB; only useful when writing output to S3", "-1"));
    options.addOption(buildOption("nsrdbIndexUrl", "URL of the page to scan for data to download", DEFAULT_URL));
    options.addOption(buildOption("dataLinkRegex", "Regular Expression to find data links", DEFAULT_REGEX));
    options.addOption(buildOption("downloadDir", "Local directory where downloaded data files are written to", DEFAULT_DOWNLOAD_DIR));
    options.addOption(buildOption("s3nAwsAccessKeyId", "Amazon S3 (native) Access ID (use s3AwsAccessKeyId for s3 URIs)", ""));
    options.addOption(buildOption("s3nAwsSecretAccessKey", "Amazon S3 (native) Secret Access Key (use s3AwsSecretAccessKey for s3 URIs)", ""));
    return options;
  }

  /**
   * Displays usage information to the console and then exits.
   * 
   * @param exitCode
   * @param errorMsg
   */
  protected static void showUsageAndExit(int exitCode, String errorMsg, String appName, Option[] aOptions) {
    if (errorMsg != null) {
      System.err.format("%nERROR: %s%n%n", errorMsg);
    }
    HelpFormatter formatter = new HelpFormatter();
    Options options = new Options();
    for (Option option : aOptions) {
      options.addOption(option);
    }
    formatter.printHelp(appName, options);
    System.exit(exitCode);
  }

  private static Option buildOption(String argName, String shortDescription, String defaultValue) {
    if (defaultValue != null) {
      shortDescription += (" Default is " + defaultValue);
    }
    return OptionBuilder.hasArg().isRequired((defaultValue == null)).withDescription(shortDescription).create(argName);
  }
}
