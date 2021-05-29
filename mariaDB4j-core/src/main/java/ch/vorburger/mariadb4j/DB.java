/*
 * #%L
 * MariaDB4j
 * %%
 * Copyright (C) 2012 - 2017 Michael Vorburger
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package ch.vorburger.mariadb4j;

import ch.vorburger.exec.ManagedProcess;
import ch.vorburger.exec.ManagedProcessBuilder;
import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.exec.OutputStreamLogDispatcher;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides capability to install, start, and use an embedded database.
 *
 * @author Michael Vorburger
 * @author Michael Seaton
 * @author Gordon Little
 */
public class DB {

    private static final Logger logger = LoggerFactory.getLogger(DB.class);

    protected final DBConfiguration configuration;

    private Path baseDir;
    private File libDir;
    private File dataDir;
    private ManagedProcess mysqldProcess;

    protected int dbStartMaxWaitInMS = 30000;

    protected DB(DBConfiguration config) {
        this.configuration = config;
    }

    public DBConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * This factory method is the mechanism for constructing a new embedded database for use. This
     * method automatically installs the database and prepares it for use.
     *
     * @param config Configuration of the embedded instance
     * @return a new DB instance
     * @throws ManagedProcessException if something fatal went wrong
     */
    public static DB newEmbeddedDB(DBConfiguration config) throws ManagedProcessException {
        DB db = new DB(config);
        db.prepareDirectories();
        db.unpackEmbeddedDb();
        db.install();
        return db;
    }

    /**
     * This factory method is the mechanism for constructing a new embedded database for use. This
     * method automatically installs the database and prepares it for use with default
     * configuration, allowing only for specifying port.
     *
     * @param port the port to start the embedded database on
     * @return a new DB instance
     * @throws ManagedProcessException if something fatal went wrong
     */
    public static DB newEmbeddedDB(int port) throws ManagedProcessException {
        DBConfigurationBuilder config = new DBConfigurationBuilder();
        config.setPort(port);
        return newEmbeddedDB(config.build());
    }

    protected ManagedProcess createDBInstallProcess() throws ManagedProcessException, IOException {
        logger.info("Installing a new embedded database to: " + baseDir);
        Path installDbCmdFile = newExecutableFile("bin", "mysql_install_db");
        if (Files.notExists(installDbCmdFile))
            installDbCmdFile = newExecutableFile("scripts", "mysql_install_db");
        if (Files.notExists(installDbCmdFile))
            throw new ManagedProcessException(
                    "mysql_install_db was not found, neither in bin/ nor in scripts/ under " + baseDir.toAbsolutePath());
        ManagedProcessBuilder builder = new ManagedProcessBuilder(installDbCmdFile.toFile());
        builder.setOutputStreamLogDispatcher(getOutputStreamLogDispatcher("mysql_install_db"));
        builder.getEnvironment().put(configuration.getOSLibraryEnvironmentVarName(), libDir.getAbsolutePath());
        builder.setWorkingDirectory(baseDir.toFile());
        if (!configuration.isWindows()) {
            builder.addFileArgument("--datadir", dataDir);
            builder.addFileArgument("--basedir", baseDir.toFile());
            builder.addArgument("--no-defaults");
            builder.addArgument("--force");
            builder.addArgument("--skip-name-resolve");
            // builder.addArgument("--verbose");
        } else {
            builder.addFileArgument("--datadir", toWindowsPath(dataDir));
        }
        ManagedProcess mysqlInstallProcess = builder.build();
        return mysqlInstallProcess;
    }

    private static File toWindowsPath(File file) throws IOException {
        return new File(file.getCanonicalPath().replace(" ", "%20"));
    }

    /**
     * Installs the database to the location specified in the configuration.
     *
     * @throws ManagedProcessException if something fatal went wrong
     */
    synchronized protected void install() throws ManagedProcessException {
        try {
            ManagedProcess mysqlInstallProcess = createDBInstallProcess();
            mysqlInstallProcess.start();
            mysqlInstallProcess.waitForExit();
        } catch (Exception e) {
            throw new ManagedProcessException("An error occurred while installing the database", e);
        }
        logger.info("Installation complete.");
    }

    protected String getWinExeExt() {
        return configuration.isWindows() ? ".exe" : "";
    }

    /**
     * Starts up the database, using the data directory and port specified in the configuration.
     *
     * @throws ManagedProcessException if something fatal went wrong
     */
    public synchronized void start() throws ManagedProcessException {
        logger.info("Starting up the database...");
        boolean ready = false;
        try {
            mysqldProcess = startPreparation();
            ready = mysqldProcess.startAndWaitForConsoleMessageMaxMs(getReadyForConnectionsTag(), dbStartMaxWaitInMS);
        } catch (Exception e) {
            logger.error("failed to start mysqld", e);
            throw new ManagedProcessException("An error occurred while starting the database", e);
        }
        if (!ready) {
            if (mysqldProcess != null && mysqldProcess.isAlive())
                mysqldProcess.destroy();
            throw new ManagedProcessException("Database does not seem to have started up correctly? Magic string not seen in "
                    + dbStartMaxWaitInMS + "ms: " + getReadyForConnectionsTag() + mysqldProcess.getLastConsoleLines());
        }
        logger.info("Database startup complete.");
    }

    protected String getReadyForConnectionsTag() {
        return "mysqld" + getWinExeExt() + ": ready for connections.";
    }

    synchronized ManagedProcess startPreparation() throws ManagedProcessException, IOException {
        ManagedProcessBuilder builder = new ManagedProcessBuilder(newExecutableFile("bin", "mysqld").toFile());
        builder.setOutputStreamLogDispatcher(getOutputStreamLogDispatcher("mysqld"));
        builder.getEnvironment().put(configuration.getOSLibraryEnvironmentVarName(), libDir.getAbsolutePath());
        builder.addArgument("--no-defaults"); // *** THIS MUST COME FIRST ***
        builder.addArgument("--console");
        if (this.configuration.isSecurityDisabled()) {
            builder.addArgument("--skip-grant-tables");
        }
        if (! hasArgument("--max_allowed_packet")) {
            builder.addArgument("--max_allowed_packet=64M");
        }
        builder.addFileArgument("--basedir", baseDir.toFile()).setWorkingDirectory(baseDir.toFile());
        if (!configuration.isWindows()) {
            builder.addFileArgument("--datadir", dataDir);
        } else {
            builder.addFileArgument("--datadir", toWindowsPath(dataDir));
        }
        addPortAndMaybeSocketArguments(builder);
        for (String arg : configuration.getArgs()) {
            builder.addArgument(arg);
        }
        cleanupOnExit();
        // because cleanupOnExit() just installed our (class DB) own
        // Shutdown hook, we don't need the one from ManagedProcess:
        builder.setDestroyOnShutdown(false);
        logger.info("mysqld executable: " + builder.getExecutable());
        return builder.build();
    }

    protected boolean hasArgument(final String argumentName) {
        for (String argument : this.configuration.getArgs()) {
            if (argument.startsWith(argumentName)) {
                return true;
            }
        }
        return false;
    }

    protected Path newExecutableFile(String dir, String exec) {
        return baseDir.resolve(dir).resolve(exec + getWinExeExt());
    }

    protected void addPortAndMaybeSocketArguments(ManagedProcessBuilder builder) throws IOException {
        builder.addArgument("--port=" + configuration.getPort());
        if (!configuration.isWindows()) {
            builder.addFileArgument("--socket", getAbsoluteSocketFile());
        }
    }

    protected void addSocketOrPortArgument(ManagedProcessBuilder builder) throws IOException {
        if (!configuration.isWindows()) {
            builder.addFileArgument("--socket", getAbsoluteSocketFile());
        } else {
            builder.addArgument("--port=" + configuration.getPort());
        }
    }

    /**
     * Config Socket as absolute path. By default this is the case because DBConfigurationBuilder
     * creates the socket in /tmp, but if a user uses setSocket() he may give a relative location,
     * so we double check.
     *
     * @return config.getSocket() as File getAbsolutePath()
     */
    protected File getAbsoluteSocketFile() {
        String socket = configuration.getSocket();
        File socketFile = new File(socket);
        return socketFile.getAbsoluteFile();
    }

    public void source(String resource) throws ManagedProcessException {
        source(resource, null, null, null);
    }

    public void source(InputStream resource) throws ManagedProcessException {
        source(resource, null, null, null);
    }

    public void source(String resource, String dbName) throws ManagedProcessException {
        source(resource, null, null, dbName);
    }

    public void source(InputStream resource, String dbName) throws ManagedProcessException {
        source(resource, null, null, dbName);
    }

    /**
     * Takes in a {@link InputStream} and sources it via the mysql command line tool.
     *
     * @param resource an {@link InputStream} InputStream to source
     * @param username the username used to login to the database
     * @param password the password used to login to the database
     * @param dbName the name of the database (schema) to source into
     * @throws ManagedProcessException if something fatal went wrong
     */
    public void source(InputStream resource, String username, String password, String dbName) throws ManagedProcessException {
        run("script file sourced from an InputStream", resource, username, password, dbName, false);
    }

    /**
     * Takes in a string that represents a resource on the classpath and sources it via the mysql
     * command line tool.
     *
     * @param resource the path to a resource on the classpath to source
     * @param username the username used to login to the database
     * @param password the password used to login to the database
     * @param dbName the name of the database (schema) to source into
     * @throws ManagedProcessException if something fatal went wrong
     */
    public void source(String resource, String username, String password, String dbName) throws ManagedProcessException {
        source(resource, username, password, dbName, false);
    }

    /**
     * Takes in a string that represents a resource on the classpath and sources it via the mysql
     * command line tool. Optionally force continue if individual statements fail.
     *
     * @param resource the path to a resource on the classpath to source
     * @param username the username used to login to the database
     * @param password the password used to login to the database
     * @param dbName the name of the database (schema) to source into
     * @param force if true then continue on error (mysql --force)
     * @throws ManagedProcessException if something fatal went wrong
     */
    public void source(String resource, String username, String password, String dbName, boolean force) throws ManagedProcessException {
        try (InputStream from = getClass().getClassLoader().getResourceAsStream(resource)) {
            if (from == null)
                throw new IllegalArgumentException("Could not find script file on the classpath at: " + resource);
            run("script file sourced from the classpath at: " + resource, from, username, password, dbName, force);
        } catch (IOException ioe) {
            logger.warn("Issue trying to close source InputStream. Raise warning and continue.", ioe);
        }
    }

    public void run(String command, String username, String password, String dbName) throws ManagedProcessException {
        run(command, username, password, dbName, false, true);
    }

    public void run(String command) throws ManagedProcessException {
        run(command, null, null, null);
    }

    public void run(String command, String username, String password) throws ManagedProcessException {
        run(command, username, password, null);
    }

    public void run(String command, String username, String password, String dbName, boolean force) throws ManagedProcessException {
        run(command, username, password, dbName, force, true);
    }

    public void run(String command, String username, String password, String dbName, boolean force, boolean verbose) throws ManagedProcessException {
        // If resource is created here, it should probably be released here also (as opposed to in protected run method)
        // Also move to try-with-resource syntax to remove closeQuietly deprecation errors.
        try (InputStream from = IOUtils.toInputStream(command, Charset.defaultCharset())) {
            final String logInfoText = verbose
                                       ? "command: " + command
                                       : "command (" + (command.length() / 1_024) + " KiB long)";
            run(logInfoText, from, username, password, dbName, force);
        } catch (IOException ioe) {
            logger.warn("Issue trying to close source InputStream. Raise warning and continue.", ioe);
        }
    }

    protected void run(String logInfoText, InputStream fromIS, String username, String password, String dbName, boolean force)
            throws ManagedProcessException {
        logger.info("Running a " + logInfoText);
        try {
            ManagedProcessBuilder builder = new ManagedProcessBuilder(newExecutableFile("bin", "mysql").toFile());
            builder.setOutputStreamLogDispatcher(getOutputStreamLogDispatcher("mysql"));
            builder.setWorkingDirectory(baseDir.toFile());
            if (username != null && !username.isEmpty())
                builder.addArgument("-u", username);
            if (password != null && !password.isEmpty())
                builder.addArgument("-p", password);
            if (dbName != null && !dbName.isEmpty())
                builder.addArgument("-D", dbName);
            if (force == true)
                builder.addArgument("-f");
            addSocketOrPortArgument(builder);
            if (fromIS != null)
                builder.setInputStream(fromIS);
            if (this.configuration.getProcessListener() != null) {
                builder.setProcessListener(this.configuration.getProcessListener());
            }

            ManagedProcess process = builder.build();
            process.start();
            process.waitForExit();
        } catch (Exception e) {
            throw new ManagedProcessException("An error occurred while running a " + logInfoText, e);
        }
        logger.info("Successfully ran the " + logInfoText);
    }

    public void createDB(String dbName) throws ManagedProcessException {
        this.run("create database if not exists `" + dbName + "`;");
    }

    public void createDB(String dbName, String username, String password) throws ManagedProcessException {
        this.run("create database if not exists `" + dbName + "`;", username, password);
    }

    protected OutputStreamLogDispatcher getOutputStreamLogDispatcher(@SuppressWarnings("unused") String exec) {
        return new MariaDBOutputStreamLogDispatcher();
    }

    /**
     * Stops the database.
     *
     * @throws ManagedProcessException if something fatal went wrong
     */
    public synchronized void stop() throws ManagedProcessException {
        if (mysqldProcess != null && mysqldProcess.isAlive()) {
            logger.debug("Stopping the database...");
            mysqldProcess.destroy();
            logger.info("Database stopped.");
        } else {
            logger.debug("Database was already stopped.");
        }
    }

    /**
     * Based on the current OS, unpacks the appropriate version of MariaDB to the file system based
     * on the configuration.
     */
    protected void unpackEmbeddedDb() {
        if (configuration.getBinariesClassPathLocation() == null) {
            logger.info("Not unpacking any embedded database (as BinariesClassPathLocation configuration is null)");
            return;
        }

        try {
            Util.extractFromClasspathToFile(configuration.getBinariesClassPathLocation(), baseDir);
            if (!configuration.isWindows()) {
                Stream.of("bin", "scripts").map(baseDir::resolve).filter(Files::isDirectory).forEach(dir -> {
                    healSymlinks(dir);
                    try (Stream<Path> executables = Files.list(dir)) {
                        executables
                                .filter(Predicate.not(Files::isSymbolicLink))
                                .forEach(Util::forceExecutable);
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                });
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Error unpacking embedded DB", e);
        }
    }

    // MariaDB 10.5 makes the 'mysql' commands symlinks to the mariadb commands
    // However, the MariaDB4j packaging and extracting process removes symlinks
    private static final Map<String, String> SYMLINKS = Map.of(
            "mariadb", "mysql",
            "mariadbd", "mysqld",
            "mariadb-dump", "mysqldump",
            "mariadb-install-db", "mysql_install_db");
    private void healSymlinks(Path directory) {
        Logger logger = LoggerFactory.getLogger(getClass());
        SYMLINKS.forEach((mariadbName, mysqlName) -> {
            logger.trace("Fixing symlink from {} to {}", mariadbName, mysqlName);
            Path executable = directory.resolve(mariadbName);
            Path symlink = directory.resolve(mysqlName);
            try {
                if (Files.exists(executable) && Files.exists(symlink) && Files.size(symlink) == 0) {
                    Files.delete(symlink);
                    Files.createSymbolicLink(symlink, executable);
                    logger.trace("Fixed symlink from {} to {}", mysqlName, mariadbName);
                }
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        });
    }

    /**
     * If the data directory specified in the configuration is a temporary directory, this deletes
     * any previous version. It also makes sure that the directory exists.
     *
     * @throws ManagedProcessException if something fatal went wrong
     */
    protected void prepareDirectories() throws ManagedProcessException {
        baseDir = Util.getDirectory(configuration.getBaseDir());
        libDir = Util.getDirectory(configuration.getLibDir()).toFile();
        try {
            String dataDirPath = configuration.getDataDir();
            if (Util.isTemporaryDirectory(dataDirPath)) {
                FileUtils.deleteDirectory(new File(dataDirPath));
            }
            dataDir = Util.getDirectory(dataDirPath).toFile();
        } catch (Exception e) {
            throw new ManagedProcessException("An error occurred while preparing the data directory", e);
        }
    }

    /**
     * Adds a shutdown hook to ensure that when the JVM exits, the database is stopped, and any
     * temporary data directories are cleaned up.
     */
    protected void cleanupOnExit() {
        String threadName = "Shutdown Hook Deletion Thread for Temporary DB " + dataDir.toString();
        final DB db = this;
        boolean cleanupDirs = configuration.isDeletingTemporaryBaseAndDataDirsOnShutdown();
        Path dataDir = this.dataDir.toPath();
        Path baseDir = this.baseDir;
        Runtime.getRuntime().addShutdownHook(new Thread(threadName) {

            @Override
            public void run() {
                // ManagedProcess DestroyOnShutdown ProcessDestroyer does
                // something similar, but it shouldn't hurt to better be save
                // than sorry and do it again ourselves here as well.
                try {
                    // Shut up and don't log if it was already stop() before
                    if (mysqldProcess != null && mysqldProcess.isAlive()) {
                        logger.info("cleanupOnExit() ShutdownHook now stopping database");
                        db.stop();
                    }
                } catch (ManagedProcessException e) {
                    logger.warn("cleanupOnExit() ShutdownHook: An error occurred while stopping the database", e);
                }
                if (cleanupDirs) {
                    deleteRecursively(dataDir);
                    deleteRecursively(baseDir);
                }
            }
        });
    }

    private static void deleteRecursively(Path directory) {
        if (true || !Files.isDirectory(directory) || !Util.isTemporaryDirectory(directory.toAbsolutePath().toString())) {
            return;
        }
        logger.info("cleanupOnExit() ShutdownHook quietly deleting temporary DB directory: {}", directory);
        try (Stream<Path> toDelete = Files.walk(directory)) {
            toDelete.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
            });
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    // The dump*() methods are intentionally *NOT* made "synchronized",
    // (even though with --lock-tables one could not run two dumps concurrently anyway)
    // because in theory this could cause a long-running dump to deadlock an application
    // wanting to stop() a DB. Let it thus be a caller's responsibility to not dump
    // concurrently (and if she does, it just fails, which is much better than an
    // unexpected deadlock).

    public ManagedProcess dumpXML(File outputFile, String dbName, String user, String password)
            throws IOException, ManagedProcessException {
        return dump(outputFile, Arrays.asList(dbName), true, true, true, user, password);
    }

    public ManagedProcess dumpSQL(File outputFile, String dbName, String user, String password)
            throws IOException, ManagedProcessException {
        return dump(outputFile, Arrays.asList(dbName), true, true, false, user, password);
    }

    protected ManagedProcess dump(File outputFile, List<String> dbNamesToDump,
                                               boolean compactDump, boolean lockTables, boolean asXml,
                                               String user, String password)
            throws ManagedProcessException, IOException {
        ManagedProcessBuilder builder = new ManagedProcessBuilder(newExecutableFile("bin", "mysqldump").toFile());

        builder.addStdOut(new BufferedOutputStream(new FileOutputStream(outputFile)));
        builder.setOutputStreamLogDispatcher(getOutputStreamLogDispatcher("mysqldump"));
        builder.addArgument("--port=" + configuration.getPort());
        if (!configuration.isWindows()) {
            builder.addFileArgument("--socket", getAbsoluteSocketFile());
        }
        if (lockTables) {
            builder.addArgument("--flush-logs");
            builder.addArgument("--lock-tables");
        }
        if (compactDump) {
            builder.addArgument("--compact");
        }
        if (asXml) {
            builder.addArgument("--xml");
        }
        if (StringUtils.isNotBlank(user)) {
            builder.addArgument("-u");
            builder.addArgument(user);
            if (StringUtils.isNotBlank(password)) {
                builder.addArgument("-p" + password);
            }
        }
        builder.addArgument(StringUtils.join(dbNamesToDump, StringUtils.SPACE));
        builder.setDestroyOnShutdown(true);
        return builder.build();
    }
}
