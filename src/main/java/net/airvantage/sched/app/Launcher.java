package net.airvantage.sched.app;

import java.security.CodeSource;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.session.HashSessionIdManager;
import org.eclipse.jetty.util.log.Slf4jLog;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main to launch the AvSched application with an embedded Jetty.
 * 
 * <p>
 * The database is automatically updated if needed before to start the application. The <b>--clear</b> parameter can be
 * used to remove all existing data.
 * </p>
 */
public class Launcher {

    private static final Logger LOG = LoggerFactory.getLogger(Launcher.class);
    private static final String DEFAULT_SERVLET_CTX_PATH = "/sched";

    public static void main(String[] args) throws Exception {

        long start = System.currentTimeMillis();
        JobDefServlet.startupTime = start;

        ServiceLocator.getInstance().init();
        Server server = null;

        try {

            // Check and update db schema

            ServiceLocator.getInstance().getSchemaMigrator().migrate();

            // Clean the db if required
            
            if (shouldClean(args)) {
                LOG.warn("Clean storage - all data is deleting.");
                ServiceLocator.getInstance().getJobSchedulingService().clearJobs();
            }

            // Start AvSched application
            
            ServiceLocator.getInstance().servicesPreload();
            server = createAndConfigureServer();
            server.start();

            System.out.println("");

            System.out.println("   ___   __   ___  ___ _  _ ___ ___");
            System.out.println("  /_\\ \\ / /__/ __|/ __| || | __|   \\");
            System.out.println(" / _ \\ V /___\\__ \\ (__| __ | _|| |) |");
            System.out.println("/_/ \\_\\_/    |___/\\___|_||_|___|___/");
            System.out.println("");

            JobDefServlet.startupDuration = System.currentTimeMillis() - start;
            LOG.info("[AV-SCHED] Application started, port : {}, context : {}, duration : {}.\n", ServiceLocator
                    .getInstance().getPort(), DEFAULT_SERVLET_CTX_PATH, JobDefServlet.startupDuration);

        } catch (Exception ex) {

            LOG.error("AvSched application failure.", ex);
            ex.printStackTrace();

            if (server != null) {
                server.setStopTimeout(60_000);
                server.stop();
            }
            System.exit(1);
        }
    }

    private static Server createAndConfigureServer() throws Exception {

        // Configure thread pool

        int poolSize = ServiceLocator.getInstance().getServletCnxPoolSize();
        
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(5000);
        QueuedThreadPool pool = new QueuedThreadPool(poolSize, poolSize, 60_000, queue);
        pool.setStopTimeout(30_000);

        Server server = new Server(pool);

        // Configure HTTP port

        ServerConnector connector = new ServerConnector(server);
        connector.setPort(ServiceLocator.getInstance().getPort());
        server.setConnectors(new Connector[] { connector });

        // Configure HTTP handler

        WebAppContext context = new WebAppContext();
        context.setResourceBase(getResourceLocation());
        context.setContextPath(DEFAULT_SERVLET_CTX_PATH);
        context.setLogger(new Slf4jLog());
        server.setHandler(context);

        // Configure session manager

        HashSessionIdManager sessionManager = new HashSessionIdManager();
        sessionManager.setWorkerName("avsched");
        server.setSessionIdManager(sessionManager);

        return server;
    }

    private static String getResourceLocation() {

        CodeSource source = Launcher.class.getProtectionDomain().getCodeSource();
        String location = source.getLocation().toExternalForm();

        LOG.info("Load application resources from {}", location);

        if (location.endsWith(".jar")) {
            return "jar:" + location + "!/";

        } else {
            return location;
        }
    }

    private static boolean shouldClean(String[] args) {

        boolean clean = false;
        if (args.length > 0 && "--clear".equals(args[0])) {
            clean = true;
        }

        return clean;
    }

}
