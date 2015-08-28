package net.airvantage.sched.conf;

public class Keys {

    public static final String SECRET = "av-sched.secret";

    public class Db {

        public static final String SERVER = "av-sched.db.server";
        public static final String PORT = "av-sched.db.port";
        public static final String DB_NAME = "av-sched.db.dbName";
        public static final String USER = "av-sched.db.user";
        public static final String PASSWORD = "av-sched.db.password";

        public static final String POOL_MIN = "av-sched.db.cnx.pool.min";
        public static final String POOL_MAX = "av-sched.db.cnx.pool.max";

    }

    public class Io {

        public static final String OUT_CNX_POOL_SIZE = "av-sched.output.cnx.pool.size";
        public static final String OUT_THREAD_POOL_SIZE = "av-sched.wakeup.job.thread.pool.size";
        public static final String OUT_THREAD_QUEUE_SIZE = "av-sched.wakeup.job.max.queue.size";

        public static final String IN_CNX_POOL_SIZE = "av-sched.servlet.cnx.pool.size";

    }

}
