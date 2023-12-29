(ns jepsen.ravendb.client
  "Wraps the RavenDB JVM client."
  (:refer-clojure :exclude [load])
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [util :as util]])
  (:import (java.io Closeable)
           (net.ravendb.client.documents DocumentStore)
           (net.ravendb.client.documents.session IDocumentSession
                                                 SessionOptions
                                                 TransactionMode)
           (net.ravendb.client.exceptions ClusterTransactionConcurrencyException
                                          ConcurrencyException
                                          RavenException)
           (net.ravendb.client.serverwide DatabaseRecord)
           (net.ravendb.client.serverwide.operations CreateDatabaseOperation
                                                     GetDatabaseNamesOperation)))

(def db
  "The database we work with."
  "jepsendb")

(defn close!
  [^Closeable c]
  (.close c)
  c)

(defn ^DocumentStore open
  "Opens a connection to the given node."
  [node]
  (let [ds (DocumentStore. (into-array [(str "http://" node ":8080")]) db)]
    (try (.initialize ds)
         ds)))

(defn db-names
  "Lists database names on a DocumentStore."
  [^DocumentStore ds]
  (.. ds
      maintenance
      server
      (send (GetDatabaseNamesOperation. 0 128))))

(defn await-open
  "Blocks until open succeeds."
  [node]
  (util/await-fn (fn attempt []
                   (let [conn (open node)]
                     (db-names conn) ; Will explode if conn not ready
                     conn))
                 {:timeout        60000
                  :retry-interval 1000
                  :log-interval   5000
                  :log-message    "Waiting for RavenDB to accept connections"}))


(defn ^IDocumentSession session
  "Opens a session on the document store. Applies session options based on the
  test."
  [test ^DocumentStore ds]
  (let [opts (doto (SessionOptions.)
               (.setTransactionMode (case (:txn-mode test)
                                      :cluster-wide TransactionMode/CLUSTER_WIDE
                                      :single-node TransactionMode/SINGLE_NODE)))
        session (.openSession ds opts)]
    ; Not exactly sure what this does! The comments
    ; (https://github.com/ravendb/ravendb-jvm-client/blob/3594b8c813e3403c496805f7ec082d022d5e3cd1/src/main/java/net/ravendb/client/documents/session/IAdvancedDocumentSessionOperations.java#L101-L106)
    ; say "When set to true, a check is made so that a change made behind
    ; the session back would fail and raise ConcurrencyException", which...
    ; sounds like it's actually critical for obtaining ACID? Why is this
    ; buried in the advanced settings then?
    (when (:optimistic-concurrency test)
      (.. session advanced (setUseOptimisticConcurrency true)))
    session))

(defn store!
  "Stores an entity in a session."
  ([^IDocumentSession session, entity]
   (.store session entity))
  ([^IDocumentSession session, id, entity]
   (.store session entity id))
  ([^IDocumentSession session, id, change-vector, entity]
   (.store session entity change-vector id)))

(defn load
  "Loads an entity of the given class from a session."
  [^IDocumentSession session, class, id]
  (.load session class id))

(defn save-changes!
  "Saves changes to a session."
  [^IDocumentSession session]
  (.saveChanges session))

(defmacro with-session
  "Takes a binding vector of a session name and a session expression. Evaluates
  session-expr, binds it to `session-name`, executes body, saves changes and
  closes session. Returns result of body."
  [[session-name session-expr] & body]
  `(with-open [~session-name ~session-expr]
     (let [r# (do ~@body)]
       (save-changes! ~session-name)
       r#)))

(defmacro with-errors
  "Takes an op. Evaluates body, converting known errors to info/fail ops."
  [op & body]
  `(try
    ~@body
    (catch ConcurrencyException e#
      (assoc ~op :type :fail, :error [:concurrency (.getMessage e#)]))
    (catch ClusterTransactionConcurrencyException e#
      (assoc ~op :type :fail, :error [:cluster-txn-concurrency (.getMessage e#)]))
    (catch RavenException e#
      (condp re-find (.getMessage e#)
        #"Cannot assign requested address" (assoc ~op :type :fail, :error [:cannot-assign-requested-address])
        (throw e#)))))

(defn create-db!
  "Creates the default DB we use"
  [test ^DocumentStore ds]
  (let [dbr (DatabaseRecord. db)
        op (CreateDatabaseOperation. dbr (:replication-factor test 1))
        res (-> ds
                .maintenance
                .server
                (.send op))]
    res))
