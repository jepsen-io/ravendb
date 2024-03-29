(ns jepsen.ravendb.cli
  "Command-line entry point for RavenDB tests."
  (:require [clojure [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
             [cli :as cli]
             [control :as c]
             [db :as jepsen.db]
             [generator :as gen]
             [nemesis :as nemesis]
             [os :as os]
             [tests :as tests]
             [util :as util]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.nemesis.combined :as nc]
            [jepsen.os.debian :as debian]
            [jepsen.ravendb [append :as append]
                            [db :as db]]))

(def workloads
  "A map of workload names to functions that take CLI options and return
  workload maps."
  {:append append/workload
   :none   (fn [_] tests/noop-test)})

(def all-workloads
  "A collection of workloads we run by default."
  [])

(def all-nemeses
  "Combinations of nemeses for tests"
  [[]])

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none []
   :all  [:pause :kill :partition :clock]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(defn ravendb-test
  "Given options from the CLI, constructs a test map."
  [opts]
  (let [workload-name (:workload opts)
        workload ((workloads workload-name) opts)
        db       (db/db opts)
        os       debian/os
        nemesis  (case (:db opts)
                   :none nil
                   (nc/nemesis-package
                     {:db db
                      :nodes (:nodes opts)
                      :faults (:nemesis opts)
                      :partition {:targets [:one :majority]}
                      :pause {:targets [:one]}
                      :kill  {:targets [:one :all]}
                      :interval (:nemesis-interval opts)}))]
    (merge tests/noop-test
           opts
           {:name (str (name workload-name)
                       (when (:lazyfs opts) " lazyfs")
                       " " (name (:txn-mode opts))
                       " " (when (:optimistic-concurrency opts) " optimistic")
                       " " (str/join "," (map name (:nemesis opts))))
            :os os
            :db db
            :checker (checker/compose
                       {:perf (checker/perf
                                {:nemeses (:perf nemesis)})
                        :clock (checker/clock-plot)
                        :stats (checker/stats)
                        :exceptions (checker/unhandled-exceptions)
                        ;:timeline (timeline/html)
                        :workload (:checker workload)})
            :client    (:client workload)
            :nemesis   (:nemesis nemesis nemesis/noop)
            :generator (->> (:generator workload)
                            (gen/stagger (/ (:rate opts)))
                            (gen/nemesis (:generator nemesis))
                            (gen/time-limit (:time-limit opts)))
            :logging {:overrides {"net.ravendb.client.documents.session.operations.LoadOperation" :warn}}})))

(def cli-opts
  "Command line options"
  [
   [nil "--key-count NUM" "Number of keys in active rotation."
    :default  10
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--lazyfs" "If set, mounts RavenDB in a lazy filesystem that loses un-fsycned writes on nemesis kills."]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? #{:pause :kill :partition :clock})
               "Faults must be pause, kill, partition, clock, or member, or the special faults all or none."]]

   [nil "--max-txn-length NUM" "Maximum number of operations in a transaction."
    :default  4
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-writes-per-key NUM" "Maximum number of writes to any given key."
    :default  16
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default 5
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   ["-o" "--optimistic-concurrency" "If set, enables optimistic concurrency on sessions."
    :default false]

   ["-r" "--rate HZ" "Approximate request rate, in hz"
    :default 100
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   ["-t" "--txn-mode MODE" "Whether to use cluster-wide transactions or regular ones."
    :default :single-node
    :parse-fn keyword
    :validate [#{:single-node :cluster-wide}
               "must be one of single-node or cluster-wide"]]

   ["-v" "--version STRING" "What version of RavenDB should we test?"
    :default "60005"] ; 60005 is version 6.0.2? I don't get this URL scheme.

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :default  :append
    :missing  (str "Must specify a workload: " (cli/one-of workloads))
    :validate [workloads (cli/one-of workloads)]]
   ])

(defn all-tests
  "Turns CLI options into a sequence of tests."
  [opts]
  (let [nemeses   (if-let [n (:nemesis opts)] [n] all-nemeses)
        workloads (if-let [w (:workload opts)] [w] all-workloads)]
    (for [n nemeses, w workloads, i (range (:test-count opts))]
      (ravendb-test (assoc opts :nemesis n :workload w)))))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  ravendb-test
                                         :opt-spec cli-opts})
                   (cli/test-all-cmd {:tests-fn all-tests
                                      :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
