(ns jepsen.ravendb.db
  "Installs and runs RavenDB"
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [cheshire.core :as json]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [lazyfs :as lazyfs]
                    [util :as util :refer [meh]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.os.debian :as debian]
            [jepsen.ravendb [client :as rc]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def dir
  "Where do we install RavenDB?"
  "/opt/ravendb")

(def port
  "What port do we bind?"
  8080)

(def data-dir
  "Where does RavenDB store data files?"
  (str dir "/data"))

(def config-file
  "Path to the server config file."
  (str dir "/Server/settings.json"))

(def daemon-log-file
  "Path to the stdout/stderr log file"
  (str dir "/daemon.log"))

(def pid-file
  "Path to the pidfile"
  (str dir "/ravendb.pid"))

(defn install!
  "Installs RavenDB"
  [test node]
  ; See https://ravendb.net/docs/article-page/6.0/csharp/start/installation/manual
  (c/su
    (let [url (str "https://hibernatingrhinos.com/downloads/RavenDB%20for%20Linux%20x64/" (:version test))]
      (cu/install-archive! url dir)
      (c/exec :chmod "+x" (str dir "/run.sh")))))

(defn configure!
  "Writes config files."
  [test node]
  (-> {:ServerUrl (str "http://" node ":" port)
       :DataDir "data"
       :Security.UnsecuredAccessAllowed "PublicNetwork"
       :License.Eula.Accepted true}
      json/generate-string
      (cu/write-file! config-file)))

(defn kill!
  "Kills RavenDB"
  []
  (c/su (cu/stop-daemon! pid-file)))

(defrecord DB [lazyfs] ; A LazyFS object
  db/DB
  (setup! [this test node]
    (install! test node)
    (when (:lazyfs test)
      (db/setup! lazyfs test node))
    (configure! test node)
    (db/start! this test node)
    (with-open [c (rc/await-open node)]
      (info :conn c)
      (info (rc/create-db! test c)))

    (when (:lazyfs test)
      (lazyfs/checkpoint! lazyfs)))

  (teardown! [this test node]
    (kill!)
    (when (:lazyfs test)
      (db/teardown! lazyfs test node))
    (c/su
      (c/exec :rm :-rf dir)))

  db/LogFiles
  (log-files [this test node]
    (merge {daemon-log-file "daemon.log"}
           (try+ (->> (cu/ls (str dir "/Server/Logs"))
                     (map (juxt (partial str data-dir "/Server/Logs/") identity))
                     (into {}))
                 (catch [:type :jepsen.control/nonzero-exit] e nil))
           (when (:lazyfs test) (db/log-files lazyfs test node))))

  db/Kill
  (start! [this test node]
    (c/su
      (cu/start-daemon! {:chdir dir
                         :logfile daemon-log-file
                         :pidfile pid-file}
                        (str dir "/run.sh"))))

  (kill! [this test node]
    (kill!)
    (when (:lazyfs test)
      (lazyfs/lose-unfsynced-writes! lazyfs))))

(defn db
  "A RavenDB database. Takes CLI options."
  [opts]
  (map->DB {:lazyfs (lazyfs/db {:dir        data-dir
                                :cache-size "1GB"})}))
