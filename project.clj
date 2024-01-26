(defproject jepsen.ravendb "0.1.0"
  :description "Jepsen tests for the RavenDB distributed database"
  :url "https://github.com/jepsen-io/ravendb"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [cheshire "5.12.0"]
                 [jepsen "0.3.5"]
                 [net.ravendb/ravendb "5.4.0"]]
  :main jepsen.ravendb.cli
  :repl-options {:init-ns jepsen.ravendb.cli}
  :jvm-opts ["-Djava.awt.headless=true"
             "-server"])
