(ns jepsen.ravendb.append
  "A list-append workload for Elle"
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [string :as str]]
            [elle.core :as elle]
            [jepsen [checker :as checker]
                    [client :as client]
                    [core :as jepsen]
                    [generator :as gen]
                    [util :as util]]
            [jepsen.tests.cycle.append :as append]
            [jepsen.ravendb [client :as c]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util ArrayList
                      HashMap
                      List
                      Map)))

(defn apply-mop!
  "Applies a single micro-op to a session. Returns completed mop."
  [session [f k v :as mop]]
  ; We're doing this weird dance with Map/List because RavenDB's Java client
  ; uses Jackson serialization, and it understands how to serialize those by
  ; default.
  (case f
    :r (if-let [obj (c/load session Map (str k))]
         [f k (vec (.get obj "v"))]
         [f k nil])

    :append (let [obj (or (c/load session Map (str k))
                          (doto (HashMap.)
                                (.put "v" (ArrayList.))))
                  l   (.get obj "v")]
              ;(info :obj obj :list l)
              ; Terrifyingly, the client will actually get mad at you if
              ; you try to store a different object back, throwing a
              ; `NonUniqueObjectException`. Are you... are you *supposed*
              ; to mutate it in-place? What on earth? Who DOES this?
              (.add l v)
              (c/store! session (str k) obj)
              mop)))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [_ test])

  (invoke! [_ test op]
    (c/with-errors op
      (c/with-session [session (c/session test conn)]
        (let [txn' (mapv (partial apply-mop! session) (:value op))]
          (assoc op :type :ok, :value txn')))))


  (teardown! [_ test])

  (close! [this test]
    (c/close! conn))

  client/Reusable
  (reusable? [this test]
    true))

(defn workload
  "A list-append workload"
  [opts]
  (-> (append/test (assoc (select-keys opts [:key-count
                                             :min-txn-length
                                             :max-txn-length
                                             :max-writes-per-key])
                          :consistency-models [:strict-serializable]))
      (assoc :client (Client. nil))))
