(ns io.pithos.cassandra-keystore
  "A keystore backed by a cassandra table."
  (require [io.pithos.store :as store]
           [qbits.alia :refer [execute]]
           [qbits.hayt :refer [column-definitions create-table select where]]))

(def users-table
  (create-table
   :users
   (column-definitions {:key         :text
                        :master      :boolean
                        :tenant      :text
                        :secret      :text
                        :primary-key [:key]})))

(defn get-user-q
  [key]
  (select :users
          (where [[= :key key]])))

(defn cassandra-keystore
  [config]
  (let [session (store/cassandra-store config)]
    (reify
      store/Convergeable
      (converge! [this]
        (execute session users-table))
      clojure.lang.ILookup
      (valAt [this id]
        (first (execute session (get-user-q id)))))))
