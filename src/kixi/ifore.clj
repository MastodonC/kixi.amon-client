(ns kixi.ifore
  "Data munging for IFORE."
  (:require [kixi.amon-client :as client]
            [kixi.ifore-devices :as d]
            [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [chan >! <! go <!!]]
            [schema.core :as s]
            [clj-time.format :as tf]))


(defn add-ifore-devices [entity-id device-definitions username password]
  (map
   #(client/add-device {:username username
                        :password password
                        :body (assoc % :entityId entity-id)})
   device-definitions))

(defn clean-timestamp [dirty-timestamp]
  (tf/unparse (tf/formatters :date-hour-minute-second)
              (tf/parse (tf/formatter "dd/MM/yyyy HH:mm:ss") dirty-timestamp)))

;; Assumes that there is only one set of readings per device
(defn enrich-measurement [device-definition time-idx measurement-idx row]
  (let [type (get-in device-definition [:readings 0 :type])
        timestamp (clean-timestamp (nth row time-idx))
        row-count (count row)]
    (if (< time-idx row-count)
      (if (< measurement-idx row-count)
        (hash-map :type type :timestamp timestamp :value (nth row measurement-idx))
        (hash-map :type type :timestamp timestamp :value ""))
      nil)))

;; device comes back from the API and assumes only one sensor per device
(defn csv->measurements
  [device time-key header rows]
  (let [description (:description device)
        device-definition ((keyword description) d/device-definition-map)
        time-idx (.indexOf header time-key)
        measurement-idx (.indexOf header description)]
    (println "Index of measurement: " measurement-idx)
    (println "Index of time: " time-idx)
    (map #(enrich-measurement device-definition time-idx measurement-idx %) rows)))

(defn file->hecuba [file-name entity username password]
  (println "Processing: " file-name)
  (let [creds                  {:username username :password password}
        rows                   (csv/read-csv (slurp file-name))
        header                 (first rows)
        data                   (drop 1 rows)
        measurement-partitions (for [partition (partition-all 50 data)
                                     device (client/devices (merge creds {:entity entity}))]
                                 {:device (:id device)
                                  :measurements {:measurements (csv->measurements device "Date Time" header partition)}})]
    (map #(client/add-measurements
           (-> (merge creds %)
               (assoc :entity entity)))
         measurement-partitions)))
