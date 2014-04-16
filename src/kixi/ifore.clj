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
            [kixi.ifore-schema :as is]
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

(defn handle-short-row [time-idx measurement-idx row-count row]
  (log/warnf "Expected a row of greater than %s and %s recieved one of %s." time-idx measurement-idx row-count row)
  (log/warnf "Problem Row [%s]" row)
  nil)

(defn handle-broken-row [row validation-errors validator]
  (log/debugf "Row Length: %s Validator Length: %s validator: %s"
              (count row) (count validator) (s/explain validator))
  (log/warnf "Errors: %s Problem Row Below:%n%s" (prn-str validation-errors) row)
  nil)

;; Assumes that there is only one set of readings per device
(defn enrich-measurement [device-definition validator time-idx measurement-idx row]
  (let [validation-errors (s/check validator row)]
    (if validation-errors
      (handle-broken-row row validation-errors validator)
      (let [type      (get-in device-definition [:readings 0 :type])
            timestamp (clean-timestamp (nth row time-idx))]
        (try
          (hash-map :type type :timestamp timestamp :value (nth row measurement-idx))
          (catch Throwable t
            (log/errorf t "Throwing looking for time-idx %s measurement-idx %s on a %s length row [%s]." time-idx measurement-idx (count row) row)
            (throw t)))))))

;; device comes back from the API and assumes only one sensor per device
(defn csv->measurements
  [device time-key header validator rows]
  (let [description       (:description device)
        device-definition ((keyword description) d/device-definition-map)
        time-idx          (.indexOf header time-key)
        measurement-idx   (.indexOf header description)]
    (keep #(enrich-measurement device-definition validator time-idx measurement-idx %) rows)))

(defn file->amon [file-name entity username password]
  (with-open [r (io/reader file-name)]
    (org.slf4j.MDC/put "file.amon" (.getName file-name))
    (let [creds                  {:username username :password password}
          [header & data]        (csv/read-csv r)
          validator              (is/validator header)
          measurement-partitions (for [partition (partition-all 100 data)
                                       device    (client/devices (merge creds {:entity entity}))]
                                   {:device       (:id device)
                                    :measurements {:measurements (csv->measurements device "Date Time" header validator partition)}})]
      (try 
        (doseq [p (pmap
                   #(client/add-measurements
                     (-> (merge creds %)
                         (assoc :entity entity)))
                   measurement-partitions)]
          (log/debugf "Adding %s measurements for %s" (count p) file-name))
        (catch Throwable t
          (log/errorf t "Caught an exception trying to process %s" (.getName file-name))
          (throw t))))))


