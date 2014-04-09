(ns kixi.heatpump
  (:require [kixi.amon-client :as client]
            [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clj-time.format :as tf]
            [org.httpkit.client :as http]
            [clojure.string :as string]
            [clojure.core.async :refer [chan >! <! go <!!]]))

(def column-headings
  [{:description "Time"}
   {:description "Ambient temperature (°C)" :readings [{:type "temperatureAmbient" :unit "C" :period "INSTANT"}]}
   {:description "Ground Ambient Temperature (°C)" :readings [{:type "temperatureAmbient" :unit "C" :period "INSTANT"}]}
   {:description "Lounge temperature (°C)" :readings [{:type "temperatureAir" :unit "C" :period "INSTANT"}]}
   {:description "Upstairs temperature (°C)" :readings [{:type "temperatureAir" :unit "C" :period "INSTANT"}]}
   {:description "Ambient humidity (%)" :readings [{:type "relativeHumidity" :unit "%RH" :period "INSTANT"}]}
   {:description "Heat Pump Electricity Meter (Wh)" :readings [{:type "electricityConsumption" :unit "Wh" :period "CUMULATIVE"}]}
   {:description "Heat Pump Heat Meter 1 (Wh)" :readings [{:type "electricityConsumption" :unit "Wh" :period "CUMULATIVE"}]}
   {:description "Heat Pump Heat Meter 2 (Wh)" :readings [{:type "electricityConsumption" :unit "Wh" :period "CUMULATIVE"}]}
   {:description "Heat Pump Flow Temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Heat Pump Return Temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Heat Pump Evaporator Air On Temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Heat Pump Evaporator Air Off Temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Ground Source Flow temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Ground Source Return Temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Heat Pump Sink Balance (Wh)" :readings [{:type "electricityConsumption" :unit "Wh" :period "CUMULATIVE"}]}
   {:description "Heat Pump Source Balance (Wh)" :readings [{:type "electricityConsumption" :unit "Wh" :period "CUMULATIVE"}]}
   {:description "Cylinder 1 Environmental Temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Circulation Pump Electricity Meter (Wh)" :readings [{:type "electricityConsumption" :unit "Wh" :period "CUMULATIVE"}]}
   {:description "Electricity Meter 2 (Wh)" :readings [{:type "electricityConsumption" :unit "Wh" :period "CUMULATIVE"}]}
   {:description "Electricity Meter 3 (Wh)" :readings [{:type "electricityConsumption" :unit "Wh" :period "CUMULATIVE"}]}
   {:description "Electricity Meter 4 (kWh)" :readings [{:type "electricityConsumption" :unit "Wh" :period "CUMULATIVE"}]}
   {:description "Central Heating Heat Meter (Wh)" :readings [{:type "electricityConsumption" :unit "Wh" :period "CUMULATIVE"}]}
   {:description "Central Heating Flow Temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Central Heating Return Temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Central Heating Buffer Tank temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Immersion Heater Electricity Use (Wh)" :readings [{:type "electricityConsumption" :unit "Wh" :period "CUMULATIVE"}]}
   {:description "DHW temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "DHW Tank Temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Cold Feed DHW Temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "DHW volume (L)" :readings [{:type "volume" :unit "L" :period "INSTANT"}]}
   {:description "DHW heat meter (Wh)" :readings [{:type "electricityConsumption" :unit "Wh" :period "CUMULATIVE"}]}
   {:description "Fourth Heat Meter (Wh)" :readings [{:type "electricityConsumption" :unit "Wh" :period "CUMULATIVE"}]}
   {:description "Second Heat Meter (Wh)" :readings [{:type "electricityConsumption" :unit "Wh" :period "CUMULATIVE"}]}
   {:description "Third Heat Meter (Wh)" :readings [{:type "electricityConsumption" :unit "Wh" :period "CUMULATIVE"}]}
   {:description "Heating Volume (L)" :readings [{:type "volume" :unit "L" :period "INSTANT"}]}
   {:description "Underfloor flow temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Underfloor Return Temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Air On Exhaust Temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Air Off Exhaust Temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "DHW flow temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "DHW return temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "DHW volume heated (L)" :readings [{:type "volume" :unit "L" :period "INSTANT"}]}
   {:description "Engine gas meter (L)" :readings [{:type "volume" :unit "L" :period "INSTANT"}]}
   {:description "Calorific value (MJ/m3)" :readings [{:type "calorificValue" :unit "MJ/m3" :period "INSTANT"}]}
   {:description "Gas Boiler Flue temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Gas Line temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Gas Boiler Flow temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Gas Boiler Return temperature (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Ambient temp in duct (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Humidity in duct (%)" :readings [{:type "relativeHumidity" :unit "%RH" :period "INSTANT"}]}
   {:description "Extra Temperature Sensor 1 (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Extra Temperature Sensor 2 (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Extra Temperature Sensor 3 (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Extra Temperature Sensor 4 (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Extra temperature sensor 5 (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Extra temperature sensor 6 (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Extra temperature sensor 7 (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "Extra temperature sensor 8 (°C)" :readings [{:type "temperatureRadiant" :unit "C" :period "INSTANT"}]}
   {:description "DHW Heat Cross Check (Wh)" :readings [{:type "electricityConsumption" :unit "Wh" :period "CUMULATIVE"}]}
   ])


(defn devices-metadata []
  (drop 1 column-headings))


(defn add-devices [entity username password]
  (map (fn [d]
          (client/add-device
           {:body (merge {:privacy "private" :entityId entity} d)
            :username username :password password}))
        (devices-metadata)))

;; FIXME this should return the devices metadata in the same fashion as entity-devices-metadata
(defn add-entity-and-devices [property-code project-id username password]
  (let [entity (re-find #"ies/(.+)$" (->
                                     (client/add-entity {:body {:propertyCode property-code :projectId project-id} :username username :password password })
                                     :body
                                     (json/read-str :key-fn keyword)
                                     :location))]
    (add-devices entity username password)))

;; (defn device-id
;;   "Extracts device identfier maps from the URL of the responses"
;;   [resp]
;;   (let [[_ entity device] (re-find #"ies/(.+)/devices/(.+)$"
;;                                    (-> resp :body (json/read-str :key-fn keyword) :location))]
;;     {:entity entity :device device}))
(defn device-id
  "Extracts device identifier maps from the devices with metadata aggregate"
  [device-with-metadata]
  (:deviceId device-with-metadata))

(defn entity-devices
  "Retrieve the devices for a given entity"
  [entity username password]
  (let [device-ids (:deviceIds (client/entity {:entity entity
                                                :username username
                                                :password password }))]
    (map (fn [id] (client/device {:entity entity
                                  :device id
                                  :username username
                                  :password password }))
         device-ids)))

(defn entity-device-ids
  "Extracts only Device IDs and Description for a given entity"
  [entity username password]
  (map (fn [ed] (select-keys ed [:deviceId :description]))
       (entity-devices entity username password)))

(defn entity-devices-metadata
  "Merges the devices metadata with the proper device IDs for the given entity"
  [entity username password]
  (let [devices (entity-device-ids entity username password)]
  (map
   (fn [m]
     (assoc m
       :deviceId (:deviceId
                  (first
                   (filter
                    (fn [d] (= (:description m) (:description d)))
                    devices)))
       :entity entity))
   (devices-metadata))))
(def entity-devices-memoized (memoize entity-devices-metadata))

(defn device-with-measurement [timestamp data {:keys [deviceId entity readings] :as metadata}]
  (hash-map :device deviceId
            :entity entity
            :measurements [{:timestamp timestamp
                            :value data
                            :type (-> readings first :type)}]))

(defn device-with-measurements
  [timestamps data {:keys [deviceId entity readings] :as metadata}]
  (hash-map :device deviceId
            :entity entity
            :measurements (map
                           (fn [timestamp value]
                             {:timestamp timestamp
                              :value value
                              :type (-> readings first :type)})
                             timestamps
                             data)))

(def page-size 50)

(defn lazy-paginate-csv
  [csv-file]
  (let [in-file (io/reader csv-file)
        csv-seq (csv/read-csv in-file)
        lazy-page (fn lazy-page [wrapped]
               (lazy-seq
                 (if-let [s (seq wrapped)]
                   (cons (take page-size s) (lazy-page (drop page-size s)))
                   (.close in-file))))]
    (lazy-page csv-seq)))

(defn process-measurements-page [page devices]
  (let [[raw-timestamps & data] (apply map vector page)
        timestamps (map (fn [raw-timestamp]
                          (tf/unparse (tf/formatters :date-time)
                                      (tf/parse (tf/formatter "dd/MM/yyyy HH:mm") raw-timestamp)))
                        raw-timestamps)]
  (doall (map
          (partial device-with-measurements timestamps)
            ;; (println "device: " (:deviceId device)
            ;;          " data ->" (count data)
            ;;          " tstamps -> " (count timestamps) "\n\n" )
          data
          devices))
))


(defn add-measurements-page [measurements username password]
  (let [c (chan)
        res (atom [])
        _ (println "Adding page...")
        ]
    (doseq [m measurements]
      (-> (merge m {:username username :password password })
          (client/add-measurements c)))

    (doseq [_ (<!! c)]
        (println "Request completed"))))


(defn read-file-in-pages [filename devices username password]
  (doseq [page (lazy-paginate-csv filename)]
    (add-measurements-page (process-measurements-page page devices) username password)
    (println "\t... page added")
))

;; (time (read-file-in-pages  "/Users/bru/Code/mastodonC/kixi.amon-client/data/embed_csv/heat_pump_data/D407T_head.csv" (entity-devices-memoized "5086aff9126038d35fc6f9887e1f0479c7b63ed9" "alice" "password") "alice" "password")) 
;; async posts stats:
;; - 3200 writes/seq in cass
;; - 10000 rows (* 58 devices) in 9'20"

;;;;

(defn timestamped-readings
  "Takes a raw row from the csv and turns it into a measurements map."
  [row devices-with-metadata]
  (let [[raw-timestamp & data] row
        timestamp (tf/unparse (tf/formatters :date-time)
                              (tf/parse (tf/formatter "dd/MM/yyyy HH:mm") raw-timestamp))]
    (map (partial device-with-measurement timestamp)
         data
         devices-with-metadata)))

(defn has-value? [m]
  (when (not-empty (get-in m [:measurements 0 :value]))
    m))

(defn measurement-row
  "Creates a vector measurment maps ready to be posted to embed per csv row w/o blanks."
  [readings-row devices-with-metadata]
  (->>  (timestamped-readings readings-row devices-with-metadata)
        (keep has-value?)))


;; "/Users/bld/Dropbox/heat pump data/D407T.csv"
;; FIXME put in drop/take paging
;; TODO wrap map in a reduce that captures a seq of the errors,
;;   the start and end time of the run, the number of each type of measurement uploaded,
;;   the total number uploaded and the earliest and latest record timestamps
(defn add-measurements-from-file [measurement-file-name project-id username password]
  (with-open [r (io/reader measurement-file-name)]
    (let [property-code (string/replace measurement-file-name #".*/(.*)$" "$1")
          devices (entity-devices-memoized "65c8940dbfcfd94ffe5ddfa0da4c040d51c065cf" username password) ; (add-entity-and-devices property-code project-id username password)
          msgs (mapcat
                (fn [readings-row]
                  (measurement-row readings-row devices))
                (take 10 (drop 0 (csv/read-csv r))))]
      (add-measurements-page msgs username password)
    )))

;; (def batch-results (add-all-measurements "/Users/bld/Dropbox/heat pump data/D407T.csv" hp407 "bd700d16-5d74-4569-8f4c-0262cb02f0c5" "42kjOHljkb"))
;; Edwins credentials: "edwin.carter@passivsystems.com" "yVRyh2L4yuMD"
(comment
(def batch-results (add-all-measurements
                      "/Users/bru/Code/mastodonC/kixi.amon-client/data/embed_csv/heat_pump_data/D407T_head.csv"
                      (entity-devices-memoized "c2290395dbf9da2523e1805d45f1ddb69960d936" "edwin.carter@passivsystems.com" "yVRyh2L4yuMD")
                      "bd700d16-5d74-4569-8f4c-0262cb02f0c5"
                      "42kjOHljkb"))
)
