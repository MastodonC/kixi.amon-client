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


(def devices-metadata
  (drop 1 column-headings))

(defn extract-id-part [url section]
  (nth (re-find (re-pattern (str section "/(.+)$"))
                url)
       1))
;; FIXME this should return the devices metadata in the same fashion as entity-devices-metadata
(defn add-devices [entity username password]
  "Adds all devices specified in devices-metadata for a given entity"
  (doseq [device-metadata devices-metadata]
    (->
     (client/add-device
      {:body (merge {:privacy "private" :entityId entity} device-metadata)
       :username username :password password})
     :body
     (json/read-str :key-fn keyword)
     :location
     (extract-id-part "devices")
     (->> (println "\tdevice: ")))))

(defn add-entity [property-code project-id username password]
  (->
   (client/add-entity {:body {:propertyCode property-code :projectId project-id} :username username :password password })
   :body
   (json/read-str :key-fn keyword)
   :location
   (extract-id-part "entities")))

(defn device-id
  "Extracts device identifier maps from the devices with metadata aggregate"
  [device-with-metadata]
  (:deviceId device-with-metadata))

(defn entity-devices
  "Retrieve the devices for a given entity"
  [entity username password]
  (let [device-ids (:device-ids (client/entity {:entity entity
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
   devices-metadata)))
(def entity-devices-memoized (memoize entity-devices-metadata))

(defn device-with-measurements
  "Builds a device with measurements request"
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
  "Lazily paginate through a CSV file"
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
  "Builds a vector of devices with measurements form the given page"
  (let [[raw-timestamps & data] (apply map vector page)
        timestamps (map (fn [raw-timestamp]
                          (tf/unparse (tf/formatters :date-time)
                                      (tf/parse (tf/formatter "dd/MM/yyyy HH:mm") raw-timestamp)))
                        raw-timestamps)]
  (doall (map
          (partial device-with-measurements timestamps)
          data
          devices))
))


(defn add-measurements-page [measurements username password]
  "asynchronously POST the measurements for one page (one request for each device), then wait for the threads to join again"
  (let [c (chan)
        res (atom [])
        pagesize (count measurements)]
    (doseq [m measurements]
      (-> (merge m {:username username :password password })
          (client/add-measurements c)))

    (doseq [_ (range 1 pagesize)]
      (swap! res conj (<!! c))
      @res)))

(defmacro doseq-indexed [index-sym [item-sym coll] & body]
  `(let [idx-atom# (atom 0)]
     (doseq [~item-sym ~coll]
       (let [~index-sym (deref idx-atom#)]
         ~@body
         (swap! idx-atom# inc)))))

(defn upload-measurement-file-in-pages [filename devices username password]
  "Paginates through the file and uploads the processed pages"
  (doseq-indexed idx [page (lazy-paginate-csv filename)]
    (print ">>>>> adding page: " idx " ... ")(flush)
    (let [doctored-page (process-measurements-page page devices)]
      (time (add-measurements-page doctored-page username password)))))

(defn add-entity-and-upload-measurements [filename project-id username password]
  "Create the entity and its devices, and uploads the measurements from the file"
  (let [property-code (nth (re-find #"/([^\/]+)\.csv$" filename) 1)
        _ (println "Creating the entity for " property-code "...")
        entity-id (add-entity property-code project-id username password)
        _ (println "Created entity with id " entity-id)
        ]
    (add-devices entity-id username password)
    (println "Created devices for entity")
    (println "Adding measurements for " property-code " devices from file")
    (upload-measurement-file-in-pages filename
                                      (entity-devices-metadata entity-id username password)
                                      username
                                      password)))

;; async posts stats:
;; - 3200 writes/seq in cass
;; - 10000 rows (* 58 devices) in 9'20"
;; - 70000 rows (* 58 devices) in 54'
;;
;; project_id: cb3062b688dfb22ff679ef1e5a5daedcb8906a8a
;; entities:
;; | D407T | ce8bccdfc14a678783c52993d2effde4c269a14d |
;; | D408T | 319e295e57d3aca5ac470ae9849fc7f1b032d2a3 |
;; | D409T | bcbcb08149a7e3997e313c4fcbeccf11eefc0fb8 |
;; | D410T | 4dfcf016f272dadc9dd0e341e70dddfa3a06fbb3 |
;; | D411T | c6d88517583a78eda47dd95b122b273da954c06a |
;; | D412T | c47eaacade11a52f3efb8885b82643702da06827 |
;; | D413T | bed0558be9dfcc8b58b9d60487ea87d3130960c6 | missing file - ignore me
;; | D414T | f2c6e28b7921d28ab899606eb152af94878ab045 |
;; | D416T | 37ec68ec6a56076f6172204adb09cf912f9be665 |
;; | D417T | 40482aa9e73345ca68d2d2832995d46643c4457d |
;; | D418ALLPHASE2 | 7ea2ed2bd700f90b27388ff6c39480d4f37178f6 |
;; | D418T | 4caec469c74738446d42a485c470ec86662b6835 |
;; | D419TwithT | 87b7223eec106d4498932bc0e95b4ba6b1c35357 |
;; | D421TwithT | a44c1282f953ee160c09d780c24832ecf42a398f |
;; | D422ALLPHASE2 | 0c637447578d466f02a09089f3ca347e77d9becc | incomplete
;; | D422T | 0fe73af0ff7d83b6fc5c32d9b9c12e5eaad51308 |
;; | D423T | aa758323268cffeec843f3feb5ce6dd2b09cbad7 |
;; | D424T | dacdff34b73d0bc2aaf07cfb64b0ee8dc7ee7722 |
;; | D426T | bc9f370d63f7980939ca3c997b4fb89b5c5b00d8 | incomplete
;; | D427T | 7835027a100d8c8fd32ab37235a60c4153c0e31a |
;; | D428T | 28b25267f3c4b537ea019400628cfffe8d4c7d8f |
;; | D430T | 5a181bca53fcf6aefa2a7c789615c57b331cc320 |
;; | D431T | 123dbc6ff9717d2e293e873fa4c9dec2b9687be8 |
;; | D432T | 5d12bf8e3022cf3e502cc7ec6014e19640e81e5e |
;; | D433T | 32dfb8893ef476a15d4cf1bdf6c75cfe0dd6e4a5 |
;; | D434T | ok
;; | D435T | ok
;; | D437T | ok
;; | D438T | ok
;; | D439T | ok
;; | D440T | ok
;; | D441T | ok
;; | D442T | ok
;; | D443T | ok
;; | D444T | ok
;; | D448H | ok
;; | D449T | ok?
;; | D460T | fbc519873a548926f34a6b5781a7a188f1c5701d | check last page
;; | D461T | 95713b93314ef8d2a96c7ef0eabb0a83c8c15f94 |
;; | D462T | 4778c4ceed86efc9d26e73a4d6ef230a9c0dd09f |
;; | D463T | 62e46abd1f346f92e87b88233d9ae76b5037ac73 |
;; | D465T | 7c7c816cbd20992ee68eb18b219d87acb397e493 |
;;
;; Process:
;; (add-entity-and-upload-measurements  "/Users/bru/Code/mastodonC/kixi.amon-client/data/embed_csv/heat_pump_data/D465T.csv" "cb3062b688dfb22ff679ef1e5a5daedcb8906a8a" "alice" "password")
;;
;; TODO: cleanup D410 (it received data for D409)
