(ns kixi.amon-client
  (:require [clojure.data.json :as json]
            [org.httpkit.client :as http]))

;;
;; low level GET & POST
;;

(defn get-data
  "GET a JSON string from the given URL protected by basic HTTP Auth"
  ([url username password]
     ;(println "GET " url)
     (json/read-str
      (:body @(http/get url {:basic-auth [username password]
                    :accept :json
                    :content-type :json
                    :keepalive 60000}
                   ))))
  ;; ([url query username password]
  ;;    (json/read-str
  ;;     (:body @(http/get url {:basic-auth [username password]
  ;;                            :accept :json
  ;;                            :content-type :json
  ;;                            :query-params query
  ;;                            :keepalive 60000}))
  ;;     :key-fn keyword)))
)

(defn post-data
  "POST a JSON string to a given URL protected by basic HTTP Auth"
  ([url body username password]
     (println "POST " url)
     (http/post url {:basic-auth [username password]
                     :accept :json
                     :content-type :json
                     :form-params body
                     :throw-exceptions false
                     :save-request? true
                     :debug-body true
                     :keepalive 60000}))
  ([url body username password callback]
     (println "POST " url)
     (http/post url {:basic-auth [username password]
                     :accept :json
                     :content-type :json
                     :form-params body
                     :throw-exceptions false
                     :save-request? true
                     :debug-body true
                     :keepalive 60000}
                callback)))

;;
;; Endpoint URL builders
;;

;; (def api-url "https://metering-api-live.amee.com/3") ; old url
(def api-url "http://kixi-production-1162624566.us-west-2.elb.amazonaws.com/4") ; kixi url
(def entities-url
  (format "%s/entities" api-url))
(defn entity-url [entity]
  (format "%s/%s" entities-url entity))
(defn entity-devices-url [entity]
  (format "%s/devices" (entity-url entity)))
(defn entity-device-url [entity device]
  (format "%s/%s" (entity-devices-url entity) device))
(defn entity-device-measurements-url [entity device]
  (format "%s/measurements" (entity-device-url entity device)))


;;
;; high level functions
;;

;; ENTITIES

(defn entity [{:keys [entity username password] :as request}]
  (let [url (entity-url entity)]
    (get-data url username password)))

;; FIXME this doesn't work the way the API doc describe it!
(defn add-entity [{ :keys [body username password] :as request}]
  (let [url entities-url]
    (post-data url body username password)))

;; DEVICES

(defn device [{:keys [entity device username password] :as request}]
  (let [url (entity-device-url entity device)]
    (get-data url username password)))

(defn add-device [{:keys [body username password] :as request}]
  (let [entity (:entityId body)
        url (entity-devices-url entity)]
    (post-data url body username password)))

;; (defn add-devices [{:keys [entity metadata username password] :as request}]
;;   (map (fn [d]
;;          (add-device { :body (merge {:privacy "private" :entityId entity } d)
;;                        :username username
;;                        :password password}))
;;        (metadata)))

;; MEASUREMENTS
(defn measurements [{:keys [entity device startDate endDate raw username password] :as request}]
  (let [url (entity-device-measurements-url entity device)
        query (select-keys request [:startDate :endDate :raw])]
    (get-data url query username password)))

(defn add-measurements [{:keys [entity device username password] :as request} callback]
  (let [url (entity-device-measurements-url entity device)
        body (select-keys request [:measurements])]
    (post-data url body username password callback)))

(def amon-units
  [{:description "absoluteHumidity" :unit "g/Kg" :type "Number"}
   {:description "barometricPressure" :unit "mbar" :type "Number"}
   {:description "co2" :unit "ppm" :type "Number"}
   {:description "currentSignal" :unit "mA" :type "Number"}
   {:description "electricityAmps" :unit "Amps" :type "Number"}
   {:description "electricityConsumption" :unit "kWh" :type "Number"}
   {:description "electricityExport" :unit "kWh" :type "Number"}
   {:description "electricityFrequency" :unit "Hz" :type "Number"}
   {:description "electricityGeneration" :unit "kWh" :type "Number"}
   {:description "electricityImport" :unit "kWh" :type "Number"}
   {:description "electricityKiloVoltAmpHours" :unit "kVArh" :type "Number"}
   {:description "electricityKiloWatts" :unit "kW" :type "Number"}
   {:description "electricityVolts" :unit "V" :type "Number"}
   {:description "electricityVoltAmps" :unit "VA" :type "Number"}
   {:description "electricityVoltAmpsReactive" :unit "VAr" :type "Number"}
   {:description "flowRateAir" :unit "m^3/h" :type "Number"}
   {:description "flowRateLiquid" :unit "Ls^-1" :type "Number"}
   {:description "gasConsumption" :unit "m^3, ft^3,kWh" :type "Number"}
   {:description "heatConsumption" :unit "kWh" :type "Number"}
   {:description "heatExport" :unit "kWh" :type "Number"}
   {:description "heatGeneration" :unit "kWh" :type "Number"}
   {:description "heatImport" :unit "kWh" :type "Number"}
   {:description "heatTransferCoefficient" :unit "W/m^2.K" :type "Number"}
   {:description "liquidFlowRate" :unit "Litres/5min" :type "Number"}
   {:description "oilConsumption" :unit "m^3, ft^3,kWh" :type "Number"}
   {:description "powerFactor" :unit "" :type "Number (0-1)"}
   {:description "pulseCount" :unit "" :type "Number"}
   {:description "relativeHumidity" :unit "%RH" :type "Number"}
   {:description "relativeHumidity" :unit "wm-2" :type "Number"}
   {:description "solarRadiation" :unit "W/m^2" :type "Number"}
   {:description "status" :unit "" :type "Number (0/1)"}
   {:description "temperatureAir" :unit "C" :type "Number"}
   {:description "temperatureAmbient" :unit "C" :type "Number"}
   {:description "temperatureFluid" :unit "C" :type "Number"}
   {:description "temperatureGround" :unit "C" :type "Number"}
   {:description "temperatureRadiant" :unit "C" :type "Number"}
   {:description "temperatureSurface" :unit "C" :type "Number"}
   {:description "thermalEnergy" :unit "kWhth" :type "Number"}
   {:description "time" :unit "millisecs" :type "Number"}
   {:description "voltageSignal" :unit "mV" :type "Number"}
   {:description "waterConsumption" :unit "L" :type "Number"}
   {:description "windDirection" :unit "degrees" :type "Number"}
   {:description "windSpeed" :unit "ms^-1" :type "Number"}
   ])
