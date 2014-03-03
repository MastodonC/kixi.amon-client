(ns kixi.amon-client
  (:require [clojure.data.json :as json]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clj-http.client :as client]
            [clojure.tools.logging :as log]))

(defn get-data
  ([url username password]
     (json/read-str
      (:body (client/get url {:basic-auth [username password]
                              :accept :json
                              :content-type :json}))
      :key-fn keyword))
  ([url query username password]
     (json/read-str
      (:body (client/get url {:basic-auth [username password]
                              :accept :json
                              :content-type :json
                              :query-params query}))
      :key-fn keyword)))

(defn post-data [url body username password]
  (client/post url {:basic-auth [username password]
                    :accept :json
                    :content-type :json
                    :form-params body
                    :throw-exceptions false
                    :save-request? true
                    :debug-body true}))

(defn devices [{:keys [entity username password] :as request}]
  (let [url (format "https://metering-api-live.amee.com/3/entities/%s" entity)]
    (get-data url username password)))

(defn device [{:keys [entity device username password] :as request}]
  (let [url (format "https://metering-api-live.amee.com/3/entities/%s/devices/%s" entity device)]
    (get-data url username password)))

(defn add-device [{:keys [body username password] :as request}]
  (let [entity (:entityId body)
        url (format "https://metering-api-live.amee.com/3/entities/%s/devices/" entity)]
    (post-data url body username password)))

(defn measurements [{:keys [entity device startDate endDate raw username password] :as request}]
  (let [url (format "https://metering-api-live.amee.com/3/entities/%s/devices/%s/measurements" entity device)
        query (select-keys request [:startDate :endDate :raw])]
    (get-data url query username password)))

(defn add-measurements [{:keys [entity device username password] :as request}]
  (let [url (format "https://metering-api-live.amee.com/3/entities/%s/devices/%s/measurements" entity device)
        body (select-keys request [:measurements])]
    (post-data url body username password)))

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
