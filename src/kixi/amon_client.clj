(ns kixi.amon-client
  (:require [clojure.data.json :as json]
            [org.httpkit.client :as http]
            [clojure.core.async :refer [chan >! <! go <!!]]
            [schema.core :as s]
            [clojure.tools.logging :as log]
            [clj-time.format :as tf]))

;;
;; low level GET & POST
;;

(defn get-body [{:keys [body] :as resp}]
  (try 
    (json/read-str body
                   :key-fn keyword)
    (catch Exception e
      (log/errorf e "Caught an exception parsing the body: %s" body))))

(defn print-body [resp]
  (println (get-body resp)))

(defn http-options [username password]
  {:basic-auth [username password]
   :headers {"Accept" "application/json"}
   :accept :json
   :content-type :json
   :keepalive 60000})

(defn async-get [url options]
  (http/get url options))
(defn async-post
  ([url options]
     (http/post url options))
  ([url options result]
     (http/post url options #(go (>! result %)))))

(defn get-data
  "GET a JSON string from the given URL protected by basic HTTP Auth"
  ([url username password]
     (get-body @(async-get url (http-options username password))))
  ([url query username password]
     (get-body @(async-get url (assoc (http-options username password) :query-params query)))))

(defn post-data
  "POST a JSON string to a given URL protected by basic HTTP Auth"
  ([url body username password]
     @(async-post url (assoc (http-options username password)
                     :body (json/write-str body)
                     :throw-exceptions false
                     :save-request? true
                     :debug-body true)))

  ([url body username password result]
     (async-post url (assoc (http-options username password)
                     :body (json/write-str body)
                     :throw-exceptions false
                     :save-request? true
                     :debug-body true)
                result)))

;;
;; Endpoint URL builders
;;

;; (def api-url "https://metering-api-live.amee.com/3") ; old url
(def api-url "http://kixi-production-1162624566.us-west-2.elb.amazonaws.com/4") ; kixi url
(defn project-url [projectId]
  (format "%s/projects/%s" api-url projectId))
(defn project-entities-url [projectId]
  (format "%s/projects/%s/properties/" api-url projectId))
(def entities-url
  (format "%s/entities/" api-url))
(defn entity-url [entity]
  (format "%s%s" entities-url entity))
(defn entity-devices-url [entity]
  (format "%s/devices/" (entity-url entity)))
(defn entity-device-url [entity device]
  (format "%s%s" (entity-devices-url entity) device))
(defn entity-device-measurements-url [entity device]
  (format "%s/measurements/" (entity-device-url entity device)))


;;
;; Types
;;

(def Programme
  {:name s/Str})

(def Project
  {:programmeId s/Str
   :name s/Str})

(def Measurement
  {:type s/Str
   :timestamp s/Str
   (s/optional-key :value) (s/either s/Str s/Num)
   (s/optional-key :error) s/Str})

(def Measurements
  {:measurements [Measurement]})

(def Device
  {(s/optional-key :deviceId) s/Str
   :entityId s/Str
   (s/optional-key :parentId) s/Str
   (s/optional-key :description) s/Str
   (s/optional-key :meteringPointId) s/Str
   :privacy (s/enum "private" "public")
   (s/optional-key :location) {(s/optional-key :name) s/Str
                               (s/optional-key :latitude) (s/either s/Str s/Num)
                               (s/optional-key :longitude) (s/either s/Str s/Num)}
   (s/optional-key :metadata) {s/Keyword s/Any}
   (s/optional-key :readings) [{:type s/Str
                                (s/optional-key :unit) s/Str
                                (s/optional-key :resolution) (s/either s/Str s/Num)
                                (s/optional-key :accuracy) (s/either s/Str s/Num)
                                :period (s/enum "INSTANT" "CUMULATIVE" "PULSE")
                                (s/optional-key :min) (s/either s/Str s/Num)
                                (s/optional-key :max) (s/either s/Str s/Num)
                                (s/optional-key :correction) s/Bool
                                (s/optional-key :correctedUnit) s/Str
                                (s/optional-key :correctionFactor) (s/either s/Str s/Num)
                                (s/optional-key :CorrectionFactorBreakdown) s/Str}]
   (s/optional-key :measurements) Measurements})

(def Entity
  {:projectId s/Str
   :propertyCode s/Str
   (s/optional-key :entityId) s/Str
   (s/optional-key :deviceIds) [s/Str]
   (s/optional-key :meteringPointIds) [s/Str]})

;;
;; high level functions
;;

;; PROJECTS
(defn project [{:keys [projectId username password] :as request}]
  (let [url (project-url projectId)]
    (get-data url username password)))

;; ENTITIES
(defn entities [{:keys [projectId username password] :as request}]
  (let [url (project-entities-url projectId)]
    (get-data url username password)))

(defn entity [{:keys [entity username password] :as request}]
  (let [url (entity-url entity)]
    (get-data url username password)))

;; FIXME this doesn't work the way the API doc describe it!
(defn add-entity [{:keys [body username password] :as request}]
  (s/validate Entity body)
  (let [url entities-url]
    (post-data url body username password)))

;; DEVICES
(defn devices [{:keys [entity username password] :as request}]
  (let [url (entity-devices-url entity)]
    (get-data url username password)))

(defn device [{:keys [entity device username password] :as request}]
  (let [url (entity-device-url entity device)]
    (get-data url username password)))

(defn add-device [{:keys [body username password] :as request}]
  (let [entity (:entityId body)
        url (entity-devices-url entity)]
    (s/validate Device body)
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

(defn add-measurements
  ([{:keys [entity device measurements username password] :as request} result]
     (let [url (entity-device-measurements-url entity device)]
       (s/validate Measurements measurements)
       (post-data url measurements username password result)))
  ([{:keys [entity device measurements username password] :as request}]
     (let [url (entity-device-measurements-url entity device)]
       (s/validate Measurements measurements)
       (post-data url measurements username password))))
