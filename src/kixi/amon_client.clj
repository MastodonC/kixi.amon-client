(ns kixi.amon-client
  (:require [clojure.data.json :as json]
            [org.httpkit.client :as http]
            [clojure.core.async :refer [chan >! <! go <!!]]))

;;
;; low level GET & POST
;;

(defn get-body [resp]
  (json/read-str (:body resp)
                 :key-fn keyword))

(defn print-body [resp]
  (println (get-body resp)))

(defn http-options [username password]
  {:basic-auth [username password]
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
     (get-body @(async-get url (assoc (http-options username password) :query-params query))))
)

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

(defn add-measurements [{:keys [entity device username password] :as request} result]
  (let [
        url (entity-device-measurements-url entity device)
        body (select-keys request [:measurements])
        ]
    (post-data url body username password result)))

