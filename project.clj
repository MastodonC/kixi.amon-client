(defproject kixi.amon-client "0.1.0-SNAPSHOT"
  :description "A client for the AMON API"
  :url "http://github.com/kixi.amon-client"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/core.async "0.1.278.0-76b25b-alpha"]
                 [clj-http "0.7.8"]
                 [clj-time "0.6.0"]
                 [http-kit "2.1.14"]
                 [org.clojure/data.csv "0.1.2"]
                 [org.clojure/data.json "0.2.4"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.slf4j/slf4j-api "1.7.5"]]
  :jvm-opts ["-Xmx8192m" "-Xss180k"])
