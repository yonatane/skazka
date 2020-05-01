(defproject skazka "0.1.0-SNAPSHOT"
  :description "Apache Kafka codec for clojure"
  :url "https://github.com/yonatane/skazka"
  :license {:name "MIT License"}
  :dependencies [[org.clojure/clojure "1.10.1"]]
  :profiles {:dev {:source-paths ["dev"]
                   :dependencies [[org.clojure/tools.namespace "1.0.0"]
                                  [criterium "0.4.5"]
                                  [com.clojure-goes-fast/clj-async-profiler "0.4.1"]
                                  [org.apache.kafka/kafka-clients "2.5.0"]]}}
  :repl-options {:init-ns skazka.dev}
  :global-vars {*warn-on-reflection* true}
  :pedantic? :abort)
