(ns skazka.protocol
  (:require [bytegeist.bytegeist :as g]))

(def compact-string
  "String with unsigned varint length delimiter set to N+1 (N is number of bytes).
  N=0 means empty string \"\". N=-1 means nil"
  (g/spec [:string {:length :uvarint32 :adjust 1}]))

(defn compact-array
  "Array with unsigned varint length delimiter set to N+1 (N is number of items).
  N=0 means empty vector `[]`. N=-1 means nil"
  [s]
  (g/spec [:vector {:length :uvarint32 :adjust 1} s]))

(def tagged-fields
  (g/spec [:map-of {:length :uvarint32} :uvarint32 [:bytes {:length :uvarint32}]]))

(def frame-length
  (g/spec :int32))

(def req-header+
  (g/spec [:map {:length frame-length}
           [:api-key :int16]
           [:api-ver :int16]
           [:correlation-id :int32]
           [:client-id [:string {:length :short}]]]))

(def res-header+
  (g/spec [:map {:length frame-length}
           [:correlation-id :int32]
           [:tagged-fields tagged-fields]]))

(def broker
  (g/spec [:map
           [:node-id :int32]
           [:host compact-string]
           [:port :int32]
           [:rack compact-string]
           [:tagged-fields tagged-fields]]))

(def brokers
  (compact-array broker))

(def partition
  (g/spec [:map
           [:error-code :int16]
           [:partition-index :int32]
           [:leader-id :int32]
           [:leader-epoch :int32]
           [:replica-nodes (compact-array :int32)]
           [:isr-nodes (compact-array :int32)]
           [:offline-replicas (compact-array :int32)]
           [:tagged-fields tagged-fields]]))

(def topic
  (g/spec [:map
           [:error-code :int16]
           [:name compact-string]
           [:is-internal :bool]
           [:partitions (compact-array partition)]
           [:topic-authorized-operations :int32]
           [:tagged-fields tagged-fields]]))

(def metadata-res-v9
  (g/spec [:map {:length frame-length}
           [:correlation-id :int32]
           [:header-tagged-fields tagged-fields]
           [:throttle-time-ms :int32]
           [:brokers brokers]
           [:cluster-id compact-string]
           [:controller-id :int32]
           [:topics (compact-array topic)]
           [:cluster-authorized-operations :int32]
           [:tagged-fields tagged-fields]]))

(defn read-req-header+ [b]
  (g/read req-header+ b))

(defn read-res-header+ [b]
  (g/read res-header+ b))

(defn read-metadata-res [b]
  (g/read metadata-res-v9 b))
