(ns skazka.protocol
  (:require [bytegeist.bytegeist :as g]))

(def compact-string
  "String with unsigned varint length delimiter set to N+1 (N is number of bytes).
  N=0 means empty string \"\". N=-1 means nil"
  (g/spec [:string :uvarint32 1]))

(defn compact-array
  "Array with unsigned varint length delimiter set to N+1 (N is number of items).
  N=0 means empty vector `[]`. N=-1 means nil"
  [s]
  (g/spec [:vector :uvarint32 s 1]))

(defn tagged-fields-spec
  [& [known-fields]]
  (let [s-num-fields (g/spec :uvarint32)
        offset 1]
    (reify
      g/Spec
      (read [_ b]
        (let [num-fields (- (g/read s-num-fields b) offset)]
          (if (< num-fields 0)
            nil
            (throw (Exception. "Not yet supporting non-nil tagged-fields")))))
      (write [_ b v]
        (if (nil? v)
          (g/write s-num-fields b (dec offset))
          (throw (Exception. "Not yet supporting non-nil tagged-fields")))))))

(def tagged-fields (tagged-fields-spec))

(def frame-length
  (g/spec :int32))

(def req-header+
  (g/spec [:map
           [:frame-length frame-length]
           [:api-key :int16]
           [:api-ver :int16]
           [:correlation-id :int32]
           [:client-id [:string :short]]]))

(def res-header+
  (g/spec [:map
           [:frame-length frame-length]
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
  (g/spec [:map
           [:correlation-id :int32]
           [:header-tagged-fields tagged-fields]
           [:throttle-time-ms :int32]
           [:brokers brokers]
           [:cluster-id compact-string]
           [:controller-id :int32]
           [:topics (compact-array topic)]
           [:cluster-authorized-operations :int32]
           [:tagged-fields tagged-fields]]))

(def metadata-res-v9+
  (g/spec [:map
           [:frame-length frame-length]
           [:res metadata-res-v9]]))

(defn read-req-header+ [b]
  (g/read req-header+ b))

(defn read-res-header+ [b]
  (g/read res-header+ b))

(defn read-metadata-res+ [b]
  (g/read metadata-res-v9+ b))
