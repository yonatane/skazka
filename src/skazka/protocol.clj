(ns skazka.protocol
  (:require [octet.core :as oct]
            [octet.buffer :as buf]
            [octet.spec :as spec])
  (:import (io.netty.buffer ByteBuf)))

(def frame-length
  oct/int32)

(def string-length
  oct/int16)

(defn read-unsigned-varint*
  [^ByteBuf b offset]
  (loop [r (int 1) o (int offset) v (int 0) i (int 0) byt (.getByte b o)]
    (if (not= 0 (bit-and byt 0x80))
      (let [r (int (inc r))
            o (int (inc o))
            v (int (bit-or v (bit-shift-left (bit-and byt 0x7f) i)))
            i (int (+ i 7))]
        (when (> i 28)
          (throw (IllegalArgumentException. "Varint too long")))
        (recur r o v i (.getByte b o)))
      [r (bit-or v (bit-shift-left byt i))])))

(defn set-unsigned-varint!
  [^ByteBuf b offset v]
  (loop [v (int v) o (int offset)]
    (if (not= 0 (bit-and v 0xffffff80))
      (do (.setByte b o (byte (bit-or (bit-and v 0x7f) 0x80)))
          (recur (int (unsigned-bit-shift-right v 7)) (int (inc o))))
      (do (.setByte b o (byte v))
          (inc (- o offset))))))

(defn unsigned-varint-size
  [v]
  (loop [v (int v) o (int 1)]
    (if (not= (long 0) (long (bit-and v 0xffffff80)))
      (recur (int (unsigned-bit-shift-right v 7)) (int (inc o)))
      o)))

(def string*
  (reify
    spec/ISpecDynamicSize
    (size* [_ data]
      (let [data (octet.spec.string/string->bytes data)]
        (+ 2 (count data))))

    spec/ISpec
    (read [_ buff pos]
      (let [datasize (buf/read-short buff pos)]
        (if (> datasize -1)
          (let [data (buf/read-bytes buff (+ pos 2) datasize)
                data (octet.spec.string/bytes->string data datasize)]
            [(+ datasize 2) data])
          [2 nil])))

    (write [_ buff pos value]
      (if (some? value)
        (let [input (octet.spec.string/string->bytes value)
              length (count input)]
          (buf/write-int buff pos length)
          (buf/write-bytes buff (+ pos 2) length input)
          (+ length 2))
        (do (buf/write-int buff pos -1)
            2)))))

(def compact-string
  (reify
    spec/ISpecDynamicSize
    (size* [_ str]
      (if (nil? str)
        (unsigned-varint-size 0)
        (let [str-bytes (octet.spec.string/string->bytes str)
              n (count str-bytes)
              n-plus-1 (inc n)
              length-length (unsigned-varint-size n-plus-1)]
          (+ length-length n))))

    spec/ISpec
    (read [_ buff pos]
      (let [[readed n-plus-1] (read-unsigned-varint* buff pos)
            datasize (dec n-plus-1)]
        ;TODO this is actually COMPACT_NULLABLE_STRING behavior. Add spec.
        (if (> datasize -1)
          (let [data (buf/read-bytes buff (+ pos readed) datasize)
                data (octet.spec.string/bytes->string data datasize)]
            [(+ datasize readed) data])
          [readed nil])))

    (write [_ buff pos str]
      (if (some? str)
        (let [str-bytes (octet.spec.string/string->bytes str)
              n (count str-bytes)
              n-plus-1 (inc n)
              length-length (set-unsigned-varint! buff pos n-plus-1)]
          (buf/write-bytes buff (+ pos length-length) n str-bytes)
          (+ length-length n))
        (set-unsigned-varint! buff pos 0)))))

(defn compact-array
  [t k]
  (reify
    spec/ISpecDynamicSize
    (size* [_ data]
      (let [length (reduce #(+ %1 (if (satisfies? spec/ISpecDynamicSize t)
                                    (spec/size* t %2)
                                    (spec/size t))) 0 data)
            length-length (unsigned-varint-size length)
            result (+ length-length length)]
        result))
    spec/ISpec
    (read [_ buff pos]
      (let [[len-readed n-plus-1] (read-unsigned-varint* buff pos)
            nitems (dec n-plus-1)]
        (if (> nitems -1)
          (let [typespec (spec/repeat nitems t)
                [readed data] (spec/read typespec buff (+ pos len-readed))]
            [(+ readed len-readed) data])
          [len-readed nil])))

    (write [_ buff pos input]
      (if (nil? input)
        (set-unsigned-varint! buff pos 0)
        (let [nitems (count input)
              n-plus-1 (inc nitems)
              typespec (spec/repeat nitems t)
              length-length (set-unsigned-varint! buff pos n-plus-1)]
          (+ length-length (spec/write typespec buff (+ pos length-length) input)))))))

(def unsigned-varint
  ;TODO complete
  (reify
    ;spec/ISpecSize
    spec/ISpecDynamicSize
    (size* [_ data]
      )

    spec/ISpec
    (read [_ buff pos]
      (read-unsigned-varint* buff pos))

    (write [_ buff pos input]
      )))

(def tagged-fields
  ;TODO complete implementation for non-nil, non-empty values.
  (reify
    spec/ISpecDynamicSize
    (size* [_ data]
      (unsigned-varint-size 0))

    spec/ISpec
    (read [_ buff pos]
      (let [[len-readed nitems] (read-unsigned-varint* buff pos)]
        (if (> nitems 0)
          [len-readed nil]
          [len-readed nil])))

    (write [_ buff pos input]
      (set-unsigned-varint! buff pos 0))))

(def req-header
  (oct/spec :api-key oct/int16
            :api-ver oct/int16
            :correlation-id oct/int32
            :client-id string*))

(def req-header+
  (oct/spec :frame-length frame-length
            :api-key oct/int16
            :api-ver oct/int16
            :correlation-id oct/int32
            :client-id string*))

(def res-header+
  (oct/spec :frame-length frame-length
            :correlation-id oct/int32
            :tagged-fields tagged-fields))

(def broker
  (oct/spec :node-id oct/int32
            :host compact-string
            :port oct/int32
            :rack compact-string
            :tagged-fields tagged-fields))

(def brokers
  (compact-array broker :brokers))

(def partition
  (oct/spec :error-code oct/int16
            :partition-index oct/int32
            :leader-id oct/int32
            :leader-epoch oct/int32
            :replica-nodes (compact-array oct/int32 :replica-nodes)
            :isr-nodes (compact-array oct/int32 :isr-nodes)
            :offline-replicas (compact-array oct/int32 :offline-replicas)
            :tagged-fields tagged-fields))

(def topic
  (oct/spec :error-code oct/int16
            :name compact-string
            :is-internal oct/bool
            :partitions (compact-array partition :partitions)
            :topic-authorized-operations oct/int32
            :tagged-fields tagged-fields))

(def metadata-res-v9
  (oct/spec :correlation-id oct/int32
            :header-tagged-fields tagged-fields
            :throttle-time-ms oct/int32
            :brokers brokers
            :cluster-id compact-string
            :controller-id oct/int32
            :topics (compact-array topic :topics)
            :cluster-authorized-operations oct/int32
            :tagged-fields tagged-fields))

(def metadata-res-v9+
  (oct/spec :frame-length frame-length
            :res metadata-res-v9))

(defn read-frame-length [b]
  (oct/read b frame-length))

(defn read-req-header [b]
  (oct/read* b req-header))

(defn read-req-header+ [b]
  (oct/read* b req-header+))

(defn read-res-header+ [b]
  (oct/read* b res-header+))

(defn read-metadata-res+ [b]
  (oct/read* b metadata-res-v9+))

(defn write-metadata-res+! [b m]
  (oct/write! b m metadata-res-v9+))

(defn metadata-res-size [m]
  (spec/size* metadata-res-v9 m))

(defn brokers-byte-length [bks]
  (spec/size* brokers bks))
