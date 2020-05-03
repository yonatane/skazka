(ns skazka.net
  (:require [taoensso.timbre :as timbre]
            [skazka.api-keys :as api-keys]
            [skazka.protocol :as proto])
  (:import (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.bootstrap ServerBootstrap Bootstrap)
           (io.netty.channel.socket.nio NioServerSocketChannel)
           (io.netty.handler.logging LoggingHandler LogLevel)
           (io.netty.channel ChannelInitializer ChannelHandler ChannelOption ChannelInboundHandlerAdapter ChannelHandlerContext ChannelFutureListener Channel ChannelFuture)
           (io.netty.util AttributeKey AttributeMap)
           (io.netty.buffer Unpooled ByteBuf)
           (io.netty.handler.codec LengthFieldBasedFrameDecoder)
           (java.util ArrayDeque Deque)
           (java.nio ByteBuffer)
           (org.apache.kafka.common.message MetadataResponseData ResponseHeaderData)
           (org.apache.kafka.common.protocol ByteBufferAccessor)))

(defn ->attribute-key
  [k]
  (-> k name AttributeKey/valueOf))

(defn set-attribute
  [^AttributeMap ch k v]
  (let [attr-k (->attribute-key k)
        attr (.attr ch attr-k)]
    (.set attr v)))

(defn get-attribute
  [^AttributeMap ch k]
  (let [attr-k (->attribute-key k)
        attr (.attr ch attr-k)]
    (.get attr)))

(defn channel-handlers-array
  ^"[Lio.netty.channel.ChannelHandler;"
  [& handlers]
  (into-array ChannelHandler handlers))

(defn close-on-flush [^Channel ch]
  (when (.isActive ch)
    (-> ch
        (.writeAndFlush Unpooled/EMPTY_BUFFER)
        (.addListener ChannelFutureListener/CLOSE))))

(defn frame-decoder []
  (let [max-frame-length (int (* 1024 1024))
        length-field-offset (int 0)
        length-field-length 4] ;int32
    (LengthFieldBasedFrameDecoder. max-frame-length length-field-offset length-field-length)))

(defn debug-response [req-header res-header correlated]
  (timbre/info "\nBROKER RESPONSE HEADER:" res-header
               "\nCORRELL REQUEST HEADER:" req-header
               "\nCORRELATED:" correlated))

(defn back-handler
  "Handles the connection to the back host.
  Writes the received data from back host to the proxy client through front-ch."
  [^Channel front-ch]
  (proxy [ChannelInboundHandlerAdapter] []
    (channelActive
      [^ChannelHandlerContext ctx]
      (.read ctx)) ;Why here we read from ctx and not ctx channel?
    ;(channelReadComplete
    ;  [^ChannelHandlerContext ctx]
    ;  (.flush ctx))
    (channelRead
      [^ChannelHandlerContext ctx ^ByteBuf b]
      (let [^Deque q (get-attribute front-ch :q)
            req-header (.remove q)
            [n res-header] (proto/read-res-header+ b)
            correlated (= (:correlation-id req-header)
                          (:correlation-id res-header))
            _ (debug-response req-header res-header correlated)

            b'
            (when correlated
              (when (= :metadata (-> req-header :api-key api-keys/->keyword))
                (let [; Debug read
                      ;^ByteBuffer nb (.nioBuffer b)
                      ;fl (.getInt nb)
                      ;^ByteBufferAccessor bba (ByteBufferAccessor. nb)
                      ;rh (ResponseHeaderData. bba (short 1))
                      ;mr (MetadataResponseData. bba (short 9))

                      [_ metadata+] (proto/read-metadata-res+ b)
                      metadata (:res metadata+)
                      _ (timbre/info "\nMETADATA:" metadata)
                      proxy-brokers [{:node-id 0
                                      :host "localhost"
                                      :port 9999
                                      :rack nil
                                      :tagged-fields nil}]
                      new-meta (assoc metadata :brokers proxy-brokers)

                      _ (timbre/info "\nOVERRIDE METADATA:" new-meta)
                      new-meta-length (proto/metadata-res-size new-meta)
                      new-meta+ {:frame-length new-meta-length
                                 :res new-meta}

                      total-length (+ 4 new-meta-length)
                      ^ByteBuf b' (Unpooled/buffer total-length total-length)
                      wrote (int (proto/write-metadata-res+! b' new-meta+))
                      _ (.writerIndex b' wrote)

                      ; Debug writing
                      ;^ByteBuffer nb (.nioBuffer b')
                      ;fl (.getInt nb)
                      ;^ByteBufferAccessor bba (ByteBufferAccessor. nb)
                      ;rh (ResponseHeaderData. bba (short 1))
                      ;mr (MetadataResponseData. bba (short 9))
                      ]
                  b')))
            ]

        (-> front-ch
            (.writeAndFlush (or b' b))
            (.addListener (proxy [ChannelFutureListener] []
                            (operationComplete
                              [^ChannelFuture ch-f]
                              (if (.isSuccess ch-f)
                                (do (timbre/info "\nWROTE TO FRONT") ;Writing to client succeeded, read more from back.
                                    (.read (.channel ctx)))
                                (do (timbre/error "\nERROR WRITING TO FRONT, CLOSING")
                                    (-> ch-f .channel .close)))))))))
    (channelInactive
      [_ctx]
      (timbre/error "\nBACK INACTIVE. Closing front")
      (close-on-flush front-ch))
    (exceptionCaught
      [^ChannelHandlerContext ctx ^Throwable cause]
      (timbre/error cause "Exception in back-handler")
      (close-on-flush (.channel ctx)))
    ))

(defn back-initializer
  [^Channel front-ch]
  (proxy [ChannelInitializer] []
    (initChannel [^Channel ch]
      (let [handlers (channel-handlers-array
                       (frame-decoder)
                       (back-handler front-ch))]
        (-> ch
            (.pipeline)
            (.addLast handlers))))))

(defn back-bootstrap
  [^Channel front-ch ^String back-host back-port]
  (let [^Bootstrap
        b (doto (Bootstrap.)
            (.group (.eventLoop front-ch))
            (.channel (class front-ch))
            (.handler (back-initializer front-ch))
            (.option ChannelOption/AUTO_READ false) ; for backpressure
            ;(.option ChannelOption/AUTO_CLOSE false)
            ;(.option ChannelOption/SO_KEEPALIVE true)
            ;(.option ChannelOption/TCP_NODELAY true)
            )
        ch-f (.connect b back-host (int back-port))
        back-ch (.channel ch-f)]
    (.addListener ch-f (proxy [ChannelFutureListener] []
                         (operationComplete
                           [^ChannelFuture ch-f]
                           (if (.isSuccess ch-f)
                             (do (timbre/info "\nCONNECTED TO BACK")
                                 (.read front-ch)) ;read first data
                             (do (timbre/error "\nERROR CONNECT TO BACK.")
                                 (.close front-ch))))))
    back-ch))

(defn front-handler
  "Handles the client accepted connection to the proxy.
  Opens a connection to the back host per client connection.
  Puts the back channel as an attribute on the front-channel to gain access across handler methods."
  [^String back-host back-port]
  (proxy [ChannelInboundHandlerAdapter] []
    (channelActive
      [^ChannelHandlerContext ctx]
      (let [front-ch (.channel ctx)
            back-ch (back-bootstrap front-ch back-host back-port)]
        (set-attribute front-ch :back-ch back-ch)
        (set-attribute front-ch :q (ArrayDeque.))))
    ;(channelReadComplete
    ;  [^ChannelHandlerContext ctx]
    ;  (.flush ctx))
    (channelRead
      [^ChannelHandlerContext ctx ^ByteBuf msg]
      (let [^Channel front-ch (.channel ctx)
            ^Channel back-ch (get-attribute front-ch :back-ch)
            ^Deque q (get-attribute front-ch :q)
            [n req-header] (proto/read-req-header+ msg)]

        (timbre/info "\nCLIENT REQUEST HEADER:" req-header)
        (.add q req-header)

        (when (.isActive back-ch)
          (-> back-ch
              (.writeAndFlush msg)
              (.addListener (proxy [ChannelFutureListener] []
                              (operationComplete
                                [^ChannelFuture ch-f]
                                (if (.isSuccess ch-f)
                                  (do (timbre/info "\nDELEGATED TO BACK")
                                      (.read front-ch)) ;Writing to back succeeded, read more from client.
                                  (do (timbre/error "\nERRER DELEGATING TO BACK")
                                      (-> ch-f .channel .close))))))))))
    (channelInactive
      [^ChannelHandlerContext ctx]
      (timbre/error "\nFRONT INACTIVE. Closing back")
      (let [front-ch (.channel ctx)
            back-ch (get-attribute front-ch :back-ch)]
        (when back-ch
          (close-on-flush back-ch))))
    (exceptionCaught
      [^ChannelHandlerContext ctx ^Throwable cause]
      (timbre/error cause "Exception in front-handler")
      (close-on-flush (.channel ctx)))
    ))

(defn front-initializer
  [back-host back-port]
  (proxy [ChannelInitializer] []
    (initChannel [^Channel ch]
      (let [handlers (channel-handlers-array
                       (LoggingHandler. LogLevel/INFO)
                       (frame-decoder)
                       (front-handler back-host back-port))]
        (-> ch
            (.pipeline)
            (.addLast handlers))))))

(defn proxy-bootstrap
  [listen-port back-host back-port]
  (let [boss-group (NioEventLoopGroup. 1)
        worker-group (NioEventLoopGroup.)]
    (try
      (let [^ServerBootstrap
            b (doto (ServerBootstrap.)
                (.group boss-group worker-group)
                (.channel NioServerSocketChannel)
                (.handler (LoggingHandler. LogLevel/INFO))
                (.childHandler (front-initializer back-host back-port))
                (.childOption ChannelOption/AUTO_READ false) ; for backpressure
                ;(.childOption ChannelOption/AUTO_CLOSE false)
                ;(.childOption ChannelOption/SO_KEEPALIVE true)
                ;(.childOption ChannelOption/TCP_NODELAY true)
                )
            ch (-> b (.bind (int listen-port)) (.sync) (.channel))
            sync-f #(-> ch (.closeFuture) (.sync))]
        {:ch ch
         :sync-f sync-f
         :boss-group boss-group
         :worker-group worker-group})
      (catch Throwable cause
        (timbre/error cause "Caught while bootstrapping. Shutting down.")
        (.shutdownGracefully boss-group)
        (.shutdownGracefully worker-group)))))

(defn start-proxy
  []
  (let [listen-port 9999
        back-host "localhost"
        back-port 9092]
    (proxy-bootstrap listen-port back-host back-port)))

(defn stop-proxy
  [{:keys [^NioEventLoopGroup boss-group ^NioEventLoopGroup worker-group]}]
  (.shutdownGracefully boss-group)
  (.shutdownGracefully worker-group))

(defn wait-for-close
  [{:keys [sync-f]}]
  (sync-f))
