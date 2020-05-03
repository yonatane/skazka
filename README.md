# skazka

Apache Kafka proxy in clojure

```clojure
; In REPL:
(def x (skazka.net/start-proxy))

; Connect with a consumer or producer to localhost:9999 instead of port 9092.
; Watch the protocol printed as clojure data.

; When done:
(skazka.net/stop-proxy x)
```

## License

Copyright © 2020 Yonatan Elhanan

Distributed under the MIT License
