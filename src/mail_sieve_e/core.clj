(ns mail-sieve-e.core
  (:gen-class)
  (:require [clojure.test :refer :all]
            [clojure.core.async :as a :refer [go-loop chan <!! >!!
                                        go alt!! alt! timeout
                                        <! >!]]
            [com.gearswithingears.async-sockets :refer :all]
            [mail-sieve-e.sieve :as s]
            [clojure.java.io :as io])
  (:import [java.net Socket ServerSocket]
           [java.io PrintWriter InputStreamReader BufferedReader]))

(declare read-handler write-handler)

(defn connect
  [server port]
  (let [socket (Socket. server port)
        in (read-handler (BufferedReader. (InputStreamReader.(.getInputStream socket))))
        out (write-handler (PrintWriter. (.getOutputStream socket)))]
    {:socket socket
     :in in
     :out out}))


(defn receive
  "Read a line of textual data from the given socket"
  [socket]
  (.readLine (io/reader socket)))

(defn write
  "Send the given string message out over the given socket"
  [socket msg]
  (let [writer (io/writer socket)]
    (.write writer (str msg "\n"))
    (.flush writer)))

(defn send-to-all
  [connected msg]
  (doall
   (map #(write % msg) @connected)))

(defn read-handler
  [socket]
  (let [read-chan (chan 1000000000)
        reader (io/reader socket)]
    (go-loop []
      (>!! read-chan (.readLine reader))
      (recur))
    read-chan))

(defn write-handler
  [socket]
  (let [write-chan (chan 10000000000)
        writer (io/writer socket)]
    (go-loop []
      (when-let [msg (<! write-chan)]
        (do
          (.write writer (str msg "\n"))
          (.flush writer))
        (recur)))
    write-chan))

(defn echo-server
  [port handler]
  (with-open [server-sock (ServerSocket. port)
              sock (.accept server-sock)]
    (let [msg-in  (receive sock)
          msg-out (handler msg-in)]
      (write sock msg-out))))

(defn async-echo-server
  "Takes: port      - port number
          handler   - a function that takes msgs from connected clients
          send-chan - A channel to immediately send information"
  [port handler send-chan]
  (let [running? (atom true)
        connected (atom [])
        server-socket (ServerSocket. port)]
    (go
      (while @running?
        (when-let [sock (.accept server-socket)]
          (swap! connected conj sock)
          (let [client-in (read-handler sock)]
            (go-loop []
              (if @running?
                (do
                  (alt! [client-in] ([msg] (write sock (handler msg)))
                        [send-chan] ([msg] (send-to-all connected msg)))
                  (recur))
                (.close sock)))))))
    (go-loop []
      (when-not running?
        (.close server-socket))
      (recur))
    {:server-socket server-socket
     :running running?
     :connected connected}))

(defn wait-for-clients
  [num-expected connected]
  ; First, wait for all computers to join
  (println "Waiting for computers to join...")
  (while (not= (count @connected) num-expected)
    (Thread/sleep 500)
    (println "# Connected: " (count @connected))))

(defn transfer-primes
  "All machines send lead their primes vector,
  and lead must send these to the right computers."
  [connections]
  (let [in-stream (a/merge (mapv :out @connections))
        comps @connections]
    (go-loop []
      (when-let [prime-vec (<! in-stream)]
        (let [[mi ps p] (read-string prime-vec)
              r-comps (drop mi comps)] ; respective computers
          (doall
           (map #(write % prime-vec) r-comps)))
        (recur)))))

(defn lead-start
  [num-expected port num-primes]
  (let [send-chan (chan 10000000)
        server    (async-echo-server port #(.toUpperCase %) send-chan)
        connected (:connected server)
        ; Wait for connected clients
        _         (wait-for-clients num-expected connected)
        __        (transfer-primes connected)]
    ; TODO
    ; Since appoint message is sent to all respective comps, fix that.
    ; Finish rest of this function




    ;Now, make the reader channel

    ))

(defn -main
  "I don't do a whole lot ... yet."
  ([]
   (time (s/dis-sieve-e 4 1000000))
   (read-line))
  ([num-comps num & args]
   (time (s/dis-sieve-e (Integer. num-comps) (Integer. num)))
   (read-line)))
