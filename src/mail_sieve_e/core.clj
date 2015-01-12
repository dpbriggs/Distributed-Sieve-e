(ns mail-sieve-e.core
  (:gen-class)
  (:require [clojure.core.async :as a :refer [go-loop chan <!! >!!
                                        go alt!! alt! timeout
                                        <! >!]]
            [mail-sieve-e.sieve :as s]
            [clojure.java.io :as io])
  (:import [java.net Socket ServerSocket]
           [java.io PrintWriter InputStreamReader BufferedReader]))

; TODO: Fix bug that prevents machines with number > 2 to fail

(declare read-handler write-handler)

(defn connect
  "Takes: server - host ip to connect to
          port   - port to connect to"
  [server port]
  (let [socket (Socket. server port)
        in (-> socket
               .getInputStream
               InputStreamReader.
               BufferedReader.
               (read-handler))
        out (-> socket
                .getOutputStream
                PrintWriter.
                (write-handler))]
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
  (let [read-chan (chan 1e18)
        reader (io/reader socket)]
    (go-loop []
      (let [msg-in (.readLine reader)]
        (when-not (empty? msg-in)
          (>!! read-chan (read-string msg-in))))
      (recur))
    read-chan))

(defn write-handler
  [socket]
  (let [write-chan (chan 1e18)
        writer (io/writer socket)]
    (go-loop []
      (when-let [msg (<! write-chan)]
        (when-not (nil? msg)
          (do
            (.write writer (str msg "\n"))
            (.flush writer)))
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
          handler   - a atom of a function that takes msgs from connected clients
          send-chan - A channel to immediately send information"
  [port handler send-chan]
  (let [running?      (atom true)
        connected     (atom [])
        server-socket (ServerSocket. port)]
    (go
      (while @running?
        (when-let [sock (.accept server-socket)]
          (swap! connected conj sock)
          (let [client-in (read-handler sock)]
            (go-loop []
              (if @running?
                (do
                  (alt! [client-in] ([msg] (write sock (@handler msg)))
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
    (Thread/sleep 2000))
    (println "# Connected: " (count @connected))
  (Thread/sleep 200))

(defn transfer-primes
  "All machines send lead their primes vector,
  and lead must send these to the right computers."
  [connections done? prime-vec]
  (let [[mi ps p] prime-vec
        r-comps (drop (dec mi) connections) ; computers we actually have to send the nums to
        appoint? (= ps -1)]
    ; Send prime-vec
    (mapv #(write % prime-vec) r-comps)
    ; Check if it's an appoint signal
    ; When ps = -1, appoint the next machine,
    ; since machines are indexed starting at 1, and vectors at 0,
    ; to get the next comp get comps at my-num
    ; ex machine 1 appoints machine two, [m1 m2 m3...], m2 at index 1
    (when (and appoint?
               (= (count connections) (dec mi)))
      (reset! done? true))))

(defn lead-start
  [num-expected num-primes port]
  (let [send-chan  (chan 10000000)
        handler    (atom #(.toUpperCase %))
        server     (async-echo-server port handler send-chan)
        connected  (:connected server)
        ; Wait for connected clients
        _          (wait-for-clients (dec num-expected) connected)
        connected  @connected ; Ensure connected computers stays constant
        done?      (atom false) ; Tells when we're finished
        ; Change handler function to transfer-primes function
        ___        (reset! handler (partial transfer-primes connected done?))
        chunks     (s/spread-work num-primes num-expected)
        lead-chunk (s/gen-table (first chunks))]
    ; First thing to do is send machine numbers and bounds
    (doall
     (for [mi (range (count connected))
           :let [machine (get connected mi)
                 bounds  (get chunks (inc mi))]] ; first chunk is for lead
       (do
         (write machine (+ mi 2))
         (write machine bounds))))
    ; Now we can start the sieve
    (s/sieve-e 1 true (chan 10) lead-chunk send-chan)
    (println "Waiting for other machines to finish...\n")
    (while (not @done?)
      (<!! (timeout 500)))
    ; shut down server
    (println "Shutting down server...\n")
    (do
      (>!! send-chan 0)
      (reset! (:running server) false)
      (.close (:server-socket server)))
    (println "Sieve completed!")))

(defn client-start
  [host port]
  (println "connecting to host...")
  (let [lead (connect host port)]
    (println "connected to host!\n")
    (println "waiting for all other computers to connect...")
    (let [my-num (<!! (:in lead))
          bounds (<!! (:in lead))
          chunk  (s/gen-table bounds)]
      (when-let [start? (<!! (:in lead))]
        (s/sieve-e my-num false (:in lead) chunk (:out lead))
        (println "Waiting for kill signal...")
        (while (not= 0 (<!! (:in lead)))
          (<!! (timeout 50)))
        (.close (:socket lead))
        (println "Done!")))))

(defn -main
  "I don't do a whole lot ... yet."
  ([num-comps num-primes port]
   (lead-start (Integer. num-comps) (Integer. num-primes) (Integer. port)))
  ([host port]
   (client-start host (Integer. port))))
