(ns mail-sieve-e.core
  (:gen-class)
  (:require [clojure.core.async :as a :refer [go-loop chan <!! >!!
                                        go alt!! alt! timeout
                                        <! >!]]
            [mail-sieve-e.sieve :as s]
            [clojure.java.io :as io])
  (:import [java.net Socket ServerSocket]
           [java.io PrintWriter InputStreamReader BufferedReader]))

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
  "Sends a msg to all connected machines"
  [connected msg]
  (doall
   (map #(write % msg) @connected)))

(defn read-handler
  "Given a socket and returns a channel which is populated
  with all of information coming from that socket"
  [socket]
  (let [read-chan (chan 1e18)
        reader (io/reader socket)]
    (go-loop []
      (try
        (let [msg-in (.readLine reader)]
          (when-not (empty? msg-in)
            (>!! read-chan (read-string msg-in))))
        (catch Exception e (str "Socket closed.")))
      (recur))
    read-chan))

(defn write-handler
  "Given a socket it returns a channel which writes to the socket
  whatever is put into the channel"
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

(defn async-echo-server
  "Takes: port      - port number
          handler   - a atom of a function that takes msgs from connected clients
          send-chan - A channel to immediately send information
  and starts an ayncronous server which handles all connections"
  [port handler send-chan]
  (let [running?      (atom true)
        connected     (atom [])
        server-socket (ServerSocket. port)]
    (try
      (go
        (while @running?
          (when-let [sock (.accept server-socket)]
            (swap! connected conj sock)
            (let [client-in (read-handler sock)]
              (go-loop []
                (when @running?
                  (do
                    (alt! [client-in] ([msg] (write sock (@handler msg)))
                          [send-chan] ([msg] (send-to-all connected msg)))
                    (recur))))))))
      (catch Exception e nil))
    (go-loop []
      (if-not running?
        (.close server-socket)
        (recur)))
    {:server-socket server-socket
     :running running?
     :connected connected}))

(defn wait-for-clients
  "Given a num-expected number of machines to connect
  and the list of currently connected computers waits until
  the expected number of computers connect."
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
  "Given: num-expected - number of machines to run on the network
          num-primes   - number to find all primes underneath
          port         - port to listen on"
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
      (<!! (timeout 200))
      (reset! (:running server) false)
      (mapv #(.close %) connected)
      (a/close! send-chan)
      (.close (:server-socket server)))
    (println "Sieve completed!")))

(defn client-start
  "Starts a following process listening on host:port"
  [host port]
  (println "connecting to host...")
  (let [lead (connect host port)]
    (println "connected to host!\n")
    (println "waiting for all other computers to connect...\n")
    (let [my-num (int(<!! (:in lead)))
          bounds (mapv int (<!! (:in lead)))
          chunk  (s/gen-table bounds)]
      (when-let [start? (<!! (:in lead))]
        (s/sieve-e my-num false (:in lead) chunk (:out lead))
        (println "Waiting for kill signal...")
        (while (not= 0 (<!! (:in lead)))
          (<!! (timeout 50)))
        (do (a/close! (:in lead))
            (a/close! (:out lead)))
        (println "Done!")))))

(defn -main
  "Distributed implementation of the Sieve of Eratosthenes"
  ([num-comps num-primes port]
   (lead-start (Integer. num-comps) (Integer. num-primes) (Integer. port)))
  ([host port]
   (client-start host (Integer. port))))
