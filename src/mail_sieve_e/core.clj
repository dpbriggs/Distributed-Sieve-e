(ns mail-sieve-e.core
  (:gen-class)
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [go-loop chan <!! >!!
                                        go alt!! alt! timeout
                                        <! >!]]
            [com.tbaldridge.hermod :refer :all]))
; Home box is the mailbox that recieves generic information
(def home-box (mailbox :home))
; Generic mailbox for all heart beat request (is sorted later)
(def heart-beat (mailbox :heart-beat))
; list of connected clients
(def connected-clients (atom {}))
; reasonably unique name
(def my-name (-> (System/currentTimeMillis)
                 (- (rand-int 100000000))
                 String/valueOf
                 keyword))

; port that this program will work on
(def my-port 4111)

(defn ping-mailbox [port]
  (listen port)
  (go
   (with-open [m (mailbox :ping)]
     (loop []
       (when-let [{:keys [return-to msg]} (<! m)]
         (>! return-to msg)
         (recur))))))

(defn send-to
  "Sends "
  [rbx key msg]
  (>!! rbx {:return-to home-box
            key msg}))

(defn connect
  [host port]
  (let [unique-id   (System/currentTimeMillis) ; generate unique id to make mail boxes
        rbx-home    (remote-mailbox host port :home) ; connect to their home box
        rbx-ping    (remote-mailbox host port :heart-beat) ; connect to their ping box
        lbx-home    (mailbox unique-id)
        lbx-ping    (mailbox (inc unique-id)) ; make a local box to recieve pings
        rbx-connect (remote-mailbox host port unique-id) ; shit mailbox to get a name
        _           (>!! rbx-connect my-name) ; Send them my-name
        rbx-name    (<!! lbx-home)         ; recieve their name
        client      {:rbx-ping rbx-ping
                     :rbx-home rbx-home
                     :lbx-ping lbx-ping
                     :lbx-home lbx-home}]
    (swap! connected-clients assoc rbx-name client)
    client))

; Vector of potentially dead clients
(def dead-clients (atom #{}))

(defn pong
  "handles dumb ping requests"
  []
  (go-loop []
    ; sort the ping request into the correct local ping mailbox
    (when-let [id (<! heart-beat)]
      (let [client (:lbx-ping (id @connected-clients))]
        (>! client 0)))
    (recur)))

(defn ping
  "Handles pinging service"
  []
  ; current --> current clients at time of ping cycle
  (go-loop [current @connected-clients]
    (for [i current]
      (go
        (let [rbx-ping (:rbx-ping (first (vals i)))
              lbx-ping (:lbx-ping (first (vals i)))
              id       (first (keys i))]
          (>! rbx-ping id)
          (alt! [lbx-ping] (print "im really dumb")  ; do nothing
                ; otherwise add to list of dead clients
                [(timeout 30000)] (swap! dead-clients conj id)))))
    ; Send a ping request every 10 seconds
    (<! (timeout 10000))
    (recur [@connected-clients])))

(deftest ping-test
  ;; Only used during testing to make sure we have a clean state, you shouldn't
  ;; need to do this in normal use cases.
  (restart-selector!)

  ;; Create a local box with a known name, and wire up a echo service
  (ping-mailbox 4242)


  ;; Create a pointer to the :ping-box
  (let [rbx (remote-mailbox "localhost" 4242 :ping)]

    ;; Create a response box
    (with-open [lbx (mailbox)]
      (dotimes [x 100]
        ;; Send to the remote box and wait for the reply. In a request/response
        ;; situation, like this, we will alt on a timeout and throw an exception
        ;; if the message is dropped. We could also resend and wait again.
        (>!! rbx {:return-to lbx
                  :msg :a})
        (alt!! [lbx] ([v] (print (keyword? v)))
               [(timeout 1000)] (assert false "Timeout after 1000 ms"))))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
