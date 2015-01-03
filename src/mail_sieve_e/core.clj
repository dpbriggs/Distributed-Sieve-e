(ns mail-sieve-e.core
  (:gen-class)
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [go-loop chan <!! >!!
                                        go alt!! alt! timeout
                                        <! >!]]
            [com.tbaldridge.hermod :refer :all]
            [sieve.core :as s]))

(def my-name (-> (System/currentTimeMillis)
                 (- (rand-int 100000000))
                 String/valueOf
                 keyword))

;;;;;;;;;;;
;; All followers that are not lead have two mailboxes, sieve and etc
;; sieve only deals with sieve related business (mi cs p ps)
;; etc is for all other purposes (starting sieve, sending results, etc)

;; TODO: Get start-follower function working (recieving client list)

(def connected-clients (atom []))

(def etc (mailbox :etc))

(def sieve (mailbox :sieve 8096))

(defn send-to-all
  [box msg]
  (map #(>!! (box %) msg) @connected-clients))

(def connected-clients-follower (atom []))

(defn start-follower
  [lead-host lead-port]
  (let [lead-rbx   (remote-mailbox lead-host lead-port :lead)
        lead-sieve (remote-mailbox lead-host lead-port :sieve)
        lead-etc   (remote-mailbox lead-host lead-port :etc)]
    (>!! lead-rbx {:etc   etc
                   :sieve sieve})
    (println "Waiting for all other clients to connect")
    (when-let [clients-list (<!! etc)]
      (swap! connected-clients-follower (fn [n] clients-list)))
    {:lead lead-rbx
     :sieve lead-sieve
     :lead-etc lead-etc}))

(defn start-leader
  [port expected-number]
  (listen port)
  (go
    (let [lead (mailbox :lead)]
      ; Add myself to the connected client list
      (swap! connected-clients conj {:lead  lead
                                     :sieve sieve
                                     :etc   etc})
      (loop []
        (when (< (count @connected-clients) expected-number)
          (when-let [{:keys [etc sieve]} (<! lead)]
            (println "Someone Connected!")
            (swap! connected-clients conj {:sieve sieve
                                           :etc   etc})
            (recur))))
      (send-to-all :etc connected-clients)
      (println "made it here"))))

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
