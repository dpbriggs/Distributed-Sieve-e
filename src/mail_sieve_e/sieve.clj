(ns sieve.core
  (:gen-class)
  (:require [clojure.core.async :refer [>!! <!! chan thread]]
            [clojure.java.io :refer [writer]]
            [clojure.string :refer [join]])
  (:import [java.io FileWriter]))

(def chunk-size (atom 0))

(def comp-num (atom 0))

(defn gen-table
  [[lower upper]]
  (transient (vec (range lower upper 2))))

(defn spread-work
  "Takes n         - number of prime
         num-comps - number of computers on the network
   Divides work evenly across each computer"
  [n num-comps]
  (let [; Amount of numbers, as we're skip counting by 2
        nums (Math/floor (/ (dec n) 2))
        ; Number of numbers to give to each
        chunk-size (Math/floor (/ nums num-comps))]
    (loop [comp 1
           head 3.0
           tail (+ 3 (* 2 comp chunk-size))
           hold [[head tail]]]
      (if (= num-comps comp)
        hold
        (let [n-comp (inc comp)
              n-head tail
              n-tail (+ 3 ( * 2 n-comp chunk-size))
              n-head (conj hold [n-head n-tail])]
          (recur n-comp n-head n-tail n-head))))))

(defn indices
  "Takes mi - reporting machine number
         cs - chunk size
         ps - prime position (in reporting machine chunk)
         p  - the prime number
   Generates a lazy-seq of indices to mark"
  [mi cs ps p]
  (let [k (+ ps (* cs (dec mi)))] ; chunk adjustment
    (for [i (drop 1 (range))]
      (+ k (* i p)))))

(defn mark-composites
  "Takes mi     - reporting machine number
         cs     - chunk size
         ps     - prime position (in reporting machine chunk)
         p      - the prime number
         my-num - this machines number
         coll   - transient collection to mark indices
   Applies sieve marking step to prime chunk"
  [mi cs ps p my-num coll]
  (let [;early-indices (- (Math/floorDiv (* my-num cs) p) 2)
        to-mark       (drop 0 (indices mi cs ps p))
        lower-bound   (* (dec my-num) cs)
        upper-bound   (dec (* my-num cs))
        _             (println "% " (take 5 to-mark))
        __            (println "lower: " lower-bound)
        ___           (println "upper: " upper-bound)]
    (loop [head (first to-mark)
           tail (rest to-mark)]
      ; Ensure indice is within upper-bound
      (when (<= head upper-bound)
        (if (>= head lower-bound)
          (do
            ; modify the collection...
            (assoc! coll (mod head cs) 0)
            ; ... and recur the new values
            (recur (first tail) (rest tail)))
          (recur (first tail) (rest tail)))))
    coll))

(defn find-next-non-zero
  "Finds next non-zero number aka a new prime"
  [coll start cs]
  (loop [stop (inc start)]
    (when (<= stop cs)
      (if (not= 0 (get coll stop))
        stop
        (recur (inc stop))))))

(defn finish
  [raw-chunk]
  (println "Writing primes to file...")
  (let [chunk (persistent! raw-chunk)]
    (when (= 3 (first chunk))
      ; Dirty hack to get the number two in the first chunk.
      ; Basically move 7 into the space where 9 was, and move the other primes.
      ; [3 5 7 0 ...] --> [2 3 5 7 ...]
      (assoc chunk 3 7)
      (assoc chunk 2 5)
      (assoc chunk 1 3)
      (assoc chunk 0 2))
    (let [new-line (System/getProperty "line.separator")
          file-name (str (System/getProperty "user.home") "/primes.txt")
          filtered-chunk (filter #(not (zero? %)) chunk)
          primes (partition-all 10 filtered-chunk)]
      (with-open [w (FileWriter. file-name)]
        (doall
         (for [i primes]
           (.write w (str (clojure.string/join ", " i) new-line)))))
      (println "Done!")
      (println "Primes saved in:" file-name))))

(defn sieve-e
  "Parallel sieve of eratosthenes
  Takes: my-num      - this machine's number
         bounds      - vector of [lower-bound upper-bound]
         lead?       - boolean value if lead or not
         in-channel  - Merged channel of all messages sent to this machine
         chunk-in    - If there's a previously made chunk, pass it.
         & clients   - list of connected clients"
  [my-num lead? in-channel chunk [& clients]]
  (let [cs    (count chunk)]
    (if lead?
      (loop [start 0]
        ; Lead logic
        (let [prime (get chunk start)
              n-start (find-next-non-zero chunk start cs)]
          (if-not (nil? n-start)
            ; if n-start is nil, we've run out of primes.
            (do
              (println "start: " start)
              ; Send prime to connected clients
              (map #(>!! % [my-num start prime]) clients)
              ; Mark the primes
              (mark-composites my-num cs start prime my-num chunk)
              ; Complete the step.
              (recur n-start))
            ; If nil, I need to finish and elect new lead
            (do
              ;Appoint the next machine as lead.
              (println "appoint " (inc my-num) " as next machine (TODO)")
              ; Then finish the sieve
              (finish chunk)))))
      ; Follower logic
      (loop []
        ; Wait for other machines to give num
        (when-let [[mi ps p] (<!! in-channel)]
         ; when mi = -1, that's the election code.
         ; ps --> start
         ; p  --> prime
          (if-not (= mi -1)
            ; If we're not being appointed...
            (do
              ; Mark composite numbers
              (mark-composites mi cs ps p my-num chunk)
              ; Finish follower step
              (recur))
            ; If we're being appointed...
            (do
              (println "Appointed new lead.")
              (thread (sieve-e my-num true chunk clients)))))))))
