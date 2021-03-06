(ns mail-sieve-e.sieve
  (:gen-class)
  (:require [clojure.core.async :refer [>!! <!! chan thread go timeout]]
            [clojure.string :refer [join]])
  (:import [java.io FileWriter]))

;; Main logic for the sieve

(defn gen-table
  "Given: [lower upper] - lower bound int and upper bound int
  produces a transient vector between those ranges skip counting by 2"
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
      (+ k (* i (.intValue p))))))

(defn mark-composites
  "Takes mi     - reporting machine number
         cs     - chunk size
         ps     - prime position (in reporting machine chunk)
         p      - the prime number
         my-num - this machines number
         coll   - transient collection to mark indices
   Applies sieve marking step to prime chunk"
  [mi cs ps p my-num coll]
  (let [early-indices (* (dec my-num) (Math/floor (/ (- cs ps) p)))
        to-mark       (drop (dec early-indices) (indices mi cs ps p))
        lower-bound   (* (dec my-num) cs)
        upper-bound   (dec (* my-num cs))]
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
  "Takes raw-chunk - transient vector of primes to process
         my-num    - this machines number
  and writes the contents to a file"
  [raw-chunk my-num]
  (println "Writing primes to file...")
  (when (= 1 my-num)
    ; Dirty hack to get the number two in the first chunk.
    ; Basically move 7 into the space where 9 was, and move the other primes.
    ; [3 5 7 0 ...] --> [2 3 5 7 ...]
    (do
      (assoc! raw-chunk 3 7.0)
      (assoc! raw-chunk 2 5.0)
      (assoc! raw-chunk 1 3.0)
      (assoc! raw-chunk 0 2.0)))
  (let [chunk (persistent! raw-chunk)
        new-line (System/getProperty "line.separator")
        file-name (str (System/getProperty "user.home") "/primes" my-num ".txt")
        filtered-chunk (filter #(not (zero? %)) chunk)
        primes (partition-all 10 filtered-chunk)]
    (with-open [w (FileWriter. file-name)]
      (doall
       (for [i primes]
         (.write w (str (clojure.string/join ", " i) new-line)))))
    (println "Done!")
    (println "Primes saved in:" file-name "\n")
    (println "")))

(defn find-first-prime
  "Find first non-zero number in a chunk"
  [coll cs]
  (if-not (zero? (get coll 0))
    0
    ; start = -1 as we increment start in the function making it 0
    (find-next-non-zero coll -1 cs)))

(defn sieve-e
  "Parallel sieve of eratosthenes
  Takes: my-num      - this machine's number
         lead?       - boolean value if lead or not
         in-channel  - Merged channel of all messages sent to this machine
         chunk       - If there's a previously made chunk, pass it.
         out-channel - channel to write to
  And starts the Sieve of Eraosthenes"
  [my-num lead? in-channel chunk out-channel]
  (println "Generating chunk...")
  (let [cs (count chunk)]
    (println "Starting Sieve...")
    (if lead?
      (loop [start (find-first-prime chunk cs)]
        ; Lead logic
        (let [prime (get chunk start)
              n-start (find-next-non-zero chunk start cs)]
          (if-not (nil? n-start)
            ; if n-start is nil, we've run out of primes.
            (do
              ; Send prime to connected clients
              (>!! out-channel [my-num start prime])
              ; Mark the primes
              (mark-composites my-num cs start prime my-num chunk)
              ; Complete the step.
              (recur n-start))
            ; If nil, I need to finish and elect new lead
            (do
              ;Appoint the next machine as lead.
              (println "appointing" (inc my-num) "as next machine.")
              (>!! out-channel [my-num -1 0])
              ; Then finish the sieve
              (finish chunk my-num)))))
      ; Follower logic
      (do
        (println "Following lead computer...")
        (loop []
          ; Wait for other machines to give num
          (when-let [[mi ps p] (mapv int (<!! in-channel))]
            ; when mi = -1, that's the code to appoint this machine as lead.
            ; ps --> start
            ; p  --> prime
            (if-not (= ps -1)
              ; If we're not being appointed...
              (do
                ; Mark composite numbers
                (mark-composites mi cs ps p my-num chunk)
                ; Finish follower step
                (recur))
              ; If we're being appointed...
              (if (= mi (dec my-num))
                (do
                  (println "Appointed as new lead.\n")
                  (sieve-e my-num true (chan 100) chunk out-channel))
                (recur)))))))))
