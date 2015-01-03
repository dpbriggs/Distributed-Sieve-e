(ns sieve.core
  (:gen-class))

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
    (when (< stop cs)
      (if (not= 0 (get coll stop))
        stop
        (recur (inc stop))))))

(defn sieve-e
  "Parallel sieve of eratosthenes"
  [connected-clients ])
