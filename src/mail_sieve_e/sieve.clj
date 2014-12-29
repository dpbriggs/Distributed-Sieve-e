(ns sieve.core
  (:gen-class))

(defn spread-work
  [n num-comps]
  (let [nums (Math/floor (/ (dec n) 2)) ; number of numbers
        chunk-size (Math/floor (/ nums num-comps))]
    (loop [comp 1
           head 3
           tail (+ 3 (* 2 comp chunk-size))
           hold [[head tail]]]
      (if (= num-comps comp)
        hold
        (let [n-comp (inc comp)
              n-head tail
              n-tail (+ 3 ( * 2 n-comp chunk-size))
              n-head (conj hold [n-head n-tail])]
          (recur n-comp n-head n-tail n-head))))))

(defn start-point
  [lead-start chunk-size prime queue-pos]
  (if (= queue-pos 1)
    lead-start
    (let [k (Math/floor (/ (- chunk-size lead-start) prime))
          n-lead-start (+ lead-start (- (* (inc k) prime) 10))
          n-queue-pos (dec queue-pos)]
      (if (> n-lead-start 10)
        (start-point nil 0 0 1)
        (start-point n-lead-start chunk-size prime n-queue-pos)))))
