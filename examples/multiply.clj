(ns examples.multiply
  (:require [nano-mapreduce.core :as mr]))

;; matrix multiplication

(defn map-f [key record]
  (let [matrix (first record)
        x (first (rest record)) ;; line coordinate
        y (first (rest (rest record)))] ;; column coordinate
    (for [i (range 0 5)]
      (cond (= "a" matrix) [[x i] record]
            (= "b" matrix) [[i y] record]))))

(defn reduce-f [key list_of_values] ; key is a vector containing the cell coordinates
  (let [products (for [i (range 0 5)]
                   (let [a (first (for [x list_of_values :when (and (= "a" (first x)) (= i (nth x 2)))] (nth x 3))) ;; value is in the 3rd position
                         b (first (for [x list_of_values :when (and (= "b" (first x)) (= i (nth x 1)))] (nth x 3)))]
                     (if (and (not (nil? a)) (not (nil? b))) (* a b)
                       0)))
        sum (apply + products)]
    [[(first key) (second key) sum]]))

(mr/exec "data/in/matrix.json" map-f reduce-f)
;; output should look like ../data/out/multiply.json
