(ns examples.join
  (:require [nano-mapreduce.core :as mr]))

;; relational join

(defn map-f [key record]
  (let [join_on_key (nth record 1)]
    [[join_on_key record]]))

(defn reduce-f [key list_of_values]
  (let [orders (for [x list_of_values :when (= "order" (first x))] x)
        items (for [x list_of_values :when (= "line_item" (first x))] x)]
    (vec (for [o orders
          i items]
      (concat o i)))))

(mr/exec "data/in/records.json" map-f reduce-f)
;; output should look like ../data/out/join.json
