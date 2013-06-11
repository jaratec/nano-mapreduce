(ns examples.inverted-index
  (:require [nano-mapreduce.core :as mr])
  (:require [clojure.string :as s]))

(defn map-f [key record]
  (let [document (first record)
        words (s/split (last record) #"\s+")]
    (map #(vec [% document]) words)))

(defn reduce-f [key list_of_values]
  [[key (set list_of_values)]])

(mr/exec "data/in/books.json" map-f reduce-f)
;; output should look like ../data/out/inverted_index.json
