(ns examples.wordcount
  (:require [nano-mapreduce.core :as mr])
  (:require [clojure.string :as s]))

(defn map-f [key record]
  (let [words (s/split (second record) #"\s+")] ; split on whitespace
    (map #(vec [% 1]) words)))

(defn reduce-f [key list_of_values]
  [[key (count list_of_values)]])

(mr/exec "data/in/books.json" map-f reduce-f)
