(ns nano-mapreduce.core
  (:require [cheshire.core :as json])
  (:require [clojure.java.io :as io]))

;; read input file (json format)
(defn read-file [path-to-file]
  (with-open [rdr (io/reader path-to-file)]
    (doall (line-seq rdr))))

;; reads json data
;; json should contain a list
;; every item of the list will be fed to a 'map' function
(defn read-json [lines]
  (map #(json/parse-string % true) lines))

;; writes output data (as json) to the (default) output stream
(defn write-json [tuples]
  (map #(println (json/generate-string %)) tuples))

;; adds numeric keys to each item
(defn assign-keys [data]
  (map list (range 1 (inc (count data))) data))

;; feeds data to the 'map' function
;; map-fun returns a list of pairs (tuples)
(defn mapper [tuples map-fun]
  (let [ms (pmap #(apply map-fun %) tuples)
        fs (filter #(> (count %) 0) ms)]
    (apply concat fs)))

;; groups intermediate data by 'key'
(defn group-by-key [tuples]
  (let [groups (group-by first tuples)
        vals (fn [x]
               (mapcat #(rest %) x))] ; drop the key from each value and concatenate the values (merge to a list)
    (map #(list (key %) (vals (val %))) groups)))

;; feeds intermediate data to the 'reduce' function
;; reduce-fun returns a list of results
(defn reducer [tuples reduce-fun]
  (let [rs (pmap #(apply reduce-fun %) tuples)]
    (apply concat rs)))

(defn sorter [results]
  (try
    (sort results)
    (catch Exception e results))) ; not all results are 'naturally' sortable

(defn exec [file-name map-fun reduce-fun]
  (->
   file-name
   (read-file)
   (read-json)
   (assign-keys)
   (mapper map-fun)
   (group-by-key)
   (reducer reduce-fun)
   (sorter)
   (write-json)))

