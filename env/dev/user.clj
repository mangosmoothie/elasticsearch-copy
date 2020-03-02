(ns user
  (:require
   [elasticsearch-copy.elasticsearch :as es]
   [elasticsearch-copy.core :as cp]
   [clojure.core.async :as async]))

(def source-es
  {:index "abc123"
   :url "http://localhost:9200"
   :size 100
   :concurrency 4
   })

(def target-es
  {:index "def456"
   :url "http://localhost:9200"
   :concurrency 12
   :max-count 100
   :max-chars 2000000
   })

(def local-es
  {:source source-es
   :destination target-es})

(defn clear-target [target]
  (es/delete-index target))

(defn test-data
  ([] (test-data 0))
  ([n]
   (cons [{:create {:_index "abc123" :_id (str n)}}
          {:field-a (str "val-a-" n) :field-b (str "val-b-" n)}]
         (lazy-seq (test-data (inc n))))))

(defn write-test-data [source-es target-es]
  (let [xf (async/chan 10000 (comp
                              (map cp/ndjson)
                              (cp/make-bulk-body target-es)))
        throttle (async/chan 1)]
    (async/>!! throttle true)
    (async/onto-chan xf (take 990 (test-data)))
    (cp/make-bulk-loader xf throttle target-es)))
