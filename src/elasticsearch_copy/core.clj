(ns elasticsearch-copy.core
  (:require
   [clj-http.client :as client]
   [clojure.edn]
   [clojure.core.async :as async]
   [cheshire.core :as json]))

;; app-state maps where keys are job-id
(def jobs (atom {}))
(def scroll-totals (atom {}))
(def pulled (atom {}))
(def posted (atom {}))
(def posted-details (atom {}))
(def scroll-clients (atom {}))
(def errors (atom {}))

(defn get-job-ids []
  (vec (keys @jobs)))

(defn job-exists? [job-id]
  (some? (@jobs job-id)))

(defn get-job-total [job-id target]
  (reduce (fnil + 0 0) 0 (vals (@target job-id))))

(defn get-scroll-total
  ([job-id]
   (get-job-total job-id scroll-totals))
  ([job-id scroll-id]
   (get-in @scroll-totals [job-id scroll-id])))

(defn get-pulled-total [job-id]
  (get-job-total job-id pulled))

(defn get-posted-total [job-id]
  (@posted job-id))

(defn set-scroll-total [job-id scroll-id total]
  (when (number? total)
    (swap! scroll-totals assoc-in [job-id scroll-id] total)))

(defn capture-ex [job-id worker-id ex]
  (swap! errors
         update-in [job-id] conj (assoc (Throwable->map ex)
                                        :job-id job-id
                                        :worker-id worker-id)))

(defn update-addition-atom [job-id worker-id n target]
  (when (number? n)
    (swap! target update-in [job-id worker-id] (fnil + 0) n)))

(defn update-posted [job-id n]
  (when (number? n)
    (swap! posted update job-id (fnil + 0) n)
    (swap! posted-details
           (fn [m]
             (-> m
                 (update-in [job-id :batch-posted-count] (fnil inc 0))
                 (assoc-in [job-id :last-batch-posted-at]
                           (str (java.time.LocalDateTime/now))))))))

(defn update-pulled [job-id worker-id n]
  (update-addition-atom job-id worker-id n pulled))

(defn create-job
  "create new job with next id in sequence - return the new id"
  [job-params]
  (let [clean-params (-> job-params
                         (update :source dissoc :auth)
                         (update :destination dissoc :auth)
                         (assoc :started (str (java.time.LocalDateTime/now))))]
    (apply max
           (keys
            (swap! jobs
                   (fn [m]
                     (assoc m
                            (inc (apply max (or (keys m) [0])))
                            clean-params)))))))

(defn register-scroll-client [job-id]
  (swap! scroll-clients update job-id (fnil inc 0)))

(defn deregister-scroll-client [job-id]
  (let [client-counts (swap! scroll-clients update job-id (fnil dec 0))]
    (max 0 (client-counts job-id))))

(defn build-summary [job-id]
  (sorted-map
   :active-scrollers (@scroll-clients job-id)
   :job-parameters (@jobs job-id)
   :job-total-docs-to-load (get-scroll-total job-id)
   :posted-to-destination-docs (get-posted-total job-id)
   :posted-to-destination-details (@posted-details job-id)
   :pulled-from-source (get-pulled-total job-id)
   :pulled-from-source-by-scroll (@pulled job-id)
   :to-pull-by-scroll (@scroll-totals job-id)
   :x-errors (@errors job-id)))

(defn ndjson [data]
  (apply str (mapv (fn [s] (str (json/generate-string s) "\n"))
                   data)))

(defn make-action-source [target-index]
  (fn [hit]
    [{:create {:_index (or target-index (:_index hit))
               :_id (:_id hit)}}
     (:_source hit)]))

(defn make-bulk-body [{:keys [max-count max-chars]}]
  (fn [rf]
    (let [buf (volatile! (transient []))
          char-count (volatile! 0)]
      (fn
        ([] (rf))
        ([result]
         (let [result (if (pos? (count @buf))
                        (let [s (apply str (persistent! @buf))]
                          (vreset! buf (transient []))
                          (vreset! char-count 0)
                          (unreduced (rf result s)))
                        result)]
           (rf result)))
        ([result input]
         (vswap! char-count + (.length input))
         (if (or
              (<= max-count (count (vswap! buf conj! input)))
              (<= max-chars @char-count))
           (let [res (apply str (persistent! @buf))]
             (vreset! buf (transient []))
             (vreset! char-count 0)
             (rf result res))
           result))))))

(defn make-scroll-query-handler [job-id slice-id hit-chan scroll-chan
                                 {:keys [size] :as source}]
  (fn [resp]
    (async/go
      (let [body (-> resp :body ((fn [s] (json/parse-string s true))))
            scroll-id (:_scroll_id body)
            hits (-> body :hits :hits)
            scroll-total (-> body :hits :total :value)]
        (when scroll-total
          (set-scroll-total job-id slice-id scroll-total))
        (update-pulled job-id slice-id (count hits))
        (doseq [hit hits] (async/>! hit-chan hit))
        (if (< (count hits) size)
          (let [client-count (deregister-scroll-client job-id)]
            (when (= 0 client-count)
              (async/close! hit-chan)
              (async/close! scroll-chan)))
          (async/>! scroll-chan
                    (assoc source :scroll-id scroll-id :slice-id slice-id)))))))

(defn make-scroll-ex-handler [job-id slice-id]
  (fn [exc]
    (capture-ex job-id (str "scroll-" slice-id) exc)))

(defn scroll-query [hit-chan scroll-chan job-id
                    {:keys [auth index size scroll-id slice-id url] :as source}]
  (let [handler (make-scroll-query-handler
                 job-id slice-id hit-chan scroll-chan source)
        ex-handler (make-scroll-ex-handler job-id slice-id)]
    (if scroll-id
      (client/post
       (str url "/_search/scroll")
       {:form-params {:scroll "5m"
                      :scroll_id scroll-id}
        :content-type :json
        :accept :json
        :basic-auth auth
        :async? true}
       handler
       ex-handler)
      (client/post
       (str url "/" index "/_search?scroll=5m")
       {:form-params {:size size}
        :content-type :json
        :accept :json
        :basic-auth auth
        :async? true}
       handler
       ex-handler))))

(defn sliced-scroll-query [hit-chan scroll-chan job-id
                           slice-id max-slice
                           {:keys [auth index size url] :as source}]
  (let [handler (make-scroll-query-handler
                 job-id slice-id hit-chan scroll-chan source)
        ex-handler (make-scroll-ex-handler job-id slice-id)]
    (client/post
     (str url "/" index "/_search?scroll=5m")
     {:form-params {:slice {:id slice-id
                            :max max-slice}
                    :sort ["_doc"]
                    :size size}
      :content-type :json
      :accept :json
      :basic-auth auth
      :async? true}
     handler
     ex-handler)))

(defn post-bulk [batch-chan
                 throttle-chan
                 job-id
                 {:keys [url auth]}
                 data]
  (client/post (str url "/_bulk")
               {:content-type "application/x-ndjson"
                :accept "application/json"
                :body data
                :basic-auth auth
                :async? true}
               (fn [resp]
                 (async/go
                   (async/>! throttle-chan true)
                   (let [body (json/parse-string (:body resp) true)]
                     (update-posted job-id (count (:items body))))))
               (fn [exc]
                 (async/go (async/>! throttle-chan true)
                           (async/>! batch-chan data))
                 (capture-ex job-id "loader" exc))))


(defn make-bulk-getter [hit-chan
                        scroll-chan
                        job-id
                        slice-id
                        client-count
                        source-es]
  (register-scroll-client job-id)
  (if (< 1 client-count)
    (sliced-scroll-query
     hit-chan scroll-chan job-id slice-id client-count source-es)
    (scroll-query
     hit-chan scroll-chan job-id (assoc source-es :slice-id slice-id)))
  (async/go-loop [scroll (async/<! scroll-chan)]
    (when scroll
      (scroll-query hit-chan scroll-chan job-id scroll)
      (recur (async/<! scroll-chan)))))

(defn make-bulk-loader [batch-chan throttle-chan job-id target-es]
  (async/go-loop [_ (async/<! throttle-chan)]
    (when-let [data (async/<! batch-chan)]
      (post-bulk batch-chan throttle-chan job-id target-es data)
      (recur (async/<! throttle-chan)))))

(defn run-copy-job [job-id {:keys [source destination]}]
  (let [source-concurrency (:concurrency source)
        destination-concurrency (:concurrency destination)
        hit-chan (async/chan source-concurrency
                             (comp
                              (map (make-action-source (:index destination)))
                              (map ndjson)
                              (make-bulk-body destination)))
        batch-chan (async/chan)
        scroll-chan (async/chan)
        load-throttle-chan (async/chan destination-concurrency)]
    (async/pipe hit-chan batch-chan)
    (dotimes [_ destination-concurrency] (async/>!! load-throttle-chan true))
    (doseq [n (range source-concurrency)]
      (make-bulk-getter
       hit-chan scroll-chan job-id n source-concurrency source))
    (make-bulk-loader
     batch-chan load-throttle-chan job-id destination)
    nil))

(def source-defaults
  {:concurrency 4
   :size 500})

(def destination-defaults
  {:report-interval 3000
   :concurrency 12
   :max-size 1000
   :max-chars 2000000})

(defn merge-default-config [{:keys [source destination]}]
  {:source (merge source-defaults source)
   :destination (merge destination-defaults destination)})

(defn wait-for-init [job-id]
  (loop [n 40]
    (async/<!! (async/timeout 500))
    (when (nil? (:posted-to-destination-docs (build-summary job-id)))
      (if (pos? n)
        (recur (dec n))
        (do (.println *err* "failed to initialize job, no records posted")
            (System/exit 1))))))

(defn -main [& args]
  (let [filepath (or (first args)
                     (clojure.java.io/file (System/getProperty "user.dir")
                                           "config.edn"))
        _ (println "reading job config from" filepath)
        cfg (merge-default-config (clojure.edn/read-string (slurp filepath)))
        job-id (create-job cfg)
        report-interval (:report-interval (:destination cfg))]
    (println "starting copy job")
    (run-copy-job job-id cfg)
    (wait-for-init job-id)
    (println "reporting on job progress every"
             (quot report-interval 1000) "seconds")
    (loop []
      (async/<!! (async/timeout report-interval))
      (let [rpt (build-summary job-id)]
        (println (json/generate-string (dissoc rpt :x-errors) {:pretty true}))
        (when-let [errors (seq (:x-errors rpt))]
          (spit "errors.json"
                (json/generate-string errors {:pretty true}))
          (.println *err*
                    (str (count errors) " errors detected: view errors.json")))
        (if (> (:job-total-docs-to-load rpt)
                 (:posted-to-destination-docs rpt))
          (if (= 0 (:active-scrollers rpt))
            (do
              (async/<!! (async/timeout (* 2 report-interval)))
              (let [old-posted-count (-> rpt
                                         :posted-to-destination-details
                                         :batch-posted-count)
                    new-posted-count (-> (build-summary job-id)
                                         :posted-to-destination-details
                                         :batch-posted-count)]
                (if (= new-posted-count old-posted-count)
                  (.println *err* "job finished without loading all docs")
                  (recur))))
            (recur))
          (println "jobs done"))))))
