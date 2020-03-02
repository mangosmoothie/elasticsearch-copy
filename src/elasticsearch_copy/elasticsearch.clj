(ns elasticsearch-copy.elasticsearch
  (:require
   [clj-http.client :as client]))

(defn get-doc-count [{:keys [url index auth]}]
  (client/get (str url "/" index "/_count") {:basic-auth auth}))

(defn delete-index [{:keys [url index auth]}]
  (client/delete (str url "/" index) {:basic-auth auth}))

