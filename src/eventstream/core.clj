(ns eventstream.core
  (:require [schema.core :as s]))

(defprotocol EventStore
  (store [event-store entity])
  (fetch [event-store entity]))

(defmulti merge-event
  (fn [entity event]
    [(get-in entity [:stream :collection]) (:type event)]))

(defmethod merge-event :default [entity event]
  (update-in entity [:snapshot] merge (:attributes event)))

(defn stream [collection & opts]
  (let [options (apply assoc {} opts)]
    {:collection collection
     :identity-schema (:identity options)
     :event-schemas (:events options)}))

(defn new-entity [event-stream id]
  (s/validate (:identity-schema event-stream) id)
  {:stream event-stream
   :version 0
   :created-at nil
   :updated-at nil
   :identity id
   :events []
   :snapshot {}})

(defn assoc-version [{:keys [events] :as entity}]
  (assoc entity :version (apply max (map :version events))))

(defn assoc-timestamps [{:keys [events] :as entity}]
  (-> entity
    (assoc :created-at (:created_at (first events)))
    (assoc :updated-at (:created_at (last events)))))

(defn validate-event [entity event-name event-data]
  (let [schema (get-in entity [:stream :event-schemas event-name])]
    (if (nil? schema)
      (throw (RuntimeException. (str "Unknown event: " event-name)))
      (s/validate schema event-data))))

(defn append [entity event-name event-data]
  (validate-event entity event-name event-data)
  (let [event (assoc (:identity entity) :attributes event-data
                                        :created_at (java.sql.Timestamp. (System/currentTimeMillis))
                                        :version (inc (:version entity))
                                        :type event-name)]
    (-> entity
      (update-in [:events] conj event)
      (merge-event event)
      (assoc-version)
      (assoc-timestamps))))
;
;(defn store! [db stream entity]
;  (-> entity
;    (assoc :events (map #(if (nil? (:id %))
;                          (db/insert db (:table stream) %)
;                          %)
;                        (:events entity)))
;    (assoc-version)
;    (assoc-snapshot)))
;
;(defn fetch! [db stream metadata]
;  (let [events (db/find-where db (:table stream) metadata)]
;    (-> {:events (vec events) :metadata metadata}
;      (assoc-version)
;      (assoc-snapshot))))
