(ns eventstream.core
  (:require [schema.core :as s]))

(defprotocol EventStore
  (store [event-store entity])
  (fetch [event-store entity]))

(defn stream [collection & opts]
  (let [options (apply assoc {} opts)]
    {:collection collection
     :identity-schema (:identity options)
     :event-schemas (:events options)}))

(defn new-entity [event-stream id]
  (s/validate (:identity-schema event-stream) id)
  {:stream event-stream
   :version 0
   :identity id
   :events []
   :snapshot {}})

(defn event->delta [metadata event]
  (letfn [(assoc-created-at [delta]
            (if (= 1 (:version event))
              (assoc delta :created_at (:created_at event))
              delta))]
    (-> metadata
      (merge (:attributes event))
      (assoc-created-at)
      (assoc :updated_at (:created_at event)))))

(defn assoc-snapshot [{:keys [events metadata] :as entity}]
  (->> events
    (map (partial event->delta metadata))
    (reduce merge {})
    (assoc entity :snapshot)))

(defn assoc-version [{:keys [events] :as entity}]
  (assoc entity :version (apply max (map :version events))))

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
      (assoc-version)
      (assoc-snapshot))))
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
