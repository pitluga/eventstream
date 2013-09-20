(ns eventstream.core-test
  (:require [clojure.test :refer :all]
            [schema.core :as s]
            [eventstream.core :refer :all]))

(def test-stream
  (stream :test-events
          :identity {:id s/Number}
          :events {:double {:number s/Number}
                   :start {:value s/String}
                   :stop  {:value s/String}}))

(defmethod merge-event [:test-events :double] [entity event]
  (update-in entity [:snapshot] merge
             (:attributes (update-in event [:attributes :number] (partial * 2)))))

(deftest create-a-new-entity-from-a-stream
  (let [entity (new-entity test-stream {:id 1})]
    (is (= test-stream (:stream entity)))
    (is (= 0 (:version entity)))
    (is (= {:id 1} (:identity entity)))
    (is (= [] (:events entity)))
    (is (nil? (:created-at entity)))
    (is (nil? (:updated-at entity)))
    (is (= {} (:snapshot entity)))))

(deftest raises-exception-if-metadata-does-not-match-schema
  (is (thrown? RuntimeException #"Value does not match schema:"
        (new-entity stream {:id "NaN"}))))

(deftest appends-an-event
  (let [entity (append (new-entity test-stream {:id 1}) :start {:value "started"})]
    (is (= 1 (:version entity)))
    (is (= 1 (count (:events entity))))
    (is (not (nil? (:created-at entity))))
    (is (not (nil? (:updated-at entity))))
    (is (= (:created-at entity) (:updated-at entity)))
    (is (= "started" (get-in entity [:snapshot :value])))))

(deftest appends-multiple-events
  (let [entity (-> (new-entity test-stream {:id 1})
                 (append :start {:value "value"})
                 (append :stop  {:value "different"}))]
    (is (= 2 (:version entity)))
    (is (= 2 (count (:events entity))))
    (is (not (nil? (:created-at entity))))
    (is (not (nil? (:updated-at entity))))
    (is (not (= (:created-at entity) (:updated-at entity))))
    (is (= "different" (get-in entity [:snapshot :value])))))

(deftest cannot-append-events-not-matching-schema
  (let [entity (new-entity test-stream {:id 1})]
    (testing "bad key"
      (is (thrown? RuntimeException #"Value does not match schema:"
                   (append entity :stop {:wrong-key "different"}))))
    (testing "bad value"
      (is (thrown? RuntimeException #"Value does not match schema:"
                   (append entity :stop {:value 42}))))
    (testing "bad event"
      (is (thrown? RuntimeException #"Unknown event: pause"
                   (append entity :pause {:value "foo"}))))))

(deftest can-define-custom-event-application-functions
  (let [entity (-> (new-entity test-stream {:id 1})
                 (append :double {:number 1}))]
    (is (= 2 (get-in entity [:snapshot :number])))))

;
;(deftest can-store-and-fetch-events-from-the-db
;  (let [stream (es/define-stream :transaction_events {:merchant_id s/Number :public_id s/String})
;        entity (-> (es/new-entity stream {:merchant_id 1 :public_id "1"})
;                 (es/append simple-event {:key "value"}))]
;    (es/store! (:db system) stream entity)
;    (let [found (es/fetch! (:db system) stream {:merchant_id 1 :public_id "1"})]
;      (is (= 1 (:version found)))
;      (is (= 1 (count (:events found))))
;      (is (= "1" (get-in found [:snapshot :public_id])))
;      (is (not (nil? (get-in found [:snapshot :created_at]))))
;      (is (not (nil? (get-in found [:snapshot :updated_at]))))
;      (is (= (get-in found [:snapshot :created_at]) (get-in found [:snapshot :updated_at])))
;      (is (= "value" (get-in found [:snapshot :key]))))))
;
;(deftest can-append-more-events-to-the-db
;  (let [stream (es/define-stream :transaction_events {:merchant_id s/Number :public_id s/String})
;        entity (-> (es/new-entity stream {:merchant_id 1 :public_id "1"})
;                 (es/append simple-event {:key "value"}))
;        saved-entity (es/store! (:db system) stream entity)]
;
;    (es/store! (:db system) stream (es/append saved-entity simple-event {:key "different"}))
;
;    (let [found (es/fetch! (:db system) stream {:merchant_id 1 :public_id "1"})]
;      (is (= 2 (:version found)))
;      (is (= 2 (count (:events found))))
;      (is (= "1" (get-in found [:snapshot :public_id])))
;      (is (not (nil? (get-in found [:snapshot :created_at]))))
;      (is (not (nil? (get-in found [:snapshot :updated_at]))))
;      (is (= (get-in found [:snapshot :created_at])
;             (get-in found [:events 0 :created_at])))
;      (is (= (get-in found [:snapshot :updated_at])
;             (get-in found [:events 1 :created_at])))
;      (is (= "different" (get-in found [:snapshot :key]))))))
