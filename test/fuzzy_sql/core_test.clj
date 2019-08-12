(ns fuzzy-sql.core-test
  (:require [clojure.test :refer :all]
            [fuzzy-sql.exporter :refer :all]
            [next.jdbc :as jdbc])
  (:import (java.util UUID)))



(def test-data [["header" "age" "someNulls"]
                ["value 1" 3 nil]
                ["value 2" 4 3.4]])


(def db {:dbtype   "mysql"
         :dbname   "dsd"
         :user     "root"
         :password "pass"
         :host     "localhost"
         :useSSl   false})

(def ds (jdbc/get-datasource db))

(deftest a-test
  (testing "FIXME, I fail."
    (is (= 1 1))))

(deftest test-index-of
  (let [data [:a :b :c :d]
        c-index (index-of #(= :c %1) data)]
    (is (= c-index 2))))

(deftest test-detect-data-type
  (testing "detecting data type"
    (is (= (detect-data-type test-data "someNulls") Double))
    (is (= (detect-data-type test-data "header") String))
    (is (= (detect-data-type test-data "age") Long))))


(deftest test-detect-db-type
  (testing "detecting data type"
    (is (= (detect-db-type test-data "someNulls") "numeric(19,6)"))
    (is (= (detect-db-type test-data "header") "varchar(255)"))
    (is (= (detect-db-type test-data "age") "bigint"))))


(deftest test-generate-ddl
  (testing "testing generate ddl"
    (is (= (generate-ddl test-data "my_table")
           "create table `my_table` (`header` varchar(255),`age` bigint,`someNulls` numeric(19,6))"))))

(deftest test-insertion
  (testing "creating the table"
    (let [rand-tb-name (-> (UUID) (randomUUid))
          ddl (generate-ddl test-data "table1")]
      (jdbc/execute! ds [ddl]))))




(defn get-column-names
  "Take database spec, return all column names from the database metadata"
  [db]
  (with-connection db
                   (into #{}
                         (map #(str (% :table_name) "." (% :column_name))
                              (resultset-seq (->
                                               (connection)
                                               (.getMetaData)
                                               (.getColumns nil nil nil "%")))))))


