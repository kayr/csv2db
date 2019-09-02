(def data (loop [arr3 [], x 5]
            (if (zero? x)
              arr3
              (recur (conj arr3 x) (dec x)))))


(defn last-index [vec] (- (count vec) 1))

(defn head [vector] (subvec vector 0 (last-index vector)))

(defn recursive-reverse [coll]
  (loop [result [],
         x coll]
    (if (= (count x) 0)
      result
      (recur (conj result (last x)) (head x)))))

(println "recursive reverse")
(recursive-reverse [1, 2, 3])

; =====================================================
; Index of any
;===============================================

(defn indexed [coll] (map-indexed vector coll))

(defn index-filter [pred coll]
  (when pred
    (for [[idx elt] (indexed coll) :when (pred elt)] idx)))


(index-filter #{\a \b} "abcdbbb")

(nth (index-filter #{:h} [:t :t :h :t :h :t :t :t :h :h]) 2)

(meta #'iterate)

(class (rest '(1 2 3)))
;===========
;list comprehension

(defn whole-numbers [] (iterate inc 1))

(for [word ["the" "quick" "brown" "fox"]]
  (format "<p>%s</p>" word))


(take 10 (for [n (whole-numbers) :when (even? n)] n))

; interesting fibonacci
(take 5 (iterate (fn [[a b]] [b (+ a b)]) [0 1]))