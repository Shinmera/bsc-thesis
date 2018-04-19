#!sbcl --script
(load #p"~/quicklisp/setup.lisp")
(ql:quickload '(parse-float cl-ppcre))

(defparameter *order* '("HiBench Identity" "HiBench Repartition" "HiBench Wordcount" "HiBench Fixwindow"
                        "Yahoo Streaming Benchmark"
                        "NEXMark Query 0" "NEXMark Query 1" "NEXMark Query 2" "NEXMark Query 3"
                        "NEXMark Query 4" "NEXMark Query 5" "NEXMark Query 6" "NEXMark Query 7"
                        "NEXMark Query 8" "NEXMark Query 9" "NEXMark Query 11"))
(defparameter *rates* '(1000 10000 100000 1000000 2000000 4000000 8000000 10000000))
(defparameter *workers* '(1 2 4 8 16 32))

(defun read-file (file)
  (with-open-file (s file)
    (loop for l = (read-line s NIL)
          while l collect (destructuring-bind (name dat) (cl-ppcre:split "  +" l)
                            (list* name (sort (mapcar #'parse-float:parse-float (cl-ppcre:split " +" dat))
                                              #'<))))))

(defun median-latency (&key (path "") (file "latency.csv") (workers "32"))
  (with-open-file (out file :direction :output :if-exists :supersede)
    (dolist (rate *rates*)
      (with-simple-restart (continue "Ignore the error.")
        (let ((group (read-file (merge-pathnames (format NIL "~a@~a.csv" rate workers) path))))
          (format out "~&~a~{ ~f~}" rate (loop for (k . v) in group
                                               for sorted = (sort v #'<)
                                               collect (nth (round (/ (length sorted) 2)) sorted))))))))

(defun scaling (&key (path "") (file "scaling.csv") (rate "10000000"))
  (with-open-file (out file :direction :output :if-exists :supersede)
    (dolist (workers *workers*)
      (with-simple-restart (continue "Ignore the error.")
        (let ((group (read-file (merge-pathnames (format NIL "~a@~a.csv" rate workers) path))))
          (format out "~%~a~{ ~f~}" workers (loop for (k . v) in group
                                                  for sorted = (sort v #'<)
                                                  collect (nth (round (/ (length sorted) 2)) sorted))))))))

(defun cdf (&key (path "10000000@32.csv") (file "cdf.csv"))
  (with-open-file (out file :direction :output :if-exists :supersede)
    (let* ((group (read-file path))
           (length (loop for cons in group minimize (length (cdr cons)))))
      (loop for i from 1 to length
            do (fresh-line out)
               (format out "~f" (* 100 (/ i length)))
               (loop for cons in group
                     do (format out " ~f" (pop (cdr cons))))))))

(defun sync ()
  (uiop:run-program (list "rsync" "-avz" "hafnern@sgs-r815-03:/mnt/local/hafnern/thesis/benchmarks/*.csv" ".") :output T :error-output T))

(defun run ()
  (sync)
  (median-latency)
  (scaling)
  (cdf))

(run)
