;;;;; Benchmark Data Transformer
;;;; This script crunches the latency measurements from the timely runs
;;;; and converts them into CSV files as used in pgfplots in the TeX
;;;; sources.
;;;;
;;;; Usage: sbcl --load transform.lisp [--sync]
;;;;
;;;; Requires a Quicklisp installation in the user's home directory.
;;;;

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

(defun median-latency (&key (path "") (workers "32") (file (format NIL "latency-~a.csv" workers)))
  (with-open-file (out file :direction :output :if-exists :supersede)
    (dolist (rate *rates*)
      (with-simple-restart (continue "Ignore the error.")
        (let ((group (read-file (merge-pathnames (format NIL "~a@~a.csv" rate workers) path))))
          (format out "~&~a~{ ~f~}" rate (loop for (k . v) in group
                                               for sorted = (sort v #'<)
                                               collect (nth (round (/ (length sorted) 2)) sorted))))))))

(defun scaling (&key (path "") (rate "10000000") (file (format NIL "scaling-~a.csv" rate)))
  (with-open-file (out file :direction :output :if-exists :supersede)
    (dolist (workers *workers*)
      (with-simple-restart (continue "Ignore the error.")
        (let ((group (read-file (merge-pathnames (format NIL "~a@~a.csv" rate workers) path))))
          (format out "~%~a~{ ~f~}" workers (loop for (k . v) in group
                                                  for sorted = (sort v #'<)
                                                  collect (nth (round (/ (length sorted) 2)) sorted))))))))

(defun cdf (&key (path "") (workers "32") (rate "10000000") (file (format NIL "cdf-~a-~a.csv" workers rate)))
  (with-open-file (out file :direction :output :if-exists :supersede)
    (let* ((group (read-file (merge-pathnames (format NIL "~a@~a.csv" rate workers) path)))
           (length (loop for cons in group minimize (length (cdr cons)))))
      (loop for i from 1 to length
            do (fresh-line out)
               (format out "~f" (* 100 (/ i length)))
               (loop for cons in group
                     do (format out " ~f" (pop (cdr cons))))))))

(defun sync ()
  (uiop:run-program (list "rsync" "-avz" "hafnern@sgs-r815-03:/mnt/local/hafnern/thesis/benchmarks/*.csv" ".") :output T :error-output T))

(defun run (&key sync)
  (when sync (sync))
  (median-latency :workers 8)
  (median-latency :workers 16)
  (median-latency :workers 32)
  (scaling)
  (cdf))

(defun toplevel (&rest args)
  (let ((args ()))
    (when (find "--sync" args :test #'string-equal)
      (setf args (list* :sync T args)))
    (apply #'run args)))

(apply #'toplevel (rest (uiop:raw-command-line-arguments)))
(uiop:quit)
