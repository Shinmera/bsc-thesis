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
(defparameter *rates* '(1000 10000 100000 1000000 10000000))
(defparameter *workers* '(1 2 4 8 16 32))
(defparameter *windows* '(20 40 60 80 120))
(defparameter *slides* '(5 10 20 40 60))

(defun read-file (file)
  (with-open-file (s file)
    (loop for l = (read-line s NIL)
          while l collect (destructuring-bind (name dat) (cl-ppcre:split "  +" l)
                            (list* name (sort (mapcar #'parse-float:parse-float (cl-ppcre:split " +" dat))
                                              #'<))))))

(defun median (values)
  (let ((sorted (sort values #'<)))
    (nth (round (/ (length sorted) 2)) sorted)))

(defun maximum (values)
  (let ((val (loop for val in values maximize val)))
    (unless (= 0 val) val)))

(defun median-latency (&key (path "") (workers "32") (file (format NIL "latency-~a.csv" workers)))
  (with-open-file (out file :direction :output :if-exists :supersede)
    (dolist (rate *rates*)
      (with-simple-restart (continue "Ignore the error.")
        (let ((group (read-file (merge-pathnames (format NIL "~a@~a.csv" rate workers) path))))
          (format out "~&~a~{ ~f~}" rate (loop for (k . v) in group
                                               collect (median v))))))))

(defun scaling (&key (path "") (rate "10000000") (file (format NIL "scaling-~a.csv" rate)))
  (with-open-file (out file :direction :output :if-exists :supersede)
    (dolist (workers *workers*)
      (with-simple-restart (continue "Ignore the error.")
        (let ((group (read-file (merge-pathnames (format NIL "~a@~a.csv" rate workers) path))))
          (format out "~&~a~{ ~f~}" workers (loop for (k . v) in group
                                                  collect (median v))))))))

(defun cdf (&key (path "") (workers "32") (rate "10000000") (file (format NIL "cdf-~a-~a.csv" workers rate)))
  (with-open-file (out file :direction :output :if-exists :supersede)
    (let* ((group (read-file (merge-pathnames (format NIL "~a@~a.csv" rate workers) path)))
           (length (loop for cons in group minimize (length (cdr cons)))))
      (loop for i from 1 to length
            do (format out "~&~f" (* 100 (/ i length)))
               (loop for cons in group
                     do (format out " ~f" (pop (cdr cons))))))))

(defun window (&key (path "") (rate "10000000") (workers "32") (file (format NIL "window-~a-~a.csv" workers rate)))
  (with-open-file (out file :direction :output :if-exists :supersede)
    (dolist (window *windows*)
      (with-simple-restart (continue "Ignore the error.")
        (let ((group (read-file (merge-pathnames (format NIL "~a@~a-w~a.csv" rate workers window) path))))
          (format out "~&~a~{ ~f~}" window
                  (loop for (k . v) in group
                        collect (maximum v))))))))

(defun slide (&key (path "") (rate "10000000") (workers "32") (file (format NIL "slide-~a-~a.csv" workers rate)))
  (with-open-file (out file :direction :output :if-exists :supersede)
    (dolist (window *windows*)
      (with-simple-restart (continue "Ignore the error.")
        (format out "~&~a" window)
        (dolist (slide *slides*)
          (handler-case
              (let ((group (read-file (merge-pathnames (format NIL "~a@~a-w~as~a.csv" rate workers window slide) path))))
                (format out " ~f" (or (maximum (cdr (first group))) "nan")))
            (error (err)
              (declare (ignore err))
              (format out " nan"))))))))

(defun sync ()
  (uiop:run-program (list "rsync" "-avz" "hafnern@sgs-r815-03:/mnt/local/hafnern/thesis/benchmarks/*.csv" ".") :output T :error-output T))

(defun run (&key sync)
  (when sync (sync))
  (median-latency :workers 8)
  (median-latency :workers 16)
  (median-latency :workers 32)
  (scaling)
  (cdf)
  (window)
  (slide))

(defun toplevel (&rest args)
  (let ((kargs ()))
    (when (find "--sync" args :test #'string-equal)
      (setf kargs (list* :sync T kargs)))
    (apply #'run kargs)))

(apply #'toplevel (rest (uiop:raw-command-line-arguments)))
(uiop:quit)
