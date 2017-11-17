LATEX = lualatex
BIBTEX = biber
MAKE = make
OUTPUT = thesis
TEX = $(wildcard content/*.tex)
BIB = $(wildcard content/*.bib)

all:
	$(MAKE) tex
	$(MAKE) bib
	$(MAKE) tex

tex: $(TEX) $(BIB)
	$(LATEX) --jobname=$(OUTPUT) thesis.tex

bib: $(OUTPUT).bcf
	$(BIBTEX) $(OUTPUT)
