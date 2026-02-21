# Makefile for SmartCopy ICD Documentation
#
# Usage:
#   make        - Build SmartCopy_ICD.pdf
#   make clean  - Remove build artifacts
#   make distclean - Remove all generated files including PDF

LATEX = pdflatex
BIBTEX = bibtex

# Main document
DOC = SmartCopy_ICD

# Source directory
SRCDIR = SmartCopy_ICD

# LaTeX flags
LATEXFLAGS = -interaction=nonstopmode -halt-on-error

.PHONY: all clean distclean

all: $(DOC).pdf

$(DOC).pdf: $(SRCDIR)/$(DOC).tex $(wildcard $(SRCDIR)/**/*.tex)
	@echo "Building $(DOC).pdf..."
	cd $(SRCDIR) && $(LATEX) $(LATEXFLAGS) $(DOC).tex
	cd $(SRCDIR) && $(LATEX) $(LATEXFLAGS) $(DOC).tex
	cd $(SRCDIR) && $(LATEX) $(LATEXFLAGS) $(DOC).tex
	cp $(SRCDIR)/$(DOC).pdf .
	@echo "Done: $(DOC).pdf"

clean:
	@echo "Cleaning build artifacts..."
	cd $(SRCDIR) && rm -f *.aux *.log *.toc *.out *.lof *.lot *.bbl *.blg *.nav *.snm *.vrb
	cd $(SRCDIR) && rm -f config/*.aux frontmatter/*.aux sections/*.aux appendices/*.aux

distclean: clean
	@echo "Removing generated PDF..."
	rm -f $(DOC).pdf $(SRCDIR)/$(DOC).pdf
