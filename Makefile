PROJECTNAME=mcd
PROJECTVERSION=1.0

INSTALLDIR=$(prefix)/$(LIBDIR)/$(PROJECTNAME)-$(PROJECTVERSION)/ebin
LIBDIR=$(shell erl -eval 'io:format("~s~n", [code:lib_dir()])' -s init stop -noshell)

all:
	mkdir -p ebin
	for srcfile in src/*.erl; do erlc -o ebin $$srcfile; done

clean:
	rm -rf ebin

install:
	mkdir -p $(INSTALLDIR)
	for f in ebin/*.beam; do install $$f $(INSTALLDIR); done
