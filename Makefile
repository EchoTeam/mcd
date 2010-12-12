PROJECTNAME=mcd
PROJECTVERSION=1.0

INSTALLDIR=$(prefix)/$(LIBDIR)/$(PROJECTNAME)-$(PROJECTVERSION)/ebin
LIBDIR=$(shell erl -eval 'io:format("~s~n", [code:lib_dir()])' -s init stop -noshell)

all:
	./rebar compile

clean:
	./rebar clean

install:
	mkdir -p $(INSTALLDIR)
	for f in ebin/*.beam; do install $$f $(INSTALLDIR); done
