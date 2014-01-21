REBAR=$(shell which rebar || echo ./rebar)
PROJECTNAME=mcd
PROJECTVERSION=1.1.0

INSTALLDIR=$(LIBDIR)/$(PROJECTNAME)-$(PROJECTVERSION)/ebin
LIBDIR=$(shell erl -eval 'io:format("~s~n", [code:lib_dir()])' -s init stop -noshell)

all: 	$(REBAR)
		$(REBAR) get-deps
		$(REBAR) compile

clean:
		$(REBAR) clean
		$(REBAR) delete-deps
		rm -rf ./ebin
		rm -rf ./deps
		rm -rf ./logs
		rm -rf ./erl_crash.dump
		rm -f ./test/*.beam

install-lib:
	mkdir -p $(INSTALLDIR)
	for f in ebin/*.beam; do install $$f $(INSTALLDIR); done

# Get rebar if it doesn't exist

REBAR_URL=https://cloud.github.com/downloads/basho/rebar/rebar

./rebar:
	erl -noinput -noshell -s inets -s ssl \
		-eval 'httpc:request(get, {"${REBAR_URL}", []}, [], [{stream, "${REBAR}"}])' \
		-s init stop
	chmod +x ${REBAR}
