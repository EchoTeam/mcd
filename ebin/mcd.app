{application, mcd,
 [{description, "Fast memcached protocol client in pure Erlang"},
  {vsn, "1.0.0"},
  {modules, [
	dht_ring,
	mcd,
	mcd_cluster,
	mcd_starter
            ]},
  {applications, [
                  kernel,
                  stdlib
                 ]},
  {registered, []},
  {env, [ ]}
 ]}.
